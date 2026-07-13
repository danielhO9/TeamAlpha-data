"""marcap(FinanceData) 정적 데이터셋 → bronze 개별종목 시세 (전종목·전기간, 안정적).

GitHub FinanceData/marcap: 1995~현재 **일별 전종목**(KOSPI/KOSDAQ/KONEX) OHLCV + 시총 + 주식수.
연도별 parquet 파일로 배포되어 스크래핑·API 한도·차단이 전혀 없다(git 으로 매일 갱신).
가격은 **raw 체결가(무수정, 액면분할 반영 전)** 라 bronze 원칙(원본 무수정)에 부합.
값 교차검증: 삼성 2023-04-05 종가·거래량·시총·상장주식수가 KRX OpenAPI·datago 와 정확히 일치.

컬럼(18): Code,Name,Close,Dept,ChangeCode,Changes,ChangesRatio,Volume,Amount,
          Open,High,Low,Marcap,Stocks,Market,MarketId,Rank,Date

연도 파일을 Date 별로 나눠 저장(값 무수정, 컨벤션 <종류>/<소스>/date=):
  <base>/stock/marcap/date=YYYY-MM-DD/all.parquet   (그 날짜 전종목 1파일)

재개: 이미 있는 날짜는 스킵. 중단 후 같은 명령 재실행하면 이어서 진행.
사용:
  uv run python -m pipeline.bronze.stock_marcap --from 2015 --to 2026
  uv run python -m pipeline.bronze.stock_marcap --from 2015 --to 2026 --dest s3
"""
from __future__ import annotations

import argparse
import io
import time

import pandas as pd
import requests

from pipeline.common.paths import base_uri
from pipeline.common.sink import exists, write_parquet

RAW_URL = "https://raw.githubusercontent.com/FinanceData/marcap/master/data/marcap-{year}.parquet"


def _load_year(year: int, tries: int = 4) -> pd.DataFrame:
    """연도 parquet 다운로드 → DataFrame. 네트워크 blip 재시도."""
    last: Exception | None = None
    for attempt in range(tries):
        try:
            r = requests.get(RAW_URL.format(year=year), timeout=120)
            r.raise_for_status()
            return pd.read_parquet(io.BytesIO(r.content))
        except Exception as exc:  # noqa: BLE001
            last = exc
            time.sleep(2 * (attempt + 1))
    raise last  # type: ignore[misc]


def run(fromyear: int, toyear: int, dest: str) -> None:
    base = base_uri(dest)
    print(f"[stock_marcap] {fromyear}~{toyear} → {base}/stock/marcap/date=.../all.parquet, dest={dest}")

    saved = skipped = 0
    for year in range(fromyear, toyear + 1):
        try:
            df = _load_year(year)
        except Exception as exc:  # noqa: BLE001
            print(f"  ✗ {year} 다운로드 실패(스킵): {exc}")
            continue
        # Date 를 YYYY-MM-DD 문자열로 정규화(값 아님, 파티션 키 산출용)
        ds_all = pd.to_datetime(df["Date"]).dt.strftime("%Y-%m-%d")
        yr_saved = 0
        for ds, idx in df.groupby(ds_all).groups.items():
            path = f"{base}/stock/marcap/date={ds}/all.parquet"
            if exists(path):
                skipped += 1
                continue
            write_parquet(df.loc[idx].reset_index(drop=True), path)  # 값 무수정, 원본 18컬럼 그대로
            saved += 1
            yr_saved += 1
        print(f"  ✓ {year}: {df['Date'].nunique()}거래일 (저장 {yr_saved}, 누적 저장 {saved}, 스킵 {skipped})")

    print(f"[stock_marcap] 완료: 저장 {saved}일 / 스킵 {skipped}일")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--from", dest="fromyear", type=int, required=True, help="시작 연도 (예: 2015)")
    p.add_argument("--to", dest="toyear", type=int, required=True, help="종료 연도 (예: 2026)")
    p.add_argument("--dest", choices=["local", "s3"], default="local")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    run(args.fromyear, args.toyear, args.dest)


if __name__ == "__main__":
    main()
