"""pykrx by_date 과거 백필 — by_ticker 스냅샷이 안 되는 옛날 구간(예: 2015-2019)용.

data.go.kr 은 2020~ 만, pykrx by_ticker(날짜 스냅샷)는 이 환경에서 옛날 날짜가 빈 응답이라,
옛날 구간은 by_date(종목별 시계열)로 받는다. 종목/지수별 파일에 저장(date= 와 상호보완).

저장:
  pykrx/ohlcv/ticker=<code>.parquet        (한 종목 전 기간 OHLCV, by_date)
  pykrx/market_cap/ticker=<code>.parquet   (시총/주식수)
  pykrx/index_ohlcv/index=<name>.parquet   (지수)

silver 구분: date= 파티션=by_ticker 스냅샷 / ticker=·index= 파티션=by_date 시계열.
주의: by_date OHLCV 는 거래대금·시총이 없어 market_cap 이 보완. 유니버스는 지수 구성종목 union.
재개: 이미 있는 파일 스킵. 빈 응답(스로틀)은 저장 안 함 → 재실행 시 재시도.
병렬: --tickers 로 종목을 나눠 여러 인스턴스로 돌릴 수 있음.

사용:
  uv run python -m bronze.hist --from 20150101 --to 20191231
  uv run python -m bronze.hist --from 20150101 --to 20191231 --tickers 005930 000660
"""
from __future__ import annotations

import argparse
import time

from bronze import krx, members
from bronze.common import base_uri
from bronze.sink import exists, write_parquet

CALL_GAP_SEC = 0.5


def run(fromdate: str, todate: str, dest: str, tickers: list[str] | None) -> None:
    base = base_uri(dest)
    if not tickers:
        print("[hist] 유니버스 산출(구성종목 union)...")
        tickers = members.member_universe(fromdate, todate)
    print(f"[hist] {fromdate}~{todate}, 종목 {len(tickers)}개, dest={dest}")

    saved = skipped = empty = 0
    for i, t in enumerate(tickers, 1):
        for ds, fetch in (
            ("ohlcv", lambda t=t: krx.fetch_ohlcv_history(fromdate, todate, t)),
            ("market_cap", lambda t=t: krx.fetch_market_cap_history(fromdate, todate, t)),
        ):
            path = f"{base}/pykrx/{ds}/ticker={t}.parquet"
            if exists(path):
                skipped += 1
                continue
            try:
                df = fetch()
            except Exception as exc:  # noqa: BLE001
                print(f"  ✗ {ds}/{t}: {exc}")
                continue
            if df.empty:  # 스로틀·무데이터 → 저장 안 함(재실행 시 재시도)
                empty += 1
                continue
            write_parquet(df, path)
            saved += 1
            time.sleep(CALL_GAP_SEC)
        if i % 50 == 0:
            print(f"  ... {i}/{len(tickers)} (저장 {saved}, 스킵 {skipped}, 빈응답 {empty})")

    # 지수 OHLCV (지수당 1콜)
    for name, code in krx.INDEX_CODES.items():
        path = f"{base}/pykrx/index_ohlcv/index={name}.parquet"
        if exists(path):
            continue
        df = krx.fetch_index_ohlcv_history(fromdate, todate, code)
        if not df.empty:
            write_parquet(df, path)
            saved += 1

    print(f"[hist] 완료: 저장 {saved} / 스킵 {skipped} / 빈응답(재시도대상) {empty}")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--from", dest="fromdate", required=True, help="시작일 YYYYMMDD")
    p.add_argument("--to", dest="todate", required=True, help="종료일 YYYYMMDD")
    p.add_argument("--dest", choices=["local", "s3"], default="local")
    p.add_argument("--tickers", nargs="+", default=None, help="지정 시 이 종목만(병렬 분할용)")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    run(args.fromdate, args.todate, args.dest, args.tickers)


if __name__ == "__main__":
    main()
