"""OpenDART 다중회사 주요계정 수집 → bronze.

fnlttMultiAcnt(다중회사 주요계정): 한 콜에 여러 회사 × 연결(CFS)+별도(OFS) × ~15 주요계정.
유니버스: 지수 구성종목 union. corp_code 를 배치로 넣어 호출하고, 응답의 stock_code(티커)별로
나눠 저장한다(datago 의 날짜별 분할과 같은 방식 — 값은 그대로, 파티션만 나눔).

저장: <base>/dart/year=<YYYY>/corp=<ticker>/<reprt>.json   (한 회사·한 보고서, CFS+OFS 주요계정)

응답 처리: status 000 → 저장, 013(무데이터) → 스킵, 020(사용한도초과) → 중단(재개 가능).
재개: 로컬은 배치 첫 종목 파일이 있으면 그 배치 스킵.

사용 예:
  uv run python -m bronze.dart --from 20150101 --to 20260630
  uv run python -m bronze.dart --from 20240101 --to 20241231 --tickers 005930 000660
"""
from __future__ import annotations

import argparse
import json
import os
import time
from collections import defaultdict
from datetime import datetime

import requests

from bronze import members
from bronze.common import base_uri
from bronze.sink import exists, write_text

DART_URL = "https://opendart.fss.or.kr/api/fnlttMultiAcnt.json"
REPRT_CODES = ["11011", "11013", "11012", "11014"]  # 사업(FY)/1분기/반기/3분기
BATCH = 100          # 한 콜에 넣을 회사 수
CALL_GAP_SEC = 0.3


class QuotaExceeded(Exception):
    """OpenDART 사용한도 초과(status 020)."""


def _corp_code_map(tickers: list[str]) -> dict[str, str]:
    """ticker → corp_code(8자리). 매핑 실패 종목은 제외."""
    from opendartreader import OpenDartReader

    dart = OpenDartReader(os.environ["DART_API_KEY"])
    out: dict[str, str] = {}
    for t in tickers:
        try:
            code = dart.find_corp_code(t)
        except Exception:  # noqa: BLE001
            code = None
        if code:
            out[t] = code
        else:
            print(f"  ! corp_code 매핑 실패(스킵): {t}")
    return out


def _fetch_multi(corp_codes: list[str], year: int, reprt: str, tries: int = 4) -> tuple[str, dict | None]:
    params = {
        "crtfc_key": os.environ["DART_API_KEY"],
        "corp_code": ",".join(corp_codes),
        "bsns_year": str(year),
        "reprt_code": reprt,
    }
    for attempt in range(tries):
        try:
            d = requests.get(DART_URL, params=params, timeout=60).json()
            return d.get("status", "?"), d
        except Exception:  # noqa: BLE001  (네트워크 blip·JSON 오류 → 재시도)
            time.sleep(2 * (attempt + 1))
    return "?", None


def run(fromdate: str, todate: str, dest: str, tickers: list[str] | None) -> None:
    base = base_uri(dest)
    y0 = datetime.strptime(fromdate, "%Y%m%d").year
    y1 = datetime.strptime(todate, "%Y%m%d").year

    if not tickers:
        print("[dart] 유니버스 산출(구성종목 union)...")
        tickers = members.member_universe(fromdate, todate)
    corp_map = _corp_code_map(tickers)
    universe = sorted(corp_map)
    batches = [universe[i:i + BATCH] for i in range(0, len(universe), BATCH)]
    print(f"[dart] {y0}~{y1}, 종목 {len(universe)}개 → 배치 {len(batches)}개 × 연도 × 보고서, dest={dest}")

    saved = skipped = nodata = 0
    try:
        for year in range(y0, y1 + 1):
            for reprt in REPRT_CODES:
                for batch in batches:
                    marker = f"{base}/dart/year={year}/corp={batch[0]}/{reprt}.json"
                    if exists(marker):
                        skipped += 1
                        continue
                    status, d = _fetch_multi([corp_map[t] for t in batch], year, reprt)
                    if status == "020":
                        raise QuotaExceeded(f"{year} {reprt}")
                    if status != "000" or not (d and d.get("list")):
                        nodata += 1
                        continue
                    by_ticker: dict[str, list] = defaultdict(list)
                    for row in d["list"]:
                        by_ticker[row.get("stock_code")].append(row)
                    for tkr, rows in by_ticker.items():
                        if not tkr:
                            continue
                        write_text(json.dumps(rows, ensure_ascii=False),
                                   f"{base}/dart/year={year}/corp={tkr}/{reprt}.json")
                        saved += 1
                    time.sleep(CALL_GAP_SEC)
    except QuotaExceeded as exc:
        print(f"[dart] 사용한도 초과로 중단: {exc} — 저장 {saved} / 스킵 {skipped} / 무데이터 {nodata}")
        print("[dart] 내일 같은 명령으로 재개하면 이어서 받음.")
        return

    print(f"[dart] 완료: 저장 {saved} / 스킵 {skipped} / 무데이터 {nodata}")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--from", dest="fromdate", required=True, help="시작 연도 포함 YYYYMMDD")
    p.add_argument("--to", dest="todate", required=True, help="종료 연도 포함 YYYYMMDD")
    p.add_argument("--dest", choices=["local", "s3"], default="local")
    p.add_argument("--tickers", nargs="+", default=None, help="지정 시 이 종목만(미지정 시 구성종목 union)")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    run(args.fromdate, args.todate, args.dest, args.tickers)


if __name__ == "__main__":
    main()
