"""data.go.kr 금융위 공식 API 수집 — 과거 대량 백필 (~2026.06).

  주식시세정보 (getStockPriceInfo)   : 전 종목 OHLCV + 시총 + 상장주식수 (한 레코드)
  지수시세정보 (getStockMarketIndex) : 전 지수 레벨 시세

날짜범위를 월 단위로 페이지네이션해 받고, basDt 기준으로 date= 파티션에 저장한다.
저장 경로: <base>/datago/<dataset>/date=YYYY-MM-DD/all.parquet   (raw JSON 레코드 그대로)

인증키는 .env 의 DATA_GO_KR_ENC_KEY(인코딩키) 사용. 값은 문자열 그대로 저장(정규화는 silver).

사용 예:
  uv run python -m bronze.datago --from 20250101 --to 20250630
  uv run python -m bronze.datago --from 20150101 --to 20260630 --dest s3
  uv run python -m bronze.datago --from 20250101 --to 20250131 --datasets stock
"""
from __future__ import annotations

import argparse
import os
import time
from datetime import date, datetime, timedelta

import pandas as pd
import requests

from bronze.common import base_uri, ymd_to_dash
from bronze.sink import exists, write_parquet

STOCK_URL = "https://apis.data.go.kr/1160100/service/GetStockSecuritiesInfoService/getStockPriceInfo"
INDEX_URL = "https://apis.data.go.kr/1160100/service/GetMarketIndexInfoService/getStockMarketIndex"

DATASETS = ["stock", "index"]
NUM_ROWS = 10000        # 페이지당 행수
CALL_GAP_SEC = 0.3


def _key() -> str:
    key = os.environ.get("DATA_GO_KR_ENC_KEY")
    if not key:
        raise SystemExit("DATA_GO_KR_ENC_KEY 환경변수가 없습니다 (.env 확인)")
    return key


def _month_ranges(fromdate: str, todate: str) -> list[tuple[str, str]]:
    """[from, to] 를 월 단위 (begin, end) YYYYMMDD 목록으로 분할."""
    start = datetime.strptime(fromdate, "%Y%m%d").date()
    end = datetime.strptime(todate, "%Y%m%d").date()
    out: list[tuple[str, str]] = []
    y, m = start.year, start.month
    while (y, m) <= (end.year, end.month):
        first = date(y, m, 1)
        nxt = date(y + 1, 1, 1) if m == 12 else date(y, m + 1, 1)
        last = nxt - timedelta(days=1)
        b, e = max(first, start), min(last, end)
        out.append((b.strftime("%Y%m%d"), e.strftime("%Y%m%d")))
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


def _fetch_range(url: str, begin: str, end: str) -> list[dict]:
    """날짜범위 전체를 페이지네이션으로 받아 raw 레코드 리스트 반환."""
    key = _key()
    records: list[dict] = []
    page = 1
    while True:
        u = (f"{url}?serviceKey={key}&resultType=json&numOfRows={NUM_ROWS}"
             f"&pageNo={page}&beginBasDt={begin}&endBasDt={end}")
        body = requests.get(u, timeout=60).json()["response"]["body"]
        total = int(body.get("totalCount", 0))
        items = body.get("items")
        item = items.get("item") if isinstance(items, dict) else None
        if not item:
            break
        item = item if isinstance(item, list) else [item]
        records.extend(item)
        if page * NUM_ROWS >= total:
            break
        page += 1
        time.sleep(CALL_GAP_SEC)
    return records


def _write_by_date(records: list[dict], base: str, dataset: str) -> int:
    """레코드를 basDt 기준으로 묶어 date= 파티션 parquet 로 저장. 반환=저장한 날짜 수."""
    if not records:
        return 0
    df = pd.DataFrame(records)
    days = 0
    for basdt, grp in df.groupby("basDt"):
        dest = f"{base}/datago/{dataset}/date={ymd_to_dash(str(basdt))}/all.parquet"
        if exists(dest):  # 재개: 이미 있는 날짜는 재작성 안 함
            continue
        write_parquet(grp.reset_index(drop=True), dest)
        days += 1
    return days


def run(fromdate: str, todate: str, dest: str, datasets: list[str]) -> None:
    base = base_uri(dest)
    print(f"[datago] {fromdate}~{todate} dest={dest} base={base}")
    url_of = {"stock": STOCK_URL, "index": INDEX_URL}

    for begin, end in _month_ranges(fromdate, todate):
        for ds in datasets:
            try:
                recs = _fetch_range(url_of[ds], begin, end)
                days = _write_by_date(recs, base, ds)
                print(f"  ✓ {ds} {begin}~{end}: {len(recs)}레코드 → {days}일 파티션")
            except Exception as exc:  # noqa: BLE001
                print(f"  ✗ {ds} {begin}~{end}: 실패 {exc}")
        time.sleep(CALL_GAP_SEC)

    print("[datago] 완료")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--from", dest="fromdate", required=True, help="시작일 YYYYMMDD")
    p.add_argument("--to", dest="todate", required=True, help="종료일 YYYYMMDD")
    p.add_argument("--dest", choices=["local", "s3"], default="local")
    p.add_argument("--datasets", nargs="+", default=DATASETS, choices=DATASETS)
    return p.parse_args()


def main() -> None:
    args = parse_args()
    run(args.fromdate, args.todate, args.dest, args.datasets)


if __name__ == "__main__":
    main()
