"""지수 구성종목 수집 (pykrx) — 분기 첫 거래일 스냅샷.

지수 리밸런싱은 반기(6·12월)라 분기 스냅샷이면 정기변경 + 수시변경을 충분히 포착한다.
휴장일이면 다음 거래일로 넘어가며 첫 거래일을 찾는다(1/1·10/1 누락 방지).

저장: <base>/pykrx/index_member/date=<리밸일>/<KOSPI200|KOSDAQ150>.parquet

member_universe(): 기간 내 구성종목 티커 union — DART 유니버스로도 사용.

사용 예:
  uv run python -m bronze.members --from 20150101 --to 20260630
"""
from __future__ import annotations

import argparse
import time
from datetime import date, datetime, timedelta

import pandas as pd

from bronze import krx
from bronze.common import base_uri, ymd_to_dash
from bronze.sink import exists, write_parquet

CALL_GAP_SEC = 1.0
LOOKAHEAD = 10  # 분기 첫 거래일 스캔 최대 일수


def quarter_first_days(fromdate: str, todate: str) -> list[str]:
    start = datetime.strptime(fromdate, "%Y%m%d").date()
    end = datetime.strptime(todate, "%Y%m%d").date()
    out: list[str] = []
    for year in range(start.year, end.year + 1):
        for month in (1, 4, 7, 10):
            d = date(year, month, 1)
            if start <= d <= end:
                out.append(d.strftime("%Y%m%d"))
    return out


def first_trading_membership(code: str, quarter_start: str) -> tuple[str, pd.DataFrame] | tuple[None, None]:
    """분기 시작일부터 앞으로 스캔해 구성종목이 나오는 첫 거래일과 df 반환."""
    start = datetime.strptime(quarter_start, "%Y%m%d").date()
    for offset in range(LOOKAHEAD + 1):
        d = (start + timedelta(days=offset)).strftime("%Y%m%d")
        tickers = krx.stock.get_index_portfolio_deposit_file(code, d)
        if tickers:
            return d, pd.DataFrame({"ticker": tickers})
        time.sleep(CALL_GAP_SEC)
    return None, None


def collect_snapshots(fromdate: str, todate: str) -> list[tuple[str, str, pd.DataFrame]]:
    """분기별 (첫거래일, 지수명, 구성종목 df) 목록."""
    out: list[tuple[str, str, pd.DataFrame]] = []
    for qd in quarter_first_days(fromdate, todate):
        for name, code in krx.INDEX_CODES.items():
            d, df = first_trading_membership(code, qd)
            if df is not None:
                out.append((d, name, df))
            else:
                print(f"  ! {name}: {qd} 부근 구성종목 못 찾음")
    return out


def member_universe(fromdate: str, todate: str) -> list[str]:
    """기간 내 지수 구성종목 티커 union (한 번이라도 편입된 종목, 상폐 포함)."""
    uni: set[str] = set()
    for _, _, df in collect_snapshots(fromdate, todate):
        uni.update(df["ticker"].tolist())
    return sorted(uni)


def run(fromdate: str, todate: str, dest: str) -> None:
    base = base_uri(dest)
    print(f"[members] {fromdate}~{todate} dest={dest}")
    snaps = collect_snapshots(fromdate, todate)
    for d, name, df in snaps:
        dest = f"{base}/pykrx/index_member/date={ymd_to_dash(d)}/{name}.parquet"
        if exists(dest):  # 재개: 이미 있으면 스킵
            continue
        write_parquet(df, dest)
    uni = {t for _, _, df in snaps for t in df["ticker"]}
    print(f"[members] 완료: {len(snaps)}개 분기 스냅샷, 유니버스 {len(uni)}종목")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--from", dest="fromdate", required=True, help="시작일 YYYYMMDD")
    p.add_argument("--to", dest="todate", required=True, help="종료일 YYYYMMDD")
    p.add_argument("--dest", choices=["local", "s3"], default="local")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    run(args.fromdate, args.todate, args.dest)


if __name__ == "__main__":
    main()
