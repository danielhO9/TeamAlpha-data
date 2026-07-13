"""silver 오케스트레이터 — bronze 읽어 정규화 → RDS 적재.

backfill: asset → price_daily → fundamental 전체.
incremental: 지정 날짜 price_daily 삭제 후 재적재 + 해당 연도 fundamental upsert.

현재 bronze 읽기는 로컬 ./data 만 지원(글롭 기반). S3 직접 읽기는 후속.

사용:
  python -m pipeline.silver.load --mode backfill
  python -m pipeline.silver.load --mode incremental --date 20260710
"""
from __future__ import annotations

import argparse
from datetime import date, datetime

from pipeline.common import db
from pipeline.common.paths import base_uri
from pipeline.silver import assets, financials, prices


def backfill(src: str = "local") -> None:
    base = base_uri(src)
    conn = db.connect()
    try:
        krx_map = assets.build(conn, base)
        prices.run(conn, base, krx_map)
        financials.run(conn, base, krx_map)
    finally:
        conn.close()


def _parse_day(day: str | None) -> date:
    if not day:
        raise SystemExit("incremental 은 --date YYYYMMDD 가 필요합니다.")
    for fmt in ("%Y%m%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(day, fmt).date()
        except ValueError:
            pass
    raise SystemExit("날짜 형식은 YYYYMMDD 또는 YYYY-MM-DD 여야 합니다.")


def _delete_price_daily_for_date(conn, target_date: date) -> int:
    """같은 거래일 silver 가격을 먼저 지워 daily 재적재를 결정적으로 만든다."""
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM price_daily WHERE source = 'KRX' AND trade_date = %s",
            (target_date,),
        )
        deleted = cur.rowcount
    conn.commit()
    return deleted


def incremental(day: str | None = None, src: str = "local", financial_files: list[str] | None = None) -> None:
    target_date = _parse_day(day)
    base = base_uri(src)
    conn = db.connect()
    try:
        krx_map = assets.build(conn, base)
        deleted = _delete_price_daily_for_date(conn, target_date)
        print(f"[prices] 기존 price_daily {target_date.isoformat()} {deleted}행 삭제")
        prices.run(conn, base, krx_map, target_date=target_date)
        if financial_files is not None:
            print(f"[financials] 변경 파일 {len(financial_files)}개만 반영")
            financials.run(conn, base, krx_map, files=financial_files, replace_existing=True)
        else:
            financials.run(conn, base, krx_map, years={target_date.year})
    finally:
        conn.close()


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--mode", choices=["backfill", "incremental"], default="backfill")
    p.add_argument("--src", choices=["local"], default="local", help="bronze 위치 (현재 local 만)")
    p.add_argument("--date", help="incremental 대상일 YYYYMMDD")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    if args.mode == "backfill":
        backfill(args.src)
    else:
        incremental(args.date, args.src)


if __name__ == "__main__":
    main()
