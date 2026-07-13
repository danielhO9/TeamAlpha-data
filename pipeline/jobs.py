"""파이프라인 엔트리포인트 — 백필(초기 1회) vs 일별(증분).

  backfill : bronze 전체 적재 → silver 전체 반영
             시세=marcap(전기간·전종목), 지수=KRX OpenAPI, 재무=DART
  daily    : 오늘치 bronze 적재 → silver 증분 반영
             시세·지수=KRX OpenAPI(공식·일별), 재무=DART(당해 연도 재실행=신규 공시만)

ECS/Fargate 에서 `--mode daily` 를 EventBridge 로 매일 실행. silver 는 현재 로컬 bronze 읽기만 지원한다.

사용:
  python -m pipeline.jobs --mode backfill --from 2015 --to 2026
  python -m pipeline.jobs --mode daily                       # 오늘
  python -m pipeline.jobs --mode daily --date 20260710 --dest s3
"""
from __future__ import annotations

import argparse
from datetime import date

from pipeline.bronze import financials, index, stock_krxapi, stock_marcap
from pipeline.silver import load

# silver 는 현재 로컬 bronze(./data)를 읽는다 → dest='local' 일 때 end-to-end.
# dest='s3' 는 bronze 만 S3 적재(silver S3 직접읽기는 후속).


def run_backfill(fromyear: int, toyear: int, dest: str) -> None:
    """초기 1회: bronze 전 구간 적재 → silver 전체 반영."""
    stock_marcap.run(fromyear, toyear, dest)                  # 시세 marcap (전종목·전기간)
    index.run(f"{fromyear}0101", f"{toyear}1231", dest)        # 지수 KRX OpenAPI
    financials.run(fromyear, toyear, dest)                     # 재무 DART
    if dest == "local":
        load.backfill()                                       # silver 전체 (bronze→RDS)


def run_daily(day: str, dest: str) -> None:
    """매일 증분: 오늘치 bronze → silver 증분 반영. 재개(exists)로 중복 방지."""
    stock_krxapi.run(day, day, dest)                          # 시세 KRX OpenAPI (오늘치)
    index.run(day, day, dest)                                  # 지수 KRX OpenAPI (오늘치)
    financials.run(int(day[:4]), int(day[:4]), dest, refresh_existing=True)  # 재무: 당해 연도 재조회 → 신규 공시 반영
    if dest == "local":
        load.incremental(day)                                 # silver 증분 (bronze→RDS)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--mode", choices=["backfill", "daily"], required=True)
    p.add_argument("--from", dest="fromyear", type=int, help="backfill 시작 연도")
    p.add_argument("--to", dest="toyear", type=int, help="backfill 종료 연도")
    p.add_argument("--date", help="daily 대상일 YYYYMMDD (기본: 오늘)")
    p.add_argument("--dest", choices=["local", "s3"], default="local")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    if args.mode == "backfill":
        if not (args.fromyear and args.toyear):
            raise SystemExit("backfill 은 --from 과 --to (연도) 가 필요합니다.")
        run_backfill(args.fromyear, args.toyear, args.dest)
    else:
        day = args.date or date.today().strftime("%Y%m%d")
        run_daily(day, args.dest)


if __name__ == "__main__":
    main()
