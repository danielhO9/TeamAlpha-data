"""pykrx 시세 적재 — 하루치 or 기간을 parquet 로 로컬/S3에 저장 (최근·매일 담당).

두 모드 (같은 by_ticker 스냅샷 → date= 파티션, 백필과 일 배치가 통일됨):
  --date        하루치 (일 배치)
  --from/--to   기간 (과거 백필). 거래일만 순회, 재개 가능(이미 있는 날짜 스킵).

경로 규칙:  <base>/pykrx/<dataset>/date=YYYY-MM-DD/<part>.parquet

사용 예:
  uv run python -m bronze.ingest --date 20260706                     # 하루
  uv run python -m bronze.ingest --from 20260701 --to 20260706       # 기간(백필)
  uv run python -m bronze.ingest --date 20260706 --dest s3           # S3 적재
  uv run python -m bronze.ingest --date 20260706 --datasets ohlcv    # 일부만
"""
from __future__ import annotations

import argparse
import time
from bronze import krx
from bronze.common import base_uri, ymd_to_dash
from bronze.sink import exists, write_parquet

DATASETS = ["ohlcv", "market_cap", "index_ohlcv"]
CALL_GAP_SEC = 1.0  # KRX 레이트리밋 완화용 호출 간격


def _path(base: str, dataset: str, date_dash: str, part: str) -> str:
    # pykrx 소스는 pykrx/ 프리픽스 (data.go.kr 은 datago/) → silver 가 형식 구분
    return f"{base}/pykrx/{dataset}/date={date_dash}/{part}.parquet"


# 데이터셋별 파티션 파일(part) 목록 — 재개 완료 판정용
def _parts(dataset: str) -> list[str]:
    return list(krx.INDEX_CODES) if dataset == "index_ohlcv" else krx.MARKETS


def run_date(date: str, dest: str, datasets: list[str], verbose: bool = True) -> tuple[int, int]:
    """하루치 전 종목 스냅샷을 적재. 반환=(성공, 실패) 콜 수."""
    date_dash = ymd_to_dash(date)
    base = base_uri(dest)
    if verbose:
        print(f"[bronze] date={date_dash} dest={dest} base={base}")

    jobs = []  # (dataset, part, fetch_callable)
    if "ohlcv" in datasets:
        for m in krx.MARKETS:
            jobs.append(("ohlcv", m, lambda m=m: krx.fetch_ohlcv(date, m)))
    if "market_cap" in datasets:
        for m in krx.MARKETS:
            jobs.append(("market_cap", m, lambda m=m: krx.fetch_market_cap(date, m)))
    if "index_ohlcv" in datasets:
        for name, code in krx.INDEX_CODES.items():
            jobs.append(("index_ohlcv", name, lambda code=code: krx.fetch_index_ohlcv(date, code)))

    ok, fail = 0, 0
    for dataset, part, fetch in jobs:
        dest_uri = _path(base, dataset, date_dash, part)
        if exists(dest_uri):  # 재개: 이미 있으면 재수집 안 함
            ok += 1
            continue
        try:
            df = fetch()
            if df.empty:
                if verbose:
                    print(f"  ! {dataset}/{part}: 빈 응답 (휴장일이거나 레이트리밋)")
                fail += 1
                continue
            write_parquet(df, dest_uri)
            if verbose:
                print(f"  ✓ {dataset}/{part}: {df.shape[0]}행 x {df.shape[1]}열 → {dest_uri}")
            ok += 1
        except Exception as exc:  # noqa: BLE001
            print(f"  ✗ {dataset}/{part}: 실패 {exc}")
            fail += 1
        time.sleep(CALL_GAP_SEC)

    if verbose:
        print(f"[bronze] 완료: 성공 {ok} / 실패 {fail}")
    return ok, fail


def _date_complete(base: str, date_dash: str, datasets: list[str]) -> bool:
    """요청한 데이터셋의 모든 파티션 파일이 있으면 그 날짜 완료 (로컬·S3 공통 재개)."""
    return all(
        exists(_path(base, ds, date_dash, part))
        for ds in datasets
        for part in _parts(ds)
    )


def run_range(fromdate: str, todate: str, dest: str, datasets: list[str]) -> None:
    """기간 백필 — 거래일마다 run_date 반복. 이미 있는 날짜는 스킵(재개)."""
    base = base_uri(dest)
    days = krx.trading_days(fromdate, todate)
    print(f"[backfill] {fromdate}~{todate}: 거래일 {len(days)}일, dest={dest}")

    done = fail = skipped = 0
    for i, ymd in enumerate(days, 1):
        if _date_complete(base, ymd_to_dash(ymd), datasets):
            skipped += 1
            continue
        _, bad = run_date(ymd, dest, datasets, verbose=False)
        done += 1
        fail += bad
        if i % 20 == 0 or i == len(days):
            print(f"  ... {i}/{len(days)} (적재 {done}, 스킵 {skipped}, 실패콜 {fail})")

    print(f"[backfill] 완료: 적재 {done}일 / 스킵 {skipped}일 / 실패콜 {fail}")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--date", help="하루치 YYYYMMDD")
    p.add_argument("--from", dest="fromdate", help="기간 시작 YYYYMMDD (--to 와 함께)")
    p.add_argument("--to", dest="todate", help="기간 종료 YYYYMMDD")
    p.add_argument("--dest", choices=["local", "s3"], default="local", help="저장 위치 (기본 local)")
    p.add_argument("--datasets", nargs="+", default=DATASETS, choices=DATASETS, help="수집할 데이터셋")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    if args.date:
        run_date(args.date, args.dest, args.datasets)
    elif args.fromdate and args.todate:
        run_range(args.fromdate, args.todate, args.dest, args.datasets)
    else:
        raise SystemExit("--date 또는 (--from 과 --to) 중 하나가 필요합니다.")


if __name__ == "__main__":
    main()
