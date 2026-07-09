"""KRX OpenAPI 지수 일별시세 → bronze (전 지수, 무수정).

data-dbg.krx.co.kr/svc/apis/idx/{svc} : 한 날짜(basDd)의 그 시리즈 전 지수 시세.
응답(OutBlock_1) 전체를 수정 없이 저장(bronze 원칙). 3개 시리즈를 각각 받는다:
  krx_dd_trd(KRX시리즈) · kospi_dd_trd(KOSPI시리즈, 코스피200 포함) · kosdaq_dd_trd(KOSDAQ시리즈, 코스닥150 포함)

한 호출 = 그 날짜 해당 시리즈 전 지수(코스피 48·코스닥 38개 등) → marcap 과 같은 "날짜→전체" 형태.
저장(값 무수정, 파티션만 날짜×시리즈로 분할) — 컨벤션: <데이터종류>/<소스>/date=:
  <base>/index/krxapi/date=YYYY-MM-DD/<series>.parquet   (series ∈ krx|kospi|kosdaq)

응답 필드(무수정 그대로): BAS_DD, IDX_CLSS, IDX_NM, CLSPRC_IDX, CMPPREVDD_IDX, FLUC_RT,
  OPNPRC_IDX, HGPRC_IDX, LWPRC_IDX, ACC_TRDVOL, ACC_TRDVAL, MKTCAP

인증: 헤더 AUTH_KEY (.env 의 KRX_API_KEY). 데이터 2010-01-04~, 하루 10,000콜 제한.
재개: 이미 있는 (날짜×시리즈) 스킵. 빈 응답(휴장일)은 저장 안 함(재실행 시 재조회).
거래일 캘린더가 없어 평일만 순회하고, 휴장일은 빈 응답이라 자연히 건너뛴다.

사용:
  uv run python -m bronze.index --from 20150101 --to 20260707
  uv run python -m bronze.index --from 20150101 --to 20260707 --dest s3
"""
from __future__ import annotations

import argparse
import os
import time
from datetime import datetime, timedelta

import pandas as pd
import requests

from bronze.common import base_uri, ymd_to_dash
from bronze.sink import exists, write_parquet

BASE_URL = "http://data-dbg.krx.co.kr/svc/apis/idx"
# 저장용 시리즈명 -> KRX 서비스명
SERIES = {"krx": "krx_dd_trd", "kospi": "kospi_dd_trd", "kosdaq": "kosdaq_dd_trd"}
CALL_GAP_SEC = 0.2


class AuthError(Exception):
    """KRX OpenAPI 인증 실패(401) — 키 미승인/미활성. 재개 가능하므로 즉시 중단."""


def _fetch(svc: str, basdd: str, key: str, tries: int = 4) -> list[dict]:
    """한 (서비스×날짜) 호출 → OutBlock_1 리스트. 401 은 AuthError, 그 외 blip 은 재시도."""
    for attempt in range(tries):
        try:
            r = requests.get(
                f"{BASE_URL}/{svc}",
                headers={"AUTH_KEY": key},
                params={"basDd": basdd},
                timeout=30,
            )
            if r.status_code == 401:
                raise AuthError(r.text[:80])
            return r.json().get("OutBlock_1") or []
        except AuthError:
            raise
        except Exception:  # noqa: BLE001  (네트워크·JSON blip → 재시도)
            time.sleep(2 * (attempt + 1))
    return []


def _weekdays(fromdate: str, todate: str):
    """[fromdate, todate] 평일(월~금) YYYYMMDD 순회. 휴장일은 빈 응답으로 걸러진다."""
    d = datetime.strptime(fromdate, "%Y%m%d").date()
    end = datetime.strptime(todate, "%Y%m%d").date()
    while d <= end:
        if d.weekday() < 5:
            yield d.strftime("%Y%m%d")
        d += timedelta(days=1)


def run(fromdate: str, todate: str, dest: str) -> None:
    key = os.environ.get("KRX_API_KEY")
    if not key:
        raise SystemExit("KRX_API_KEY 환경변수가 없습니다 (.env 확인)")
    base = base_uri(dest)
    print(f"[index] {fromdate}~{todate} → {base}/index/krxapi/date=.../<series>.parquet, dest={dest}")

    saved = skipped = empty = 0
    try:
        for i, ymd in enumerate(_weekdays(fromdate, todate), 1):
            ds = ymd_to_dash(ymd)
            for series, svc in SERIES.items():
                path = f"{base}/index/krxapi/date={ds}/{series}.parquet"
                if exists(path):  # 재개
                    skipped += 1
                    continue
                rows = _fetch(svc, ymd, key)
                if not rows:  # 휴장일·미도래 → 저장 안 함
                    empty += 1
                    continue
                write_parquet(pd.DataFrame(rows), path)  # 응답 무수정
                saved += 1
                time.sleep(CALL_GAP_SEC)
            if i % 60 == 0:
                print(f"  ... {ds} (저장 {saved}, 스킵 {skipped}, 빈응답 {empty})")
    except AuthError as exc:
        print(f"[index] 인증 실패로 중단(키 승인/활성 확인): {exc}")
        print("[index] 승인 후 같은 명령 재실행하면 이어서 진행.")
        return

    print(f"[index] 완료: 저장 {saved} / 스킵 {skipped} / 빈응답 {empty}")


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
