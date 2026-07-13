"""KRX OpenAPI 개별종목 일별매매정보 → bronze 시세 (일별 증분용, 무수정).

data-dbg.krx.co.kr/svc/apis/sto/{svc} : 한 날짜(basDd)의 그 시장 전 종목 시세.
응답(OutBlock_1) 전체를 수정 없이 저장. 시장별로 각각:
  stk_bydd_trd(유가증권=KOSPI) · ksq_bydd_trd(코스닥)

한 호출 = 그 날짜 해당 시장 전 종목 → marcap(백필)과 같은 "날짜→전종목" 형태, 공식·일별.
저장(값 무수정, 컨벤션 <종류>/<소스>/date=):
  <base>/stock/krxapi/date=YYYY-MM-DD/<market>.parquet   (market ∈ kospi|kosdaq)

응답 필드(무수정): BAS_DD, ISU_CD, ISU_NM, MKT_NM, SECT_TP_NM, TDD_CLSPRC, CMPPREVDD_PRC,
  FLUC_RT, TDD_OPNPRC, TDD_HGPRC, TDD_LWPRC, ACC_TRDVOL, ACC_TRDVAL, MKTCAP, LIST_SHRS

인증: 헤더 AUTH_KEY (.env 의 KRX_API_KEY). 데이터 2010-01-04~, 하루 10,000콜 제한.
재개: 이미 있는 (날짜×시장) 스킵. 빈 응답(휴장일)은 저장 안 함.
용도: 매일 증분(오늘치). 과거 백필은 marcap(stock_marcap) 담당.

사용:
  uv run python -m pipeline.bronze.stock_krxapi --from 20260710 --to 20260710
  uv run python -m pipeline.bronze.stock_krxapi --from 20260701 --to 20260710 --dest s3
"""
from __future__ import annotations

import argparse
import os
import time
from datetime import datetime, timedelta

import pandas as pd
import requests

from pipeline.common.paths import base_uri, ymd_to_dash
from pipeline.common.sink import exists, write_parquet

BASE_URL = "http://data-dbg.krx.co.kr/svc/apis/sto"
# 저장용 시장명 -> KRX 서비스명
MARKETS = {"kospi": "stk_bydd_trd", "kosdaq": "ksq_bydd_trd"}
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
    print(f"[stock_krxapi] {fromdate}~{todate} → {base}/stock/krxapi/date=.../<market>.parquet, dest={dest}")

    saved = skipped = empty = 0
    try:
        for i, ymd in enumerate(_weekdays(fromdate, todate), 1):
            ds = ymd_to_dash(ymd)
            for market, svc in MARKETS.items():
                path = f"{base}/stock/krxapi/date={ds}/{market}.parquet"
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
        print(f"[stock_krxapi] 인증 실패로 중단(키 승인/활성 확인): {exc}")
        print("[stock_krxapi] 승인 후 같은 명령 재실행하면 이어서 진행.")
        return

    print(f"[stock_krxapi] 완료: 저장 {saved} / 스킵 {skipped} / 빈응답 {empty}")


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
