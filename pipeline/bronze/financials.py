"""OpenDART 다중회사 주요계정 → bronze 재무 (전 상장사, 무수정).

유니버스: bronze 에 저장한 DART corpCode.xml 에서 stock_code 가 있는 상장사(상폐 포함) 전체 — pykrx 불필요.
API: fnlttMultiAcnt(다중회사 주요계정) — 한 콜에 corp_code 100개 × 연결(CFS)+별도(OFS) × 주요계정 ~15.
응답(list[])을 stock_code 별로 나눠 저장(값 무수정, 파티션만 분할).

저장(컨벤션 <종류>/<소스>/):
  <base>/financials/dart/corpCode.xml                         (DART 회사코드 XML)
  <base>/financials/dart/year=YYYY/corp=<ticker>/<reprt>.json   (한 회사·한 보고서, CFS+OFS 주요계정 raw rows)

reprt_code: 11011 사업(FY) / 11012 반기 / 11013 1분기 / 11014 3분기
status: 000 저장 / 013 무데이터 스킵 / 020 사용한도초과 → 중단(재개 가능)
재개: (배치 첫 종목 × 연도 × 보고서) 파일이 있으면 그 배치 스킵. 중단 후 같은 명령 재실행하면 이어서.

사용:
  uv run python -m pipeline.bronze.financials --from 2015 --to 2026
  uv run python -m pipeline.bronze.financials --from 2015 --to 2026 --dest s3
"""
from __future__ import annotations

import argparse
import io
import json
import os
import time
import zipfile
from collections import defaultdict
from pathlib import Path
from xml.etree import ElementTree as ET

import requests

from pipeline.common.paths import base_uri
from pipeline.common.sink import exists, write_bytes, write_text_if_changed

CORPCODE_URL = "https://opendart.fss.or.kr/api/corpCode.xml"
MULTI_URL = "https://opendart.fss.or.kr/api/fnlttMultiAcnt.json"
REPRT_CODES = ["11011", "11013", "11012", "11014"]  # 사업(FY)/1분기/반기/3분기
BATCH = 100          # 한 콜에 넣을 회사 수
CALL_GAP_SEC = 0.3
CORPCODE_BRONZE_PATH = "financials/dart/corpCode.xml"


class QuotaExceeded(Exception):
    """OpenDART 사용한도 초과(status 020)."""


def _parse_listed_corps(xml_bytes: bytes) -> list[tuple[str, str]]:
    """corpCode.xml bytes → [(corp_code, stock_code)] (상장사=stock_code 있음, 상폐 포함)."""
    root = ET.fromstring(xml_bytes)
    out: list[tuple[str, str]] = []
    for x in root.findall("list"):
        sc = (x.findtext("stock_code") or "").strip()
        cc = (x.findtext("corp_code") or "").strip()
        if sc and cc:
            out.append((cc, sc))
    return sorted(out)  # corp_code 기준 정렬(배치 결정적)


def _download_corp_code_xml() -> bytes:
    """OpenDART corpCode.zip 다운로드 후 내부 CORPCODE.xml bytes 반환."""
    r = requests.get(CORPCODE_URL, params={"crtfc_key": os.environ["DART_API_KEY"]}, timeout=60)
    r.raise_for_status()
    z = zipfile.ZipFile(io.BytesIO(r.content))
    return z.read(z.namelist()[0])


def load_listed_corps_from_bronze(base: str) -> list[tuple[str, str]]:
    """bronze corpCode.xml → [(corp_code, stock_code)]. silver 는 이 함수만 사용한다."""
    if base.startswith("s3://"):
        raise SystemExit("silver 는 현재 로컬 bronze 만 지원합니다. corpCode.xml 도 로컬 ./data 에 있어야 합니다.")
    path = Path(base) / CORPCODE_BRONZE_PATH
    if not path.exists():
        raise SystemExit(f"bronze corpCode.xml 이 없습니다: {path}\n"
                         "먼저 `python -m pipeline.bronze.financials --from <YYYY> --to <YYYY>` 를 실행하세요.")
    return _parse_listed_corps(path.read_bytes())


def ensure_corp_code_xml(base: str) -> list[tuple[str, str]]:
    """bronze corpCode.xml 이 있으면 읽고, 없으면 다운로드해 저장한 뒤 파싱한다."""
    dest = f"{base}/{CORPCODE_BRONZE_PATH}"
    if not base.startswith("s3://") and exists(dest):
        return load_listed_corps_from_bronze(base)

    xml_bytes = _download_corp_code_xml()
    if not exists(dest):
        write_bytes(xml_bytes, dest)
    return _parse_listed_corps(xml_bytes)


def _fetch_multi(corp_codes: list[str], year: int, reprt: str, tries: int = 4) -> tuple[str, dict | None]:
    params = {
        "crtfc_key": os.environ["DART_API_KEY"],
        "corp_code": ",".join(corp_codes),
        "bsns_year": str(year),
        "reprt_code": reprt,
    }
    for attempt in range(tries):
        try:
            d = requests.get(MULTI_URL, params=params, timeout=60).json()
            return d.get("status", "?"), d
        except Exception:  # noqa: BLE001  (네트워크 blip·JSON 오류 → 재시도)
            time.sleep(2 * (attempt + 1))
    return "?", None


def run(fromyear: int, toyear: int, dest: str, refresh_existing: bool = False) -> list[str]:
    base = base_uri(dest)
    corps = ensure_corp_code_xml(base)
    corp_to_stock = dict(corps)
    universe = [cc for cc, _ in corps]
    batches = [universe[i:i + BATCH] for i in range(0, len(universe), BATCH)]
    print(f"[financials] {fromyear}~{toyear}, 상장사 {len(universe)}개 → 배치 {len(batches)}개 "
          f"× 연도 × 보고서, dest={dest}")

    saved = skipped = nodata = unchanged = 0
    changed_paths: list[str] = []
    try:
        for year in range(fromyear, toyear + 1):
            for reprt in REPRT_CODES:
                for batch in batches:
                    # 재개 마커: 배치 첫 종목 파일 (배치 단위 스킵)
                    marker = f"{base}/financials/dart/year={year}/corp={corp_to_stock[batch[0]]}/{reprt}.json"
                    if not refresh_existing and exists(marker):
                        skipped += 1
                        continue
                    status, d = _fetch_multi(batch, year, reprt)
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
                        path = f"{base}/financials/dart/year={year}/corp={tkr}/{reprt}.json"
                        if write_text_if_changed(json.dumps(rows, ensure_ascii=False), path):
                            changed_paths.append(path)
                            saved += 1
                        else:
                            unchanged += 1
                    time.sleep(CALL_GAP_SEC)
    except QuotaExceeded as exc:
        print(f"[financials] 사용한도 초과로 중단: {exc} — 저장 {saved} / 변경없음 {unchanged} / 스킵 {skipped} / 무데이터 {nodata}")
        print("[financials] 내일 같은 명령으로 재개하면 이어서 받음.")
        return changed_paths

    print(f"[financials] 완료: 저장 {saved} / 변경없음 {unchanged} / 스킵 {skipped} / 무데이터 {nodata}")
    return changed_paths


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
