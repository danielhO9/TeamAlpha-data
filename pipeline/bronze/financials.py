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
import re
import time
import zipfile
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from xml.etree import ElementTree as ET

import requests

from pipeline.common.paths import base_uri
from pipeline.common.sink import (
    exists,
    read_bytes,
    write_bytes,
    write_text_if_changed,
)

CORPCODE_URL = "https://opendart.fss.or.kr/api/corpCode.xml"
DISCLOSURE_LIST_URL = "https://opendart.fss.or.kr/api/list.json"
MULTI_URL = "https://opendart.fss.or.kr/api/fnlttMultiAcnt.json"
SINGLE_ALL_URL = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"
REPRT_CODES = ["11011", "11013", "11012", "11014"]  # 사업(FY)/1분기/반기/3분기
BATCH = 100          # 한 콜에 넣을 회사 수
CALL_GAP_SEC = 0.3
CORPCODE_BRONZE_PATH = "financials/dart/corpCode.xml"
DEFERRED_REQUIREMENTS_PATH = "financials/dart_deferred/pending.json"
MAJOR_ACCOUNT_NAMES = {
    "자산총계",
    "매출액",
    "당기순이익",
    "당기순이익(손실)",
}
_REGULAR_REPORT_RE = re.compile(
    r"(사업|반기|분기)보고서.*?\(((?:19|20)\d{2})\.(\d{2})\)"
)


class QuotaExceeded(Exception):
    """OpenDART 사용한도 초과(status 020)."""


class CorpCodeDownloadError(RuntimeError):
    """Secret-free failure raised when the DART corp-code download fails."""


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
    failure: tuple[str, int | None] | None = None
    response = None
    try:
        response = requests.get(
            CORPCODE_URL,
            params={"crtfc_key": os.environ["DART_API_KEY"]},
            timeout=60,
        )
        response.raise_for_status()
    except Exception as exc:  # noqa: BLE001
        # A requests exception retains its fully prepared URL, including the
        # ``crtfc_key`` query value.  Preserve only non-secret diagnostics and
        # discard both the exception and response before raising below.
        raw_status = getattr(
            getattr(exc, "response", None),
            "status_code",
            None,
        )
        status_code = raw_status if isinstance(raw_status, int) else None
        failure = (
            type(exc).__name__,
            status_code,
        )
        response = None
    if failure is not None:
        failure_name, status_code = failure
        status = f", http_status={status_code}" if status_code is not None else ""
        raise CorpCodeDownloadError(
            "OpenDART corp-code request failed: "
            f"endpoint=corpCode.xml, failure={failure_name}{status}"
        ) from None
    assert response is not None
    z = zipfile.ZipFile(io.BytesIO(response.content))
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
    existing = read_bytes(dest)
    if existing is not None:
        return _parse_listed_corps(existing)

    xml_bytes = _download_corp_code_xml()
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


def _fetch_regular_disclosure_page(
    day: str,
    page_no: int,
    tries: int = 4,
) -> dict:
    """Fetch one complete-day periodic-disclosure page, including corrections."""
    params = {
        "crtfc_key": os.environ["DART_API_KEY"],
        "bgn_de": day,
        "end_de": day,
        "last_reprt_at": "N",
        "pblntf_ty": "A",
        "page_no": str(page_no),
        "page_count": "100",
    }
    for attempt in range(tries):
        try:
            payload = requests.get(
                DISCLOSURE_LIST_URL,
                params=params,
                timeout=60,
            ).json()
            status = str(payload.get("status") or "?")
            if status == "020":
                raise QuotaExceeded(f"regular-disclosure-list {day}")
            if status in {"000", "013"}:
                return payload
            raise RuntimeError(
                "OpenDART regular-disclosure list rejected: "
                f"status={status}, day={day}, page={page_no}"
            )
        except QuotaExceeded:
            raise
        except RuntimeError:
            raise
        except Exception:  # noqa: BLE001
            if attempt + 1 < tries:
                time.sleep(2 * (attempt + 1))
    raise RuntimeError(
        "OpenDART regular-disclosure list failed after retries: "
        f"day={day}, page={page_no}"
    )


def _regular_disclosure_path(base: str, day: str) -> str:
    rendered = datetime.strptime(day, "%Y%m%d").date().isoformat()
    return (
        f"{base}/financials/dart_disclosures/date={rendered}/"
        "regular-reports.json"
    )


def _regular_disclosures(base: str, day: str) -> list[dict]:
    """Return a durable complete list for one filing date."""
    path = _regular_disclosure_path(base, day)
    raw = read_bytes(path)
    if raw is not None:
        rows = json.loads(raw.decode("utf-8"))
        if not isinstance(rows, list):
            raise RuntimeError(f"invalid regular-disclosure checkpoint: {path}")
        return rows

    by_receipt: dict[str, dict] = {}
    page_no = 1
    while True:
        payload = _fetch_regular_disclosure_page(day, page_no)
        for row in payload.get("list") or []:
            if not isinstance(row, dict):
                continue
            receipt = str(row.get("rcept_no") or "").strip()
            if re.fullmatch(r"\d{14}", receipt):
                by_receipt[receipt] = row
        total_page = int(payload.get("total_page") or 0)
        if str(payload.get("status") or "?") == "013" or page_no >= total_page:
            break
        page_no += 1
    rows = [by_receipt[key] for key in sorted(by_receipt)]
    write_text_if_changed(json.dumps(rows, ensure_ascii=False), path)
    return rows


def _regular_report_scopes(report_name: object) -> tuple[int, tuple[str, ...]] | None:
    """Map a DART periodic report title to financial API report scopes.

    DART uses one detail type for both first- and third-quarter reports.  Query
    both quarter codes for an affected company so non-December fiscal years are
    not guessed from the calendar month in the title.
    """
    compact = re.sub(r"\s+", "", str(report_name or ""))
    match = _REGULAR_REPORT_RE.search(compact)
    if match is None:
        return None
    report_kind, year, _month = match.groups()
    codes = {
        "사업": ("11011",),
        "반기": ("11012",),
        "분기": ("11013", "11014"),
    }[report_kind]
    return int(year), codes


def _incremental_disclosure_days(day: str) -> list[str]:
    """Include weekend filing dates preceding a Monday pipeline target."""
    target = datetime.strptime(day, "%Y%m%d").date()
    days = [target]
    cursor = target - timedelta(days=1)
    while cursor.weekday() >= 5:
        days.append(cursor)
        cursor -= timedelta(days=1)
    return [value.strftime("%Y%m%d") for value in sorted(days)]


def _load_deferred_requirements(
    base: str,
) -> set[tuple[str, int, tuple[str, ...]]]:
    """Load periodic-report scopes that were filed before the API exposed them."""
    raw = read_bytes(f"{base}/{DEFERRED_REQUIREMENTS_PATH}")
    if raw is None:
        return set()
    payload = json.loads(raw.decode("utf-8"))
    if not isinstance(payload, list):
        raise RuntimeError("invalid deferred financial requirement checkpoint")
    requirements: set[tuple[str, int, tuple[str, ...]]] = set()
    for item in payload:
        if not isinstance(item, dict):
            raise RuntimeError("invalid deferred financial requirement row")
        corp_code = str(item.get("corp_code") or "").strip()
        year = item.get("year")
        report_codes = tuple(sorted({
            str(value).strip() for value in item.get("report_codes", [])
            if str(value).strip() in REPRT_CODES
        }))
        if not re.fullmatch(r"\d{8}", corp_code) or not isinstance(year, int) \
                or not report_codes:
            raise RuntimeError("invalid deferred financial requirement identity")
        requirements.add((corp_code, year, report_codes))
    return requirements


def _save_deferred_requirements(
    base: str,
    requirements: list[tuple[str, int, list[str]]],
) -> None:
    payload = [
        {
            "corp_code": corp_code,
            "year": year,
            "report_codes": report_codes,
        }
        for corp_code, year, report_codes in requirements
    ]
    write_text_if_changed(
        json.dumps(payload, ensure_ascii=False, sort_keys=True),
        f"{base}/{DEFERRED_REQUIREMENTS_PATH}",
    )


def run_incremental(day: str, dest: str) -> list[str]:
    """Refresh only companies that filed a periodic report on ``day``.

    A one-call disclosure search replaces the former daily current-year sweep.
    Affected companies are still grouped into the provider's 100-company
    endpoint, and amended reports are included by ``last_reprt_at=N``.
    """
    datetime.strptime(day, "%Y%m%d")
    base = base_uri(dest)
    corps = ensure_corp_code_xml(base)
    corp_to_stock = dict(corps)
    stock_to_corp = {stock: corp for corp, stock in corps}
    groups: dict[tuple[int, str], set[str]] = defaultdict(set)
    requirements = _load_deferred_requirements(base)
    for corp_code, year, report_codes in requirements:
        for report_code in report_codes:
            groups[(year, report_code)].add(corp_code)
    disclosures = [
        row
        for disclosure_day in _incremental_disclosure_days(day)
        for row in _regular_disclosures(base, disclosure_day)
    ]
    for row in disclosures:
        corp_code = str(row.get("corp_code") or "").strip()
        if corp_code not in corp_to_stock:
            continue
        scope = _regular_report_scopes(row.get("report_nm"))
        if scope is None:
            continue
        year, report_codes = scope
        requirements.add((corp_code, year, tuple(sorted(report_codes))))
        for report_code in report_codes:
            groups[(year, report_code)].add(corp_code)

    print(
        f"[financials-incremental] day={day} disclosures={len(disclosures)} "
        f"scopes={sum(len(values) for values in groups.values())} dest={dest}",
        flush=True,
    )
    changed_paths: list[str] = []
    satisfied: set[tuple[str, int, str]] = set()
    api_calls = full_calls = 0
    for (year, report_code), affected in sorted(groups.items()):
        ordered = sorted(affected)
        for offset in range(0, len(ordered), BATCH):
            batch = ordered[offset:offset + BATCH]
            status, payload = _fetch_multi(batch, year, report_code)
            api_calls += 1
            if status == "020":
                raise QuotaExceeded(f"{year} {report_code}")
            if status == "013":
                continue
            if status != "000" or payload is None:
                raise RuntimeError(
                    "OpenDART incremental financial request failed: "
                    f"status={status}, year={year}, report={report_code}"
                )
            by_ticker: dict[str, list[dict]] = defaultdict(list)
            for row in payload.get("list") or []:
                ticker = str(row.get("stock_code") or "").strip()
                if ticker:
                    by_ticker[ticker].append(row)
            for ticker, rows in sorted(by_ticker.items()):
                returned_corp = stock_to_corp.get(ticker)
                if returned_corp is None:
                    raise RuntimeError(
                        "OpenDART returned an unexpected listed ticker: "
                        f"ticker={ticker}, year={year}, report={report_code}"
                    )
                satisfied.add((returned_corp, year, report_code))
                path = (
                    f"{base}/financials/dart/year={year}/corp={ticker}/"
                    f"{report_code}.json"
                )
                if write_text_if_changed(
                    json.dumps(rows, ensure_ascii=False), path,
                ):
                    changed_paths.append(path)
                for fs_div in _missing_major_scopes(rows):
                    full_calls += 1
                    status_full, full_payload = _fetch_single_all(
                        stock_to_corp[ticker], year, report_code, fs_div,
                    )
                    if status_full == "020":
                        raise QuotaExceeded(
                            f"full-statement {year} {report_code} "
                            f"{ticker} {fs_div}"
                        )
                    if status_full == "013":
                        continue
                    if status_full != "000" or full_payload is None:
                        raise RuntimeError(
                            "OpenDART incremental full-statement request failed: "
                            f"status={status_full}, year={year}, "
                            f"report={report_code}, ticker={ticker}, "
                            f"fs_div={fs_div}"
                        )
                    full_rows = full_payload.get("list") or []
                    if not full_rows:
                        continue
                    full_path = (
                        f"{base}/financials/dart_full/year={year}/"
                        f"corp={ticker}/{report_code}-{fs_div}.json"
                    )
                    if write_text_if_changed(
                        json.dumps(full_rows, ensure_ascii=False), full_path,
                    ):
                        changed_paths.append(full_path)
                    time.sleep(CALL_GAP_SEC)
            time.sleep(CALL_GAP_SEC)
    missing = sorted(
        (corp_code, year, list(report_codes))
        for corp_code, year, report_codes in requirements
        if not any(
            (corp_code, year, report_code) in satisfied
            for report_code in report_codes
        )
    )
    _save_deferred_requirements(base, missing)
    if missing:
        print(
            "[financials-incremental] deferred until provider availability: "
            f"{missing[:20]}",
            flush=True,
        )
    print(
        f"[financials-incremental] complete changed={len(changed_paths)} "
        f"major_calls={api_calls} full_calls={full_calls}",
        flush=True,
    )
    return sorted(set(changed_paths))


def _fetch_single_all(
    corp_code: str,
    year: int,
    reprt: str,
    fs_div: str,
    tries: int = 4,
) -> tuple[str, dict | None]:
    params = {
        "crtfc_key": os.environ["DART_API_KEY"],
        "corp_code": corp_code,
        "bsns_year": str(year),
        "reprt_code": reprt,
        "fs_div": fs_div,
    }
    for attempt in range(tries):
        try:
            payload = requests.get(
                SINGLE_ALL_URL,
                params=params,
                timeout=60,
            ).json()
            return payload.get("status", "?"), payload
        except Exception:  # noqa: BLE001
            time.sleep(2 * (attempt + 1))
    return "?", None


def _missing_major_scopes(rows: list[dict]) -> list[str]:
    """주요계정 응답에서 핵심 계정이 하나도 없는 CFS/OFS를 찾는다."""
    missing = []
    fs_divisions = sorted({
        str(row.get("fs_div") or "").strip()
        for row in rows
        if str(row.get("fs_div") or "").strip() in {"CFS", "OFS"}
    })
    for fs_div in fs_divisions:
        names = {
            str(row.get("account_nm") or "").strip()
            for row in rows
            if str(row.get("fs_div") or "").strip() == fs_div
        }
        if not names & MAJOR_ACCOUNT_NAMES:
            missing.append(fs_div)
    return missing


def run(fromyear: int, toyear: int, dest: str, refresh_existing: bool = False) -> list[str]:
    base = base_uri(dest)
    corps = ensure_corp_code_xml(base)
    corp_to_stock = dict(corps)
    stock_to_corp = {stock: corp for corp, stock in corps}
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
                        for fs_div in _missing_major_scopes(rows):
                            full_path = (
                                f"{base}/financials/dart_full/year={year}/"
                                f"corp={tkr}/{reprt}-{fs_div}.json"
                            )
                            if exists(full_path) and not refresh_existing:
                                changed_paths.append(full_path)
                                continue
                            status_full, full_payload = _fetch_single_all(
                                stock_to_corp[tkr],
                                year,
                                reprt,
                                fs_div,
                            )
                            if status_full == "020":
                                raise QuotaExceeded(
                                    f"full-statement {year} {reprt} "
                                    f"{tkr} {fs_div}"
                                )
                            if (
                                status_full == "000"
                                and full_payload
                                and full_payload.get("list")
                            ):
                                if write_text_if_changed(
                                    json.dumps(
                                        full_payload["list"],
                                        ensure_ascii=False,
                                    ),
                                    full_path,
                                ):
                                    saved += 1
                                else:
                                    unchanged += 1
                                changed_paths.append(full_path)
                                time.sleep(CALL_GAP_SEC)
                    time.sleep(CALL_GAP_SEC)
    except QuotaExceeded as exc:
        print(f"[financials] 사용한도 초과로 중단: {exc} — 저장 {saved} / 변경없음 {unchanged} / 스킵 {skipped} / 무데이터 {nodata}")
        print("[financials] 내일 같은 명령으로 재개하면 이어서 받음.")
        raise

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
