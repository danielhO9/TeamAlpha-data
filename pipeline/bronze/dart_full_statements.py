"""OpenDART 전체 재무제표를 공시 revision별 immutable Bronze로 보존한다.

기존 ``financials/dart`` 주요계정 파일에서 실제 존재하는 회사·보고서·CFS/OFS
scope만 찾아 ``fnlttSinglAcntAll``을 호출한다. 응답은 content hash 경로에 그대로
저장하고, scope별 ``latest.json``은 그 immutable 응답을 가리킨다. 사용한도 초과나
연결 단절 뒤 같은 명령을 다시 실행하면 완료 scope를 건너뛴다.

이 수집기는 원계정을 표준 metric으로 매핑하지 않는다. 분석용 변환은
``pipeline.silver.full_statements``의 책임이다.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

from pipeline.bronze import financials
from pipeline.common.paths import base_uri
from pipeline.common.sink import exists, read_bytes, write_bytes, write_text_if_changed

ENDPOINT = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"
_MAJOR_KEY_RE = re.compile(
    r"financials/dart/year=(?P<year>\d{4})/corp=(?P<ticker>[0-9A-Z]{6})/"
    r"(?P<report>11011|11012|11013|11014)\.json$"
)


class DartRequestError(RuntimeError):
    """Secret-free OpenDART request failure."""


def _api_key() -> str:
    value = os.environ.get("DART_API_KEY", "").strip()
    if not value:
        raise RuntimeError("DART_API_KEY is required")
    return value


def _list_major_uris(base: str, from_year: int, to_year: int) -> list[str]:
    if base.startswith("s3://"):
        import boto3

        without = base.removeprefix("s3://")
        bucket, _, root_prefix = without.partition("/")
        root_prefix = root_prefix.rstrip("/")
        prefix_root = f"{root_prefix}/" if root_prefix else ""
        keys: list[str] = []
        client = boto3.client("s3")
        for year in range(from_year, to_year + 1):
            prefix = f"{prefix_root}financials/dart/year={year}/"
            for page in client.get_paginator("list_objects_v2").paginate(
                Bucket=bucket, Prefix=prefix,
            ):
                keys.extend(
                    obj["Key"] for obj in page.get("Contents", [])
                    if _MAJOR_KEY_RE.search(obj["Key"])
                )
        return [f"s3://{bucket}/{key}" for key in sorted(set(keys))]

    root = Path(base)
    paths: list[str] = []
    for year in range(from_year, to_year + 1):
        paths.extend(
            str(path) for path in sorted(
                (root / "financials" / "dart" / f"year={year}").glob(
                    "corp=*/*.json"
                )
            )
            if _MAJOR_KEY_RE.search(path.as_posix())
        )
    return paths


def discover_scopes(
    base: str,
    from_year: int,
    to_year: int,
) -> list[tuple[str, int, str, str]]:
    """Return deterministic ticker/year/report/fs_type scopes with real filings."""
    scopes: set[tuple[str, int, str, str]] = set()
    for uri in _list_major_uris(base, from_year, to_year):
        match = _MAJOR_KEY_RE.search(uri.replace("\\", "/"))
        if match is None:
            continue
        raw = read_bytes(uri)
        if raw is None:
            raise RuntimeError(f"Bronze object disappeared while listing: {uri}")
        rows = json.loads(raw.decode("utf-8"))
        fs_types = {
            str(row.get("fs_div") or "").strip()
            for row in rows
            if str(row.get("fs_div") or "").strip() in {"CFS", "OFS"}
        }
        for fs_type in fs_types:
            scopes.add((
                match.group("ticker"),
                int(match.group("year")),
                match.group("report"),
                fs_type,
            ))
    return sorted(scopes)


def discover_scopes_from_files(
    files: list[str],
) -> list[tuple[str, int, str, str]]:
    """Discover only scopes represented by explicitly changed major files."""
    scopes: set[tuple[str, int, str, str]] = set()
    for uri in sorted(set(files)):
        match = _MAJOR_KEY_RE.search(uri.replace("\\", "/"))
        if match is None:
            continue
        raw = read_bytes(uri)
        if raw is None:
            raise RuntimeError(f"Bronze object is missing: {uri}")
        rows = json.loads(raw.decode("utf-8"))
        if not isinstance(rows, list):
            raise RuntimeError(f"invalid major-account Bronze object: {uri}")
        for fs_type in sorted({
            str(row.get("fs_div") or "").strip()
            for row in rows if isinstance(row, dict)
        } & {"CFS", "OFS"}):
            scopes.add((
                match.group("ticker"),
                int(match.group("year")),
                match.group("report"),
                fs_type,
            ))
    return sorted(scopes)


def _request_scope(
    corp_code: str,
    year: int,
    report_code: str,
    fs_type: str,
    *,
    tries: int = 4,
) -> tuple[bytes, dict]:
    params = {
        "crtfc_key": _api_key(),
        "corp_code": corp_code,
        "bsns_year": str(year),
        "reprt_code": report_code,
        "fs_div": fs_type,
    }
    failure: tuple[str, int | None] | None = None
    for attempt in range(tries):
        try:
            response = requests.get(ENDPOINT, params=params, timeout=60)
            response.raise_for_status()
            payload = response.json()
            status = str(payload.get("status") or "?")
            if status == "020":
                raise financials.QuotaExceeded(
                    f"full-statement {year}:{report_code}:{fs_type}"
                )
            if status not in {"000", "013"}:
                raise DartRequestError(
                    "OpenDART full-statement response rejected: "
                    f"status={status}, year={year}, report={report_code}, "
                    f"fs_type={fs_type}"
                )
            return response.content, payload
        except financials.QuotaExceeded:
            raise
        except DartRequestError:
            raise
        except Exception as exc:  # noqa: BLE001
            raw_status = getattr(getattr(exc, "response", None), "status_code", None)
            failure = (
                type(exc).__name__, raw_status if isinstance(raw_status, int) else None,
            )
            if attempt + 1 < tries:
                time.sleep(2 * (attempt + 1))
    failure_name, status_code = failure or ("UnknownError", None)
    suffix = f", http_status={status_code}" if status_code is not None else ""
    raise DartRequestError(
        "OpenDART full-statement request failed: "
        f"failure={failure_name}{suffix}, year={year}, "
        f"report={report_code}, fs_type={fs_type}"
    ) from None


def _scope_root(
    base: str, ticker: str, year: int, report_code: str, fs_type: str,
) -> str:
    return (
        f"{base}/financials/dart_statement_lines/year={year}/corp={ticker}/"
        f"report={report_code}/fs_type={fs_type}"
    )


def _promote_legacy_response(
    base: str,
    ticker: str,
    year: int,
    report_code: str,
    fs_type: str,
) -> str | None:
    """Reuse a verified ``financials/dart_full`` body without another API call.

    The pre-existing collector stored the OpenDART ``list`` bytes rather than
    the surrounding response object.  The Silver parser deliberately accepts
    that legacy shape.  Copy the exact bytes into the new content-addressed
    namespace and publish its scope pointer only after the rows prove that
    they belong to the requested scope.
    """
    legacy_uri = (
        f"{base}/financials/dart_full/year={year}/corp={ticker}/"
        f"{report_code}-{fs_type}.json"
    )
    raw = read_bytes(legacy_uri)
    if raw is None:
        return None
    try:
        rows = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    if not isinstance(rows, list) or not rows:
        return None
    for row in rows:
        if not isinstance(row, dict):
            return None
        if str(row.get("fs_div") or "").strip() != fs_type:
            return None
        row_report = str(row.get("reprt_code") or report_code).strip()
        row_year = str(row.get("bsns_year") or year).strip()
        row_ticker = str(row.get("stock_code") or ticker).strip()
        if (
            row_report != report_code
            or row_year != str(year)
            or row_ticker != ticker
        ):
            return None

    digest = hashlib.sha256(raw).hexdigest()
    root = _scope_root(base, ticker, year, report_code, fs_type)
    response_uri = f"{root}/sha256={digest}/response.json"
    pointer_uri = f"{root}/latest.json"
    write_bytes(raw, response_uri)
    filing_ids = sorted({
        str(row.get("rcept_no") or "").strip()
        for row in rows
        if str(row.get("rcept_no") or "").strip()
    })
    pointer = {
        "schema_version": "dart-full-statement-pointer-v1",
        "ticker": ticker,
        "year": year,
        "report_code": report_code,
        "fs_type": fs_type,
        "status": "000",
        "filing_ids": filing_ids,
        "sha256": digest,
        "response_uri": response_uri,
        "source_format": "legacy-financials-dart-full-list-v1",
        "source_uri": legacy_uri,
        "promoted_at": datetime.now(timezone.utc).isoformat(),
    }
    write_text_if_changed(
        json.dumps(pointer, ensure_ascii=False, sort_keys=True), pointer_uri,
    )
    return response_uri


def _existing_response(pointer_uri: str) -> str | None:
    raw = read_bytes(pointer_uri)
    if raw is None:
        return None
    pointer = json.loads(raw.decode("utf-8"))
    response_uri = pointer.get("response_uri")
    if not isinstance(response_uri, str) or not response_uri:
        raise RuntimeError(f"invalid full-statement pointer: {pointer_uri}")
    if not exists(response_uri):
        raise RuntimeError(
            f"full-statement pointer target is missing: {response_uri}"
        )
    return response_uri


def _collect_scopes(
    base: str,
    scopes: list[tuple[str, int, str, str]],
    *,
    dest: str,
    refresh_existing: bool,
    changed_only: bool,
) -> list[str]:
    corp_by_stock: dict[str, str] | None = None
    print(
        f"[dart-full-statements] scopes={len(scopes)} "
        f"dest={dest} refresh={refresh_existing}",
        flush=True,
    )
    responses: list[str] = []
    fetched = skipped = reused_legacy = 0
    for index, (ticker, year, report_code, fs_type) in enumerate(scopes, 1):
        root = _scope_root(base, ticker, year, report_code, fs_type)
        pointer_uri = f"{root}/latest.json"
        previous = _existing_response(pointer_uri)
        if not refresh_existing:
            if previous is not None:
                if not changed_only:
                    responses.append(previous)
                skipped += 1
                continue
            promoted = _promote_legacy_response(
                base, ticker, year, report_code, fs_type,
            )
            if promoted is not None:
                responses.append(promoted)
                reused_legacy += 1
                continue
        if corp_by_stock is None:
            corp_by_stock = {
                stock_code: corp_code
                for corp_code, stock_code
                in financials.ensure_corp_code_xml(base)
            }
        corp_code = corp_by_stock.get(ticker)
        if corp_code is None:
            raise RuntimeError(f"DART corp code missing for ticker={ticker}")
        body, payload = _request_scope(corp_code, year, report_code, fs_type)
        digest = hashlib.sha256(body).hexdigest()
        response_uri = f"{root}/sha256={digest}/response.json"
        if previous == response_uri:
            if not changed_only:
                responses.append(previous)
            skipped += 1
            time.sleep(financials.CALL_GAP_SEC)
            continue
        write_bytes(body, response_uri)
        filing_ids = sorted({
            str(row.get("rcept_no") or "").strip()
            for row in payload.get("list", [])
            if str(row.get("rcept_no") or "").strip()
        })
        pointer = {
            "schema_version": "dart-full-statement-pointer-v1",
            "ticker": ticker,
            "year": year,
            "report_code": report_code,
            "fs_type": fs_type,
            "status": str(payload.get("status") or "?"),
            "filing_ids": filing_ids,
            "sha256": digest,
            "response_uri": response_uri,
            "retrieved_at": datetime.now(timezone.utc).isoformat(),
        }
        write_text_if_changed(
            json.dumps(pointer, ensure_ascii=False, sort_keys=True), pointer_uri,
        )
        responses.append(response_uri)
        fetched += 1
        if index % 100 == 0 or index == len(scopes):
            print(
                f"[dart-full-statements] {index}/{len(scopes)} "
                f"fetched={fetched} skipped={skipped} "
                f"reused_legacy={reused_legacy}",
                flush=True,
            )
        time.sleep(financials.CALL_GAP_SEC)
    return sorted(set(responses))


def run(
    from_year: int,
    to_year: int,
    dest: str,
    *,
    refresh_existing: bool = False,
    max_scopes: int | None = None,
) -> list[str]:
    """Collect all discovered scopes and return immutable response URIs."""
    if from_year < 2015 or to_year < from_year:
        raise ValueError("OpenDART full statements require 2015 <= from_year <= to_year")
    base = base_uri(dest)
    scopes = discover_scopes(base, from_year, to_year)
    if max_scopes is not None:
        if max_scopes < 1:
            raise ValueError("max_scopes must be positive")
        scopes = scopes[:max_scopes]
    return _collect_scopes(
        base,
        scopes,
        dest=dest,
        refresh_existing=refresh_existing,
        changed_only=False,
    )


def run_incremental(major_files: list[str], dest: str) -> list[str]:
    """Refresh full statements only for changed major-account scopes."""
    base = base_uri(dest)
    scopes = discover_scopes_from_files(major_files)
    return _collect_scopes(
        base,
        scopes,
        dest=dest,
        refresh_existing=True,
        changed_only=True,
    )


def run_incremental_day(day: str, dest: str) -> list[str]:
    """Refresh exact full-statement scopes disclosed on the target day."""
    datetime.strptime(day, "%Y%m%d")
    base = base_uri(dest)
    corps = financials.ensure_corp_code_xml(base)
    ticker_by_corp = dict(corps)
    major_files: set[str] = set()
    for disclosure_day in financials._incremental_disclosure_days(day):
        for row in financials._regular_disclosures(base, disclosure_day):
            corp_code = str(row.get("corp_code") or "").strip()
            ticker = ticker_by_corp.get(corp_code)
            scope = financials._regular_report_scopes(row.get("report_nm"))
            if ticker is None or scope is None:
                continue
            year, report_codes = scope
            for report_code in report_codes:
                uri = (
                    f"{base}/financials/dart/year={year}/corp={ticker}/"
                    f"{report_code}.json"
                )
                if exists(uri):
                    major_files.add(uri)
    return run_incremental(sorted(major_files), dest)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--from", dest="from_year", type=int, required=True)
    parser.add_argument("--to", dest="to_year", type=int, required=True)
    parser.add_argument("--dest", choices=["local", "s3"], default="local")
    parser.add_argument("--refresh-existing", action="store_true")
    parser.add_argument("--max-scopes", type=int)
    args = parser.parse_args()
    run(
        args.from_year,
        args.to_year,
        args.dest,
        refresh_existing=args.refresh_existing,
        max_scopes=args.max_scopes,
    )


if __name__ == "__main__":
    main()
