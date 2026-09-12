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
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable

import requests

from pipeline.bronze import financials
from pipeline.common.paths import base_uri
from pipeline.common.sink import exists, read_bytes, write_bytes, write_text_if_changed

ENDPOINT = "https://opendart.fss.or.kr/api/fnlttSinglAcntAll.json"
BOOTSTRAP_STATE_KEY = "quality/checkpoints/dart-full-bootstrap-v1.json"
BOOTSTRAP_STATE_SCHEMA = "dart-full-bootstrap-state-v1"
BOOTSTRAP_SCOPE_CACHE_KEY = "quality/checkpoints/dart-full-bootstrap-scopes-v1.json"
BOOTSTRAP_SCOPE_CACHE_SCHEMA = "dart-full-bootstrap-scopes-v1"
_MAJOR_KEY_RE = re.compile(
    r"financials/dart/year=(?P<year>\d{4})/corp=(?P<ticker>[0-9A-Z]{6})/"
    r"(?P<report>11011|11012|11013|11014)\.json$"
)


class DartRequestError(RuntimeError):
    """Secret-free OpenDART request failure."""


class _RequestPacer:
    """Keep request starts globally spaced while network waits overlap."""

    def __init__(self, gap_seconds: float):
        self.gap_seconds = max(0.0, gap_seconds)
        self._lock = threading.Lock()
        self._next_start = 0.0

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            delay = self._next_start - now
            if delay > 0:
                time.sleep(delay)
                now = time.monotonic()
            self._next_start = now + self.gap_seconds


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
    uris = _list_major_uris(base, from_year, to_year)
    s3 = None
    if base.startswith("s3://"):
        import boto3
        from botocore.config import Config

        s3 = boto3.client("s3", config=Config(max_pool_connections=32))

    def read_rows(uri: str) -> tuple[str, list]:
        if s3 is None:
            raw = read_bytes(uri)
        else:
            without = uri.removeprefix("s3://")
            bucket, _, key = without.partition("/")
            raw = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        if raw is None:
            raise RuntimeError(f"Bronze object disappeared while listing: {uri}")
        rows = json.loads(raw.decode("utf-8"))
        if not isinstance(rows, list):
            raise RuntimeError(f"invalid major-account Bronze object: {uri}")
        return uri, rows

    scopes: set[tuple[str, int, str, str]] = set()
    if s3 is None:
        loaded = map(read_rows, uris)
    else:
        executor = ThreadPoolExecutor(max_workers=32)
        loaded = executor.map(read_rows, uris)
    try:
        for uri, rows in loaded:
            match = _MAJOR_KEY_RE.search(uri.replace("\\", "/"))
            if match is None:
                continue
            fs_types = {
                str(row.get("fs_div") or "").strip()
                for row in rows
                if isinstance(row, dict)
                and str(row.get("fs_div") or "").strip() in {"CFS", "OFS"}
            }
            for fs_type in fs_types:
                scopes.add((
                    match.group("ticker"),
                    int(match.group("year")),
                    match.group("report"),
                    fs_type,
                ))
    finally:
        if s3 is not None:
            executor.shutdown()
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
    before_request: Callable[[], None] | None = None,
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
            if before_request is not None:
                before_request()
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


def _existing_response(
    pointer_uri: str,
    *,
    known_objects: set[str] | None = None,
) -> str | None:
    if known_objects is not None and pointer_uri not in known_objects:
        return None
    raw = read_bytes(pointer_uri)
    if raw is None:
        return None
    pointer = json.loads(raw.decode("utf-8"))
    response_uri = pointer.get("response_uri")
    if not isinstance(response_uri, str) or not response_uri:
        raise RuntimeError(f"invalid full-statement pointer: {pointer_uri}")
    target_exists = (
        response_uri in known_objects
        if known_objects is not None
        else exists(response_uri)
    )
    if not target_exists:
        raise RuntimeError(
            f"full-statement pointer target is missing: {response_uri}"
        )
    return response_uri


def _s3_inventory(base: str) -> set[str]:
    """List relevant initial-load objects once instead of issuing per-scope HEADs."""
    if not base.startswith("s3://"):
        return set()
    import boto3

    without = base.removeprefix("s3://")
    bucket, _, root_prefix = without.partition("/")
    root_prefix = root_prefix.rstrip("/")
    prefix_root = f"{root_prefix}/" if root_prefix else ""
    client = boto3.client("s3")
    uris: set[str] = set()
    for relative in (
        "financials/dart_statement_lines/",
        "financials/dart_full/",
    ):
        prefix = f"{prefix_root}{relative}"
        for page in client.get_paginator("list_objects_v2").paginate(
            Bucket=bucket, Prefix=prefix,
        ):
            uris.update(
                f"s3://{bucket}/{obj['Key']}"
                for obj in page.get("Contents", [])
                if not obj["Key"].endswith("/")
            )
    return uris


def _load_existing_responses(
    base: str,
    scopes: list[tuple[str, int, str, str]],
    known_objects: set[str],
) -> dict[str, str]:
    """Read existing scope pointers concurrently during a large initial run."""
    import boto3
    from botocore.config import Config

    pointers = [
        f"{_scope_root(base, ticker, year, report, fs_type)}/latest.json"
        for ticker, year, report, fs_type in scopes
    ]
    pointers = [uri for uri in pointers if uri in known_objects]
    if not pointers:
        return {}
    client = boto3.client("s3", config=Config(max_pool_connections=32))

    def load(pointer_uri: str) -> tuple[str, str]:
        without = pointer_uri.removeprefix("s3://")
        bucket, _, key = without.partition("/")
        raw = client.get_object(Bucket=bucket, Key=key)["Body"].read()
        pointer = json.loads(raw.decode("utf-8"))
        response_uri = pointer.get("response_uri")
        if not isinstance(response_uri, str) or response_uri not in known_objects:
            raise RuntimeError(f"invalid full-statement pointer: {pointer_uri}")
        return pointer_uri, response_uri

    with ThreadPoolExecutor(max_workers=32) as executor:
        return dict(executor.map(load, pointers))


def _collect_scopes(
    base: str,
    scopes: list[tuple[str, int, str, str]],
    *,
    dest: str,
    refresh_existing: bool,
    changed_only: bool,
    known_objects: set[str] | None = None,
) -> list[str]:
    if known_objects is None and len(scopes) > 100:
        known_objects = _s3_inventory(base)
    existing_responses = (
        _load_existing_responses(base, scopes, known_objects)
        if known_objects is not None
        else {}
    )
    print(
        f"[dart-full-statements] scopes={len(scopes)} "
        f"dest={dest} refresh={refresh_existing}",
        flush=True,
    )
    responses: list[str] = []
    fetched = skipped = reused_legacy = 0
    pending: list[tuple[int, tuple[str, int, str, str], str | None]] = []
    for index, (ticker, year, report_code, fs_type) in enumerate(scopes, 1):
        root = _scope_root(base, ticker, year, report_code, fs_type)
        pointer_uri = f"{root}/latest.json"
        previous = (
            existing_responses.get(pointer_uri)
            if known_objects is not None
            else _existing_response(pointer_uri)
        )
        if not refresh_existing:
            if previous is not None:
                if not changed_only:
                    responses.append(previous)
                skipped += 1
                continue
            legacy_uri = (
                f"{base}/financials/dart_full/year={year}/corp={ticker}/"
                f"{report_code}-{fs_type}.json"
            )
            promoted = None
            if known_objects is None or legacy_uri in known_objects:
                promoted = _promote_legacy_response(
                    base, ticker, year, report_code, fs_type,
                )
            if promoted is not None:
                responses.append(promoted)
                reused_legacy += 1
                continue
        pending.append((
            index, (ticker, year, report_code, fs_type), previous,
        ))

    if not pending:
        return sorted(set(responses))

    corp_by_stock = {
        stock_code: corp_code
        for corp_code, stock_code in financials.ensure_corp_code_xml(base)
    }
    workers = int(os.environ.get("DART_FULL_STATEMENT_WORKERS", "3"))
    if not 1 <= workers <= 8:
        raise ValueError("DART_FULL_STATEMENT_WORKERS must be between 1 and 8")
    pacer = _RequestPacer(financials.CALL_GAP_SEC)

    def fetch_one(item):
        index, scope, previous = item
        ticker, year, report_code, fs_type = scope
        root = _scope_root(base, ticker, year, report_code, fs_type)
        corp_code = corp_by_stock.get(ticker)
        if corp_code is None:
            raise RuntimeError(f"DART corp code missing for ticker={ticker}")
        body, payload = _request_scope(
            corp_code,
            year,
            report_code,
            fs_type,
            before_request=pacer.wait,
        )
        digest = hashlib.sha256(body).hexdigest()
        response_uri = f"{root}/sha256={digest}/response.json"
        if previous == response_uri:
            return index, previous if not changed_only else None, "skipped"
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
        return index, response_uri, "fetched"

    executor = ThreadPoolExecutor(max_workers=workers)
    futures = [executor.submit(fetch_one, item) for item in pending]
    completed = len(scopes) - len(pending)
    try:
        for future in as_completed(futures):
            _index, response_uri, outcome = future.result()
            completed += 1
            if response_uri is not None:
                responses.append(response_uri)
            if outcome == "fetched":
                fetched += 1
            else:
                skipped += 1
            if completed % 100 == 0 or completed == len(scopes):
                print(
                    f"[dart-full-statements] {completed}/{len(scopes)} "
                    f"fetched={fetched} skipped={skipped} "
                    f"reused_legacy={reused_legacy}",
                    flush=True,
                )
    except BaseException:
        for future in futures:
            future.cancel()
        executor.shutdown(wait=True, cancel_futures=True)
        raise
    else:
        executor.shutdown(wait=True)

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


def _bootstrap_state_uri(base: str) -> str:
    return f"{base}/{BOOTSTRAP_STATE_KEY}"


def _bootstrap_scope_cache_uri(base: str) -> str:
    return f"{base}/{BOOTSTRAP_SCOPE_CACHE_KEY}"


def _read_bootstrap_scope_cache(
    base: str, from_year: int, to_year: int,
) -> list[tuple[str, int, str, str]] | None:
    raw = read_bytes(_bootstrap_scope_cache_uri(base))
    if raw is None:
        return None
    cache = json.loads(raw.decode("utf-8"))
    if (
        cache.get("schema_version") != BOOTSTRAP_SCOPE_CACHE_SCHEMA
        or cache.get("from_year") != from_year
        or cache.get("to_year") != to_year
    ):
        return None
    generated_at = datetime.fromisoformat(str(cache.get("generated_at") or ""))
    if generated_at.tzinfo is None:
        raise RuntimeError("DART full-statement scope cache timestamp lacks timezone")
    ttl_hours = int(os.environ.get("DART_BOOTSTRAP_SCOPE_CACHE_HOURS", "168"))
    if ttl_hours < 1:
        raise ValueError("DART_BOOTSTRAP_SCOPE_CACHE_HOURS must be positive")
    if datetime.now(timezone.utc) - generated_at > timedelta(hours=ttl_hours):
        return None
    scopes = cache.get("scopes")
    if not isinstance(scopes, list) or any(
        not isinstance(scope, list) or len(scope) != 4 for scope in scopes
    ):
        raise RuntimeError("invalid DART full-statement scope cache")
    return [tuple(scope) for scope in scopes]


def _write_bootstrap_scope_cache(
    base: str,
    from_year: int,
    to_year: int,
    scopes: list[tuple[str, int, str, str]],
) -> None:
    _write_bootstrap_state(base, {
        "schema_version": BOOTSTRAP_SCOPE_CACHE_SCHEMA,
        "from_year": from_year,
        "to_year": to_year,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "scopes": [list(scope) for scope in scopes],
    }, uri=_bootstrap_scope_cache_uri(base))


def _read_bootstrap_state(base: str) -> dict | None:
    raw = read_bytes(_bootstrap_state_uri(base))
    if raw is None:
        return None
    state = json.loads(raw.decode("utf-8"))
    if state.get("schema_version") != BOOTSTRAP_STATE_SCHEMA:
        raise RuntimeError("invalid DART full-statement bootstrap state schema")
    if state.get("status") not in {"COLLECTING", "COLLECTED", "CERTIFIED"}:
        raise RuntimeError("invalid DART full-statement bootstrap state status")
    scopes = state.get("scopes")
    if not isinstance(scopes, list) or any(
        not isinstance(scope, list) or len(scope) != 4 for scope in scopes
    ):
        raise RuntimeError("invalid DART full-statement bootstrap scopes")
    return state


def _write_bootstrap_state(
    base: str, state: dict, *, uri: str | None = None,
) -> None:
    write_text_if_changed(
        json.dumps(state, ensure_ascii=False, sort_keys=True),
        uri or _bootstrap_state_uri(base),
    )


def mark_bootstrap_batch_certified(dest: str, files: list[str]) -> None:
    """Advance the initial-load checkpoint only after the DB commit succeeds."""
    base = base_uri(dest)
    state = _read_bootstrap_state(base)
    if state is None or state.get("status") != "COLLECTED":
        raise RuntimeError("no collected DART full-statement batch to certify")
    expected_count = len(state["scopes"])
    if len(set(files)) != expected_count:
        raise RuntimeError(
            "DART full-statement certification file count does not match "
            f"checkpoint: files={len(set(files))} scopes={expected_count}"
        )
    state["status"] = "CERTIFIED"
    state["certified_at"] = datetime.now(timezone.utc).isoformat()
    _write_bootstrap_state(base, state)


def run_bootstrap_batch(
    from_year: int,
    to_year: int,
    dest: str,
    *,
    max_scopes: int,
) -> tuple[list[str], int]:
    """Collect a recent-first bounded batch and return remaining scope count."""
    if max_scopes < 1:
        raise ValueError("max_scopes must be positive")
    if from_year < 2015 or to_year < from_year:
        raise ValueError("OpenDART full statements require 2015 <= from_year <= to_year")
    base = base_uri(dest)
    state = _read_bootstrap_state(base)
    if state is not None and state["status"] in {"COLLECTING", "COLLECTED"}:
        if state.get("from_year") != from_year or state.get("to_year") != to_year:
            raise RuntimeError(
                "unfinished DART full-statement batch has a different year range"
            )
        selected = [tuple(scope) for scope in state["scopes"]]
        remaining = int(state["remaining_scopes"])
        print(
            f"[dart-full-bootstrap] resuming status={state['status']} "
            f"selected={len(selected)} remaining={remaining}",
            flush=True,
        )
        known_objects = _s3_inventory(base)
    else:
        scopes = _read_bootstrap_scope_cache(base, from_year, to_year)
        cache_hit = scopes is not None
        if scopes is None:
            scopes = sorted(
                discover_scopes(base, from_year, to_year),
                key=lambda value: (-value[1], value[0], value[2], value[3]),
            )
            _write_bootstrap_scope_cache(base, from_year, to_year, scopes)
            print(
                f"[dart-full-bootstrap] scope cache refreshed scopes={len(scopes)}",
                flush=True,
            )
        else:
            print(
                f"[dart-full-bootstrap] scope cache hit scopes={len(scopes)}",
                flush=True,
            )
        known_objects = _s3_inventory(base)
        pending = [
            scope for scope in scopes
            if f"{_scope_root(base, *scope)}/latest.json" not in known_objects
        ]
        if not pending and cache_hit:
            scopes = sorted(
                discover_scopes(base, from_year, to_year),
                key=lambda value: (-value[1], value[0], value[2], value[3]),
            )
            _write_bootstrap_scope_cache(base, from_year, to_year, scopes)
            pending = [
                scope for scope in scopes
                if f"{_scope_root(base, *scope)}/latest.json" not in known_objects
            ]
            print(
                "[dart-full-bootstrap] final scope cache revalidation "
                f"scopes={len(scopes)} pending={len(pending)}",
                flush=True,
            )
        selected = pending[:max_scopes]
        remaining = max(0, len(pending) - len(selected))
        print(
            f"[dart-full-bootstrap] total={len(scopes)} pending={len(pending)} "
            f"selected={len(selected)}",
            flush=True,
        )
        if selected:
            canonical_scopes = [list(scope) for scope in selected]
            batch_id = hashlib.sha256(json.dumps(
                canonical_scopes, separators=(",", ":"),
            ).encode("utf-8")).hexdigest()
            state = {
                "schema_version": BOOTSTRAP_STATE_SCHEMA,
                "status": "COLLECTING",
                "batch_id": batch_id,
                "from_year": from_year,
                "to_year": to_year,
                "scopes": canonical_scopes,
                "remaining_scopes": remaining,
                "started_at": datetime.now(timezone.utc).isoformat(),
            }
            _write_bootstrap_state(base, state)
    if not selected:
        return [], remaining
    responses = _collect_scopes(
        base,
        selected,
        dest=dest,
        refresh_existing=False,
        changed_only=False,
        known_objects=known_objects,
    )
    if len(responses) != len(selected):
        raise RuntimeError(
            "DART full-statement collection did not produce one response per "
            f"scope: responses={len(responses)} scopes={len(selected)}"
        )
    state = _read_bootstrap_state(base)
    if state is None:
        raise RuntimeError("DART full-statement bootstrap state disappeared")
    state["status"] = "COLLECTED"
    state["collected_at"] = datetime.now(timezone.utc).isoformat()
    _write_bootstrap_state(base, state)
    return responses, remaining


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
