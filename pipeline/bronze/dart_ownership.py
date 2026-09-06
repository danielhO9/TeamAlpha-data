"""OpenDART 지분공시 전체 이력을 immutable Bronze로 수집한다.

회사별 ``elestock``(임원·주요주주)과 ``majorstock``(5% 대량보유) 응답은
내용 hash 경로에 보존한다. ``latest.json``은 mutable pointer일 뿐이며 실제 원문은
덮어쓰지 않는다. API key와 준비된 요청 URL은 어떤 산출물/오류에도 기록하지 않는다.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
from collections import defaultdict
from datetime import datetime, timezone

import requests

from pipeline.bronze import financials
from pipeline.common.paths import base_uri
from pipeline.common.sink import exists, read_bytes, write_bytes, write_text_if_changed

ENDPOINTS = {
    "EXECUTIVE_MAJOR_SHAREHOLDER": (
        "elestock",
        "https://opendart.fss.or.kr/api/elestock.json",
    ),
    "FIVE_PERCENT": (
        "majorstock",
        "https://opendart.fss.or.kr/api/majorstock.json",
    ),
}
DISCLOSURE_LIST_URL = "https://opendart.fss.or.kr/api/list.json"


class DartOwnershipRequestError(RuntimeError):
    """Secret-free OpenDART ownership request failure."""


def _api_key() -> str:
    value = os.environ.get("DART_API_KEY", "").strip()
    if not value:
        raise RuntimeError("DART_API_KEY is required")
    return value


def _request(
    endpoint_name: str,
    endpoint_url: str,
    corp_code: str,
    *,
    page_no: int = 1,
    tries: int = 4,
) -> tuple[bytes, dict]:
    params = {
        "crtfc_key": _api_key(),
        "corp_code": corp_code,
        "page_no": str(page_no),
        "page_count": "100",
    }
    failure: tuple[str, int | None] | None = None
    for attempt in range(tries):
        try:
            response = requests.get(endpoint_url, params=params, timeout=60)
            response.raise_for_status()
            payload = response.json()
            status = str(payload.get("status") or "?")
            if status == "020":
                raise financials.QuotaExceeded(f"ownership {endpoint_name}")
            if status not in {"000", "013"}:
                raise DartOwnershipRequestError(
                    "OpenDART ownership response rejected: "
                    f"endpoint={endpoint_name}, status={status}"
                )
            return response.content, payload
        except financials.QuotaExceeded:
            raise
        except DartOwnershipRequestError:
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
    raise DartOwnershipRequestError(
        "OpenDART ownership request failed: "
        f"endpoint={endpoint_name}, failure={failure_name}{suffix}"
    ) from None


def _request_all(
    endpoint_name: str,
    endpoint_url: str,
    corp_code: str,
) -> tuple[bytes, dict]:
    """Fetch every ownership page and persist one deterministic snapshot."""
    page_no = 1
    rows: list[dict] = []
    first: dict | None = None
    while True:
        _body, payload = _request(
            endpoint_name, endpoint_url, corp_code, page_no=page_no,
        )
        if first is None:
            first = dict(payload)
        rows.extend(row for row in payload.get("list") or [] if isinstance(row, dict))
        total_page = int(payload.get("total_page") or 0)
        if str(payload.get("status") or "?") == "013" or page_no >= total_page:
            break
        page_no += 1
        time.sleep(financials.CALL_GAP_SEC)
    combined = first or {"status": "013", "message": "no data"}
    combined["list"] = rows
    combined["page_no"] = 1
    combined["page_count"] = len(rows)
    combined["total_count"] = len(rows)
    combined["total_page"] = 1 if rows else 0
    body = json.dumps(
        combined, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
    ).encode("utf-8")
    return body, combined


def _existing_response(pointer_uri: str) -> str | None:
    raw = read_bytes(pointer_uri)
    if raw is None:
        return None
    pointer = json.loads(raw.decode("utf-8"))
    response_uri = pointer.get("response_uri")
    if not isinstance(response_uri, str) or not response_uri:
        raise RuntimeError(f"invalid ownership pointer: {pointer_uri}")
    if not exists(response_uri):
        raise RuntimeError(f"ownership pointer target is missing: {response_uri}")
    return response_uri


def _fetch_disclosure_page(day: str, page_no: int, *, tries: int = 4) -> dict:
    params = {
        "crtfc_key": _api_key(),
        "bgn_de": day,
        "end_de": day,
        "last_reprt_at": "N",
        "pblntf_ty": "D",
        "page_no": str(page_no),
        "page_count": "100",
    }
    for attempt in range(tries):
        try:
            response = requests.get(DISCLOSURE_LIST_URL, params=params, timeout=60)
            response.raise_for_status()
            payload = response.json()
            status = str(payload.get("status") or "?")
            if status == "020":
                raise financials.QuotaExceeded(f"ownership-disclosure-list {day}")
            if status in {"000", "013"}:
                return payload
            raise DartOwnershipRequestError(
                "OpenDART ownership disclosure list rejected: "
                f"status={status}, day={day}, page={page_no}"
            )
        except (financials.QuotaExceeded, DartOwnershipRequestError):
            raise
        except Exception:
            if attempt + 1 < tries:
                time.sleep(2 * (attempt + 1))
    raise DartOwnershipRequestError(
        "OpenDART ownership disclosure list failed after retries: "
        f"day={day}, page={page_no}"
    )


def _disclosure_path(base: str, day: str) -> str:
    rendered = datetime.strptime(day, "%Y%m%d").date().isoformat()
    return f"{base}/ownership/dart_disclosures/date={rendered}/disclosures.json"


def _disclosures(base: str, day: str) -> list[dict]:
    """Return a durable complete ownership-disclosure list for one day."""
    path = _disclosure_path(base, day)
    raw = read_bytes(path)
    if raw is not None:
        rows = json.loads(raw.decode("utf-8"))
        if not isinstance(rows, list):
            raise RuntimeError(f"invalid ownership disclosure checkpoint: {path}")
        return rows
    by_receipt: dict[str, dict] = {}
    page_no = 1
    while True:
        payload = _fetch_disclosure_page(day, page_no)
        for row in payload.get("list") or []:
            if not isinstance(row, dict):
                continue
            receipt = str(row.get("rcept_no") or "").strip()
            if receipt:
                by_receipt[receipt] = row
        total_page = int(payload.get("total_page") or 0)
        if str(payload.get("status") or "?") == "013" or page_no >= total_page:
            break
        page_no += 1
    rows = [by_receipt[key] for key in sorted(by_receipt)]
    write_text_if_changed(json.dumps(rows, ensure_ascii=False), path)
    return rows


def _types_for_title(value: object) -> tuple[str, ...]:
    compact = "".join(str(value or "").split())
    if "대량보유상황보고" in compact:
        return ("FIVE_PERCENT",)
    if "임원" in compact and "주요주주" in compact:
        return ("EXECUTIVE_MAJOR_SHAREHOLDER",)
    # DART category D also contains a few issuer ownership forms. Refreshing
    # both official snapshots is the fail-safe choice for an unknown title.
    return tuple(ENDPOINTS)


def run(
    dest: str,
    *,
    refresh_existing: bool = False,
    max_corps: int | None = None,
    disclosure_types: tuple[str, ...] = tuple(ENDPOINTS),
    corp_codes: set[str] | None = None,
    changed_only: bool = False,
) -> list[str]:
    """Collect both official ownership APIs for every listed DART corporation."""
    unknown = sorted(set(disclosure_types) - set(ENDPOINTS))
    if unknown:
        raise ValueError(f"unknown disclosure types: {unknown}")
    base = base_uri(dest)
    corps = financials.ensure_corp_code_xml(base)
    if corp_codes is not None:
        corps = [item for item in corps if item[0] in corp_codes]
    if max_corps is not None:
        if max_corps < 1:
            raise ValueError("max_corps must be positive")
        corps = corps[:max_corps]
    total = len(corps) * len(disclosure_types)
    print(
        f"[dart-ownership] corporations={len(corps)} "
        f"types={len(disclosure_types)} requests={total} dest={dest}",
        flush=True,
    )
    responses: list[str] = []
    fetched = skipped = 0
    index = 0
    for corp_code, ticker in corps:
        for disclosure_type in disclosure_types:
            index += 1
            endpoint_name, endpoint_url = ENDPOINTS[disclosure_type]
            root = (
                f"{base}/ownership/dart/disclosure_type={disclosure_type}/"
                f"corp={ticker}"
            )
            pointer_uri = f"{root}/latest.json"
            previous = _existing_response(pointer_uri)
            if not refresh_existing:
                if previous is not None:
                    if not changed_only:
                        responses.append(previous)
                    skipped += 1
                    continue
            body, payload = _request_all(endpoint_name, endpoint_url, corp_code)
            digest = hashlib.sha256(body).hexdigest()
            response_uri = f"{root}/sha256={digest}/response.json"
            if previous == response_uri:
                if not changed_only:
                    responses.append(previous)
                skipped += 1
                time.sleep(financials.CALL_GAP_SEC)
                continue
            write_bytes(body, response_uri)
            pointer = {
                "schema_version": "dart-ownership-pointer-v1",
                "ticker": ticker,
                "corp_code": corp_code,
                "disclosure_type": disclosure_type,
                "endpoint": endpoint_name,
                "status": str(payload.get("status") or "?"),
                "row_count": len(payload.get("list") or []),
                "sha256": digest,
                "response_uri": response_uri,
                "retrieved_at": datetime.now(timezone.utc).isoformat(),
            }
            write_text_if_changed(
                json.dumps(pointer, ensure_ascii=False, sort_keys=True),
                pointer_uri,
            )
            responses.append(response_uri)
            fetched += 1
            if index % 100 == 0 or index == total:
                print(
                    f"[dart-ownership] {index}/{total} "
                    f"fetched={fetched} skipped={skipped}",
                    flush=True,
                )
            time.sleep(financials.CALL_GAP_SEC)
    return sorted(set(responses))


def run_incremental(day: str, dest: str) -> list[str]:
    """Refresh only companies appearing in new category-D disclosures."""
    datetime.strptime(day, "%Y%m%d")
    base = base_uri(dest)
    requested: dict[str, set[str]] = defaultdict(set)
    for disclosure_day in financials._incremental_disclosure_days(day):
        for row in _disclosures(base, disclosure_day):
            corp_code = str(row.get("corp_code") or "").strip()
            if not corp_code:
                continue
            for disclosure_type in _types_for_title(row.get("report_nm")):
                requested[disclosure_type].add(corp_code)
    changed: list[str] = []
    for disclosure_type, affected in sorted(requested.items()):
        changed.extend(run(
            dest,
            refresh_existing=True,
            disclosure_types=(disclosure_type,),
            corp_codes=affected,
            changed_only=True,
        ))
    print(
        f"[dart-ownership-incremental] day={day} "
        f"affected={len(set().union(*requested.values())) if requested else 0} "
        f"changed={len(changed)}",
        flush=True,
    )
    return sorted(set(changed))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dest", choices=["local", "s3"], default="local")
    parser.add_argument("--refresh-existing", action="store_true")
    parser.add_argument("--max-corps", type=int)
    parser.add_argument(
        "--type",
        dest="types",
        action="append",
        choices=sorted(ENDPOINTS),
    )
    args = parser.parse_args()
    run(
        args.dest,
        refresh_existing=args.refresh_existing,
        max_corps=args.max_corps,
        disclosure_types=tuple(args.types or ENDPOINTS),
    )


if __name__ == "__main__":
    main()
