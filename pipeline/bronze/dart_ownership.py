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
    tries: int = 4,
) -> tuple[bytes, dict]:
    params = {
        "crtfc_key": _api_key(),
        "corp_code": corp_code,
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


def run(
    dest: str,
    *,
    refresh_existing: bool = False,
    max_corps: int | None = None,
    disclosure_types: tuple[str, ...] = tuple(ENDPOINTS),
) -> list[str]:
    """Collect both official ownership APIs for every listed DART corporation."""
    unknown = sorted(set(disclosure_types) - set(ENDPOINTS))
    if unknown:
        raise ValueError(f"unknown disclosure types: {unknown}")
    base = base_uri(dest)
    corps = financials.ensure_corp_code_xml(base)
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
            if not refresh_existing:
                existing = _existing_response(pointer_uri)
                if existing is not None:
                    responses.append(existing)
                    skipped += 1
                    continue
            body, payload = _request(endpoint_name, endpoint_url, corp_code)
            digest = hashlib.sha256(body).hexdigest()
            response_uri = f"{root}/sha256={digest}/response.json"
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
