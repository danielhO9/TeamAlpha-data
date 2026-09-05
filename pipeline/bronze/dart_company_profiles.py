"""OpenDART 기업개황의 현재 업종코드를 immutable Bronze로 관측한다.

기업개황은 업종의 과거 효력일을 제공하지 않는다. 따라서 response bytes와 최초
관측시각을 content-addressed manifest에 함께 고정한다. Silver는 그 관측시각보다
앞선 연구 시점에 이 업종을 소급 적용하지 않는다.
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

ENDPOINT = "https://opendart.fss.or.kr/api/company.json"


class DartCompanyRequestError(RuntimeError):
    """Secret-free OpenDART company-profile failure."""


def _api_key() -> str:
    value = os.environ.get("DART_API_KEY", "").strip()
    if not value:
        raise RuntimeError("DART_API_KEY is required")
    return value


def _request(corp_code: str, *, tries: int = 4) -> tuple[bytes, dict]:
    params = {"crtfc_key": _api_key(), "corp_code": corp_code}
    failure: tuple[str, int | None] | None = None
    for attempt in range(tries):
        try:
            response = requests.get(ENDPOINT, params=params, timeout=60)
            response.raise_for_status()
            payload = response.json()
            status = str(payload.get("status") or "?")
            if status == "020":
                raise financials.QuotaExceeded("company-profile")
            if status not in {"000", "013"}:
                raise DartCompanyRequestError(
                    "OpenDART company response rejected: "
                    f"status={status}, corp_code={corp_code}"
                )
            return response.content, payload
        except financials.QuotaExceeded:
            raise
        except DartCompanyRequestError:
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
    raise DartCompanyRequestError(
        "OpenDART company request failed: "
        f"failure={failure_name}{suffix}, corp_code={corp_code}"
    ) from None


def _existing_pointer(pointer_uri: str) -> dict | None:
    raw = read_bytes(pointer_uri)
    if raw is None:
        return None
    pointer = json.loads(raw.decode("utf-8"))
    response_uri = pointer.get("response_uri")
    manifest_uri = pointer.get("manifest_uri")
    if not isinstance(response_uri, str) or not isinstance(manifest_uri, str):
        raise RuntimeError(f"invalid DART company pointer: {pointer_uri}")
    if not exists(response_uri) or not exists(manifest_uri):
        raise RuntimeError(f"DART company pointer target is missing: {pointer_uri}")
    return pointer


def run(
    dest: str,
    *,
    refresh_existing: bool = False,
    max_corps: int | None = None,
) -> list[str]:
    base = base_uri(dest)
    corps = financials.ensure_corp_code_xml(base)
    if max_corps is not None:
        if max_corps < 1:
            raise ValueError("max_corps must be positive")
        corps = corps[:max_corps]
    responses: list[str] = []
    fetched = skipped = 0
    for index, (corp_code, ticker) in enumerate(corps, 1):
        root = f"{base}/company_profiles/dart/corp={ticker}"
        pointer_uri = f"{root}/latest.json"
        previous_pointer = _existing_pointer(pointer_uri)
        if not refresh_existing:
            if previous_pointer is not None:
                responses.append(str(previous_pointer["response_uri"]))
                skipped += 1
                continue
        body, payload = _request(corp_code)
        observed_at = datetime.now(timezone.utc).isoformat()
        digest = hashlib.sha256(body).hexdigest()
        if previous_pointer is not None and previous_pointer.get("sha256") == digest:
            responses.append(str(previous_pointer["response_uri"]))
            skipped += 1
            continue
        observed_partition = observed_at.replace(":", "").replace("+", "p")
        immutable_root = (
            f"{root}/sha256={digest}/observed_at={observed_partition}"
        )
        response_uri = f"{immutable_root}/response.json"
        manifest_uri = f"{immutable_root}/manifest.json"
        write_bytes(body, response_uri)
        immutable_manifest = {
            "schema_version": "dart-company-profile-observation-v1",
            "corp_code": corp_code,
            "ticker": ticker,
            "status": str(payload.get("status") or "?"),
            "industry_code": str(payload.get("induty_code") or "").strip() or None,
            "sha256": digest,
            "response_uri": response_uri,
            "observed_at": observed_at,
        }
        existing_manifest = read_bytes(manifest_uri)
        if existing_manifest is None:
            write_text_if_changed(
                json.dumps(immutable_manifest, ensure_ascii=False, sort_keys=True),
                manifest_uri,
            )
        else:
            previous = json.loads(existing_manifest.decode("utf-8"))
            if previous.get("sha256") != digest:
                raise RuntimeError(f"DART company immutable manifest mismatch: {manifest_uri}")
            immutable_manifest = previous
        pointer = {
            **immutable_manifest,
            "schema_version": "dart-company-profile-pointer-v1",
            "manifest_uri": manifest_uri,
        }
        write_text_if_changed(
            json.dumps(pointer, ensure_ascii=False, sort_keys=True), pointer_uri,
        )
        responses.append(response_uri)
        fetched += 1
        if index % 100 == 0 or index == len(corps):
            print(
                f"[dart-company-profiles] {index}/{len(corps)} "
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
    args = parser.parse_args()
    run(
        args.dest,
        refresh_existing=args.refresh_existing,
        max_corps=args.max_corps,
    )


if __name__ == "__main__":
    main()
