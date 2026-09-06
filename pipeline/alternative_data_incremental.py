"""Collect and publish only changed alternative research inputs for one day."""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone

import boto3

from pipeline.bronze import (
    dart_company_profiles,
    dart_full_statements,
    dart_ownership,
)
from pipeline.common.paths import base_uri
from pipeline.common.sink import read_bytes, write_text_if_changed
from pipeline.silver import alternative_data

STATE_KEY = "quality/checkpoints/alternative-inputs-v1.json"
DAY_ROOT = "quality/checkpoints/alternative-daily"


def _list_authorized_sources(bucket: str, prefix: str) -> list[str]:
    client = boto3.client("s3")
    uris: list[str] = []
    for page in client.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix=prefix,
    ):
        uris.extend(
            f"s3://{bucket}/{item['Key']}"
            for item in page.get("Contents", [])
            if "/source." in item["Key"]
            and not item["Key"].endswith("/manifest.json")
        )
    return sorted(set(uris))


def _state_uri() -> str:
    return f"{base_uri('s3')}/{STATE_KEY}"


def _read_state() -> dict:
    raw = read_bytes(_state_uri())
    if raw is None:
        return {"schema_version": "alternative-input-state-v1", "krx_files": []}
    state = json.loads(raw.decode("utf-8"))
    if state.get("schema_version") != "alternative-input-state-v1":
        raise RuntimeError("invalid alternative-input incremental state")
    if not isinstance(state.get("krx_files"), list):
        raise RuntimeError("invalid alternative-input KRX file state")
    return state


def mark_krx_published(files: list[str]) -> None:
    """Advance the KRX source checkpoint only after a certified DB publish."""
    state = _read_state()
    state["krx_files"] = sorted(set(state["krx_files"]) | set(files))
    state["updated_at"] = datetime.now(timezone.utc).isoformat()
    write_text_if_changed(
        json.dumps(state, ensure_ascii=False, sort_keys=True), _state_uri(),
    )


def _day_uri(day: str) -> str:
    rendered = datetime.strptime(day, "%Y%m%d").date().isoformat()
    return f"{base_uri('s3')}/{DAY_ROOT}/date={rendered}/complete.json"


def run(day: str, *, conn, industry_shards: int | None = None) -> dict:
    """Run a retry-safe alternative-data increment for one pipeline date."""
    datetime.strptime(day, "%Y%m%d")
    completion_uri = _day_uri(day)
    prior = read_bytes(completion_uri)
    if prior is not None:
        summary = json.loads(prior.decode("utf-8"))
        print(f"[alternative-incremental] already complete day={day}; skipped", flush=True)
        return summary

    bucket = os.environ["S3_BRONZE_BUCKET"]
    shards = industry_shards or int(
        os.environ.get("DART_INDUSTRY_SHARDS", "20")
    )
    full_files = dart_full_statements.run_incremental_day(day, "s3")
    ownership_files = dart_ownership.run_incremental(day, "s3")
    industry_files = dart_company_profiles.run_incremental(
        day, "s3", shard_count=shards,
    )

    state = _read_state()
    known_krx = set(state["krx_files"])
    all_investor = _list_authorized_sources(bucket, "investor_flows/krx/")
    all_short = _list_authorized_sources(bucket, "short_balances/krx/")
    investor_files = sorted(set(all_investor) - known_krx)
    short_files = sorted(set(all_short) - known_krx)

    requested = any((
        full_files, ownership_files, industry_files,
        investor_files, short_files,
    ))
    published = None
    if requested:
        published = alternative_data.publish_files(
            full_statement_files=full_files,
            ownership_files=ownership_files,
            investor_flow_files=investor_files,
            industry_files=industry_files,
            short_balance_files=short_files,
            conn=conn,
        )
        mark_krx_published(investor_files + short_files)

    summary = {
        "schema_version": "alternative-daily-complete-v1",
        "day": day,
        "changed_files": {
            "full_statements": len(full_files),
            "ownership": len(ownership_files),
            "industry": len(industry_files),
            "investor_flows": len(investor_files),
            "short_balances": len(short_files),
        },
        "published": published["published"] if published else {},
        "completed_at": datetime.now(timezone.utc).isoformat(),
    }
    write_text_if_changed(
        json.dumps(summary, ensure_ascii=False, sort_keys=True), completion_uri,
    )
    print(
        "[alternative-incremental] "
        + json.dumps(summary, ensure_ascii=False, sort_keys=True),
        flush=True,
    )
    return summary
