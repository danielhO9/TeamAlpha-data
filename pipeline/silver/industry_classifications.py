"""OpenDART 기업개황 업종코드 observation을 PIT-safe Silver로 변환한다."""
from __future__ import annotations

import glob
import hashlib
import json
import re
from datetime import datetime
from pathlib import Path

import pandas as pd
from psycopg.types.json import Jsonb

from pipeline.common import db
from pipeline.common.sink import read_bytes

_PATH_RE = re.compile(
    r"company_profiles/dart/corp=(?P<ticker>[0-9A-Z]{6})/"
    r"sha256=(?P<sha>[0-9a-f]{64})/observed_at=[^/]+/response\.json$"
)


def _iter_files(base: str) -> list[str]:
    return sorted(glob.glob(
        f"{base}/company_profiles/dart/corp=*/sha256=*/observed_at=*/response.json"
    ))


def _manifest(uri: str) -> dict:
    manifest_uri = str(Path(uri).with_name("manifest.json"))
    if uri.startswith("s3://"):
        manifest_uri = uri.rsplit("/", 1)[0] + "/manifest.json"
    raw = read_bytes(manifest_uri)
    if raw is None:
        raise ValueError(f"DART company manifest missing: {manifest_uri}")
    manifest = json.loads(raw.decode("utf-8"))
    if manifest.get("schema_version") != "dart-company-profile-observation-v1":
        raise ValueError(f"DART company manifest contract invalid: {manifest_uri}")
    return manifest


def prepare(
    base: str | None = None,
    *,
    files: list[str] | None = None,
) -> tuple[pd.DataFrame, dict]:
    selected = sorted(set(files if files is not None else _iter_files(str(base))))
    records: list[dict] = []
    excluded_rows = rejected_rows = 0
    for uri in selected:
        match = _PATH_RE.search(uri.replace("\\", "/"))
        if match is None:
            raise ValueError(f"unrecognized DART company path: {uri}")
        raw = read_bytes(uri)
        if raw is None:
            raise RuntimeError(f"DART company Bronze object missing: {uri}")
        digest = hashlib.sha256(raw).hexdigest()
        manifest = _manifest(uri)
        if manifest.get("sha256") != digest or match.group("sha") != digest:
            raise ValueError(f"DART company response hash mismatch: {uri}")
        payload = json.loads(raw.decode("utf-8"))
        if str(payload.get("status") or "?") == "013":
            excluded_rows += 1
            continue
        corp_code = str(payload.get("corp_code") or "").strip()
        industry_code = str(payload.get("induty_code") or "").strip()
        if not corp_code or not industry_code:
            rejected_rows += 1
            continue
        observed_at = datetime.fromisoformat(str(manifest["observed_at"]))
        if observed_at.tzinfo is None:
            raise ValueError(f"DART company observed_at must have timezone: {uri}")
        records.append({
            "natural_key": corp_code,
            "source": "DART",
            "taxonomy": "DART_INDUTY_CODE",
            "industry_code": industry_code,
            "industry_name": None,
            "observed_at": observed_at,
            "available_at": observed_at,
            "effective_from": None,
            "effective_to": None,
            "observation_key": digest,
            "source_file_sha256": digest,
            "raw_row": payload,
            "source_file": uri,
        })
    frame = pd.DataFrame(records)
    if not frame.empty:
        keys = ["natural_key", "source", "taxonomy", "observation_key"]
        frame = frame.sort_values(keys).drop_duplicates(keys).reset_index(drop=True)
    return frame, {
        "file_count": len(selected),
        "input_rows": len(selected),
        "transformed_rows": len(frame),
        "excluded_rows": excluded_rows,
        "rejected_rows": rejected_rows,
    }


def publish(conn, frame: pd.DataFrame, asset_map: dict[str, int], run_id) -> int:
    if frame.empty:
        return 0
    columns = [
        "asset_id", "source", "taxonomy", "industry_code", "industry_name",
        "observed_at", "available_at", "effective_from", "effective_to",
        "observation_key", "source_file_sha256", "raw_row", "quality_run_id",
    ]
    rows = []
    for row in frame.itertuples(index=False):
        values = row._asdict()
        values["asset_id"] = asset_map[str(row.natural_key)]
        values["raw_row"] = Jsonb(row.raw_row)
        values["quality_run_id"] = run_id
        rows.append(tuple(values[column] for column in columns))
    conflict = ["asset_id", "source", "taxonomy", "observation_key"]
    # observed_at is the first immutable observation time and must never move.
    update = [
        "industry_code", "industry_name", "effective_from", "effective_to",
        "source_file_sha256", "raw_row", "quality_run_id",
    ]
    return db.upsert(
        conn,
        "industry_classification_observation",
        columns,
        rows,
        conflict,
        update,
        temp_name="_stg_industry_classifications",
    )
