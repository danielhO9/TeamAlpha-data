"""Lossless research facts, independent of price/return writers and provider APIs.

Caller owns the quality-run transaction. Immutable keys include payload and
availability, so corrections and profile vintages never overwrite old facts.
"""
from __future__ import annotations

import hashlib
import json
import math
from datetime import datetime, time, timedelta, timezone

import pandas as pd
from psycopg.types.json import Jsonb

from pipeline.common import db


def clean(value):
    if isinstance(value, dict):
        return {str(k): clean(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [clean(v) for v in value]
    if value is None or value is pd.NA or value is pd.NaT:
        return None
    if hasattr(value, "item"):
        value = value.item()
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def observation(*, identifier, source, dataset, raw_row, source_file,
                available_at, observed_at=None, metadata=None):
    if available_at is None or available_at.tzinfo is None:
        raise ValueError("research observation requires timezone-aware availability")
    if observed_at is not None and observed_at.tzinfo is None:
        raise ValueError("research observation requires timezone-aware observation time")
    if dataset == "FMP_PROFILE" and observed_at != available_at:
        raise ValueError("profile availability must equal its actual observation time")
    raw_row, metadata = clean(raw_row), clean(metadata or {})
    identity = json.dumps(
        [source, dataset, str(identifier), raw_row, metadata,
         available_at.astimezone(timezone.utc).isoformat()],
        ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False,
    )
    return dict(identifier=str(identifier), source=source, dataset=dataset,
                observation_key=hashlib.sha256(identity.encode()).hexdigest(),
                available_at=available_at, observed_at=observed_at,
                source_file=str(source_file), raw_row=raw_row, metadata=metadata)


def dart_events(candidates):
    records = []
    for row in candidates.to_dict("records"):
        raw = row.get("research_raw_row")
        if row.get("source") != "DART_STRUCTURED" or not isinstance(raw, dict):
            continue
        # Inherited preferred-share price evidence is not another issuer filing.
        if row.get("issuer_event_inherited") is True:
            continue
        announced = row.get("announcement_date")
        if announced is None or pd.isna(announced):
            raise ValueError("structured research event has no acceptance date")
        records.append(observation(
            identifier=row["identifier"], source="DART", dataset="DART_EVENT",
            raw_row=raw, source_file=row["source_file"],
            available_at=datetime.combine(announced + timedelta(days=1), time(), timezone.utc),
            metadata={"event_type": row["event_type"],
                      "action_scope": row.get("action_scope"),
                      "report_name": row.get("report_name"),
                      "source_body_sha256": row.get("source_body_sha256")},
        ))
    return records


def publish(conn, records, identifier_map, run_id):
    """Check only supplied keys; COPY only new observations. Never scan history."""
    if not records:
        return 0
    columns = ["asset_id", "source", "dataset", "observation_key", "available_at",
               "observed_at", "source_file", "raw_row", "metadata", "quality_run_id"]
    unique = {}
    for record in records:
        row = dict(record)
        row["asset_id"] = identifier_map[str(row["identifier"])]
        row["quality_run_id"] = run_id
        key = (row["asset_id"], row["source"], row["dataset"], row["observation_key"])
        unique[key] = row
    # Index lookups scoped to supplied asset/dataset keys, not a whole-table scan.
    keys = list(unique)
    with conn.cursor() as cur:
        for offset in range(0, len(keys), 2000):
            batch = keys[offset:offset + 2000]
            cur.execute(
                "SELECT r.asset_id,r.source,r.dataset,r.observation_key "
                "FROM research_observation r JOIN "
                "unnest(%s::bigint[],%s::text[],%s::text[],%s::text[]) "
                "AS k(asset_id,source,dataset,observation_key) "
                "USING(asset_id,source,dataset,observation_key)",
                tuple([key[i] for key in batch] for i in range(4)),
            )
            for key in cur.fetchall():
                unique.pop(tuple(key), None)
    rows = []
    for row in unique.values():
        row["raw_row"], row["metadata"] = Jsonb(row["raw_row"]), Jsonb(row["metadata"])
        rows.append(tuple(row[c] for c in columns))
    return db.upsert(conn, "research_observation", columns, rows,
                     ["asset_id", "source", "dataset", "observation_key"], [],
                     temp_name="_stg_research_observation")
