"""Bounded, resumable Bronze replay; never calls a data-provider API.

Input: explicit JSONL entries {path, sha256, dataset}; DART_EVENT also requires
disclosure_file containing the official list row (acceptance date/title).
Completed, certified checksum entries skip even the Bronze body read.
"""
from __future__ import annotations

import argparse
import hashlib
import io
import json
from datetime import datetime, time, timedelta, timezone
from pathlib import Path

import pandas as pd

from pipeline.common import db
from pipeline.common.sink import read_bytes
from pipeline.silver import corporate_actions, fmp, research_observations as research
from pipeline.silver_quality import migrate, repository
from pipeline.silver_quality.models import CheckResult, CheckStatus, Severity

VERSION = "research-expansion-v2"


def checkpoint_version(entry):
    # Official acceptance metadata is an input too, not just the event body.
    version = VERSION + ":" + entry["dataset"]
    if entry["dataset"] == "DART_EVENT":
        return version + ":" + entry["disclosure_sha256"]
    return version


def prepare(entry, *, body=None, disclosure_body=None):
    path, dataset = entry["path"], entry["dataset"]
    body = read_bytes(path) if body is None else body
    if body is None or hashlib.sha256(body).hexdigest() != entry["sha256"]:
        raise ValueError(f"Bronze checksum mismatch: {path}")
    records = []
    excluded = 0
    if dataset == "DART_EVENT":
        payload = json.loads(body)
        receipt = str(payload.get("rcept_no") or "")
        disclosure_body = read_bytes(entry["disclosure_file"]) if disclosure_body is None else disclosure_body
        if disclosure_body is None:
            raise ValueError("official disclosure list is missing")
        if hashlib.sha256(disclosure_body).hexdigest() != entry["disclosure_sha256"]:
            raise ValueError("official disclosure list checksum mismatch")
        disclosure_payload = json.loads(disclosure_body)
        rows = (
            [disclosure_payload] if isinstance(disclosure_payload, dict) and "rcept_no" in disclosure_payload
            else disclosure_payload.get("list", []) if isinstance(disclosure_payload, dict)
            else disclosure_payload
        )
        matches = [r for r in rows if str(r.get("rcept_no")) == receipt]
        if len(matches) != 1 or not matches[0].get("rcept_dt"):
            raise ValueError("exactly one official acceptance date is required")
        disclosure = matches[0]
        row = corporate_actions._structured_row(
            path, payload, disclosure.get("report_nm"),
            disclosure.get("corp_cls"), disclosure["rcept_dt"],
            source_body_sha256=entry["sha256"],
        )
        if row is None:
            raise ValueError("unsupported structured event path")
        records = research.dart_events(pd.DataFrame([row]))
        return records, 0
    if dataset not in {"FMP_STATEMENT", "FMP_PROFILE"}:
        raise ValueError(f"unsupported research dataset: {dataset}")
    # Decode the already verified bytes, without another S3 GET.
    stripped = body.lstrip()
    if stripped[:1] in (b"[", b"{"):
        raw = json.loads(body)
        if not isinstance(raw, list):
            raise ValueError("FMP response must be a list of records")
        # Match the daily FMP parser's null/numeric normalization so a replay
        # and a daily publish produce identical immutable observation keys.
        raw = pd.DataFrame(raw).to_dict("records")
    else:
        raw = pd.read_csv(io.BytesIO(body)).to_dict("records")
    observed = fmp._manifest_received_at(path)
    kind = fmp._financial_kind(path) if dataset == "FMP_STATEMENT" else None
    if dataset == "FMP_STATEMENT" and kind is None:
        raise ValueError("unrecognized FMP statement path")
    if dataset == "FMP_PROFILE" and not any(
        f"/stock/fmp/universe/{endpoint}/" in path.replace("\\", "/")
        for endpoint in ("profile-bulk", "company-screener", "stock-list")
    ):
        raise ValueError("unrecognized FMP profile path")
    if dataset == "FMP_PROFILE" and observed is None:
        raise ValueError("profile requires actual manifest received_at; no date inference")
    for row in raw:
        symbol = fmp._text(row.get("symbol"))
        if not symbol:
            excluded += 1
            continue
        metadata = {}
        available = observed
        if dataset == "FMP_STATEMENT":
            period_end = fmp._parse_date(row.get("date"))
            filed = fmp._parse_date(row.get("filingDate"))
            available = fmp._parse_timestamp(row.get("acceptedDate"))
            if available is None and filed is not None:
                available = datetime.combine(filed + timedelta(days=1), time(), timezone.utc)
            if (period_end is None or available is None or available.date() <= period_end
                    or str(row.get("period", "")).upper() not in {"FY", "Q1", "Q2", "Q3", "Q4"}):
                excluded += 1
                continue
            metadata = {"statement_type": fmp.STATEMENT_TYPES[kind],
                        "period_end": period_end.isoformat()}
        records.append(research.observation(
            identifier=symbol, source="FMP", dataset=dataset, raw_row=row,
            source_file=path, available_at=available,
            observed_at=observed if dataset == "FMP_PROFILE" else None,
            metadata=metadata,
        ))
    return records, excluded


def replay(conn, entry, *, recheck=False, prepared_inputs=None):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM research_input_checkpoint c JOIN dq_run q "
            "ON q.run_id=c.quality_run_id AND q.status='CERTIFIED' "
            "WHERE source_file=%s AND content_sha256=%s AND transform_version=%s",
            (entry["path"], entry["sha256"], checkpoint_version(entry)),
        )
        if cur.fetchone() and not recheck:
            conn.rollback()
            return {"path": entry["path"], "status": "SKIPPED"}
    records, excluded = prepare(entry, **(prepared_inputs or {}))
    source = "KRX" if entry["dataset"] == "DART_EVENT" else "FMP"
    with conn.cursor() as cur:
        cur.execute(
            "SELECT identifier,asset_id,valid_from,valid_to "
            "FROM asset_identifier WHERE source=%s AND identifier_type='ticker' "
            "AND identifier=ANY(%s)",
            (source, sorted({r["identifier"] for r in records})),
        )
        identities = cur.fetchall()
    episodes = {}
    for identifier, asset_id, start, end in identities:
        episodes.setdefault(str(identifier), []).append((asset_id, start, end))
    mapping, admitted = {}, []
    ambiguous = 0
    for row in records:
        candidates = episodes.get(row["identifier"], [])
        all_owners = {asset_id for asset_id, _, _ in candidates}
        # A unique owner also covers the issuer's pre-IPO financial statements.
        # Reused tickers require a unique episode at the fact's availability.
        day = row["available_at"].date()
        owners = all_owners if len(all_owners) == 1 else {
            asset_id for asset_id, start, end in candidates
            if (start is None or start <= day) and (end is None or end >= day)
        }
        if len(owners) != 1:
            ambiguous += bool(all_owners)
            continue
        key = row["identifier"] + "@" + day.isoformat()
        mapping[key] = next(iter(owners))
        admitted.append({**row, "identifier": key})
    excluded += len(records) - len(admitted)
    conn.commit()
    context = repository.start_run(conn, mode="research_backfill", input_fingerprint=entry["sha256"])
    try:
        with conn.transaction():
            count = research.publish(conn, admitted, mapping, context.run_id)
            checks = [CheckResult(
                rule_code="RESEARCH_EXCLUDED_ROWS", dataset=entry["dataset"],
                severity=Severity.WARNING,
                status=CheckStatus.FAIL if excluded else CheckStatus.PASS,
                expected="all rows have PIT-safe dates and an admitted unambiguous asset",
                actual=f"excluded_rows={excluded}, ambiguous_identity_rows={ambiguous}", failed_count=excluded,
            )]
            # Record exclusions too, or permanent out-of-universe source rows
            # would repeatedly consume the bounded batch and stall its resume.
            # --recheck explicitly revisits inputs after asset mapping repair.
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO research_input_checkpoint "
                    "(source_file,content_sha256,transform_version,quality_run_id,row_count,excluded_row_count) "
                    "VALUES (%s,%s,%s,%s,%s,%s) ON CONFLICT "
                    "(source_file,content_sha256,transform_version) DO UPDATE SET "
                    "quality_run_id=EXCLUDED.quality_run_id,row_count=EXCLUDED.row_count,"
                    "excluded_row_count=EXCLUDED.excluded_row_count",
                    (entry["path"], entry["sha256"], checkpoint_version(entry),
                     context.run_id, len(admitted), excluded),
                )
            repository.finish_run(conn, context, "CERTIFIED", checks, commit=False)
        return {"path": entry["path"], "status": "CERTIFIED", "inserted": count,
                "excluded": excluded, "ambiguous_identity_rows": ambiguous, "checkpointed": True}
    except Exception as exc:
        conn.rollback()
        repository.finish_run(conn, context, "FAILED", [], error_message=str(exc))
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--max-files", type=int, default=100)
    parser.add_argument("--recheck", action="store_true",
                        help="revisit selected certified files after mapping repair; no data deletion")
    args = parser.parse_args()
    if args.max_files < 1:
        parser.error("--max-files must be positive")
    with db.connect() as conn:
        migrate.assert_current(conn)
        processed = 0
        with Path(args.manifest).open() as handle:
            for line in handle:
                if not line.strip():
                    continue
                result = replay(conn, json.loads(line), recheck=args.recheck)
                print(json.dumps(result), flush=True)
                processed += result["status"] != "SKIPPED"
                if processed >= args.max_files:
                    break


if __name__ == "__main__":
    main()
