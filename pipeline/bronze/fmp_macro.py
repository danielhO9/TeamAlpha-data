"""Coverage-audited Korean macro calendar -> Bronze only (never PIT-certified).

Keep byte-exact API receipts separately from the allowlisted projection. No
values, units, reference periods, release times or revisions are corrected here.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
from collections import Counter
from datetime import date, datetime, timedelta, timezone

from pipeline.bronze.fmp import (
    CALENDAR_ROW_LIMIT, FMPClient, FMPError, _month_windows, collect_raw,
    verify_raw_object,
)
from pipeline.common.paths import base_uri
from pipeline.common.sink import read_bytes, write_bytes

ENDPOINT = "economic-calendar"
CONTRACT_VERSION = "korea-coverage-v1"
AUDIT_DATE = "2026-09-19"
# Exact country + base event names only. No broad prefix matching or PMI aliases.
# Values are (country, provider event, last reference month checked in the audit).
SERIES = {
    "KR_POLICY_RATE": ("KR", "Interest Rate Decision", None),
    "KR_CPI_YOY": ("KR", "Inflation Rate YoY", "2026-08"),
    "KR_CPI_MOM": ("KR", "Inflation Rate MoM", "2026-08"),
    "KR_TRADE_BALANCE": ("KR", "Balance of Trade", "2026-08"),
    "KR_INDUSTRIAL_PRODUCTION_YOY": ("KR", "Industrial Production YoY", "2026-07"),
    "KR_INDUSTRIAL_PRODUCTION_MOM": ("KR", "Industrial Production MoM", "2026-07"),
    "KR_RETAIL_SALES_MOM": ("KR", "Retail Sales MoM", "2026-07"),
    "KR_UNEMPLOYMENT_RATE": ("KR", "Unemployment Rate", "2026-08"),
    "KR_CONSUMER_CONFIDENCE": ("KR", "Consumer Confidence", "2026-08"),
    "KR_BUSINESS_CONFIDENCE": ("KR", "Business Confidence", "2026-08"),
    "KR_PPI_YOY": ("KR", "Producer Price Index YoY", "2026-08"),
    "KR_PPI_MOM": ("KR", "Producer Price Index MoM", "2026-08"),
    "KR_CURRENT_ACCOUNT": ("KR", "Current Account", "2026-07"),
    "CN_NBS_MANUFACTURING_PMI": ("CN", "NBS Manufacturing PMI", "2026-08"),
}
MONTH_SUFFIX = re.compile(r" \((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\)$")


def _json_bytes(value: object) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False).encode("utf-8")

def _sha(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _contract() -> dict:
    return {
        "contract_version": CONTRACT_VERSION,
        "audit_date": AUDIT_DATE,
        "audit_report": "docs/fmp-korea-macro-candidates-20260919.md",
        "coverage_from": "2015-01",
        "scope": SERIES,
        "pit_approved": False,
        "publication_time_verified": False,
        "revision_history_verified": False,
        "silver_publish_allowed": False,
    }


def _rows(payload: bytes, start: date, end: date) -> list[dict]:
    try:
        rows = json.loads(payload)
    except (TypeError, ValueError) as exc:
        raise FMPError("macro response is not JSON") from exc
    if not isinstance(rows, list):
        raise FMPError("macro response must be an array")
    for row in rows:
        if not isinstance(row, dict):
            raise FMPError("macro row must be an object")
        if not all(isinstance(row.get(k), str) for k in ("date", "country", "event")):
            raise FMPError("macro date/country/event must be strings")
        try:
            # Provider promises UTC; retain its exact string, without certifying
            # that this is the actual historical publication time.
            stamp = datetime.strptime(row["date"], "%Y-%m-%d %H:%M:%S")
        except ValueError as exc:
            raise FMPError("invalid macro provider timestamp") from exc
        if not start <= stamp.date() <= end:
            raise FMPError("macro date outside requested UTC range")
    return rows


def select(rows: list[dict]) -> tuple[list[dict], dict]:
    lookup = {(country, name): series for series, (country, name, _) in SERIES.items()}
    selected, actual_counts, null_counts, identities = [], Counter(), Counter(), Counter()
    for i, row in enumerate(rows):
        name = MONTH_SUFFIX.sub("", row["event"])
        series = lookup.get((row["country"], name))
        if series is None:
            continue
        for field in ("actual", "previous", "estimate"):
            if field not in row:
                raise FMPError(f"macro selected row missing {field}")
            value = row[field]
            if value is not None and (
                isinstance(value, bool) or not isinstance(value, (int, float))
                or not math.isfinite(value)
            ):
                raise FMPError(f"macro selected row invalid {field}")
        # Keep nulls, duplicates, units and estimates exactly as supplied. This
        # is a source projection, not a cleaned or historically certified panel.
        selected.append({"series_id": series, "source_row_index": i, "payload": row})
        (null_counts if row["actual"] is None else actual_counts)[series] += 1
        identities[(series, row["date"])] += 1
    return selected, {
        "rows_by_series": dict(Counter(x["series_id"] for x in selected)),
        "nonnull_actual_by_series": dict(actual_counts),
        "null_actual_by_series": dict(null_counts),
        "duplicate_series_timestamp_rows": sum(n - 1 for n in identities.values()),
        "excluded_row_count": len(rows) - len(selected),
    }


def _write_selection(prefix: str, body: bytes, metadata: dict) -> str:
    """Payload first, manifest last; resume only the exact immutable projection."""
    uri, manifest_uri = prefix + "/selected.json", prefix + "/selection_manifest.json"
    manifest = {**metadata, "object_uri": uri, "content_length": len(body),
                "sha256": _sha(body), "complete": True}
    old = read_bytes(manifest_uri)
    if old is not None:
        if old != _json_bytes(manifest) or read_bytes(uri) != body:
            raise FMPError("macro selection changed/corrupt; use a new --snapshot")
        return manifest_uri
    write_bytes(body, uri)
    write_bytes(_json_bytes(manifest), manifest_uri)
    return manifest_uri


def _window(client: FMPClient, root: str, snapshot: str, start: date, end: date) -> list[dict]:
    prefix = (f"macro/fmp/{ENDPOINT}/{CONTRACT_VERSION}/snapshot={snapshot}/"
              f"from={start}/to={end}")
    full = root.rstrip("/") + "/" + prefix
    raw_uris = [full + "/raw/response.json", full + "/raw/manifest.json"]
    # Do not let an old complete but damaged receipt be silently overwritten.
    if read_bytes(raw_uris[1]) is not None and not verify_raw_object(*raw_uris):
        raise FMPError("macro raw receipt corrupt; use a new --snapshot")
    params = {"from": str(start), "to": str(end)}
    collect_raw(client, root=root, endpoint=ENDPOINT, params=params,
                prefix=prefix + "/raw", extension="json")
    if not verify_raw_object(*raw_uris):
        raise FMPError("macro raw checksum mismatch")
    receipt = json.loads(read_bytes(raw_uris[1]))
    if receipt.get("endpoint") != ENDPOINT or receipt.get("request_params") != params:
        raise FMPError("macro receipt request mismatch")
    received = datetime.fromisoformat(receipt["received_at"])
    if received.tzinfo is None:
        raise FMPError("macro receipt timestamp lacks timezone")
    rows = _rows(read_bytes(raw_uris[0]), start, end)
    if len(rows) >= CALENDAR_ROW_LIMIT:
        if start == end:
            raise FMPError("macro one-day response reached row cap")
        mid = start + timedelta(days=(end - start).days // 2)
        # Parent raw is retained as evidence, but never selected/counts twice.
        return (_window(client, root, snapshot, start, mid)
                + _window(client, root, snapshot, mid + timedelta(days=1), end))
    selected, stats = select(rows)
    if (end - start).days >= 27 and not selected:
        raise FMPError("macro monthly window has no allowlisted events")
    metadata = {
        **_contract(), "scope_sha256": _sha(_json_bytes(_contract())),
        "source_object_uri": raw_uris[0], "source_sha256": receipt["sha256"],
        "received_at": receipt["received_at"],
        "provider_timestamp_timezone": "UTC", "provider_row_count": len(rows),
        "selected_row_count": len(selected), "request_params": params, **stats,
    }
    manifest_uri = _write_selection(full, _json_bytes(selected), metadata)
    return [{"from": str(start), "to": str(end), "selection_manifest_uri": manifest_uri,
             "selected_row_count": len(selected), **stats}]


def run(start: date, end: date, *, dest: str = "local", snapshot: str = "backfill-v1",
        client: FMPClient | None = None) -> dict:
    if start > end or start < date(2014, 12, 1):
        raise ValueError("range must be ordered and start on/after 2014-12-01")
    if end > datetime.now(timezone.utc).date():
        raise ValueError("end cannot be a future UTC date")
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,79}", snapshot):
        raise ValueError("invalid snapshot ID")
    if dest not in ("local", "s3"):
        raise ValueError("dest must be local or s3")
    root = base_uri(dest)
    client = client or FMPClient(min_interval=0.4)
    parts, totals, actuals, nulls = [], Counter(), Counter(), Counter()
    for first, last in _month_windows(start, end):
        leaves = _window(client, root, snapshot, first, last)
        parts.extend(leaves)
        for leaf in leaves:
            totals.update(leaf["rows_by_series"])
            actuals.update(leaf["nonnull_actual_by_series"])
            nulls.update(leaf["null_actual_by_series"])
        print(f"[fmp-macro] Bronze checked {first}..{last} "
              f"selected={sum(x['selected_row_count'] for x in leaves)}", flush=True)
    # Detect a disappearing/renamed series, without assuming that every
    # calendar month contains a CPI release or a policy meeting.
    if (end - start).days >= 62:
        missing = sorted(set(SERIES) - actuals.keys())
        if missing:
            raise FMPError(f"macro series without actual in >=63-day range: {missing}")
    summary = {
        **_contract(), "snapshot": snapshot, "from": str(start), "to": str(end),
        "complete": True, "selected_row_count": sum(totals.values()),
        "rows_by_series": dict(totals), "nonnull_actual_by_series": dict(actuals),
        "null_actual_by_series": dict(nulls), "partitions": parts,
    }
    uri = (f"{root.rstrip('/')}/macro/fmp/{ENDPOINT}/{CONTRACT_VERSION}/snapshot={snapshot}/"
           f"runs/from={start}/to={end}/manifest.json")
    body, old = _json_bytes(summary), read_bytes(uri)
    if old is not None and old != body:
        raise FMPError("macro run manifest changed; use a new --snapshot")
    if old is None:
        write_bytes(body, uri)
    return {**summary, "manifest_uri": uri}


def run_daily(krx_day: str, *, dest: str = "s3") -> dict:
    # 08:30 KST execution is still on the previous UTC date. Do not reuse the
    # US-equity prior-weekday target for Korean releases. Reobserve 93 days to
    # retain provider updates without overwriting any earlier daily receipt.
    end = datetime.strptime(krx_day, "%Y%m%d").date() - timedelta(days=1)
    return run(max(date(2014, 12, 1), end - timedelta(days=92)), end,
               dest=dest, snapshot="daily-" + krx_day)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--start", type=date.fromisoformat, help="UTC range start; default 2015-01-01")
    mode.add_argument("--day", help="Daily collection for Korean processing date YYYYMMDD")
    parser.add_argument("--end", type=date.fromisoformat, help="UTC range end")
    parser.add_argument("--dest", choices=("local", "s3"), default="local")
    parser.add_argument("--snapshot", help="Immutable checkpoint ID; default backfill-v1")
    args = parser.parse_args()
    if args.day:
        if args.end or args.snapshot:
            parser.error("--day cannot be combined with --end or --snapshot")
        result = run_daily(args.day, dest=args.dest)
    else:
        if args.end is None:
            parser.error("--end is required without --day")
        result = run(args.start or date(2015, 1, 1), args.end, dest=args.dest,
                     snapshot=args.snapshot or "backfill-v1")
    print(json.dumps({k: result[k] for k in ("manifest_uri", "selected_row_count", "rows_by_series",
                                           "null_actual_by_series", "pit_approved")}, sort_keys=True))


if __name__ == "__main__":
    main()
