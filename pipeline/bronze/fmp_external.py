"""Korea-relevant FX/COT/ETF/index receipts. Bronze only; no historical PIT approval."""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import math
import re

from pipeline.bronze.fmp import FMPClient, FMPError, collect_raw
from pipeline.common.paths import base_uri
from pipeline.common.sink import read_bytes, write_bytes

VERSION = "korea-external-v1"
PRICE_ENDPOINT = "historical-price-eod/full"
COT_ENDPOINT = "commitment-of-traders-report"
SERIES = {**{s: {"kind": "fx", "endpoint": PRICE_ENDPOINT}
             for s in ("USDCNH", "USDCNY", "USDJPY", "AUDUSD", "EURUSD")},
          **{s: {"kind": "cot", "endpoint": COT_ENDPOINT, "cftc_code": code}
             for s, code in (("HG", "085692"), ("CL", "067651"), ("DX", "098662"),
                             ("J6", "097741"), ("GC", "088691"), ("VX", "1170E1"))},
          **{s: {"kind": "etf", "endpoint": PRICE_ENDPOINT} for s in ("EWY", "EEM", "FXI")}}
RISK_VERSION = "korea-risk-v1"
RISK_SERIES = {
    "EWT": {"kind": "etf", "endpoint": PRICE_ENDPOINT},
    "USDTWD": {"kind": "fx", "endpoint": PRICE_ENDPOINT},
    **{s: {"kind": "index", "endpoint": PRICE_ENDPOINT}
       for s in ("^VVIX", "^VIX3M", "^VIX9D")},
    **{s: {"kind": "cot", "endpoint": COT_ENDPOINT, "cftc_code": code}
       for s, code in (("ZT", "042601"), ("ZN", "043602"),
                       ("ZB", "020601"), ("ZQ", "045601"))},
}
BUNDLES = {"core": (VERSION, SERIES), "risk": (RISK_VERSION, RISK_SERIES)}
COT_FIELDS = ("openInterestAll", "noncommPositionsLongAll", "noncommPositionsShortAll",
              "commPositionsLongAll", "commPositionsShortAll")
PIT = {"pit_approved": False, "publication_time_verified": False,
       "revision_history_verified": False, "silver_publish_allowed": False,
       "historical_backtest_allowed": False}


def encoded(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False).encode()


def sha(body):
    return hashlib.sha256(body).hexdigest()


def immutable(body, uri):
    old = read_bytes(uri)
    if old is not None and old != body:
        raise FMPError("immutable external receipt differs/corrupt; use a new snapshot")
    if old is None:
        write_bytes(body, uri)


def scope(bundle):
    if bundle not in BUNDLES:
        raise ValueError("unknown external bundle")
    return BUNDLES[bundle]


def contract(bundle="core"):
    version, series = scope(bundle)
    return {"contract_version": version, "scope": series, **PIT,
            "historical_availability_policy": "unknown_never_inferred_from_observation_date",
            "system_observation_policy": "received_at_is_actual_API_receipt_not_release_time"}


def year_windows(start, end):
    while start <= end:
        last = min(date(start.year, 12, 31), end)
        yield start, last
        start = last + timedelta(days=1)


def receipt(root, relative, endpoint, params):
    """Verify an existing receipt completely before reuse or cache promotion."""
    prefix = root.rstrip("/") + "/" + relative
    body, meta_body = read_bytes(prefix + "/response.json"), read_bytes(prefix + "/manifest.json")
    if meta_body is None:
        return None
    try:
        meta = json.loads(meta_body)
        valid = (body is not None and meta["complete"] is True
                 and meta["object_uri"] == prefix + "/response.json"
                 and meta["provider"] == "FMP" and meta["endpoint"] == endpoint
                 and meta["request_params"] == params and meta["status_code"] == 200
                 and meta["content_length"] == len(body) and meta["sha256"] == sha(body))
        received = datetime.fromisoformat(meta["received_at"])
        if not valid or received.tzinfo is None or received > datetime.now(timezone.utc):
            raise ValueError("invalid receipt metadata")
    except (KeyError, TypeError, ValueError) as exc:
        raise FMPError("external raw receipt corrupt or request mismatch") from exc
    return body, meta, meta_body


def collect(client, root, relative, endpoint, params, cache_root):
    existing = receipt(root, relative, endpoint, params)
    if existing is not None:
        return existing
    cached = receipt(cache_root, relative, endpoint, params) if cache_root else None
    if cached:
        body, source, source_manifest = cached
        prefix = root.rstrip("/") + "/" + relative
        metadata = {**source, "object_uri": prefix + "/response.json",
                    "copied_from": {"object_uri": source["object_uri"],
                                    "manifest_sha256": sha(source_manifest)}}
        # Keep the actual API receipt timestamp, not the later upload timestamp.
        immutable(body, prefix + "/response.json")
        immutable(encoded(metadata), prefix + "/manifest.json")
    else:
        # An interrupted payload without a manifest has no authentic timestamp.
        # Do not overwrite it or silently manufacture a receipt on retry.
        if read_bytes(root.rstrip("/") + "/" + relative + "/response.json") is not None:
            raise FMPError("orphan external raw payload; investigate or use a new snapshot")
        collect_raw(client, root=root, endpoint=endpoint, params=params,
                    prefix=relative, extension="json")
    checked = receipt(root, relative, endpoint, params)
    if checked is None:
        raise FMPError("external raw receipt missing after collection")
    return checked


def project(body, symbol, start, end, received_at, *, bundle="core"):
    spec = scope(bundle)[1][symbol]
    try:
        rows = json.loads(body)
        if not isinstance(rows, list):
            raise ValueError("expected array")
        wrapped, dates, issues = [], [], Counter()
        fields = COT_FIELDS if spec["kind"] == "cot" else ("open", "high", "low", "close")
        for index, row in enumerate(rows):
            if not isinstance(row, dict) or row.get("symbol") != symbol:
                raise ValueError("symbol mismatch")
            fmt = "%Y-%m-%d %H:%M:%S" if spec["kind"] == "cot" else "%Y-%m-%d"
            stamp = datetime.strptime(row["date"], fmt)
            day = stamp.date()
            if not start <= day <= end:
                raise ValueError("date out of range")
            if spec["kind"] == "cot" and row.get("cftcContractMarketCode") != spec["cftc_code"]:
                raise ValueError("CFTC contract mismatch")
            flags = []
            for field in fields:
                value = row[field]
                if value is None:
                    flags.append("null_" + field)
                elif isinstance(value, bool) or not isinstance(value, (float, int)) or not math.isfinite(value):
                    raise ValueError("invalid numeric field")
            if day.weekday() >= 5:
                flags.append("provider_weekend_date")
            if spec["kind"] != "cot" and all(row[k] is not None for k in fields):
                if min(row[k] for k in fields) <= 0:
                    flags.append("nonpositive_price")
                if row["high"] < max(row[k] for k in ("open", "close", "low")) or row["low"] > min(row[k] for k in ("open", "close", "high")):
                    flags.append("invalid_ohlc")
            issues.update(flags)
            dates.append(day.isoformat())
            wrapped.append({
                "series_id": symbol, "source_row_index": index, "payload": row,
                "observation_date": day.isoformat(), "provider_date": row["date"],
                "provider_date_kind": "position_as_of" if spec["kind"] == "cot" else "provider_eod_date",
                "reference_date": day.isoformat() if spec["kind"] == "cot" else None,
                "released_at": None, "available_at": None, "vintage": None,
                "observed_at": received_at, "system_known_at": received_at,
                "pit_status": "HISTORICAL_AVAILABILITY_UNVERIFIED", **PIT,
                "payload_sha256": sha(encoded(row)), "quality_flags": flags,
            })
        if len(dates) != len(set(dates)):
            raise ValueError("duplicate provider date")
        # A fully requested calendar month must not silently disappear.
        current = date(start.year, start.month, 1)
        months = {d[:7] for d in dates}
        while current <= end:
            nxt = date(current.year + (current.month == 12), current.month % 12 + 1, 1)
            if current >= start and nxt - timedelta(days=1) <= end and current.strftime("%Y-%m") not in months:
                raise ValueError("empty complete calendar month")
            current = nxt
    except (KeyError, TypeError, ValueError) as exc:
        raise FMPError(f"invalid external data for {symbol}: {exc}") from exc
    return wrapped, {"row_count": len(rows), "first_date": min(dates, default=None),
                     "latest_date": max(dates, default=None), "quality_flag_counts": dict(issues)}


def partition(client, root, symbol, start, end, snapshot, cache_root, *, bundle="core"):
    version, series = scope(bundle)
    relative = f"regime/fmp-external/{version}/snapshot={snapshot}/series={symbol}/from={start}/to={end}"
    params = {"symbol": symbol, "from": str(start), "to": str(end)}
    body, raw, _ = collect(client, root, relative + "/raw", series[symbol]["endpoint"], params, cache_root)
    wrapped, stats = project(body, symbol, start, end, raw["received_at"], bundle=bundle)
    prefix = root.rstrip("/") + "/" + relative
    projection = encoded(wrapped)
    metadata = {"contract": contract(bundle), "series_id": symbol, "request_params": params,
                "received_at": raw["received_at"], "source_object_uri": raw["object_uri"],
                "source_sha256": raw["sha256"], "object_uri": prefix + "/observations.json",
                "sha256": sha(projection), "content_length": len(projection),
                "complete": True, **PIT, **stats}
    immutable(projection, metadata["object_uri"])
    uri = prefix + "/manifest.json"
    immutable(encoded(metadata), uri)
    return {"symbol": symbol, "from": str(start), "to": str(end),
            "manifest_uri": uri, **stats}


def run(start, end, *, dest="local", snapshot="backfill-v1", client=None, cache_root=None,
        bundle="core"):
    version, series = scope(bundle)
    if start > end or start < date(2015, 1, 1) or end > datetime.now(timezone.utc).date():
        raise ValueError("range must be ordered, start >= 2015-01-01, and not future UTC")
    if dest not in ("local", "s3") or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,79}", snapshot):
        raise ValueError("invalid destination or snapshot")
    root = base_uri(dest)
    client = client or FMPClient(min_interval=0.5, timeout=(10, 45))
    parts, counts, issues = [], Counter(), Counter()
    # Each bundle has an explicit priority order. Annual windows remain small enough
    # that a successful response cannot hide a multi-year endpoint row cap.
    for symbol in series:
        for first, last in year_windows(start, end):
            part = partition(client, root, symbol, first, last, snapshot, cache_root, bundle=bundle)
            parts.append(part)
            counts[symbol] += part["row_count"]
            issues.update(part["quality_flag_counts"])
        print(f"[fmp-external] {symbol} rows={counts[symbol]} Bronze only", flush=True)
    result = {"contract": contract(bundle), "snapshot": snapshot, "from": str(start), "to": str(end),
              "complete": True, **PIT, "row_count": sum(counts.values()),
              "rows_by_series": dict(counts), "quality_flag_counts": dict(issues), "partitions": parts}
    uri = (root.rstrip("/") + f"/regime/fmp-external/{version}/snapshot={snapshot}"
           f"/runs/from={start}/to={end}/manifest.json")
    immutable(encoded(result), uri)
    return {**result, "manifest_uri": uri}


def run_daily(krx_day, *, dest="s3"):
    end = datetime.strptime(krx_day, "%Y%m%d").date() - timedelta(days=1)
    # Reobserve recent changes under a fresh receipt, without rewriting older
    # snapshots. Corrections older than this overlap require a new backfill.
    start = max(date(2015, 1, 1), end - timedelta(days=119))
    # Keep the original core contract/paths unchanged. Risk is a separate receipt
    # bundle; neither can publish to Silver, and any failure propagates.
    return {bundle: run(start, end, dest=dest, snapshot="daily-" + krx_day, bundle=bundle)
            for bundle in BUNDLES}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", type=date.fromisoformat, default=date(2015, 1, 1))
    parser.add_argument("--end", type=date.fromisoformat, required=True)
    parser.add_argument("--dest", choices=("local", "s3"), default="local")
    parser.add_argument("--snapshot", required=True)
    parser.add_argument("--bundle", choices=tuple(BUNDLES), default="core")
    parser.add_argument("--cache-root", help="Reuse checksummed raw receipts under the same relative paths")
    args = parser.parse_args()
    result = run(args.start, args.end, dest=args.dest, snapshot=args.snapshot,
                 cache_root=args.cache_root, bundle=args.bundle)
    print(json.dumps({k: result[k] for k in ("manifest_uri", "row_count", "rows_by_series", "pit_approved")}))


if __name__ == "__main__":
    main()
