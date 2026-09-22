"""Replay existing S3 research inputs with bounded workers and durable checkpoints.

Only S3 and Silver are contacted. No provider collection, price publishing or
return rebuilding. The exact discovered inventory and per-file outcomes are
written to an S3 audit prefix before/after the finite run.
"""
from __future__ import annotations

import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import json
import os
import re
import threading
import time

import boto3

from pipeline.common import db
from pipeline.silver import research_backfill
from pipeline.silver_quality import migrate


def list_keys(client, bucket, prefix):
    return [obj["Key"]
            for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix)
            for obj in page.get("Contents", [])]


def receipt_identity(key):
    match = re.search(r"/corp=([^/]+)/rcept=(\d+)\.json$", key)
    return match.groups() if match else None


def discover(client, bucket, datasets):
    items = []
    if "FMP_STATEMENT" in datasets:
        items.extend({"key": k, "dataset": "FMP_STATEMENT"}
                     for k in list_keys(client, bucket, "financials/fmp/")
                     if k.endswith(("/response.json", "/response.csv")))
    if "FMP_PROFILE" in datasets:
        items.extend({"key": k, "dataset": "FMP_PROFILE"}
                     for k in list_keys(client, bucket, "stock/fmp/universe/")
                     if k.endswith(("/response.json", "/response.csv"))
                     and any(f"/{endpoint}/" in k for endpoint in ("profile-bulk", "company-screener", "stock-list")))
    if "DART_EVENT" in datasets:
        disclosures = {}
        for key in list_keys(client, bucket, "corporate_actions/dart/disclosures/"):
            identity = receipt_identity(key)
            if identity:
                disclosures.setdefault(identity, []).append(key)
        for key in list_keys(client, bucket, "corporate_actions/dart/structured/"):
            identity = receipt_identity(key)
            if identity:
                matches = disclosures.get(identity, [])
                items.append({"key": key, "dataset": "DART_EVENT",
                              "disclosures": matches})
    return sorted(items, key=lambda item: (datasets.index(item["dataset"]), item["key"]))


def run(*, bucket, datasets, workers, max_files, report_prefix):
    client = boto3.client("s3")
    with db.connect() as conn:
        migrate.assert_current(conn)
    items = discover(client, bucket, datasets)
    if max_files:
        items = items[:max_files]
    def save(name, value):
        client.put_object(Bucket=bucket, Key=report_prefix.rstrip("/") + "/" + name,
                          Body=json.dumps(value, ensure_ascii=False).encode(),
                          ContentType="application/json")
    save("inventory.json", items)
    print(json.dumps({"status": "STARTED", "files": len(items), "workers": workers,
                      "datasets": datasets, "report_prefix": report_prefix}), flush=True)
    state = threading.local()
    connections = []
    def get(key):
        return client.get_object(Bucket=bucket, Key=key)["Body"].read()
    def process(item):
        path = "s3://" + bucket + "/" + item["key"]
        try:
            if not hasattr(state, "conn") or state.conn.closed:
                state.conn = db.connect()
                state.conn.execute("SET statement_timeout='90s'")
                state.conn.execute("SET lock_timeout='5s'")
                state.eligible_identifiers = {
                    str(row[0]) for row in state.conn.execute(
                        "SELECT DISTINCT identifier FROM asset_identifier "
                        "WHERE source='FMP' AND identifier_type='ticker'"
                    ).fetchall()
                }
                state.conn.commit()
                connections.append(state.conn)
            entry = {"path": path, "dataset": item["dataset"]}
            inputs = {}
            if item["dataset"] == "DART_EVENT":
                if len(item["disclosures"]) != 1:
                    return {"path": path, "status": "REVIEW_REQUIRED", "reason": "official disclosure identity not unique", "matches": len(item["disclosures"])}
                body = get(item["key"])
                disclosure_key = item["disclosures"][0]
                disclosure = get(disclosure_key)
                entry.update(sha256=hashlib.sha256(body).hexdigest(),
                             disclosure_file="s3://" + bucket + "/" + disclosure_key,
                             disclosure_sha256=hashlib.sha256(disclosure).hexdigest())
                inputs = {"body": body, "disclosure_body": disclosure}
            else:
                manifest = json.loads(get(item["key"].rsplit("/", 1)[0] + "/manifest.json"))
                if manifest.get("complete") is not True:
                    raise ValueError("incomplete Bronze manifest")
                entry["sha256"] = manifest["sha256"]
                inputs["eligible_identifiers"] = state.eligible_identifiers
            return research_backfill.replay(state.conn, entry, prepared_inputs=inputs)
        except Exception as exc:
            if hasattr(state, "conn"):
                state.conn.rollback()
            return {"path": path, "status": "FAILED", "error": type(exc).__name__ + ": " + str(exc)}
    outcomes = []
    start = time.monotonic()
    try:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = [pool.submit(process, item) for item in items]
            for future in as_completed(futures):
                outcome = future.result()
                outcomes.append(outcome)
                if outcome["status"] in {"FAILED", "REVIEW_REQUIRED"}:
                    print(json.dumps(outcome), flush=True)
                if len(outcomes) % 25 == 0 or len(outcomes) == len(items):
                    progress = {"completed_files": len(outcomes), "total_files": len(items),
                                "inserted": sum(o.get("inserted", 0) for o in outcomes),
                                "excluded": sum(o.get("excluded", 0) for o in outcomes),
                                "statuses": dict(Counter(o["status"] for o in outcomes)),
                                "elapsed_seconds": round(time.monotonic() - start, 1)}
                    print(json.dumps(progress), flush=True)
                    save("progress.json", progress)
                    save("outcomes.json", outcomes)
    finally:
        for conn in connections:
            conn.close()
    return not any(o["status"] in {"FAILED", "REVIEW_REQUIRED"} for o in outcomes)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bucket", default=os.environ.get("S3_BRONZE_BUCKET"))
    parser.add_argument("--datasets", nargs="+", choices=["FMP_STATEMENT", "FMP_PROFILE", "DART_EVENT"],
                        default=["FMP_STATEMENT", "FMP_PROFILE", "DART_EVENT"])
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--max-files", type=int, default=0)
    parser.add_argument("--report-prefix", required=True)
    args = parser.parse_args()
    if not args.bucket or not 1 <= args.workers <= 8 or args.max_files < 0:
        parser.error("bucket, workers in 1..8, and nonnegative max-files are required")
    if not args.report_prefix.startswith("ops/research-backfill/"):
        parser.error("report-prefix must be under ops/research-backfill/")
    raise SystemExit(0 if run(**vars(args)) else 2)


if __name__ == "__main__":
    main()
