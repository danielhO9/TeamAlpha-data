"""ECS daily: KRX·DART·FMP Bronze 증분을 수집해 Silver에 인증 반영."""
from __future__ import annotations

import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import boto3
import exchange_calendars as xcals

from pipeline import alternative_data_incremental, dart_silver_backfill_ecs, kis_flows
from pipeline.bronze import (
    corporate_actions,
    dart_support_action_families,
    dart_viewer_corrections,
    dividends,
    financials,
    fmp as fmp_bronze,
    index,
    stock_krxapi,
)
from pipeline.common.paths import base_uri, ymd_to_dash
from pipeline.silver import load
from pipeline.silver import fmp_load
from pipeline.silver.dart_action_snapshot import DEFAULT_COVERAGE_START
from pipeline.silver_quality import freshness, migrate, repository


KST = ZoneInfo("Asia/Seoul")
EXPECTED_STOCK_FILES = {"kospi.parquet", "kosdaq.parquet"}
EXPECTED_INDEX_FILES = {"krx.parquet", "kospi.parquet", "kosdaq.parquet"}
CORPORATE_ACTION_DISCOVERY_LOOKBACK_DAYS = 14
CORPORATE_ACTION_EVIDENCE_LOOKBACK_DAYS = 180
ACTION_DISCLOSURE_MANIFEST_NAME = "disclosures_v3.json"
LEGACY_ACTION_DISCLOSURE_MANIFEST_NAME = "disclosures.json"


def _target_day() -> str:
    """Return explicit PIPELINE_DATE or the previous KST calendar day."""
    override = os.environ.get("PIPELINE_DATE")
    if override:
        return override
    return (datetime.now(KST).date() - timedelta(days=1)).strftime("%Y%m%d")


def _fmp_target_day(krx_day: str) -> str:
    """Use a completed US session at the existing 08:30 KST schedule.

    FMP documents bulk refreshes as taking several hours.  At 08:30 KST the
    same-calendar US session has only just closed, so use the prior weekday.
    """
    candidate = datetime.strptime(krx_day, "%Y%m%d").date() - timedelta(days=1)
    while candidate.weekday() >= 5:
        candidate -= timedelta(days=1)
    return candidate.strftime("%Y%m%d")


def _pending_krx_sessions(
    last_price_day: date,
    target_day: date,
) -> list[date]:
    """Return KRX sessions after persisted coverage through the target day."""
    start = last_price_day + timedelta(days=1)
    if target_day < start:
        return []
    calendar = xcals.get_calendar("XKRX")
    return [
        value.date()
        for value in calendar.sessions_in_range(start, target_day)
    ]


def _key_from_s3_uri(uri: str) -> str:
    return uri.removeprefix(base_uri("s3") + "/")


def _action_disclosure_manifest_key(action_from: str, day: str) -> str:
    return (
        "corporate_actions/dart/manifests/"
        f"from={action_from}/to={day}/{ACTION_DISCLOSURE_MANIFEST_NAME}"
    )


def _reject_legacy_action_manifests(keys: list[str]) -> None:
    legacy = [
        key for key in keys
        if key.endswith(f"/{LEGACY_ACTION_DISCLOSURE_MANIFEST_NAME}")
        and "/corporate_actions/dart/manifests/" in f"/{key}"
    ]
    if legacy:
        raise RuntimeError(
            "legacy corporate-action disclosure manifests cannot authenticate "
            f"the v3 collector universe: {sorted(legacy)}"
        )


def _list_prefix(bucket: str, prefix: str) -> list[str]:
    s3 = boto3.client("s3")
    keys: list[str] = []
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith("/"):
                keys.append(key)
    return keys


def _download_keys(bucket: str, keys: list[str], root: Path) -> list[str]:
    s3 = boto3.client("s3")
    keys = sorted(set(keys))
    if not keys:
        return []

    print(f"[sync] downloading {len(keys)} changed/needed objects", flush=True)
    start = time.time()
    done = 0

    def download(key: str) -> str:
        dest = root / key
        dest.parent.mkdir(parents=True, exist_ok=True)
        s3.download_file(bucket, key, str(dest))
        return str(dest)

    paths: list[str] = []
    with ThreadPoolExecutor(max_workers=16) as ex:
        futures = [ex.submit(download, key) for key in keys]
        for fut in as_completed(futures):
            paths.append(fut.result())
            done += 1
            if done % 100 == 0 or done == len(keys):
                print(f"[sync] downloaded {done}/{len(keys)} elapsed={time.time() - start:.1f}s", flush=True)
    return paths


def _assert_complete_daily_market(stock_keys: list[str], index_keys: list[str], ds: str) -> None:
    """Prevent partial trading-day loads from deleting/replacing silver rows."""
    stock_files = {Path(key).name for key in stock_keys}
    if not index_keys and not stock_files:
        return
    missing = EXPECTED_STOCK_FILES - stock_files
    index_files = {Path(key).name for key in index_keys}
    missing_index = EXPECTED_INDEX_FILES - index_files
    if missing or missing_index:
        raise RuntimeError(
            f"incomplete market bronze for {ds}: "
            f"missing_stock={sorted(missing)}, missing_index={sorted(missing_index)} "
            f"(stock={sorted(stock_files)}, index={sorted(index_files)})"
        )


def main() -> None:
    # No API/S3/RDS mutation may happen before this common session lock.  It
    # serializes the complete certification epoch with one-off dart-extras,
    # preventing an older local snapshot from certifying after another task
    # has observed a newer Bronze action generation.
    certification_lock = (
        dart_silver_backfill_ecs.acquire_daily_certification_lock()
    )
    try:
        _main_locked(certification_lock)
    finally:
        dart_silver_backfill_ecs.release_daily_certification_lock(
            certification_lock,
        )


def _main_locked(
    certification_lock,
    *,
    allow_deferred_total_return: bool = False,
    prepare_total_return: bool = True,
    preview_total_return: bool = True,
    close_total_return: bool = True,
    assert_final_freshness: bool = True,
    collect_financials: bool = True,
    full_year_financial_snapshot: bool = False,
    bounded_action_scope: bool = False,
    collect_alternative: bool = True,
) -> None:
    """Run one target day while the caller owns the certification epoch.

    Normal daily execution uses the strict defaults.  The bounded gap replay
    orchestrator may keep the return contract BUILDING between consecutive
    dates while it holds this same session lock, then rebuild and certify once
    on its final date.  No independent daily invocation can opt into that
    behavior through an environment variable or CLI flag.
    """
    bucket = os.environ["S3_BRONZE_BUCKET"]
    day = _target_day()
    ds = ymd_to_dash(day)
    coverage_end = datetime.strptime(day, "%Y%m%d").date()
    root = Path("/app/data")

    if not allow_deferred_total_return:
        last_price_day = (
            dart_silver_backfill_ecs.certified_krx_price_coverage_end(
                conn=certification_lock,
            )
        )
        pending_sessions = _pending_krx_sessions(last_price_day, coverage_end)
        if len(pending_sessions) > 1:
            raise RuntimeError(
                "daily KRX target would skip certified sessions; run bounded "
                "gap replay first: "
                f"last={last_price_day.isoformat()} "
                f"target={coverage_end.isoformat()} "
                f"missing={[value.isoformat() for value in pending_sessions]}"
            )

    def assert_epoch() -> None:
        dart_silver_backfill_ecs.assert_daily_certification_lock(
            certification_lock,
        )

    print(f"[daily] start day={day}", flush=True)
    migrate.assert_current()
    daily_already_certified = repository.certified_target_exists(
        certification_lock, "daily", coverage_end,
    )
    total_return_ready = (
        dart_silver_backfill_ecs.total_return_contract_ready(
            conn=certification_lock,
        )
        if daily_already_certified else False
    )
    if daily_already_certified and total_return_ready:
        print(
            f"[daily] KRX/DART already certified day={day}; "
            "skipping source collection, matching, and total-return rebuild",
            flush=True,
        )
        if collect_alternative:
            alternative_data_incremental.run(day, conn=certification_lock)
        kis_flows.daily(day, conn=certification_lock)
        _run_fmp_incremental(
            bucket, root, day, certification_lock=certification_lock,
        )
        if assert_final_freshness:
            fr = freshness.assert_fresh()
            print(f"[freshness] ok {fr['sources']}", flush=True)
        else:
            print(f"[freshness] deferred after gap day={day}", flush=True)
        return
    if daily_already_certified:
        # A stopped/crashed invocation may have committed the daily source
        # transaction while leaving the total-return contract BUILDING.  The
        # immutable action snapshot was published before that transaction, so
        # restore it directly and finish the contract without repeating KRX or
        # OpenDART collection, evidence crawling, or snapshot generation.
        print(
            f"[daily] certified day={day} with unfinished total-return; "
            "starting retry-only repair",
            flush=True,
        )
        dart_silver_backfill_ecs.restore_published_total_return_snapshot(
            coverage_end,
            bucket=bucket,
            root=root,
        )
        assert_epoch()
        dart_silver_backfill_ecs.close_total_return_contract(
            coverage_end,
            root=root,
            certification_lock=certification_lock,
        )
        if collect_alternative:
            assert_epoch()
            alternative_data_incremental.run(day, conn=certification_lock)
        kis_flows.daily(day, conn=certification_lock)
        _run_fmp_incremental(
            bucket, root, day, certification_lock=certification_lock,
        )
        if assert_final_freshness:
            fr = freshness.assert_fresh()
            print(f"[freshness] ok {fr['sources']}", flush=True)
        else:
            print(f"[freshness] deferred after gap day={day}", flush=True)
        return
    stock_krxapi.run(day, day, "s3")
    index.run(day, day, "s3")
    if collect_financials:
        changed_financial_uris = financials.run_incremental(day, "s3")
    else:
        changed_financial_uris = []
        print("[daily] DART financial refresh deferred", flush=True)

    financial_input_uris = changed_financial_uris
    if full_year_financial_snapshot:
        financial_snapshot_keys = _list_prefix(
            bucket, f"financials/dart/year={day[:4]}/",
        ) + _list_prefix(
            bucket, f"financials/dart_full/year={day[:4]}/",
        )
        financial_snapshot_keys = sorted({
            key for key in financial_snapshot_keys if key.endswith(".json")
        })
        financial_input_uris = [
            f"{base_uri('s3')}/{key}" for key in financial_snapshot_keys
        ]
        print(
            "[daily] full-year financial snapshot "
            f"files={len(financial_snapshot_keys)}",
            flush=True,
        )

    if collect_financials and os.environ.get(
        "DART_DIVIDENDS_ENABLED", "true",
    ).lower() in {
        "1", "true", "yes", "on",
    }:
        changed_dividend_uris = dividends.run_for_financial_paths(
            financial_input_uris,
            "s3",
        )
    else:
        changed_dividend_uris = []
        print("[daily] DART regular-report dividends disabled", flush=True)
    changed_financial_keys = [
        _key_from_s3_uri(uri) for uri in financial_input_uris
    ]
    changed_dividend_keys = [
        _key_from_s3_uri(uri) for uri in changed_dividend_uris
    ]
    if full_year_financial_snapshot:
        changed_dividend_keys = sorted({
            key for key in _list_prefix(
                bucket,
                f"dividends/dart/alot-matter/year={day[:4]}/",
            )
            if key.endswith("/response.json")
        })
    print(f"[daily] changed financial files={len(changed_financial_keys)}", flush=True)
    print(
        f"[daily] changed dividend files={len(changed_dividend_uris)}",
        flush=True,
    )
    action_from = (
        datetime.strptime(day, "%Y%m%d").date()
        - timedelta(days=CORPORATE_ACTION_DISCOVERY_LOOKBACK_DAYS)
    ).strftime("%Y%m%d")
    action_evidence_from = (
        datetime.strptime(day, "%Y%m%d").date()
        - timedelta(days=CORPORATE_ACTION_EVIDENCE_LOOKBACK_DAYS)
    ).strftime("%Y%m%d")
    if bounded_action_scope and not allow_deferred_total_return:
        raise RuntimeError(
            "bounded action scope is only allowed inside a fenced gap replay"
        )
    # Record whether this task inherited an already-unusable return contract.
    # A new Bronze action is allowed to demote a previously healthy contract,
    # but it must not hide an older BUILDING failure by extending raw coverage.
    contract_ready_before_action_collection = (
        dart_silver_backfill_ecs.total_return_contract_ready(
            conn=certification_lock,
        )
    )
    action_contract_invalidated = False

    def invalidate_before_first_action_write(_path: str) -> None:
        nonlocal action_contract_invalidated
        # The collector invokes this boundary for every new object.  Keep
        # proving lock ownership even after the one-time DB invalidation so a
        # long S3 publication cannot continue after its PostgreSQL session
        # (and therefore its cross-task exclusion) was lost.
        assert_epoch()
        if action_contract_invalidated:
            return
        # This callback completes before _BronzeWriter schedules the first S3
        # PUT.  A new action observation can therefore never coexist with a
        # stale CERTIFIED return label, even when later preflight fails.
        dart_silver_backfill_ecs.invalidate_total_return_for_observed_action(
            coverage_end,
            conn=certification_lock,
        )
        action_contract_invalidated = True

    genuine_action_changes: list[str] = []
    changed_action_uris = corporate_actions.run(
        action_from,
        day,
        "s3",
        include_dependencies=True,
        dependency_fromdate=action_evidence_from,
        changed_sink=genuine_action_changes,
        before_change=invalidate_before_first_action_write,
    )
    has_action_change = bool(genuine_action_changes)
    changed_action_keys = [
        _key_from_s3_uri(uri) for uri in changed_action_uris
    ]
    _reject_legacy_action_manifests(changed_action_keys)
    action_disclosure_manifest = _action_disclosure_manifest_key(
        action_from,
        day,
    )
    if action_disclosure_manifest not in changed_action_keys:
        changed_action_keys.append(action_disclosure_manifest)
    print(
        f"[daily] changed corporate-action files={len(changed_action_keys)}, "
        f"genuine changes={len(genuine_action_changes)}",
        flush=True,
    )

    stock_keys = _list_prefix(bucket, f"stock/krxapi/date={ds}/")
    index_keys = _list_prefix(bucket, f"index/krxapi/date={ds}/")
    _assert_complete_daily_market(stock_keys, index_keys, ds)
    market_closed = not stock_keys and not index_keys
    source_will_invalidate_return = bool(stock_keys) or has_action_change
    # Every non-skipped Silver load currently evaluates/publishes the bounded
    # action candidate frame. A financial or regular-dividend refresh can
    # therefore cause a genuine issuer-action upsert even when the market is
    # closed and the Bronze action collector wrote no new object. Prepare the
    # complete action evidence before that transaction; the post-commit DB
    # state below remains the authority on whether recertification is needed.
    source_may_publish_actions = bool(
        changed_financial_keys or changed_dividend_keys
    )
    requires_total_return_closure = (
        source_will_invalidate_return
        or source_may_publish_actions
        or not contract_ready_before_action_collection
    )

    keys = ["financials/dart/corpCode.xml"]
    keys += stock_keys
    keys += index_keys
    keys += changed_financial_keys
    keys += changed_dividend_keys
    keys += changed_action_keys

    local_paths = _download_keys(bucket, keys, root)
    financial_files = [
        p for p in local_paths
        if (
            "/financials/dart/year=" in p
            or "/financials/dart_full/year=" in p
        )
    ]
    dividend_files = [
        p for p in local_paths if "/dividends/dart/alot-matter/" in p
    ]

    if bounded_action_scope:
        # The common manifest pointer may currently hold a prior full-history
        # checkpoint. Rebuild the exact rolling evidence scope locally before
        # Silver validates this intermediate day. This avoids refreshing the
        # entire 2015+ corpus on every gap date while preserving an exact,
        # bounded provenance contract for price DQ and action publication.
        bounded_start = datetime.strptime(
            action_evidence_from, "%Y%m%d",
        ).date()
        dart_viewer_corrections.collect_viewer_corrections(
            str(root),
            coverage_start=bounded_start,
            coverage_end=coverage_end,
            apply=True,
        )
        dart_support_action_families.collect_support_action_families(
            root,
            coverage_start=bounded_start,
            coverage_end=coverage_end,
            apply=True,
        )
        print(
            "[gap-replay] bounded action evidence prepared "
            f"coverage={bounded_start.isoformat()}..{coverage_end.isoformat()}",
            flush=True,
        )

    if requires_total_return_closure and prepare_total_return:
        current_snapshot_prepared = False
        if (
            not contract_ready_before_action_collection
            and not allow_deferred_total_return
        ):
            # Repair the exact already-published raw price horizon before
            # preparing the next day's action snapshot.  A future-dated local
            # snapshot is deliberately rejected by the PIT rebuild contract.
            inherited_coverage_end = (
                dart_silver_backfill_ecs.certified_krx_price_coverage_end(
                    conn=certification_lock,
                )
            )
            if inherited_coverage_end > coverage_end:
                raise RuntimeError(
                    "certified KRX price coverage is ahead of the target day: "
                    f"prices={inherited_coverage_end.isoformat()} "
                    f"target={coverage_end.isoformat()}"
                )
            dart_silver_backfill_ecs.prepare_total_return_snapshot(
                inherited_coverage_end,
                bucket=bucket,
                root=root,
                publish=False,
                certification_lock=certification_lock,
            )
            if preview_total_return:
                dart_silver_backfill_ecs.preview_total_return_actions(
                    inherited_coverage_end,
                    root=root,
                    conn=certification_lock,
                )
            assert_epoch()
            print(
                "[total-return] closing pre-existing uncertified coverage "
                "before daily price publication",
                flush=True,
            )
            dart_silver_backfill_ecs.close_total_return_contract(
                inherited_coverage_end,
                root=root,
                certification_lock=certification_lock,
            )
            current_snapshot_prepared = inherited_coverage_end == coverage_end

        # Complete all network/filesystem evidence first.  A missing viewer,
        # correction family, frozen cash-scale body or v5 coverage interval
        # therefore stops the task before its KRX Silver transaction.
        if not current_snapshot_prepared:
            dart_silver_backfill_ecs.prepare_total_return_snapshot(
                coverage_end,
                bucket=bucket,
                root=root,
                certification_lock=certification_lock,
            )
            if preview_total_return:
                dart_silver_backfill_ecs.preview_total_return_actions(
                    coverage_end,
                    root=root,
                    conn=certification_lock,
                )
            assert_epoch()
        if (
            not contract_ready_before_action_collection
            and allow_deferred_total_return
        ):
            print(
                "[total-return] gap replay owns the certification epoch; "
                "deferring pre-existing raw coverage closure",
                flush=True,
            )
    elif requires_total_return_closure:
        if not allow_deferred_total_return:
            raise RuntimeError(
                "total-return evidence preparation may only be deferred "
                "inside a fenced gap replay"
            )
        print(
            "[total-return] gap replay deferring complete evidence preflight "
            "until its final day",
            flush=True,
        )

    print(
        f"[silver] incremental start day={day}, "
        f"financial_files={len(financial_files)}, "
        f"dividend_files={len(dividend_files)}",
        flush=True,
    )
    assert_epoch()
    load.incremental(
        day,
        "local",
        financial_files=financial_files,
        dividend_files=dividend_files,
        market_closed=market_closed,
        has_action_change=has_action_change,
        action_coverage_start=(
            datetime.strptime(action_evidence_from, "%Y%m%d").date()
            if bounded_action_scope else DEFAULT_COVERAGE_START
        ),
        action_coverage_end=coverage_end,
        allow_bounded_action_scope=bounded_action_scope,
        conn=certification_lock,
    )
    print(f"[silver] incremental complete day={day}", flush=True)

    # The price/action prediction above decides whether expensive evidence can
    # be prepared before the Silver transaction.  It is not the authority on
    # what that transaction actually changed: even on a market holiday, a
    # financial/dividend refresh can make the action publisher upsert an
    # issuer event and atomically demote the return contract.  Re-read the
    # contract through the same K2 certification session after the commit.
    assert_epoch()
    contract_ready_after_incremental = (
        dart_silver_backfill_ecs.total_return_contract_ready(
            conn=certification_lock,
        )
    )
    if not contract_ready_after_incremental and close_total_return:
        # The source transaction has deliberately demoted the label contract to
        # BUILDING.  The same ECS invocation must now validate the new raw-price
        # day against local actions, publish the exact action snapshot, rebuild
        # the entire v3 label and pass an independent audit.  Any exception is
        # fatal and leaves the contract visibly BUILDING.
        assert_epoch()
        dart_silver_backfill_ecs.close_total_return_contract(
            coverage_end,
            root=root,
            certification_lock=certification_lock,
        )
    elif not contract_ready_after_incremental:
        print(
            f"[total-return] deferred closure after gap day={day}",
            flush=True,
        )

    if collect_alternative:
        assert_epoch()
        alternative_data_incremental.run(day, conn=certification_lock)

    kis_flows.daily(day, conn=certification_lock)

    # FMP is a separate source transaction. KRX/DART remains committed if FMP
    # later fails, and a task retry safely reuses the immutable raw objects.
    _run_fmp_incremental(
        bucket, root, day, certification_lock=certification_lock,
    )

    # A BUILDING/drifted total-return contract or stale source is a task
    # failure.  Do not emit a false-green ECS exit after source publication.
    if assert_final_freshness:
        fr = freshness.assert_fresh()
        print(f"[freshness] ok {fr['sources']}", flush=True)
    else:
        print(f"[freshness] deferred after gap day={day}", flush=True)


def _run_fmp_incremental(
    bucket: str,
    root: Path,
    krx_day: str,
    *,
    certification_lock,
) -> None:
    """Publish the FMP day once; retries reuse its certified transaction."""
    fmp_day = _fmp_target_day(krx_day)
    parsed_fmp_day = datetime.strptime(fmp_day, "%Y%m%d").date()
    dart_silver_backfill_ecs.assert_daily_certification_lock(
        certification_lock,
    )
    if repository.certified_target_exists(
        certification_lock, "fmp_daily", parsed_fmp_day,
    ):
        print(f"[fmp] already certified day={fmp_day}; skipped", flush=True)
        return
    print(f"[fmp] daily start day={fmp_day}", flush=True)
    fmp_uris = fmp_bronze.run_daily(fmp_day, "s3")
    fmp_keys = [_key_from_s3_uri(uri) for uri in fmp_uris]
    _download_keys(bucket, fmp_keys, root)
    fmp_load.run(src="local", day=fmp_day)
    print(f"[fmp] daily complete day={fmp_day}", flush=True)


if __name__ == "__main__":
    main()
