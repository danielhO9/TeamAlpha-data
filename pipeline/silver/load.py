"""Silver 오케스트레이터: 후보 생성 → 자동 품질 검사 → 원자적 publish."""
from __future__ import annotations

import argparse
from datetime import date, datetime

import pandas as pd

from pipeline.common import db
from pipeline.common.paths import base_uri
from pipeline.silver import assets, corporate_actions, dividends, financials, prices
from pipeline.silver.dart_action_snapshot import DEFAULT_COVERAGE_START
from pipeline.silver.return_contract import (
    acquire_return_writer_transaction_lock,
)
from pipeline.silver_quality.models import (
    CandidateBundle,
    CheckResult,
    CheckStatus,
    QualityGateError,
    Severity,
)
from pipeline.silver_quality import repository
from pipeline.silver_quality.runner import assert_publishable, evaluate, print_summary


def _parse_day(day: str | None) -> date:
    if not day:
        raise SystemExit("incremental 은 --date YYYYMMDD 가 필요합니다.")
    for fmt in ("%Y%m%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(day, fmt).date()
        except ValueError:
            pass
    raise SystemExit("날짜 형식은 YYYYMMDD 또는 YYYY-MM-DD 여야 합니다.")


def _build_candidates(
    base: str,
    *,
    target_date: date | None,
    action_coverage_start: date,
    action_coverage_end: date,
    financial_files: list[str] | None,
    dividend_files: list[str] | None,
) -> CandidateBundle:
    asset_df, identifier_df = assets.prepare(base, target_date=target_date)
    preferred_to_common = assets.preferred_share_issuer_map(asset_df)
    price_df, price_stats = prices.prepare(base, target_date=target_date)
    all_price_identifiers = set(
        asset_df.loc[
            asset_df["asset_type"].eq("stock"), "natural_key"
        ].astype(str)
    )
    supported_price_identifiers = set(
        price_df.loc[
            price_df["asset_type"].eq("stock"), "identifier"
        ].astype(str)
    )
    asset_df, identifier_df = assets.restrict_to_price_universe(
        asset_df,
        identifier_df,
        supported_price_identifiers,
    )
    if financial_files is not None:
        fundamental_df, fundamental_stats = financials.prepare(
            base, files=financial_files,
        )
    else:
        years = {target_date.year} if target_date else None
        fundamental_df, fundamental_stats = financials.prepare(base, years=years)
    if dividend_files is not None:
        dividend_df, dividend_stats = dividends.prepare(
            base, files=dividend_files,
        )
    else:
        years = {target_date.year} if target_date else None
        dividend_df, dividend_stats = dividends.prepare(base, years=years)
    fundamental_df = pd.concat(
        [fundamental_df, dividend_df], ignore_index=True,
    )
    fundamental_stats = dict(fundamental_stats)
    fundamental_stats["dividend"] = dividend_stats
    for metric in (
        "input_rows", "transformed_rows", "excluded_rows", "rejected_rows",
    ):
        fundamental_stats[metric] = (
            int(fundamental_stats.get(metric, 0))
            + int(dividend_stats.get(metric, 0))
        )
    action_df, action_stats = corporate_actions.prepare(
        base,
        target_date=target_date,
        coverage_start=action_coverage_start,
        coverage_end=action_coverage_end,
    )
    action_df, inherited_action_stats = corporate_actions.inherit_issuer_events(
        action_df,
        preferred_to_common,
    )
    action_stats["issuer_inheritance"] = inherited_action_stats
    return CandidateBundle(
        assets=asset_df,
        identifiers=identifier_df,
        prices=price_df,
        fundamentals=fundamental_df,
        actions=action_df,
        stats={
            "price_daily": price_stats,
            "fundamental": fundamental_stats,
            "corporate_action": action_stats,
            "_unsupported_market_identifiers": (
                all_price_identifiers - supported_price_identifiers
            ),
        },
    )


def _exclude_nontradable_candidates(
    bundle: CandidateBundle,
    full_price_universe: set[str],
    unsupported_market_identifiers: set[str],
) -> None:
    """Apply the same explicit universe exclusions to every DART fact."""
    (
        bundle.fundamentals,
        bundle.stats["fundamental"],
    ) = financials.exclude_nontradable(
        bundle.fundamentals,
        bundle.stats["fundamental"],
        full_price_universe,
        unsupported_market_identifiers,
    )
    (
        bundle.actions,
        bundle.stats["corporate_action"],
    ) = corporate_actions.exclude_nontradable(
        bundle.actions,
        bundle.stats["corporate_action"],
        full_price_universe,
        unsupported_market_identifiers,
    )


def incremental(
    day: str | None = None,
    src: str = "local",
    financial_files: list[str] | None = None,
    dividend_files: list[str] | None = None,
    market_closed: bool = False,
    has_action_change: bool = False,
    action_coverage_start: date | None = None,
    action_coverage_end: date | None = None,
    allow_bounded_action_scope: bool = False,
    conn=None,
) -> None:
    """Publish one daily Silver partition.

    The production closed-flow passes the PostgreSQL session that owns the
    outer certification advisory lock.  Reusing that exact connection fences
    the raw price transaction: if the lock session is lost, this writer cannot
    continue on a different session and later race a stale certification.
    """
    target_date = _parse_day(day)
    if action_coverage_start != DEFAULT_COVERAGE_START and not (
        allow_bounded_action_scope
        and action_coverage_start is not None
        and action_coverage_end is not None
        and action_coverage_start <= action_coverage_end
    ):
        raise ValueError(
            "daily Silver requires the certified DART action coverage start "
            f"{DEFAULT_COVERAGE_START.isoformat()}"
        )
    if action_coverage_end != target_date:
        raise ValueError(
            "daily Silver DART action coverage end must equal target_date: "
            f"coverage_end={action_coverage_end} target_date={target_date}"
        )
    base = base_uri(src)
    owns_connection = conn is None
    connection = conn or db.connect()
    context = None
    results = []
    try:
        repository.assert_schema(connection)
        context = repository.start_run(
            connection,
            mode="daily",
            target_date=target_date,
            status="RUNNING",
        )
        if (
            market_closed
            and not financial_files
            and not dividend_files
            and not has_action_change
        ):
            repository.finish_run(connection, context, "SKIPPED", [])
            print(
                f"[silver-quality] skipped market holiday date={target_date}",
                flush=True,
            )
            return
        try:
            bundle = _build_candidates(
                base,
                target_date=target_date,
                action_coverage_start=action_coverage_start,
                action_coverage_end=action_coverage_end,
                financial_files=financial_files,
                dividend_files=dividend_files,
            )
        except Exception as exc:
            transform_failure = CheckResult(
                rule_code="CANDIDATE_TRANSFORMATION",
                dataset="silver",
                severity=Severity.CRITICAL,
                status=CheckStatus.FAIL,
                expected="Bronze parses into Silver candidates",
                actual=str(exc),
                failed_count=1,
            )
            repository.finish_run(
                connection, context, "FAILED", [transform_failure],
                error_message=str(exc),
            )
            raise
        bundle.stats["_existing_krx_identifiers"] = (
            repository.existing_krx_identifiers(connection)
        )
        bundle.stats["_market_closed"] = market_closed
        bundle.stats.setdefault("price_daily", {})["coverage_baseline"] = (
            repository.recent_market_coverage_baseline(
                connection, "KRX", ["KOSPI", "KOSDAQ"], target_date,
            )
        )
        candidate_krx_identifiers = set(
            bundle.identifiers.loc[
                bundle.identifiers["source"].eq("KRX"), "identifier"
            ].astype(str)
        )
        full_price_universe = (
            candidate_krx_identifiers
            | bundle.stats["_existing_krx_identifiers"]
        )
        _exclude_nontradable_candidates(
            bundle,
            full_price_universe,
            bundle.stats["_unsupported_market_identifiers"],
        )
        history = repository.recent_price_history(
            connection,
            bundle.prices["identifier"].astype(str).unique().tolist()
            if not bundle.prices.empty else [],
            target_date,
        )
        connection.commit()
        results = evaluate(bundle, target_date=target_date, history=history)
        print_summary(results)
        try:
            assert_publishable(results)
        except QualityGateError as exc:
            repository.finish_run(
                connection,
                context,
                "FAILED",
                results,
                error_message=str(exc),
            )
            raise

        try:
            with connection.transaction():
                # The total-return rebuild holds the same advisory key while
                # reading asset/price inputs.  Acquire it before *any* KRX
                # identity or price mutation, including the daily DELETE.
                acquire_return_writer_transaction_lock(connection)
                krx_map = assets.publish(
                    connection,
                    bundle.assets,
                    bundle.identifiers,
                    context.run_id,
                )
                if not bundle.prices.empty:
                    with connection.cursor() as cur:
                        cur.execute(
                            "DELETE FROM price_daily "
                            "WHERE source='KRX' AND trade_date=%s",
                            (target_date,),
                        )
                        print(
                            f"[prices] 기존 price_daily {target_date} "
                            f"{cur.rowcount}행 교체",
                            flush=True,
                        )
                    prices.publish(
                        connection,
                        bundle.prices,
                        krx_map,
                        context.run_id,
                        target_date,
                    )
                financials.publish(
                    connection,
                    bundle.fundamentals,
                    krx_map,
                    context.run_id,
                )
                corporate_actions.publish(
                    connection,
                    bundle.actions,
                    krx_map,
                    context.run_id,
                )
                repository.save_metrics(connection, context.run_id, bundle)
                repository.finish_run(
                    connection,
                    context,
                    "CERTIFIED",
                    results,
                    commit=False,
                )
                open_scopes, open_rows = repository.open_warning_counts(
                    connection, context.mode,
                )
        except Exception as exc:
            connection.rollback()
            publish_failure = CheckResult(
                rule_code="PUBLISH_TRANSACTION",
                dataset="silver",
                severity=Severity.CRITICAL,
                status=CheckStatus.FAIL,
                expected="atomic publish commit",
                actual=str(exc),
                failed_count=1,
            )
            repository.finish_run(
                connection,
                context,
                "FAILED",
                results + [publish_failure],
                error_message=f"publish failed: {exc}",
            )
            raise
        print(
            f"[silver-quality] certified daily run={context.run_id} "
            f"date={target_date}",
            flush=True,
        )
        print(
            f"[silver-quality] open warnings mode={context.mode} "
            f"scopes={open_scopes} failed_rows={open_rows}",
            flush=True,
        )
    finally:
        if owns_connection:
            connection.close()


def backfill(src: str = "local", resume: str | None = None) -> None:
    """영구 quality_stage를 사용하는 최초 backfill."""
    from pipeline.silver_quality.backfill import run

    run(src=src, resume=resume)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--mode", choices=["backfill", "incremental"], default="backfill")
    p.add_argument("--src", choices=["local"], default="local")
    p.add_argument("--date", help="incremental 대상일 YYYYMMDD")
    p.add_argument("--resume", help="backfill dq_run UUID")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    raise RuntimeError(
        "direct Silver price load is disabled because it can leave the "
        "total-return contract BUILDING; use pipeline.daily_full for an "
        "incremental day. Destructive history rebuild remains disabled until "
        "its authenticated DART fundamental reload is implemented."
    )


if __name__ == "__main__":
    main()
