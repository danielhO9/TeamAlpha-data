"""FMP Silver source-scoped load: raw Bronze -> DQ -> atomic RDS publish."""
from __future__ import annotations

import argparse
import hashlib
import json
import re
from datetime import date, datetime, timedelta
from pathlib import Path
from uuid import UUID

import pandas as pd

from pipeline.common import db
from pipeline.common.paths import base_uri
from pipeline.silver import fmp, research_observations
from pipeline.silver_quality import repository
from pipeline.silver_quality.models import (
    CheckResult,
    CheckStatus,
    CandidateBundle,
    QualityGateError,
    Severity,
)
from pipeline.silver_quality.rules.fmp import check_fmp
from pipeline.silver_quality.runner import assert_publishable, print_summary


def _parse_day(day: str) -> date:
    for fmt in ("%Y%m%d", "%Y-%m-%d"):
        try:
            return datetime.strptime(day, fmt).date()
        except ValueError:
            pass
    raise SystemExit("날짜 형식은 YYYYMMDD 또는 YYYY-MM-DD 여야 합니다.")


def _fingerprint(base: str, target_date: date | None) -> str:
    digest = hashlib.sha256()
    root = Path(base)
    for path in sorted(root.rglob("manifest.json")):
        rendered = str(path).replace("\\", "/")
        if "/fmp/" not in rendered:
            continue
        try:
            metadata = json.loads(path.read_bytes())
        except (OSError, ValueError, TypeError):
            continue
        if target_date is not None:
            markers = re.findall(r"(?:date|snapshot_date)=(\d{4}-\d{2}-\d{2})", rendered)
            if markers and target_date.isoformat() not in markers:
                continue
        digest.update(str(path.relative_to(root)).encode())
        digest.update(str(metadata.get("sha256") or "").encode())
    return digest.hexdigest()


def _existing_identifier_map(
    conn,
    identifier_candidates: pd.DataFrame,
) -> dict[str, int]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT identifier_type, identifier, asset_id, valid_to, valid_from
            FROM asset_identifier
            WHERE source='FMP'
            ORDER BY identifier_type, identifier,
                     (valid_to IS NULL) DESC, valid_from DESC
            """
        )
        rows = cur.fetchall()
    stored: dict[tuple[str, str], int] = {}
    for identifier_type, identifier, asset_id, _, _ in rows:
        stored.setdefault(
            (str(identifier_type), str(identifier)), int(asset_id),
        )
    output: dict[str, int] = {}
    # Facts and actions can retain a historical ticker after a symbol change.
    # Resolve every stored ticker episode directly, while keeping asset identity
    # anchored to the current primary ticker below.  This avoids creating or
    # merging assets from untrusted CUSIP/ISIN values.
    for row in identifier_candidates.itertuples(index=False):
        if row.identifier_type not in {"ticker", "fx_pair", "commodity_symbol"}:
            continue
        asset_id = stored.get(
            (str(row.identifier_type), str(row.identifier)),
        )
        if asset_id is not None:
            output[str(row.identifier)] = asset_id
    for natural_key, group in identifier_candidates.groupby(
        "natural_key", sort=False,
    ):
        rendered_key = str(natural_key)
        primary_ticker = rendered_key.removeprefix("FMP:")
        if rendered_key.startswith("FMP:COMMODITY:"):
            primary_ticker = rendered_key.removeprefix("FMP:COMMODITY:")
        ordered = sorted(
            group.itertuples(index=False),
            key=lambda row: (
                0 if row.identifier_type == "ticker" and pd.isna(row.valid_to)
                else 1 if row.identifier_type == "ticker"
                else 2 if pd.isna(row.valid_to) else 3
            ),
        )
        for row in ordered:
            if not (
                row.identifier_type in {"fx_pair", "commodity_symbol"}
                or (
                    row.identifier_type == "ticker"
                    and str(row.identifier) == primary_ticker
                )
            ):
                continue
            asset_id = stored.get(
                (str(row.identifier_type), str(row.identifier)),
            )
            if asset_id is not None:
                output[str(natural_key)] = asset_id
                break
    return output


def _publish(
    conn,
    bundle,
    context,
    target_date: date | None,
    *,
    publish_asset_candidates: bool = True,
) -> None:
    identifier_map = (
        fmp.publish_assets(
            conn, bundle.assets, bundle.identifiers, context.run_id,
        )
        if publish_asset_candidates
        else _existing_identifier_map(conn, bundle.identifiers)
    )
    if target_date is not None:
        with conn.cursor() as cur:
            cur.execute(
                "DELETE FROM price_daily "
                "WHERE source IN ('FMP','FMP_FX','FMP_COMMODITY') "
                "AND trade_date=%s",
                (target_date,),
            )
            if target_date.weekday() == 0:
                cur.execute(
                    "DELETE FROM price_daily "
                    "WHERE source='FMP_COMMODITY' AND trade_date=%s",
                    (target_date - timedelta(days=1),),
                )
    fmp.publish_prices(conn, bundle.prices, identifier_map, context.run_id)
    fmp.publish_fundamentals(
        conn, bundle.fundamentals, identifier_map, context.run_id,
    )
    fmp.publish_actions(conn, bundle.actions, identifier_map, context.run_id)
    research_observations.publish(
        conn, bundle.research_observations, identifier_map, context.run_id,
    )


def _add_previous_commodity_roll_check(conn, bundle: CandidateBundle) -> None:
    """Compare a daily candidate with the latest certified commodity close."""
    if bundle.prices.empty or "source" not in bundle.prices:
        return
    current = bundle.prices[
        bundle.prices["source"].eq("FMP_COMMODITY")
    ]
    if current.empty:
        return
    symbols = sorted(current["identifier"].astype(str).unique())
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT ON (ai.identifier)
                   ai.identifier, p.trade_date, p.close
            FROM asset_identifier ai
            JOIN price_daily p ON p.asset_id=ai.asset_id
            WHERE ai.source='FMP'
              AND ai.identifier_type='commodity_symbol'
              AND ai.identifier=ANY(%s)
              AND p.source='FMP_COMMODITY'
            ORDER BY ai.identifier, p.trade_date DESC
            """,
            (symbols,),
        )
        previous = {
            str(symbol): (trade_date, float(close))
            for symbol, trade_date, close in cur.fetchall()
        }
    samples = []
    for row in current.itertuples(index=False):
        prior = previous.get(str(row.identifier))
        if prior is None or prior[1] == 0 or row.close is None:
            continue
        move = float(row.close) / prior[1] - 1
        if abs(move) <= 0.20:
            continue
        samples.append({
            "identifier": str(row.identifier),
            "trade_date": row.trade_date,
            "close": float(row.close),
            "previous_trade_date": prior[0],
            "previous_close": prior[1],
            "return": move,
        })
    detail = bundle.stats.setdefault("commodity", {}).setdefault(
        "possible_roll", {},
    )
    detail["row_count"] = len(samples)
    detail["samples"] = samples[:20]


def _build_daily_candidates(
    conn,
    base: str,
    target_date: date,
) -> CandidateBundle:
    """Build candidates and close the read transaction before publication.

    Psycopg starts a transaction for the previous-close SELECT.  Keeping that
    transaction open would make the later publish transaction a savepoint; the
    outer transaction would then be rolled back when the connection closes.
    """
    bundle = fmp.build_candidates(base, target_date)
    with conn.transaction():
        _add_previous_commodity_roll_check(conn, bundle)
        bundle.stats.setdefault("price_daily", {})["coverage_baseline"] = (
            repository.recent_source_daily_count_baseline(conn, "FMP", target_date)
        )
    return bundle


def _daily(*, src: str, day: str) -> None:
    target_date = _parse_day(day)
    base = base_uri(src)
    conn = db.connect()
    context = None
    results = []
    try:
        repository.assert_schema(conn)
        context = repository.start_run(
            conn,
            mode="fmp_daily",
            target_date=target_date,
            input_fingerprint=_fingerprint(base, target_date),
        )
        try:
            bundle = _build_daily_candidates(conn, base, target_date)
        except Exception as exc:
            failure = CheckResult(
                rule_code="FMP_CANDIDATE_TRANSFORMATION",
                dataset="silver",
                severity=Severity.CRITICAL,
                status=CheckStatus.FAIL,
                expected="FMP Bronze parses into Silver candidates",
                actual=str(exc),
                failed_count=1,
            )
            repository.finish_run(
                conn, context, "FAILED", [failure], error_message=str(exc),
            )
            raise
        results = check_fmp(bundle)
        print_summary(results)
        try:
            assert_publishable(results)
        except QualityGateError as exc:
            repository.finish_run(
                conn, context, "FAILED", results, error_message=str(exc),
            )
            raise
        try:
            with conn.transaction():
                _publish(conn, bundle, context, target_date)
                repository.save_metrics(conn, context.run_id, bundle)
                repository.finish_run(
                    conn, context, "CERTIFIED", results, commit=False,
                )
                open_scopes, open_rows = repository.open_warning_counts(
                    conn, context.mode,
                )
        except Exception as exc:
            conn.rollback()
            failure = CheckResult(
                rule_code="FMP_PUBLISH_TRANSACTION",
                dataset="silver",
                severity=Severity.CRITICAL,
                status=CheckStatus.FAIL,
                expected="source-scoped atomic FMP publish",
                actual=str(exc),
                failed_count=1,
            )
            repository.finish_run(
                conn, context, "FAILED", results + [failure],
                error_message=f"publish failed: {exc}",
            )
            raise
        print(
            f"[silver-fmp] certified run={context.run_id} date={target_date}",
            flush=True,
        )
        print(
            f"[silver-quality] open warnings mode={context.mode} "
            f"scopes={open_scopes} failed_rows={open_rows}",
            flush=True,
        )
    finally:
        conn.close()


def _discovered_years(base: str) -> list[int]:
    years: set[int] = set()
    root = Path(base)
    patterns = (
        "stock/fmp/eod-bulk/date=*/response.*",
        "financials/fmp/*/year=*/period=*/response.*",
        "corporate_actions/fmp/*/year=*/response.*",
        "corporate_actions/fmp/*/year=*/*/response.*",
    )
    for pattern in patterns:
        for path in root.glob(pattern):
            match = re.search(r"(?:date|year)=(\d{4})", str(path))
            if match:
                years.add(int(match.group(1)))
    return sorted(years)


def _completed_partitions(conn, parent_run_id: UUID) -> set[str]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT partition_key FROM dq_run "
            "WHERE parent_run_id=%s AND status='CERTIFIED'",
            (parent_run_id,),
        )
        return {str(row[0]) for row in cur.fetchall()}


def _fundamental_partitions(
    year: int,
    frame: pd.DataFrame,
) -> list[tuple[str, pd.DataFrame]]:
    if frame.empty:
        return []
    output = []
    for (statement_type, fiscal_period), partition in frame.groupby(
        ["statement_type", "fiscal_period"], sort=True,
    ):
        key = (
            f"fundamental:year={year}:statement={statement_type}:"
            f"period={fiscal_period}"
        )
        output.append((key, partition.reset_index(drop=True)))
    return output


def _certify_partition(conn, parent, key: str, bundle: CandidateBundle) -> None:
    child = repository.start_run(
        conn,
        mode="fmp_backfill_partition",
        parent_run_id=parent.run_id,
        partition_key=key,
    )
    results = check_fmp(bundle)
    print_summary(results)
    try:
        assert_publishable(results)
        with conn.transaction():
            _publish(
                conn,
                bundle,
                child,
                None,
                publish_asset_candidates=key in {"asset:all", "asset:commodity"},
            )
            repository.save_metrics(conn, child.run_id, bundle)
            repository.finish_run(
                conn, child, "CERTIFIED", results, commit=False,
            )
    except Exception as exc:
        conn.rollback()
        repository.finish_run(
            conn, child, "FAILED", results, error_message=str(exc),
        )
        raise
    print(f"[silver-fmp] certified partition={key}", flush=True)


def _backfill(
    *,
    src: str,
    fromyear: int | None,
    toyear: int | None,
    resume: str | None,
    skip_assets: bool = False,
) -> None:
    base = base_uri(src)
    fingerprint = _fingerprint(base, None)
    conn = db.connect()
    parent = None
    try:
        repository.assert_schema(conn)
        if resume:
            parent = repository.get_run(conn, UUID(resume))
            if parent.mode != "fmp_backfill":
                raise ValueError(f"not an FMP backfill run: {resume}")
            if parent.input_fingerprint != fingerprint:
                raise RuntimeError("FMP Bronze fingerprint changed; resume refused")
            repository.update_status(conn, parent.run_id, "BUILDING")
        else:
            parent = repository.start_run(
                conn,
                mode="fmp_backfill",
                status="BUILDING",
                input_fingerprint=fingerprint,
            )
        discovered = _discovered_years(base)
        if not discovered and (fromyear is None or toyear is None):
            raise RuntimeError("no FMP backfill years discovered")
        first = fromyear if fromyear is not None else min(discovered)
        last = toyear if toyear is not None else max(discovered)
        if first > last:
            raise ValueError("fromyear must be <= toyear")

        assets, identifiers, universe_stats = fmp.prepare_universe(base)
        profile_records = assets.attrs.pop("research_observations", [])
        fx_assets, fx_identifiers, _, _ = fmp.prepare_fx(base)
        commodity_assets, commodity_identifiers, _, commodity_stats = (
            fmp.prepare_commodities(base)
        )
        assets = pd.concat(
            [assets, fx_assets, commodity_assets], ignore_index=True,
        )
        identifiers = pd.concat(
            [identifiers, fx_identifiers, commodity_identifiers],
            ignore_index=True,
        )
        completed = _completed_partitions(conn, parent.run_id)

        key = "asset:all"
        if not skip_assets and key not in completed:
            _certify_partition(
                conn,
                parent,
                key,
                CandidateBundle(
                    assets=assets,
                    identifiers=identifiers,
                    research_observations=profile_records,
                    stats={
                        "asset": universe_stats,
                        "commodity": commodity_stats,
                        "_source": "FMP",
                    },
                ),
            )

        for year in range(first, last + 1):
            price_key = f"price:year={year}"
            if price_key not in completed:
                stock_prices, price_stats = fmp.prepare_prices(
                    base, assets, identifiers, year=year,
                )
                _, _, fx_prices, fx_stats = fmp.prepare_fx(base, year=year)
                _, _, commodity_prices, commodity_stats = (
                    fmp.prepare_commodities(base, year=year)
                )
                price_frame = pd.concat(
                    [stock_prices, fx_prices, commodity_prices], ignore_index=True,
                )
                if not price_frame.empty:
                    _certify_partition(
                        conn,
                        parent,
                        price_key,
                        CandidateBundle(
                            assets=assets,
                            identifiers=identifiers,
                            prices=price_frame,
                            stats={
                                "asset": universe_stats,
                                "price_daily": price_stats,
                                "fx": fx_stats,
                                "commodity": commodity_stats,
                                "_source": "FMP",
                            },
                        ),
                    )

            fundamentals, stats = fmp.prepare_fundamentals(
                base, identifiers, year=year,
            )
            research_records = fundamentals.attrs.pop("research_observations", [])
            research_key = f"research:financials:year={year}"
            if research_records and research_key not in completed:
                _certify_partition(
                    conn, parent, research_key,
                    CandidateBundle(
                        assets=assets, identifiers=identifiers,
                        research_observations=research_records,
                        stats={"asset": universe_stats, "commodity": commodity_stats,
                               "_source": "FMP"},
                    ),
                )
            for fundamental_key, partition in _fundamental_partitions(
                year, fundamentals,
            ):
                if fundamental_key not in completed:
                    _certify_partition(
                        conn,
                        parent,
                        fundamental_key,
                        CandidateBundle(
                            assets=assets,
                            identifiers=identifiers,
                            fundamentals=partition,
                            stats={
                                "asset": universe_stats,
                                "fundamental": stats,
                                # commodity assets ride in `assets`, so the
                                # commodity provider-list check runs here; give
                                # it the commodity stats or it fails on empty.
                                "commodity": commodity_stats,
                                "_source": "FMP",
                            },
                        ),
                    )

            action_key = f"corporate_action:year={year}"
            if action_key not in completed:
                actions, stats = fmp.prepare_actions(
                    base, identifiers, year=year,
                )
                if not actions.empty:
                    _certify_partition(
                        conn,
                        parent,
                        action_key,
                        CandidateBundle(
                            assets=assets,
                            identifiers=identifiers,
                            actions=actions,
                            stats={
                                "asset": universe_stats,
                                "corporate_action": stats,
                                # see fundamental partition: commodity assets are
                                # in `assets`, so supply commodity stats too.
                                "commodity": commodity_stats,
                                "_source": "FMP",
                            },
                        ),
                    )

        repository.finish_run(conn, parent, "CERTIFIED", [])
        print(f"[silver-fmp] certified backfill run={parent.run_id}", flush=True)
    except Exception as exc:
        if parent is not None:
            conn.rollback()
            repository.finish_run(
                conn, parent, "FAILED", [], error_message=str(exc),
            )
        raise
    finally:
        conn.close()


def _commodity_backfill(*, src: str, fromyear: int, toyear: int) -> None:
    """Publish only the commodity assets/prices from an S3-backed one-off."""
    if fromyear > toyear:
        raise ValueError("fromyear must be <= toyear")
    base = base_uri(src)
    conn = db.connect()
    parent = None
    try:
        repository.assert_schema(conn)
        parent = repository.start_run(
            conn,
            mode="fmp_commodity_backfill",
            status="BUILDING",
            input_fingerprint=_fingerprint(base, None),
        )
        assets, identifiers, _, stats = fmp.prepare_commodities(base)
        _certify_partition(
            conn,
            parent,
            "asset:commodity",
            CandidateBundle(
                assets=assets,
                identifiers=identifiers,
                stats={
                    "commodity": stats,
                    "_source": "FMP",
                    "_source_scope": "commodity",
                },
            ),
        )
        for year in range(fromyear, toyear + 1):
            _, _, prices, year_stats = fmp.prepare_commodities(
                base, year=year,
            )
            if prices.empty:
                continue
            _certify_partition(
                conn,
                parent,
                f"commodity_price:year={year}",
                CandidateBundle(
                    assets=assets,
                    identifiers=identifiers,
                    prices=prices,
                    stats={
                        "commodity": year_stats,
                        "_source": "FMP",
                        "_source_scope": "commodity",
                    },
                ),
            )
        repository.finish_run(conn, parent, "CERTIFIED", [])
        print(
            f"[silver-fmp] certified commodity backfill run={parent.run_id}",
            flush=True,
        )
    except Exception as exc:
        if parent is not None:
            conn.rollback()
            repository.finish_run(
                conn, parent, "FAILED", [], error_message=str(exc),
            )
        raise
    finally:
        conn.close()


def run(
    *,
    src: str = "local",
    day: str | None = None,
    fromyear: int | None = None,
    toyear: int | None = None,
    resume: str | None = None,
    skip_assets: bool = False,
    commodities_only: bool = False,
) -> None:
    if day is not None:
        _daily(src=src, day=day)
        return
    if commodities_only:
        if fromyear is None or toyear is None:
            raise ValueError("commodity backfill requires fromyear and toyear")
        _commodity_backfill(
            src=src, fromyear=fromyear, toyear=toyear,
        )
        return
    _backfill(
        src=src,
        fromyear=fromyear,
        toyear=toyear,
        resume=resume,
        skip_assets=skip_assets,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--mode", choices=("daily", "backfill", "commodities"), required=True,
    )
    parser.add_argument("--date")
    parser.add_argument("--from", dest="fromyear", type=int)
    parser.add_argument("--to", dest="toyear", type=int)
    parser.add_argument("--resume")
    parser.add_argument("--skip-assets", action="store_true")
    parser.add_argument("--src", choices=("local",), default="local")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.mode == "daily" and not args.date:
        raise SystemExit("daily mode requires --date")
    run(
        src=args.src,
        day=args.date if args.mode == "daily" else None,
        fromyear=args.fromyear,
        toyear=args.toyear,
        resume=args.resume,
        skip_assets=args.skip_assets,
        commodities_only=args.mode == "commodities",
    )


if __name__ == "__main__":
    main()
