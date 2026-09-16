"""Register and compute legacy Gold factors as one shared daily batch."""
from __future__ import annotations

import argparse
import hashlib
import json
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from pipeline.common import db
from pipeline.gold.run import ROOT, _parse_date


SQL_PATH = ROOT / "pipeline/gold/factors/legacy_daily_bulk.sql"
IMPLEMENTATION_URI = "repo://TeamAlpha-data/pipeline/gold/factors/legacy_daily_bulk.sql"
LEGACY_FACTOR_KEYS = (
    "adv20_to_book_equity",
    "asset_to_market",
    "book_to_market_change_12m",
    "book_to_market_change_6m",
    "capital_stock_growth_18m",
    "capital_stock_to_assets",
    "close_position_mean_12m",
    "current_asset_turnover",
    "current_liabilities_to_sales",
    "enterprise_sales_yield_change_6m",
    "idiosyncratic_volatility_24m",
    "market_cap_instability_24m",
    "max_daily_return_1m",
    "max_daily_return_instability_18m",
    "max_daily_return_mean_6m",
    "momentum_12_1",
    "net_equity_issuance_price_adjusted_12m",
    "net_equity_issuance_price_adjusted_36m",
    "net_income_to_liabilities",
    "net_margin_volatility_12m",
    "net_working_capital_yield",
    "nonoperating_burden_margin",
    "open_close_drift_12m",
    "operating_earnings_yield",
    "operating_income_to_current_liabilities",
    "operating_income_to_liabilities",
    "overnight_gap_mean_6m",
    "overnight_gap_volatility_12m",
    "pretax_yield_change_6m",
    "price_high_gap_volatility_24m",
    "price_range_12m",
    "realized_daily_volatility_change_24m",
    "realized_daily_volatility_instability_6m",
    "realized_volatility_252d",
    "retained_earnings_to_assets_volatility_12m",
    "retained_earnings_to_equity",
    "return_skewness_12m",
    "return_skewness_36m",
    "revenue_scale",
    "revenue_to_noncurrent_assets",
    "shares_to_capital_stock",
    "share_turnover_change_12m",
    "share_turnover_change_6m",
)


def _implementation_hash() -> str:
    return hashlib.sha256(SQL_PATH.read_bytes()).hexdigest()


def _daily_definition_hash(old_hash: str, implementation_hash: str) -> str:
    payload = (
        f"{old_hash}|daily|21_sessions_per_month|{implementation_hash}"
    )
    return hashlib.sha256(payload.encode()).hexdigest()[:16]


def register_and_promote(conn, *, approved_by: str, apply: bool) -> int:
    """Create immutable daily successors for all legacy non-rejected keys."""
    if not approved_by.strip():
        raise ValueError("approved_by must not be empty")
    sql_hash = _implementation_hash()
    changed = 0
    with conn.transaction(force_rollback=not apply):
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT ON (factor_key)
                       factor_id,factor_key,version,description,config,status
                FROM gold.factor
                WHERE factor_key=ANY(%s) AND status<>'REJECTED'
                ORDER BY factor_key,version DESC
                """,
                (list(LEGACY_FACTOR_KEYS),),
            )
            sources = {row[1]: row for row in cur.fetchall()}
            missing = sorted(set(LEGACY_FACTOR_KEYS) - set(sources))
            if missing:
                raise RuntimeError(f"legacy Gold metadata missing: {missing}")

            for factor_key in LEGACY_FACTOR_KEYS:
                _, _, source_version, description, old_config, _ = sources[factor_key]
                cur.execute(
                    """
                    SELECT factor_id,version,status,config
                    FROM gold.factor
                    WHERE factor_key=%s
                      AND implementation_uri=%s
                      AND implementation_hash=%s
                    ORDER BY version DESC LIMIT 1
                    """,
                    (factor_key, IMPLEMENTATION_URI, sql_hash),
                )
                existing = cur.fetchone()
                if existing and existing[2] == "APPROVED":
                    existing_config = existing[3]
                    if existing_config.get("predicted_sign") in (-1, 1):
                        continue

                cur.execute(
                    "SELECT coalesce(max(version),0)+1 FROM gold.factor WHERE factor_key=%s",
                    (factor_key,),
                )
                version = cur.fetchone()[0]
                config = dict(old_config)
                old_definition = str(config.get("research_definition_hash", ""))
                predicted_sign = int(config.get("predicted_sign", 1))
                config.update({
                    "frequency": "daily",
                    "predicted_sign": predicted_sign,
                    "lookback_translation": "21_krx_sessions_per_month",
                    "research_definition_hash": _daily_definition_hash(
                        old_definition, sql_hash
                    ),
                    "value_contract": {
                        "id": "raw_value_direction_adjusted_rank_v1",
                        "value": "raw",
                        "score": "value*predicted_sign",
                        "predicted_sign": predicted_sign,
                        "as_of_date": "krx_trading_day",
                        "rank_partition": "factor_trading_day_full_universe",
                    },
                    "supersedes_version": int(source_version),
                })
                evaluation = {
                    "passed": True,
                    "verdict": "APPROVED",
                    "approved_by": approved_by,
                    "approved_at": datetime.now(timezone.utc).isoformat(),
                    "approval_scope": "all_non_rejected_daily_migration",
                }
                cur.execute(
                    """
                    UPDATE gold.factor SET status='RETIRED'
                    WHERE factor_key=%s AND status='APPROVED'
                    """,
                    (factor_key,),
                )
                cur.execute(
                    """
                    INSERT INTO gold.factor(
                      factor_key,version,description,implementation_uri,
                      implementation_hash,config,evaluation,status
                    ) VALUES (%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb,'APPROVED')
                    """,
                    (
                        factor_key, version, description, IMPLEMENTATION_URI,
                        sql_hash, json.dumps(config), json.dumps(evaluation),
                    ),
                )
                changed += 1
    return changed


def _load_factor_ids(
    conn, *, allow_legacy_contracts: bool = False
) -> list[tuple[int, str, int]]:
    sql_hash = _implementation_hash()
    with conn.cursor() as cur:
        if allow_legacy_contracts:
            cur.execute(
                """
                SELECT DISTINCT ON (factor_key)
                       factor_id,factor_key,
                       coalesce((config->>'predicted_sign')::integer,1)
                FROM gold.factor
                WHERE status<>'REJECTED' AND factor_key=ANY(%s)
                ORDER BY factor_key,version DESC
                """,
                (list(LEGACY_FACTOR_KEYS),),
            )
        else:
            cur.execute(
                """
            SELECT factor_id,factor_key,(config->>'predicted_sign')::integer
            FROM gold.factor
            WHERE status='APPROVED' AND factor_key=ANY(%s)
              AND config->>'frequency'='daily'
              AND implementation_uri=%s AND implementation_hash=%s
            ORDER BY factor_key
                """,
                (list(LEGACY_FACTOR_KEYS), IMPLEMENTATION_URI, sql_hash),
            )
        rows = cur.fetchall()
    if len(rows) != len(LEGACY_FACTOR_KEYS):
        found = {row[1] for row in rows}
        missing = sorted(set(LEGACY_FACTOR_KEYS) - found)
        raise RuntimeError(f"approved legacy daily contracts missing: {missing}")
    return rows


def _statements() -> list[str]:
    parts = SQL_PATH.read_text(encoding="utf-8").split("-- gold-statement")
    return [part.strip() for part in parts[1:] if part.strip()]


def _configure_session(cur) -> None:
    cur.execute("SET LOCAL work_mem='256MB'")
    cur.execute("SET LOCAL maintenance_work_mem='512MB'")
    # temp_buffers cannot be changed after this session has used any temp
    # table (Silver and individual factors already do). Keep the connection
    # default; changing it here aborts the whole bulk transaction.


def _create_factor_ids(cur, factor_rows, *, preserve: bool = False) -> None:
    on_commit = "PRESERVE ROWS" if preserve else "DROP"
    cur.execute(
        f"""
        CREATE TEMP TABLE _gold_factor_ids(
          factor_id bigint PRIMARY KEY,
          factor_key text UNIQUE NOT NULL,
          predicted_sign integer NOT NULL CHECK(predicted_sign IN (-1,1))
        ) ON COMMIT {on_commit}
        """
    )
    cur.executemany(
        "INSERT INTO _gold_factor_ids VALUES (%s,%s,%s)", factor_rows
    )


def _validate_stage(cur) -> int:
    cur.execute(
        """
        SELECT count(*),count(DISTINCT (factor_id,asset_id,as_of_date)),
               count(*) FILTER (WHERE value::text IN
                 ('NaN','Infinity','-Infinity') OR rank<=0)
        FROM _gold_bulk_values
        """
    )
    rows, distinct_rows, invalid = cur.fetchone()
    if rows != distinct_rows or invalid:
        raise RuntimeError(
            "bulk Gold quality failed: "
            f"rows={rows} distinct={distinct_rows} invalid={invalid}"
        )
    return rows


def run_bulk(
    conn,
    *,
    start_date: date | str,
    end_date: date | str | None = None,
    apply: bool,
    validate_only: bool = False,
) -> int:
    start = _parse_date(start_date)
    end = _parse_date(end_date or start)
    if end < start:
        raise ValueError("Gold end_date precedes start_date")
    params = {"start_date": start, "end_date": end}
    with conn.transaction(force_rollback=(not apply) or validate_only):
        factor_rows = _load_factor_ids(
            conn, allow_legacy_contracts=validate_only
        )
        with conn.cursor() as cur:
            # The production RDS default is deliberately small and makes the
            # shared asset/date window sorts spill excessively.  This task is
            # the only Gold writer, so use a bounded session-local allowance.
            _configure_session(cur)
            _create_factor_ids(cur, factor_rows)
            statements = _statements()
            for number, statement in enumerate(statements, 1):
                stage_started = time.monotonic()
                print(
                    f"[gold-bulk] stage={number}/{len(statements)} "
                    f"range={start}..{end}",
                    flush=True,
                )
                # Shared backfill preserves the panel across chunks. Daily
                # invocations do not: gap replay uses the same connection on
                # consecutive days, so stale temp tables must not survive.
                daily_statement = statement.replace(
                    "ON COMMIT PRESERVE ROWS", "ON COMMIT DROP"
                )
                cur.execute(daily_statement, params)
                print(
                    f"[gold-bulk] stage={number}/{len(statements)} done "
                    f"seconds={time.monotonic() - stage_started:.1f}",
                    flush=True,
                )
            rows = _validate_stage(cur)
            if validate_only:
                return rows
            cur.execute(
                """
                DELETE FROM gold.factor_value v USING _gold_factor_ids f
                WHERE v.factor_id=f.factor_id
                  AND v.as_of_date BETWEEN %(start_date)s AND %(end_date)s
                """,
                params,
            )
            cur.execute(
                """
                INSERT INTO gold.factor_value(factor_id,asset_id,as_of_date,value,rank)
                SELECT factor_id,asset_id,as_of_date,value,rank
                FROM _gold_bulk_values
                """
            )
            affected = max(cur.rowcount, 0)
    return affected


def _year_chunks(start: date, end: date):
    cursor = start
    while cursor <= end:
        chunk_end = min(end, date(cursor.year, 12, 31))
        yield cursor, chunk_end
        cursor = chunk_end + timedelta(days=1)


def run_shared_backfill(
    conn,
    *,
    start_date: date | str,
    end_date: date | str,
    apply: bool,
) -> int:
    """Build the expensive history panel once, then commit yearly outputs."""
    start = _parse_date(start_date)
    end = _parse_date(end_date)
    if end < start:
        raise ValueError("Gold end_date precedes start_date")
    statements = _statements()
    prep, output = statements[:-1], statements[-1]
    full_params = {"start_date": start, "end_date": end}

    with conn.transaction():
        factor_rows = _load_factor_ids(conn)
        with conn.cursor() as cur:
            _configure_session(cur)
            _create_factor_ids(cur, factor_rows, preserve=True)
            for number, statement in enumerate(prep, 1):
                stage_started = time.monotonic()
                print(
                    f"[gold-backfill] prep={number}/{len(prep)} "
                    f"range={start}..{end}", flush=True,
                )
                cur.execute(statement, full_params)
                print(
                    f"[gold-backfill] prep={number}/{len(prep)} done "
                    f"seconds={time.monotonic() - stage_started:.1f}",
                    flush=True,
                )
            # Only the final daily panel and factor map are needed while the
            # yearly partitions are published.  Release the wide intermediates
            # before permanent factor_value growth consumes their disk space.
            cur.execute(
                """
                DROP TABLE _gold_price_base,_gold_price_roll_1,
                  _gold_market_returns,_gold_price_roll_2,
                  _gold_financial_states,_gold_daily_panel_1
                """
            )

    total = 0
    for chunk_start, chunk_end in _year_chunks(start, end):
        params = {"start_date": chunk_start, "end_date": chunk_end}
        with conn.transaction(force_rollback=not apply):
            with conn.cursor() as cur:
                cur.execute("SET LOCAL work_mem='256MB'")
                stage_started = time.monotonic()
                print(
                    f"[gold-backfill] publish={chunk_start}..{chunk_end}",
                    flush=True,
                )
                cur.execute(output, params)
                rows = _validate_stage(cur)
                cur.execute(
                    """
                    DELETE FROM gold.factor_value v USING _gold_factor_ids f
                    WHERE v.factor_id=f.factor_id
                      AND v.as_of_date BETWEEN %(start_date)s AND %(end_date)s
                    """,
                    params,
                )
                cur.execute(
                    """
                    INSERT INTO gold.factor_value(
                      factor_id,asset_id,as_of_date,value,rank
                    ) SELECT factor_id,asset_id,as_of_date,value,rank
                      FROM _gold_bulk_values
                    """
                )
                total += max(cur.rowcount, 0)
                print(
                    f"[gold-backfill] publish={chunk_start}..{chunk_end} "
                    f"rows={rows:,} seconds={time.monotonic() - stage_started:.1f}",
                    flush=True,
                )
    return total


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--register-and-promote", action="store_true")
    parser.add_argument("--approved-by", default="explicit_user_request")
    scope = parser.add_mutually_exclusive_group()
    scope.add_argument("--as-of-date")
    scope.add_argument("--from-date")
    parser.add_argument("--to-date")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--validate-only", action="store_true")
    parser.add_argument("--shared-backfill", action="store_true")
    args = parser.parse_args()
    if args.from_date and not args.to_date:
        parser.error("--from-date requires --to-date")
    if not args.register_and_promote and not (args.as_of_date or args.from_date):
        parser.error("choose registration or a date scope")
    conn = db.connect()
    try:
        if args.register_and_promote:
            changed = register_and_promote(
                conn, approved_by=args.approved_by, apply=args.apply
            )
            print(f"legacy daily promoted={changed}")
        if args.as_of_date or args.from_date:
            runner = run_shared_backfill if args.shared_backfill else run_bulk
            affected = runner(
                conn, start_date=args.as_of_date or args.from_date,
                end_date=args.as_of_date or args.to_date, apply=args.apply,
                **({} if args.shared_backfill else {
                    "validate_only": args.validate_only
                }),
            )
            print(f"legacy daily rows={affected:,}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
