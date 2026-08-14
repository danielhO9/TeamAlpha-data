import hashlib
import json
from pathlib import Path

from pipeline.gold.run import build_upsert_sql, validate_contract, validate_query_sql


ROOT = Path(__file__).parents[2]
MANIFEST = json.loads(
    (ROOT / "pipeline/gold/factors/manifest.json").read_text(encoding="utf-8")
)

EXPECTED_DEFINITIONS = {
    "amihud_illiquidity_1m": (1, "72bd57d66a5cb84d"),
    "max_daily_return_1m": (-1, "e29c3da27f06a3ba"),
    "net_equity_issuance_price_adjusted_12m": (-1, "01ee73e28cd8f170"),
    "operating_income_to_liabilities": (1, "5ff8c69343b28a3f"),
    "paid_in_capital_ratio": (-1, "8c82db0117290bcd"),
    "realized_volatility_252d": (-1, "e0668fb0e7c0eb69"),
    "trading_turnover_20d": (-1, "c03efb8638407bd6"),
}


def test_allowlisted_factor_sql_files_exist_and_have_stable_hashes():
    assert set(MANIFEST) == set(EXPECTED_DEFINITIONS)
    for factor_name, spec in MANIFEST.items():
        path = ROOT / spec["sql"]
        assert path.is_file()
        assert len(hashlib.sha256(path.read_bytes()).hexdigest()) == 64
        assert spec["value_contract"] == "raw_value_direction_adjusted_rank_v1"
        assert spec["feature_price_field"] == "adj_close"
        expected_sign, expected_hash = EXPECTED_DEFINITIONS[factor_name]
        assert spec["predicted_sign"] == expected_sign
        assert spec["research_definition_hash"] == expected_hash


def test_factors_return_raw_values_and_rank_in_predicted_direction():
    for spec in MANIFEST.values():
        sql = (ROOT / spec["sql"]).read_text(encoding="utf-8")
        rank_order = "DESC" if spec["predicted_sign"] == 1 else "ASC"
        assert f"ORDER BY value {rank_order}" in sql
        assert "%(start_month)s" in sql
        assert "%(end_month)s" in sql
        assert "INSERT INTO" not in sql
        assert "total_return_close" not in sql
        assert "public.price_daily" not in sql
        assert "adj_close > 0" in sql
        validate_query_sql(sql)


def test_factor_sql_rejects_gold_or_current_state_relations():
    template = (
        "SELECT asset_id, as_of_date, value, rank FROM {relation} "
        "WHERE as_of_date BETWEEN %(start_month)s AND %(end_month)s"
    )
    for relation in ("gold.factor_value", "public.fundamental_current"):
        try:
            validate_query_sql(template.format(relation=relation))
        except ValueError as exc:
            assert "Silver relation" in str(exc)
        else:
            raise AssertionError(f"forbidden relation accepted: {relation}")


def test_runner_wraps_the_same_read_only_query_for_gold_upsert():
    spec = MANIFEST["trading_turnover_20d"]
    query = (ROOT / spec["sql"]).read_text(encoding="utf-8")
    wrapped = build_upsert_sql(query)

    assert query.strip().removesuffix(";") in wrapped
    assert "INSERT INTO gold.factor_value" in wrapped
    assert "ON CONFLICT (factor_id, asset_id, as_of_date)" in wrapped


def test_paid_in_capital_is_point_in_time_and_not_current_state():
    sql = (ROOT / MANIFEST["paid_in_capital_ratio"]["sql"]).read_text(
        encoding="utf-8"
    )
    assert "f.available_date <= u.as_of_date" in sql
    assert "q.status = 'CERTIFIED'" in sql
    body = "\n".join(
        line for line in sql.splitlines() if not line.lstrip().startswith("--")
    )
    assert "fundamental_current" not in body


def test_turnover_uses_current_plus_previous_nineteen_rows():
    sql = (ROOT / MANIFEST["trading_turnover_20d"]["sql"]).read_text(
        encoding="utf-8"
    )
    assert "ROWS BETWEEN 19 PRECEDING AND CURRENT ROW" in sql
    assert "adv20 > 0" not in sql


def test_daily_price_factors_match_research_windows_and_minimum_counts():
    snippets = {
        "amihud_illiquidity_1m": (
            "avg(abs(daily_price_return) / trading_value) FILTER",
            "amihud_observations_1m >= 10",
        ),
        "max_daily_return_1m": (
            "max(daily_price_return) OVER",
            "max_daily_return_observations_1m >= 10",
        ),
        "realized_volatility_252d": (
            "stddev_samp(daily_price_return) OVER",
            "ROWS BETWEEN 251 PRECEDING AND CURRENT ROW",
            "daily_return_observations_252d >= 126",
        ),
    }
    for factor_name, required in snippets.items():
        sql = (ROOT / MANIFEST[factor_name]["sql"]).read_text(encoding="utf-8")
        assert "trade_date >= DATE '2015-01-01'" in sql
        assert "certified_feature_price" in sql
        for snippet in required:
            assert snippet in sql


def test_net_equity_issuance_uses_price_adjusted_exact_calendar_lag():
    sql = (
        ROOT / MANIFEST["net_equity_issuance_price_adjusted_12m"]["sql"]
    ).read_text(encoding="utf-8")

    assert "market_cap::double precision / adj_close::double precision" in sql
    assert "lag(adjusted_share_base, 12)" in sql
    assert "lag(signal_month, 12)" in sql
    assert "prior_adjusted_share_base > 0" in sql
    assert "signal_month = prior_signal_month + INTERVAL '12 months'" in sql


def test_operating_coverage_replays_pit_revisions_and_builds_four_quarters():
    sql = (
        ROOT / MANIFEST["operating_income_to_liabilities"]["sql"]
    ).read_text(encoding="utf-8")

    assert "f.available_date <= u.as_of_date" in sql
    assert "(f.fs_type = 'CFS') DESC" in sql
    assert "f.revision_key DESC" in sql
    assert "f.value IS NOT NULL" in sql
    assert "q.status = 'CERTIFIED'" in sql
    assert "f.metric IN ('operating_income', 'total_liabilities')" in sql
    assert "fy.fy_end - INTERVAL '370 days'" in sql
    assert "HAVING count(DISTINCT fiscal_period) = 3" in sql
    assert "WHERE recent_rank <= 4" in sql
    assert "HAVING count(*) = 4" in sql
    assert "max(period_end) - min(period_end) <= 370" in sql
    assert "latest_liabilities.total_liabilities > 0" in sql
    assert "fundamental_current" not in sql


def test_runner_accepts_structured_publisher_contract():
    spec = MANIFEST["trading_turnover_20d"]
    path = ROOT / spec["sql"]
    metadata = {
        "status": "APPROVED",
        "implementation_uri": f"repo://TeamAlpha-data/{spec['sql']}",
        "implementation_hash": hashlib.sha256(path.read_bytes()).hexdigest(),
        "config": {
            "predicted_sign": -1,
            "research_definition_hash": spec["research_definition_hash"],
            "value_contract": {"id": spec["value_contract"]},
        },
    }

    validate_contract(metadata, spec, path)


def test_runner_rejects_a_different_research_definition():
    spec = MANIFEST["trading_turnover_20d"]
    path = ROOT / spec["sql"]
    metadata = {
        "status": "APPROVED",
        "implementation_uri": f"repo://TeamAlpha-data/{spec['sql']}",
        "implementation_hash": hashlib.sha256(path.read_bytes()).hexdigest(),
        "config": {
            "predicted_sign": spec["predicted_sign"],
            "research_definition_hash": "different",
            "value_contract": {"id": spec["value_contract"]},
        },
    }

    try:
        validate_contract(metadata, spec, path)
    except ValueError as exc:
        assert "research_definition_hash" in str(exc)
    else:
        raise AssertionError("definition mismatch must fail")
