import hashlib
import json
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from pipeline.gold import run
from pipeline.gold.register_daily import candidate_rows
from pipeline.gold.run import build_replace_sql, validate_contract, validate_query_sql


ROOT = Path(__file__).parents[2]
MANIFEST = json.loads(
    (ROOT / "pipeline/gold/factors/manifest.json").read_text(encoding="utf-8")
)


def test_allowlisted_factor_sql_files_exist_and_have_stable_hashes():
    assert set(MANIFEST) == {
        "market_leverage",
        "operating_return_on_capital_employed",
        "paid_in_capital_ratio",
        "return_kurtosis_24m",
        "trading_turnover_20d",
        "turnover_volatility_12m",
    }
    for spec in MANIFEST.values():
        path = ROOT / spec["sql"]
        assert path.is_file()
        assert len(hashlib.sha256(path.read_bytes()).hexdigest()) == 64
        assert spec["value_contract"] == "raw_value_direction_adjusted_rank_v1"
        assert spec["frequency"] == "daily"
        assert spec["version"] == 2
        assert len(spec["research_definition_hash"]) == 16


def test_daily_candidate_metadata_is_complete_and_immutable():
    rows = candidate_rows()
    assert {row["factor_key"] for row in rows} == set(MANIFEST)
    assert all(row["version"] == 2 for row in rows)
    assert all(row["config"]["frequency"] == "daily" for row in rows)
    assert all(len(row["implementation_hash"]) == 64 for row in rows)


def test_negative_sign_factors_return_raw_values_and_rank_low_raw_first():
    for name in (
        "paid_in_capital_ratio",
        "return_kurtosis_24m",
        "trading_turnover_20d",
        "turnover_volatility_12m",
    ):
        spec = MANIFEST[name]
        sql = (ROOT / spec["sql"]).read_text(encoding="utf-8")
        assert spec["predicted_sign"] == -1
        assert "ORDER BY value ASC" in sql
        assert "%(start_date)s" in sql
        assert "%(end_date)s" in sql
        assert "PARTITION BY signal_date" in sql
        assert "INSERT INTO" not in sql
        validate_query_sql(sql)


def test_positive_sign_factors_rank_high_raw_first():
    for name in ("market_leverage", "operating_return_on_capital_employed"):
        spec = MANIFEST[name]
        sql = (ROOT / spec["sql"]).read_text(encoding="utf-8")
        assert spec["predicted_sign"] == 1
        assert "ORDER BY value DESC" in sql
        validate_query_sql(sql)


def test_all_v2_factor_outputs_are_ranked_by_trading_date():
    for spec in MANIFEST.values():
        sql = (ROOT / spec["sql"]).read_text(encoding="utf-8")
        assert "%(start_date)s" in sql
        assert "%(end_date)s" in sql
        assert "PARTITION BY signal_date" in sql
        assert "date_trunc('month'" not in sql
        assert "month_rank" not in sql


def test_factor_sql_rejects_gold_or_current_state_relations():
    template = (
        "SELECT asset_id, as_of_date, value, rank FROM {relation} "
        "WHERE as_of_date BETWEEN %(start_date)s AND %(end_date)s"
    )
    for relation in ("gold.factor_value", "public.fundamental_current"):
        try:
            validate_query_sql(template.format(relation=relation))
        except ValueError as exc:
            assert "Silver relation" in str(exc)
        else:
            raise AssertionError(f"forbidden relation accepted: {relation}")


def test_runner_atomically_replaces_exact_daily_partitions():
    spec = MANIFEST["trading_turnover_20d"]
    query = (ROOT / spec["sql"]).read_text(encoding="utf-8")
    wrapped = build_replace_sql(query)

    assert query.strip().removesuffix(";") in wrapped
    assert "CREATE TEMP TABLE _gold_factor_values ON COMMIT DROP" in wrapped
    assert "DELETE FROM gold.factor_value" in wrapped
    assert "as_of_date BETWEEN %(start_date)s AND %(end_date)s" in wrapped
    assert "INSERT INTO gold.factor_value" in wrapped
    assert "ON CONFLICT" not in wrapped


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


def test_market_leverage_replays_pit_total_liabilities():
    sql = (ROOT / MANIFEST["market_leverage"]["sql"]).read_text(
        encoding="utf-8"
    )
    assert "f.available_date <= u.as_of_date" in sql
    assert "f.metric = 'total_liabilities'" in sql
    assert "value::double precision / market_cap AS value" in sql
    assert "value >= 0" in sql
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


def test_new_factors_preserve_pit_and_rolling_contracts():
    roce = (ROOT / MANIFEST["operating_return_on_capital_employed"]["sql"]).read_text(
        encoding="utf-8"
    )
    kurtosis = (ROOT / MANIFEST["return_kurtosis_24m"]["sql"]).read_text(
        encoding="utf-8"
    )
    turnover_volatility = (
        ROOT / MANIFEST["turnover_volatility_12m"]["sql"]
    ).read_text(encoding="utf-8")

    assert "f.available_date <= u.as_of_date" in roce
    assert "fy.fy_end - interval '370 days'" in roce
    assert "ROWS BETWEEN 251 PRECEDING AND CURRENT ROW" in turnover_volatility
    assert "stddev_samp(log_turnover)" in turnover_volatility
    assert "LIMIT 504" in kurtosis
    assert "daily_return" in kurtosis
    assert "sample_variance" in kurtosis
    assert "sample_variance = 0 THEN -3.0" in kurtosis
    assert MANIFEST["return_kurtosis_24m"]["parity_atol"] == 5e-6
    assert MANIFEST["return_kurtosis_24m"]["parity_rtol"] == 2e-7
    assert MANIFEST["return_kurtosis_24m"]["allow_tolerance_equivalent_ranks"] is True


def test_runner_accepts_structured_publisher_contract():
    spec = MANIFEST["trading_turnover_20d"]
    path = ROOT / spec["sql"]
    metadata = {
        "version": 2,
        "status": "APPROVED",
        "implementation_uri": f"repo://TeamAlpha-data/{spec['sql']}",
        "implementation_hash": hashlib.sha256(path.read_bytes()).hexdigest(),
        "config": {
            "predicted_sign": -1,
            "research_definition_hash": spec["research_definition_hash"],
            "frequency": "daily",
            "value_contract": {"id": spec["value_contract"]},
        },
    }

    validate_contract(metadata, spec, path)


def test_runner_rejects_a_different_research_definition():
    spec = MANIFEST["trading_turnover_20d"]
    path = ROOT / spec["sql"]
    metadata = {
        "version": 2,
        "status": "APPROVED",
        "implementation_uri": f"repo://TeamAlpha-data/{spec['sql']}",
        "implementation_hash": hashlib.sha256(path.read_bytes()).hexdigest(),
        "config": {
            "predicted_sign": spec["predicted_sign"],
            "research_definition_hash": "different",
            "frequency": "daily",
            "value_contract": {"id": spec["value_contract"]},
        },
    }

    try:
        validate_contract(metadata, spec, path)
    except ValueError as exc:
        assert "research_definition_hash" in str(exc)
    else:
        raise AssertionError("definition mismatch must fail")


def test_runner_binds_exact_daily_range_and_commits(monkeypatch):
    spec = MANIFEST["trading_turnover_20d"]
    path = ROOT / spec["sql"]
    metadata = {
        "factor_id": 42,
        "factor_key": "trading_turnover_20d",
        "version": 2,
        "status": "APPROVED",
        "implementation_uri": f"repo://TeamAlpha-data/{spec['sql']}",
        "implementation_hash": hashlib.sha256(path.read_bytes()).hexdigest(),
        "config": {
            "predicted_sign": -1,
            "research_definition_hash": spec["research_definition_hash"],
            "frequency": "daily",
            "value_contract": {"id": spec["value_contract"]},
        },
    }
    monkeypatch.setattr(run, "_load_factor", lambda *_args: metadata)
    conn = MagicMock()
    cursor = conn.cursor.return_value.__enter__.return_value
    cursor.rowcount = 123
    cursor.fetchone.return_value = (123, 123, 0)

    affected = run.run_factor(
        conn,
        factor_key="trading_turnover_20d",
        start_date="2026-09-01",
        end_date="2026-09-10",
        apply=True,
    )

    assert affected == 123
    params = cursor.execute.call_args.args[1]
    assert params["factor_id"] == 42
    assert params["start_date"] == date(2026, 9, 1)
    assert params["end_date"] == date(2026, 9, 10)
    conn.commit.assert_called_once_with()
    conn.rollback.assert_not_called()


def test_runner_rejects_reversed_daily_range_before_db_work():
    with pytest.raises(ValueError, match="precedes"):
        run.run_factor(
            MagicMock(),
            factor_key="trading_turnover_20d",
            start_date="2026-09-10",
            end_date="2026-09-01",
            apply=False,
        )
