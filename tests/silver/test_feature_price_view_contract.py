import json
from pathlib import Path

from pipeline.silver.return_contract import CONTRACT_RELEASE


ROOT = Path(__file__).resolve().parents[2]


def test_feature_view_excludes_ex_post_total_return_columns():
    schema = (ROOT / "sql/schema.sql").read_text(encoding="utf-8")
    view = schema.split(
        "CREATE OR REPLACE VIEW factor_price_feature_daily AS", 1
    )[1].split(";", 1)[0]
    assert "adj_close" in view
    assert "total_return_close" not in view
    assert "total_return_quality_run_id" not in view


def test_manifest_gold_sql_uses_only_feature_price_view():
    manifest = json.loads(
        (ROOT / "pipeline/gold/factors/manifest.json").read_text(
            encoding="utf-8"
        )
    )
    assert len(manifest) == 7
    for spec in manifest.values():
        sql = (ROOT / spec["sql"]).read_text(encoding="utf-8")
        assert spec["feature_price_field"] == "adj_close"
        assert "public.factor_price_feature_daily" in sql
        assert "public.price_daily" not in sql
        assert "total_return_close" not in sql


def test_migration_preserves_a_new_final_release_on_reapply():
    migration = (
        ROOT
        / "pipeline/silver_quality/migrations/010_cash_adjustment_scale_evidence.sql"
    ).read_text(encoding="utf-8")
    invalidation = migration.split(
        "UPDATE price_return_contract", 1
    )[1]
    assert "status='CERTIFIED'" in invalidation
    assert "metadata->>'contract_release' IS DISTINCT FROM" in invalidation
    assert CONTRACT_RELEASE in invalidation


def test_source_receipt_schema_idempotently_admits_krx_alphanumeric_codes():
    schema = (ROOT / "sql/schema.sql").read_text(encoding="utf-8")
    migration = (
        ROOT / "pipeline/silver_quality/migrations/009_krx_total_return.sql"
    ).read_text(encoding="utf-8")
    for definition in (schema, migration):
        assert "CHECK (ticker ~ '^[0-9A-Z]{6}$')" in definition
        assert "CHECK (ticker ~ '^[0-9]{6}$')" not in definition
        assert (
            "DROP CONSTRAINT IF EXISTS "
            "dividend_source_receipt_ticker_check"
        ) in definition
        assert (
            "ADD CONSTRAINT dividend_source_receipt_ticker_check"
        ) in definition

    rebuild = (
        ROOT / "pipeline/silver/total_return_rebuild.py"
    ).read_text(encoding="utf-8")
    assert "pg_get_constraintdef" in rebuild
    assert "KRX_TICKER_REGEX" in rebuild


def test_cash_scale_support_ratio_precision_preserves_reviewed_terms():
    schema = (ROOT / "sql/schema.sql").read_text(encoding="utf-8")
    migration = (
        ROOT / "pipeline/silver_quality/migrations/"
        "011_cash_scale_ratio_precision.sql"
    ).read_text(encoding="utf-8")
    assert "support_ratio_numerator NUMERIC(28,12)" in schema
    assert "support_ratio_denominator NUMERIC(28,12)" in schema
    assert "support_ratio_numerator TYPE NUMERIC(28,12)" in migration
    assert "support_ratio_denominator TYPE NUMERIC(28,12)" in migration


def test_corporate_action_ratio_precision_matches_support_rows():
    schema = (ROOT / "sql/schema.sql").read_text(encoding="utf-8")
    migration = (
        ROOT / "pipeline/silver_quality/migrations/"
        "012_corporate_action_ratio_precision.sql"
    ).read_text(encoding="utf-8")
    assert "ratio_numerator NUMERIC(28,12)" in schema
    assert "ratio_denominator NUMERIC(28,12)" in schema
    assert migration.count("ratio_numerator TYPE NUMERIC(28,12)") == 2
    assert migration.count("ratio_denominator TYPE NUMERIC(28,12)") == 2
