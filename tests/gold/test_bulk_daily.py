from pipeline.gold import bulk_daily


def test_bulk_sql_covers_every_legacy_factor_once():
    sql = bulk_daily.SQL_PATH.read_text(encoding="utf-8")
    assert len(bulk_daily.LEGACY_FACTOR_KEYS) == 43
    assert len(set(bulk_daily.LEGACY_FACTOR_KEYS)) == 43
    for factor_key in bulk_daily.LEGACY_FACTOR_KEYS:
        assert sql.count(f"('{factor_key}',") == 1


def test_bulk_sql_uses_one_shared_daily_panel():
    sql = bulk_daily.SQL_PATH.read_text(encoding="utf-8")
    assert "21 KRX sessions per month" in sql
    assert "_gold_daily_panel" in sql
    assert "PARTITION BY r.factor_key,r.as_of_date" in sql
    assert "asset_last_valid_trading_day_in_signal_month" not in sql


def test_daily_definition_hash_changes_with_implementation():
    first = bulk_daily._daily_definition_hash("old", "sql-a")
    second = bulk_daily._daily_definition_hash("old", "sql-b")
    assert len(first) == 16
    assert first != second
