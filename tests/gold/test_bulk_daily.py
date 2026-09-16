from pipeline.gold import bulk_daily
from unittest.mock import MagicMock


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


def test_daily_panel_does_not_survive_transaction(monkeypatch):
    conn = MagicMock()
    cursor = conn.cursor.return_value.__enter__.return_value
    cursor.rowcount = 1
    monkeypatch.setattr(bulk_daily, "_load_factor_ids", lambda *a, **kw: [])
    monkeypatch.setattr(bulk_daily, "_create_factor_ids", lambda *a: None)
    monkeypatch.setattr(bulk_daily, "_validate_stage", lambda *a: 1)
    monkeypatch.setattr(bulk_daily, "_statements", lambda: [
        "CREATE TEMP TABLE _gold_price_base ON COMMIT PRESERVE ROWS AS SELECT 1"
    ])
    bulk_daily.run_bulk(conn, start_date="2026-09-14", end_date="2026-09-14", apply=True)
    executed = [call.args[0] for call in cursor.execute.call_args_list]
    assert any("_gold_price_base ON COMMIT DROP" in sql for sql in executed)
    assert not any("PRESERVE ROWS" in sql for sql in executed)
