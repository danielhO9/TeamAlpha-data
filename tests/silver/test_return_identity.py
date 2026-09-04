from datetime import date

import pandas as pd
import pytest

from pipeline.silver import return_identity
from pipeline.silver.return_identity import map_actions_to_pit_assets


class _Cursor:
    def __init__(self, rows):
        self.rows = rows
        self.statements = []

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def execute(self, sql, params=None):
        self.statements.append((sql, params))

    def fetchall(self):
        return self.rows


class _Connection:
    def __init__(self, rows):
        self.cursor_instance = _Cursor(rows)

    def cursor(self):
        return self.cursor_instance


def _actions():
    return pd.DataFrame([
        {
            "identifier": "005930",
            "event_type": "cash_dividend",
            "announcement_date": date(2020, 1, 1),
            "record_date": date(2020, 1, 31),
            "effective_date": None,
            "rcept_no": "cash-1",
            "revision_root_action_key": "cash-1",
            "cash_amount_status": "POSITIVE",
        },
        {
            "identifier": "000001",
            "event_type": "ex_dividend",
            "announcement_date": date(2014, 12, 30),
            "record_date": None,
            "effective_date": date(2014, 12, 30),
        },
        {
            "identifier": "005935",
            "event_type": "ex_dividend",
            "announcement_date": date(2020, 1, 2),
            "record_date": None,
            "effective_date": date(2020, 1, 2),
        },
    ])


def test_pit_mapping_uses_event_date_and_excludes_preferred_scope():
    connection = _Connection([
        (0, 101, "common_stock", True),
        (1, 202, "preferred_stock", True),
    ])

    mapped, stats = map_actions_to_pit_assets(
        connection, _actions(), coverage_start=date(2015, 1, 1)
    )

    assert mapped[["identifier", "asset_id"]].to_dict("records") == [
        {"identifier": "005930", "asset_id": 101}
    ]
    assert stats.before_contract_count == 1
    assert stats.out_of_scope_instrument_count == 1
    sql, params = connection.cursor_instance.statements[0]
    assert "ai.valid_from <= r.event_date" in sql
    assert "ai.valid_to IS NULL OR ai.valid_to >= r.event_date" in sql
    assert params[1] == ["005930", "005935"]
    assert params[2] == [date(2020, 1, 31), date(2020, 1, 2)]


def test_pit_mapping_reuses_only_same_verified_snapshot_and_identity():
    return_identity._PIT_MAP_CACHE.clear()
    first_connection = _Connection([
        (0, 101, "common_stock", True),
        (1, 202, "preferred_stock", True),
    ])
    kwargs = {
        "coverage_start": date(2015, 1, 1),
        "include_audit": True,
        "verified_snapshot_sha256": "a" * 64,
        "asset_identity_digest": "b" * 64,
    }
    first, first_stats, _ = map_actions_to_pit_assets(
        first_connection, _actions(), **kwargs,
    )
    first.loc[:, "asset_id"] = 999
    first_stats.included_corp_cls_counts["MUTATED"] = 1

    second_connection = _Connection([])
    second, second_stats, _ = map_actions_to_pit_assets(
        second_connection, _actions(), **kwargs,
    )

    assert second["asset_id"].tolist() == [101]
    assert "MUTATED" not in second_stats.included_corp_cls_counts
    assert second_connection.cursor_instance.statements == []

    map_actions_to_pit_assets(
        second_connection,
        _actions(),
        **{**kwargs, "asset_identity_digest": "c" * 64},
    )
    assert len(second_connection.cursor_instance.statements) == 1
    return_identity._PIT_MAP_CACHE.clear()


def test_pit_mapping_explicitly_partitions_unmapped_event():
    action = _actions().iloc[[0]].copy()

    mapped, stats, audit = map_actions_to_pit_assets(
        _Connection([]), action, coverage_start=date(2015, 1, 1),
        include_audit=True,
    )

    assert mapped.empty
    assert stats.excluded_reason_counts == {
        "NO_EVENT_DATE_PIT_IDENTITY": 1,
    }
    assert audit.iloc[0]["pit_mapping_status"] == "EXCLUDED"
    assert audit.iloc[0]["pit_excluded_reason"] == (
        "NO_EVENT_DATE_PIT_IDENTITY"
    )


def test_pit_mapping_fails_for_ambiguous_event():
    action = _actions().iloc[[0]].copy()

    with pytest.raises(RuntimeError, match="ambiguous"):
        map_actions_to_pit_assets(
            _Connection([
                (0, 1, "common_stock", True),
                (0, 2, "common_stock", True),
            ]),
            action,
            coverage_start=date(2015, 1, 1),
        )


def test_corp_cls_never_gates_pit_identity_or_price_scope():
    actions = pd.DataFrame([
        {
            "identifier": "005930",
            "event_type": "cash_dividend",
            "announcement_date": date(2020, 1, 1),
            "record_date": date(2020, 1, 31),
            "corp_cls": "Y",
            "rcept_no": "y",
            "revision_root_action_key": "y",
            "cash_amount_status": "POSITIVE",
        },
        {
            "identifier": "052960",
            "event_type": "cash_dividend",
            "announcement_date": date(2020, 1, 2),
            "record_date": date(2020, 1, 31),
            "corp_cls": "N",
            "rcept_no": "n",
            "revision_root_action_key": "n",
            "cash_amount_status": "POSITIVE",
        },
        {
            "identifier": "192240",
            "event_type": "cash_dividend",
            "announcement_date": date(2020, 1, 3),
            "record_date": date(2020, 1, 31),
            "corp_cls": "E",
            "rcept_no": "e",
            "revision_root_action_key": "e",
            "cash_amount_status": "POSITIVE",
        },
    ])
    connection = _Connection([
        (0, 101, "common_stock", True),
        (2, 303, "common_stock", True),
    ])

    mapped, stats = map_actions_to_pit_assets(
        connection, actions, coverage_start=date(2015, 1, 1)
    )

    assert mapped["identifier"].tolist() == ["005930", "192240"]
    assert stats.out_of_scope_market_count == 1
    assert stats.out_of_scope_market_ticker_count == 1
    assert stats.included_corp_cls_counts == {"Y": 1, "E": 1}
    assert stats.excluded_corp_cls_counts == {"N": 1}
    _, params = connection.cursor_instance.statements[0]
    assert params[1] == ["005930", "052960", "192240"]


def test_missing_corp_class_is_audited_but_never_a_scope_gate():
    action = _actions().iloc[[0]].copy()
    action["corp_cls"] = None

    mapped, stats = map_actions_to_pit_assets(
        _Connection([(0, 1, "common_stock", True)]),
        action,
        coverage_start=date(2015, 1, 1),
    )

    assert len(mapped) == 1
    assert stats.included_corp_cls_counts == {"UNKNOWN": 1}


@pytest.mark.parametrize("corp_cls", ["E", "N", "K", "Y", None])
def test_all_corp_classes_include_when_pit_common_price_episode_exists(corp_cls):
    action = _actions().iloc[[0]].copy()
    action["corp_cls"] = corp_cls

    mapped, stats = map_actions_to_pit_assets(
        _Connection([(0, 1, "common_stock", True)]),
        action,
        coverage_start=date(2015, 1, 1),
    )

    assert len(mapped) == 1
    assert stats.mapped_common_stock_count == 1


def test_price_episode_must_cover_event_but_allows_long_trading_halt():
    action = _actions().iloc[[0]].copy()
    excluded, stats = map_actions_to_pit_assets(
        _Connection([(0, 1, "common_stock", False)]),
        action,
        coverage_start=date(2015, 1, 1),
    )
    included, _ = map_actions_to_pit_assets(
        _Connection([(0, 1, "common_stock", True)]),
        action,
        coverage_start=date(2015, 1, 1),
    )

    assert excluded.empty
    assert stats.excluded_reason_counts == {
        "NO_CERTIFIED_KOSPI_KOSDAQ_PRICE_EPISODE": 1,
    }
    assert len(included) == 1


def test_alphanumeric_krx_tickers_use_family_date_pit_identity_and_price():
    codes = ["0008Z0", "0010V0", "0039P0"]
    actions = pd.DataFrame([
        {
            "identifier": ticker.lower(),
            "event_type": "cash_dividend",
            "announcement_date": date(2026, 7, 1),
            "record_date": date(2026, 7, 31),
            "corp_cls": "K",
            "rcept_no": f"2026070100000{index}",
            "revision_root_action_key": f"2026070100000{index}",
            "cash_amount_status": "POSITIVE",
        }
        for index, ticker in enumerate(codes, start=1)
    ])
    asset_ids = [6590, 6592, 6671]
    connection = _Connection([
        (index, asset_id, "common_stock", True)
        for index, asset_id in enumerate(asset_ids)
    ])

    mapped, stats = map_actions_to_pit_assets(
        connection, actions, coverage_start=date(2015, 1, 1)
    )

    assert mapped["identifier"].tolist() == codes
    assert mapped["asset_id"].tolist() == asset_ids
    assert stats.mapped_common_stock_count == 3
    _, params = connection.cursor_instance.statements[0]
    assert params[1] == codes


def test_revision_family_uses_terminal_economic_date_for_every_receipt():
    actions = pd.DataFrame([
        {
            "identifier": "065510", "event_type": "cash_dividend",
            "announcement_date": date(2020, 2, 12),
            "record_date": date(2018, 12, 31),
            "rcept_no": "root", "revision_root_action_key": "root",
            "cash_amount_status": "POSITIVE",
        },
        {
            "identifier": "065510", "event_type": "cash_dividend",
            "announcement_date": date(2020, 3, 30),
            "record_date": date(2019, 12, 31),
            "rcept_no": "terminal", "revision_root_action_key": "root",
            "cash_amount_status": "POSITIVE",
        },
    ])
    connection = _Connection([
        (0, 1, "common_stock", True),
        (1, 1, "common_stock", True),
    ])

    mapped, _ = map_actions_to_pit_assets(
        connection, actions, coverage_start=date(2015, 1, 1),
    )

    assert mapped["asset_id"].tolist() == [1, 1]
    sql, params = connection.cursor_instance.statements[0]
    assert "pc.first_on_or_after" in sql
    assert params[2] == [date(2019, 12, 31), date(2019, 12, 31)]
