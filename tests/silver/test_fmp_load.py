from datetime import date

import pandas as pd

from pipeline.silver.fmp_load import (
    _build_daily_candidates,
    _existing_identifier_map,
    _fundamental_partitions,
    _exclude_stored_security_id_conflicts,
)
from pipeline.silver_quality.models import CandidateBundle


class _Cursor:
    def __init__(self, rows):
        self._rows = rows

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def execute(self, *_):
        return None

    def fetchall(self):
        return self._rows


def test_stored_security_conflict_omits_only_optional_link():
    ids=pd.DataFrame([
        dict(natural_key='FMP:NEW',source='FMP',identifier_type='ticker',identifier='NEW',valid_to=None),
        dict(natural_key='FMP:NEW',source='FMP',identifier_type='cusip',identifier='STALE',valid_to=None),
        dict(natural_key='FMP:NEW',source='FMP',identifier_type='isin',identifier='GOOD',valid_to=None),
        dict(natural_key='FMP:OLD',source='FMP',identifier_type='cusip',identifier='STALE',valid_to=None),
    ])
    bundle=CandidateBundle(identifiers=ids,prices=pd.DataFrame([{'close':100}]))
    conn=_Connection([('ticker','NEW',2),('ticker','OLD',1),('cusip','STALE',1)])
    _exclude_stored_security_id_conflicts(conn,bundle)
    assert len(bundle.identifiers)==3
    assert len(bundle.prices)==1
    assert bundle.stats['universe']['ambiguous_identifier_rows_removed']==1
    assert bundle.stats['universe']['ambiguous_identifier_samples'][0]['stored_asset_id']==1


class _Connection:
    def __init__(self, rows):
        self._rows = rows

    def cursor(self):
        return _Cursor(self._rows)


class _Transaction:
    def __init__(self, connection):
        self.connection = connection

    def __enter__(self):
        assert self.connection.transaction_depth == 0
        self.connection.transaction_depth += 1

    def __exit__(self, *_):
        self.connection.transaction_depth -= 1
        self.connection.completed_transactions += 1


class _TransactionConnection:
    def __init__(self):
        self.transaction_depth = 0
        self.completed_transactions = 0

    def transaction(self):
        return _Transaction(self)

    def cursor(self):
        # The coverage-baseline read runs inside the roll-check transaction;
        # return no prior sessions so the baseline stays None (no false block).
        return _Cursor([])


def test_existing_identifier_map_includes_historical_ticker_episodes():
    identifiers = pd.DataFrame([
        {
            "natural_key": "FMP:NEW",
            "source": "FMP",
            "identifier": "OLD",
            "identifier_type": "ticker",
            "valid_from": date.min,
            "valid_to": date(2025, 12, 31),
        },
        {
            "natural_key": "FMP:NEW",
            "source": "FMP",
            "identifier": "NEW",
            "identifier_type": "ticker",
            "valid_from": date(2026, 1, 1),
            "valid_to": None,
        },
    ])
    stored = [
        ("ticker", "OLD", 42, date(2025, 12, 31), date.min),
        ("ticker", "NEW", 42, None, date(2026, 1, 1)),
    ]

    mapping = _existing_identifier_map(_Connection(stored), identifiers)

    assert mapping == {"OLD": 42, "NEW": 42, "FMP:NEW": 42}


def test_fundamentals_are_partitioned_by_statement_and_fiscal_period():
    frame = pd.DataFrame([
        {"statement_type": "IS", "fiscal_period": "FY", "value": 1},
        {"statement_type": "IS", "fiscal_period": "Q1", "value": 2},
        {"statement_type": "BS", "fiscal_period": "FY", "value": 3},
    ])

    partitions = _fundamental_partitions(2025, frame)

    assert [key for key, _ in partitions] == [
        "fundamental:year=2025:statement=BS:period=FY",
        "fundamental:year=2025:statement=IS:period=FY",
        "fundamental:year=2025:statement=IS:period=Q1",
    ]
    assert [len(partition) for _, partition in partitions] == [1, 1, 1]


def test_daily_candidate_roll_lookup_closes_transaction_before_publish(
    monkeypatch,
):
    connection = _TransactionConnection()
    expected = CandidateBundle(prices=pd.DataFrame())
    observed = []

    monkeypatch.setattr(
        "pipeline.silver.fmp_load.fmp.build_candidates",
        lambda base, target_date: expected,
    )

    def fake_roll_check(conn, bundle):
        observed.append((conn.transaction_depth, bundle))

    monkeypatch.setattr(
        "pipeline.silver.fmp_load._add_previous_commodity_roll_check",
        fake_roll_check,
    )

    actual = _build_daily_candidates(
        connection, "/tmp/bronze", date(2026, 8, 4),
    )

    assert actual is expected
    assert observed == [(1, expected)]
    assert connection.transaction_depth == 0
    assert connection.completed_transactions == 1
    # Baseline was fetched inside the same transaction; empty history -> None.
    assert actual.stats["price_daily"]["coverage_baseline"] is None
