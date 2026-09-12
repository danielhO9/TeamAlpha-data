import pytest

from pipeline.asset_lifecycle import candidate, check_observed_bounds


def fact(day, event="LISTING", **kwargs):
    return dict(ticker="000001", date=day, event=event, market="KOSDAQ",
                source_file="official.html", source_sha256="a" * 64, **kwargs)


def test_delisting_day_is_exclusive():
    row = candidate(1, "000001", [fact("2010-01-04"), fact("2020-01-06", "DELISTING")])
    assert row["proposed_listed_to"] == "2020-01-05"
    assert row["periods"] == [{"start": "2010-01-04", "end": "2020-01-05"}]
    assert not row["issues"]


def test_market_transfer_does_not_end_coverage():
    row = candidate(1, "000001", [fact("2010-01-04"),
        fact("2018-02-09", "DELISTING", market_transfer=True),
        fact("2018-02-09", "CURRENT_LISTING")])
    assert row["periods"] == [{"start": "2010-01-04", "end": None}]
    assert not row["issues"]


def test_relisting_retains_gap_and_blocks_flat_publication():
    row = candidate(1, "000001", [fact("2012-07-26"),
        fact("2015-03-17", "DELISTING"), fact("2025-03-28", "CURRENT_LISTING")])
    assert row["periods"] == [{"start": "2012-07-26", "end": "2015-03-16"},
                              {"start": "2025-03-28", "end": None}]
    assert row["issues"] == ["RELISTING_REQUIRES_INTERVAL_REVIEW"]


def test_konex_exit_cannot_close_first_kosdaq_listing():
    earlier = fact("2019-05-21"); earlier["market"] = "KONEX"
    row = candidate(1, "000001", [earlier, fact("2024-06-11", "DELISTING"),
                                 fact("2024-06-11", "CURRENT_LISTING")])
    assert row["periods"] == [{"start": "2024-06-11", "end": None}]
    assert not row["issues"]


def test_pre_research_delisting_drops_old_episode():
    row = candidate(1, "000001", [fact("2000-01-04"),
        fact("2005-01-04", "DELISTING"), fact("2010-01-04", "CURRENT_LISTING")])
    assert row["periods"] == [{"start": "2010-01-04", "end": None}]
    assert not row["issues"]


def test_other_share_class_is_never_inherited():
    row = candidate(2, "00000K", [fact("2010-01-04", "CURRENT_LISTING")])
    assert row["proposed_listed_from"] is None
    assert "MISSING_SUPPORTED_MARKET_LISTING" in row["issues"]


def test_missing_provenance_rejected():
    row = fact("2010-01-04"); del row["source_file"]
    with pytest.raises(ValueError, match="evidence"):
        candidate(1, "000001", [row])


def test_price_bounds_are_checks_not_listing_date_sources():
    row = candidate(1, "000001", [fact("2015-02-01", "CURRENT_LISTING")])
    assert check_observed_bounds(row, "2015-01-02", "2026-09-10") == [
        "OBSERVATION_BEFORE_PROPOSED_LISTING"]
    assert row["proposed_listed_from"] == "2015-02-01"


def contract():
    from pipeline.bronze.kis_history import digest
    c = dict(schema='asset-listing-snapshot-v1', asset_ids=[1],
             coverage_start='2015-01-01', verified_through='2026-09-11',
             observed_at='2026-09-12T00:00:00+00:00', evidence_uri='s3://evidence',
             audit_sha256='b' * 64, evidence_sha256='c' * 64, periods=[
                 dict(asset_id=1, ticker='000001', start='2012-07-26', end='2015-03-16'),
                 dict(asset_id=1, ticker='000001', start='2025-03-28', end=None)])
    c['snapshot_id'] = digest(c)
    return c


def test_snapshot_preserves_relisting_periods_and_requires_date_coverage():
    from datetime import date
    from pipeline.silver.asset_lifecycle import validate
    c = contract()
    assert len(validate(c, [1], date(2015, 1, 1), date(2026, 9, 11))) == 2
    with pytest.raises(ValueError, match='cover requested'):
        validate(c, [1], date(2015, 1, 1), date(2026, 9, 12))
    with pytest.raises(ValueError, match='universe'):
        validate(c, [1, 2], date(2015, 1, 1), date(2026, 9, 11))


def test_tampered_and_overlapping_snapshot_rejected():
    from datetime import date
    from pipeline.silver.asset_lifecycle import validate
    from pipeline.bronze.kis_history import digest
    c = contract(); c['periods'][1]['start'] = '2015-03-16'
    with pytest.raises(ValueError, match='fingerprint'):
        validate(c, [1], date(2015, 1, 1), date(2026, 9, 11))
    c['snapshot_id'] = digest({k: v for k, v in c.items() if k != 'snapshot_id'})
    with pytest.raises(ValueError, match='overlapping'):
        validate(c, [1], date(2015, 1, 1), date(2026, 9, 11))


def test_uncertified_snapshot_is_not_loaded():
    from unittest.mock import Mock
    from datetime import date
    from pipeline.silver.asset_lifecycle import load
    cur = Mock(); cur.fetchone.return_value = (contract(), 'FAILED')
    with pytest.raises(ValueError, match='uncertified'):
        load(cur, contract()['snapshot_id'], [1], date(2015, 1, 1), date(2026, 9, 11))


def test_snapshot_checks_expected_dates_even_when_asset_dates_are_null():
    from unittest.mock import patch
    from datetime import date
    from tests.kis.test_coverage import Conn
    from pipeline.kis_flows import expected_partitions
    day = date(2026, 9, 10)
    c = Conn([(1, None, None)], [(1, '000001', day, day, 'CERTIFIED')])
    with patch('pipeline.kis_flows.asset_lifecycle.load', return_value=contract()['periods']) as load:
        result = expected_partitions(c, dict(asset_ids=[1], listing_snapshot_id='snap'), day, day)
    assert result == {(1, '000001', 0): [day]}
    load.assert_called_once()
