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
