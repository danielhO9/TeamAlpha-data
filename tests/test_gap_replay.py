import os
from datetime import date

import pytest

from pipeline import gap_replay


def test_weekdays_excludes_weekend_and_validates_range():
    assert gap_replay._weekdays("20260814", "20260818") == [
        "20260814", "20260817", "20260818",
    ]
    with pytest.raises(ValueError, match="must not precede"):
        gap_replay._weekdays("20260818", "20260814")


def test_gap_replay_holds_one_epoch_and_only_certifies_final_day(monkeypatch):
    events: list[object] = []
    lock = object()
    monkeypatch.setenv("PIPELINE_DATE", "original")
    monkeypatch.setattr(
        gap_replay.dart_silver_backfill_ecs,
        "acquire_daily_certification_lock",
        lambda: events.append("acquire") or lock,
    )
    monkeypatch.setattr(
        gap_replay.dart_silver_backfill_ecs,
        "release_daily_certification_lock",
        lambda connection: events.append(("release", connection)),
    )
    monkeypatch.setattr(
        gap_replay.dart_silver_backfill_ecs,
        "total_return_contract_ready",
        lambda *, conn: conn is lock,
    )

    def run_day(connection, **kwargs):
        events.append((os.environ["PIPELINE_DATE"], connection, kwargs))

    monkeypatch.setattr(gap_replay.daily_full, "_main_locked", run_day)

    gap_replay.run("20260814", "20260818")

    assert events == [
        "acquire",
        (
            "20260814", lock,
            {
                "allow_deferred_total_return": True,
                "prepare_total_return": False,
                "preview_total_return": False,
                "close_total_return": False,
                "assert_final_freshness": False,
                "collect_financials": True,
                "full_year_financial_snapshot": False,
                "bounded_action_scope": True,
            },
        ),
        (
            "20260817", lock,
            {
                "allow_deferred_total_return": True,
                "prepare_total_return": False,
                "preview_total_return": False,
                "close_total_return": False,
                "assert_final_freshness": False,
                "collect_financials": True,
                "full_year_financial_snapshot": False,
                "bounded_action_scope": True,
            },
        ),
        (
            "20260818", lock,
            {
                "allow_deferred_total_return": True,
                "prepare_total_return": True,
                "preview_total_return": True,
                "close_total_return": True,
                "assert_final_freshness": True,
                "collect_financials": True,
                "full_year_financial_snapshot": False,
                "bounded_action_scope": False,
            },
        ),
        ("release", lock),
    ]
    assert os.environ["PIPELINE_DATE"] == "original"


def test_gap_replay_refuses_unhealthy_entry_contract(monkeypatch):
    lock = object()
    released: list[object] = []
    monkeypatch.setattr(
        gap_replay.dart_silver_backfill_ecs,
        "acquire_daily_certification_lock",
        lambda: lock,
    )
    monkeypatch.setattr(
        gap_replay.dart_silver_backfill_ecs,
        "release_daily_certification_lock",
        released.append,
    )
    monkeypatch.setattr(
        gap_replay.dart_silver_backfill_ecs,
        "total_return_contract_ready",
        lambda *, conn: False,
    )

    with pytest.raises(RuntimeError, match="CERTIFIED"):
        gap_replay.run("20260817", "20260818")
    assert released == [lock]


def test_gap_replay_resumes_matching_building_contract(monkeypatch):
    lock = object()
    calls: list[tuple] = []
    monkeypatch.setattr(
        gap_replay.dart_silver_backfill_ecs,
        "acquire_daily_certification_lock",
        lambda: lock,
    )
    monkeypatch.setattr(
        gap_replay.dart_silver_backfill_ecs,
        "release_daily_certification_lock",
        lambda connection: calls.append(("release", connection)),
    )
    monkeypatch.setattr(
        gap_replay.dart_silver_backfill_ecs,
        "total_return_contract_ready",
        lambda *, conn: False,
    )
    monkeypatch.setattr(
        gap_replay.dart_silver_backfill_ecs,
        "certified_krx_price_coverage_end",
        lambda *, conn: date(2026, 8, 11),
    )
    monkeypatch.setattr(
        gap_replay.freshness,
        "total_return_contract_report",
        lambda conn: {"status": "BUILDING", "coverage_end": "2026-08-11"},
    )
    monkeypatch.setattr(
        gap_replay.daily_full,
        "_main_locked",
        lambda connection, **kwargs: calls.append(
            (os.environ["PIPELINE_DATE"], connection, kwargs)
        ),
    )

    gap_replay.run("20260812", "20260812", resume_building=True)

    assert calls[0][0] == "20260812"
    assert calls[-1] == ("release", lock)
