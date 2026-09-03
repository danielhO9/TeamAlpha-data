from datetime import date
import math
from uuid import uuid4

import pandas as pd
import pytest

from pipeline.silver.total_returns import (
    apply_dividends_to_prices,
    build_total_return_close,
    canonicalize_cash_dividends,
    classify_cash_dividend_revisions,
    resolve_dividend_ex_dates,
    stored_price_factor_interval,
)


def _actions(rows):
    defaults = {
        "source": "DART_DISCLOSURE",
        "event_type": "cash_dividend",
        "announcement_date": date(2026, 1, 1),
        "effective_date": None,
        "record_date": date(2026, 1, 6),
        "cash_amount": 10.0,
        "rcept_no": "20260101000001",
    }
    return pd.DataFrame([{**defaults, **row} for row in rows])


def _prices(rows):
    return pd.DataFrame(rows, columns=[
        "identifier", "trade_date", "close", "adj_close",
    ])


def test_canonical_dividend_uses_latest_revision_and_rejects_incomplete_rows():
    actions = _actions([
        {
            "identifier": "005930",
            "announcement_date": date(2026, 1, 2),
            "cash_amount": 300,
            "rcept_no": "20260102000001",
        },
        {
            "identifier": "005930",
            "announcement_date": date(2026, 1, 3),
            "cash_amount": 500,
            "rcept_no": "20260103000001",
        },
        {
            "identifier": "000660",
            "cash_amount": None,
            "rcept_no": "20260104000001",
        },
    ])

    canonical = canonicalize_cash_dividends(actions)

    assert len(canonical) == 1
    assert canonical.iloc[0]["cash_amount"] == pytest.approx(500)
    assert canonical.iloc[0]["dividend_key"] == "20260103000001"
    assert canonical.attrs["canonicalization"] == {
        "input_rows": 3,
        "eligible_rows": 2,
        "canonical_rows": 1,
        "superseded_rows": 1,
        "rejected_rows": 1,
    }

    audit = classify_cash_dividend_revisions(actions)
    assert set(audit["dividend_key"]) == {
        "20260102000001", "20260103000001", "20260104000001",
    }
    assert audit.set_index("dividend_key").loc[
        "20260102000001", "excluded_reason"
    ] == "SUPERSEDED_REVISION"
    assert audit.set_index("dividend_key").loc[
        "20260103000001", "is_canonical"
    ]
    assert audit.set_index("dividend_key").loc[
        "20260104000001", "excluded_reason"
    ] == "INVALID_CASH_AMOUNT"


def test_ex_date_uses_explicit_notice_before_market_session_inference():
    cash = canonicalize_cash_dividends(_actions([{
        "identifier": "005930",
        "record_date": date(2026, 1, 6),
    }]))
    all_actions = _actions([
        {
            "identifier": "005930",
            "record_date": date(2026, 1, 6),
        },
        {
            "identifier": "005930",
            "source": "DART_DISCLOSURE",
            "event_type": "ex_dividend",
            "announcement_date": date(2026, 1, 2),
            "effective_date": date(2026, 1, 2),
            "record_date": None,
            "cash_amount": None,
            "rcept_no": "20260102000009",
        },
    ])

    resolved = resolve_dividend_ex_dates(
        cash,
        all_actions,
        pd.Series(pd.to_datetime([
            "2026-01-02", "2026-01-05", "2026-01-06",
        ])),
    )

    assert resolved.iloc[0]["resolved_ex_date"] == pd.Timestamp("2026-01-02")
    assert resolved.iloc[0]["ex_date_basis"] == "KRX_NOTICE"


def test_ex_date_falls_back_to_second_session_on_or_before_record_date():
    cash = canonicalize_cash_dividends(_actions([{
        "identifier": "005930",
        "record_date": date(2026, 1, 6),
    }]))

    resolved = resolve_dividend_ex_dates(
        cash,
        _actions([]),
        pd.Series(pd.to_datetime([
            "2026-01-02", "2026-01-05", "2026-01-06",
        ])),
    )

    assert resolved.iloc[0]["resolved_ex_date"] == pd.Timestamp("2026-01-05")
    assert resolved.iloc[0]["ex_date_basis"] == "KRX_T2_INFERRED"


def test_pending_positive_may_only_be_an_intermediate_revision():
    actions = _actions([
        {
            "identifier": "005930",
            "announcement_date": date(2025, 2, 28),
            "record_date": None,
            "cash_amount": 150.0,
            "cash_amount_status": "POSITIVE_PENDING_RECORD_DATE",
            "source_evidence_status": "VERIFIED_DART_VIEWER_BODY",
            "revision_root_action_key": "20250228801790",
            "revision_kind": "ECONOMIC_REVISION",
            "viewer_evidence_sha256": "a" * 64,
            "economic_evidence_sha256": "a" * 64,
            "rcept_no": "20250228801790",
        },
        {
            "identifier": "005930",
            "announcement_date": date(2025, 3, 4),
            "record_date": date(2025, 3, 19),
            "cash_amount": 150.0,
            "cash_amount_status": "POSITIVE",
            "source_evidence_status": "VERIFIED_DART_VIEWER_BODY",
            "revision_root_action_key": "20250228801790",
            "correction_of_action_key": "20250228801790",
            "revision_kind": "ECONOMIC_REVISION",
            "viewer_evidence_sha256": "c" * 64,
            "economic_evidence_sha256": "c" * 64,
            "rcept_no": "20250304800639",
        },
    ])

    classified = classify_cash_dividend_revisions(actions)

    assert classified.set_index("dividend_key").loc[
        "20250228801790", "excluded_reason"
    ] == "SUPERSEDED_REVISION"
    assert classified.set_index("dividend_key").loc[
        "20250304800639", "is_canonical"
    ]


def test_terminal_pending_positive_fails_closed():
    actions = _actions([{
        "identifier": "005930",
        "record_date": None,
        "cash_amount": 150.0,
        "cash_amount_status": "POSITIVE_PENDING_RECORD_DATE",
        "source_evidence_status": "VERIFIED_DART_VIEWER_BODY",
        "revision_root_action_key": "20250228801790",
        "revision_kind": "ECONOMIC_REVISION",
        "viewer_evidence_sha256": "a" * 64,
        "economic_evidence_sha256": "a" * 64,
        "rcept_no": "20250228801790",
    }])

    with pytest.raises(RuntimeError, match="still missing its record date"):
        classify_cash_dividend_revisions(actions)


def test_gross_total_return_scales_dividend_for_later_split():
    prices = _prices([
        ("005930", date(2026, 1, 1), 100.0, 50.0),
        ("005930", date(2026, 1, 2), 100.0, 50.0),
        ("005930", date(2026, 1, 5), 50.0, 50.0),
    ])
    dividends = pd.DataFrame([{
        "identifier": "005930",
        "cash_amount": 10.0,
        "resolved_ex_date": date(2026, 1, 2),
    }])

    result, events = apply_dividends_to_prices(prices, dividends)

    assert events.iloc[0]["adjusted_cash_amount"] == pytest.approx(5.0)
    assert events.iloc[0]["application_status"] == "applied"
    assert result["total_return_close"].tolist() == pytest.approx([
        50.0, 55.0, 55.0,
    ])


def test_same_day_adjustment_scale_change_fails_closed():
    prices = _prices([
        ("005930", date(2026, 1, 1), 100.0, 100.0),
        ("005930", date(2026, 1, 2), 100.0, 50.0),
    ])
    dividends = pd.DataFrame([{
        "identifier": "005930",
        "cash_amount": 10.0,
        "resolved_ex_date": date(2026, 1, 2),
    }])

    with pytest.raises(RuntimeError, match="share base is ambiguous"):
        apply_dividends_to_prices(prices, dividends)


def test_cj_changed_scale_uses_pre_event_scale_and_exact_cash_contract():
    run_id = uuid4()
    prices = _prices([
        ("001040", date(2018, 12, 26), 131500.0, 124000.0),
        ("001040", date(2018, 12, 27), 120500.0, 120500.0),
    ])
    prices["asset_id"] = 77
    dividends = pd.DataFrame([{
        "asset_id": 77,
        "identifier": "001040",
        "dividend_key": "20190213900610",
        "cash_amount": 1450.0,
        "resolved_ex_date": date(2018, 12, 27),
    }])
    source_evidence = pd.DataFrame([{
        "asset_id": 77,
        "ticker": "001040",
        "cash_receipt_no": "20190213900610",
        "action_snapshot_run_id": run_id,
        "evidence_key": "cj-2018",
        "previous_trade_date": date(2018, 12, 26),
        "adjustment_trade_date": date(2018, 12, 27),
        "raw_previous_close": 131500.0,
        "raw_applied_close": 120500.0,
        "raw_reference_price": 124000.0,
        "expected_price_factor": 124000.0 / 131500.0,
        "cash_scale_basis": "PRE_EVENT_PRICE_SCALE",
    }])

    result, events = apply_dividends_to_prices(
        prices,
        dividends,
        scale_source_evidence=source_evidence,
    )

    event = events.iloc[0]
    assert event["selected_cash_scale"] == pytest.approx(
        0.942965779468, abs=5e-13,
    )
    assert event["adjusted_cash_amount"] == pytest.approx(
        1367.30038023, abs=5e-9,
    )
    assert event["adjusted_cash_amount"] != pytest.approx(1450.0)
    assert result.iloc[1]["adjusted_cash_dividend"] == pytest.approx(
        1367.30038023, abs=5e-9,
    )


def test_four_decimal_rounding_does_not_fake_a_scale_change():
    scale = 1 / 3
    prices = _prices([
        (
            "005930", date(2026, 1, 1), 1234.0,
            round(1234.0 * scale, 4),
        ),
        (
            "005930", date(2026, 1, 2), 987.0,
            round(987.0 * scale, 4),
        ),
    ])
    dividends = pd.DataFrame([{
        "identifier": "005930",
        "cash_amount": 10.0,
        "resolved_ex_date": date(2026, 1, 2),
    }])

    _, events = apply_dividends_to_prices(prices, dividends)

    assert events.iloc[0]["application_status"] == "applied"


def test_stored_price_factor_interval_admits_actual_038620_rounding_case():
    low, high = stored_price_factor_interval(
        previous_close=1575.0,
        previous_adj_close=4550.9656,
        applied_close=1460.0,
        applied_adj_close=4400.2714,
    )
    frozen_source_factor = 0.9587301532470918
    krx_reference_factor = 1510.0 / 1575.0

    assert abs(krx_reference_factor - frozen_source_factor) > 5e-13
    assert low <= frozen_source_factor <= high
    assert low <= krx_reference_factor <= high


@pytest.mark.parametrize(
    ("previous_close", "previous_adj_close", "applied_close", "applied_adj_close", "reference_factor"),
    [
        (7850.0, 25953.4590, 7870.0, 26186.3745, 0.993630573248),
        (4510.0, 22164.9640, 4385.0, 21767.8315, 0.990022172949),
    ],
)
def test_two_stage_rounding_interval_admits_actual_033540_cases(
    previous_close,
    previous_adj_close,
    applied_close,
    applied_adj_close,
    reference_factor,
):
    low, high = stored_price_factor_interval(
        previous_close=previous_close,
        previous_adj_close=previous_adj_close,
        applied_close=applied_close,
        applied_adj_close=applied_adj_close,
    )

    assert low <= reference_factor <= high


def test_stored_price_factor_interval_has_closed_exact_boundaries():
    low, high = stored_price_factor_interval(
        previous_close=1575.0,
        previous_adj_close=4550.9656,
        applied_close=1460.0,
        applied_adj_close=4400.2714,
    )

    assert low <= low <= high
    assert low <= high <= high
    assert not low <= math.nextafter(low, -math.inf) <= high
    assert not low <= math.nextafter(high, math.inf) <= high


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("previous_close", 0.0),
        ("previous_adj_close", math.nan),
        ("applied_close", math.inf),
        ("applied_adj_close", -1.0),
    ],
)
def test_stored_price_factor_interval_rejects_invalid_inputs(field, value):
    inputs = {
        "previous_close": 1575.0,
        "previous_adj_close": 4550.9656,
        "applied_close": 1460.0,
        "applied_adj_close": 4400.2714,
    }
    inputs[field] = value

    with pytest.raises(RuntimeError, match="finite and positive"):
        stored_price_factor_interval(**inputs)


def test_three_parts_per_million_scale_jump_is_not_hidden_as_stable():
    prices = _prices([
        ("005930", date(2026, 1, 1), 100.0, 100.0),
        ("005930", date(2026, 1, 2), 100.0, 99.9997),
    ])
    dividends = pd.DataFrame([{
        "identifier": "005930",
        "cash_amount": 10.0,
        "resolved_ex_date": date(2026, 1, 2),
    }])

    with pytest.raises(RuntimeError, match="share base is ambiguous"):
        apply_dividends_to_prices(prices, dividends)


def test_actual_006740_two_stage_rounding_remains_stable_without_evidence():
    prices = _prices([
        ("006740", date(2016, 12, 27), 3085.0, 15366.1070),
        ("006740", date(2016, 12, 28), 3020.0, 15042.3480),
    ])
    dividends = pd.DataFrame([{
        "identifier": "006740",
        "cash_amount": 45.0,
        "resolved_ex_date": date(2016, 12, 28),
    }])

    _, events = apply_dividends_to_prices(prices, dividends)

    assert events.iloc[0]["application_status"] == "applied"
    assert events.iloc[0]["scale_change_detected"] is False


def test_first_listing_day_cash_event_is_explicit_and_consumes_no_evidence():
    prices = _prices([
        ("152330", date(2015, 12, 29), 100.0, 100.0),
        ("152330", date(2015, 12, 30), 101.0, 101.0),
    ])
    dividends = pd.DataFrame([{
        "identifier": "152330",
        "cash_amount": 10.0,
        "resolved_ex_date": date(2015, 12, 29),
    }])

    result, events = apply_dividends_to_prices(prices, dividends)

    assert events.iloc[0]["application_status"] == (
        "before_listing_or_episode_start"
    )
    assert result["adjusted_cash_dividend"].sum() == 0


def test_nontrading_ex_date_applies_on_first_following_trade_and_sums_cash():
    prices = _prices([
        ("005930", date(2026, 1, 2), 100.0, 100.0),
        ("005930", date(2026, 1, 5), 98.0, 98.0),
    ])
    dividends = pd.DataFrame([
        {
            "identifier": "005930", "cash_amount": 1.0,
            "resolved_ex_date": date(2026, 1, 3),
        },
        {
            "identifier": "005930", "cash_amount": 2.0,
            "resolved_ex_date": date(2026, 1, 3),
        },
    ])

    result, events = apply_dividends_to_prices(prices, dividends)

    assert set(events["application_status"]) == {"applied"}
    assert set(events["applied_trade_date"]) == {pd.Timestamp("2026-01-05")}
    assert result.iloc[1]["adjusted_cash_dividend"] == pytest.approx(3.0)
    assert result.iloc[1]["total_return_close"] == pytest.approx(101.0)


def test_dividend_does_not_cross_a_new_listing_episode():
    prices = _prices([
        ("005930", date(2024, 1, 2), 100.0, 100.0),
        ("005930", date(2026, 1, 5), 50.0, 50.0),
    ])
    dividends = pd.DataFrame([{
        "identifier": "005930",
        "cash_amount": 10.0,
        "resolved_ex_date": date(2025, 1, 2),
    }])

    result, events = apply_dividends_to_prices(prices, dividends)

    assert events.iloc[0]["application_status"] == "listing_episode_gap"
    assert result["adjusted_cash_dividend"].sum() == 0
    assert result["total_return_close"].tolist() == pytest.approx([100.0, 50.0])


def test_end_to_end_without_cash_events_equals_adjusted_close():
    prices = _prices([
        ("005930", date(2026, 1, 2), 100.0, 90.0),
        ("005930", date(2026, 1, 5), 110.0, 99.0),
    ])

    result, events = build_total_return_close(
        prices,
        _actions([]),
        prices["trade_date"],
    )

    assert events.empty
    assert result["total_return_close"].tolist() == pytest.approx([90.0, 99.0])
