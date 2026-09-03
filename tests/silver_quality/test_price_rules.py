from datetime import date, timedelta

import pandas as pd

from pipeline.silver import assets
from pipeline.silver.prices import (
    _exclude_nonpositive_prices,
    _exclude_unsupported_markets,
    _normalize_incomplete_ohlc,
    _rescale_history_for_events,
    _verify_adj_close_post_publish,
    _with_adj_close,
)
from pipeline.silver_quality.rules.prices import (
    ADJUSTMENT_SEARCH_WINDOW_DAYS,
    _dart_actions_without_krx_adjustment,
    _distribution_drift_confirmation,
    _expected_benchmarks,
    _required_markets,
    check_prices,
)


DAY = date(2026, 7, 8)


def _row(identifier, asset_type, **overrides):
    row = {
        "identifier": identifier,
        "source": "KRX",
        "trade_date": DAY,
        "open": 100.0,
        "high": 110.0,
        "low": 90.0,
        "close": 105.0,
        "adj_close": 105.0,
        "volume": 10,
        "trading_value": 1_000,
        "shares": 1_000 if asset_type == "stock" else None,
        "market_cap": 105_000.0,
        "market": "KOSPI" if asset_type == "stock" else None,
        "asset_type": asset_type,
        "prev_diff": 5.0,
        "fluc_rate": 5.0,
    }
    row.update(overrides)
    return row


def _valid_prices(stock_overrides=None):
    return pd.DataFrame([
        _row("005930", "stock", **(stock_overrides or {})),
        _row("035720", "stock", market="KOSDAQ"),
        _row("1028", "index", shares=None, market=None),
        _row("2203", "index", shares=None, market=None),
    ])


def _failed(results, code):
    return next(r for r in results if r.rule_code == code).failed_count


def _action(**overrides):
    row = {
        "identifier": "005930",
        "event_type": "stock_split",
        "announcement_date": DAY,
        "effective_date": DAY,
        "match_window_days": 3,
        "expected_factor": None,
        "expects_price_adjustment": True,
        "confidence": "EXCHANGE_NOTICE",
        "rcept_no": "20260708000001",
        "report_name": "변경상장(액면분할)",
        "source": "DART_DISCLOSURE",
        "source_file": "fixture.json",
    }
    row.update(overrides)
    return pd.DataFrame([row])


def test_distribution_drift_requires_both_benchmarks_and_market_breadth():
    drift = pd.DataFrame([{
        "trade_date": DAY,
        "median_return": -0.10,
    }])
    confirmed = pd.DataFrame([
        {
            "identifier": identifier,
            "asset_type": "stock",
            "trade_date": DAY,
            "return": value,
        }
        for identifier, value in (
            ("000001", -0.10),
            ("000002", -0.08),
            ("000003", -0.05),
            ("000004", 0.01),
        )
    ] + [
        {
            "identifier": identifier,
            "asset_type": "index",
            "trade_date": DAY,
            "return": -0.07,
        }
        for identifier in ("1028", "2203")
    ])

    evidence, inconsistent = _distribution_drift_confirmation(
        confirmed,
        drift,
    )

    assert len(evidence) == 1
    assert evidence.iloc[0]["same_direction_breadth"] == 0.75
    assert inconsistent.empty

    confirmed.loc[confirmed["identifier"].eq("2203"), "return"] = 0.07
    _, inconsistent = _distribution_drift_confirmation(confirmed, drift)
    assert len(inconsistent) == 1

    benchmarks_only = confirmed[confirmed["asset_type"].eq("index")]
    _, inconsistent = _distribution_drift_confirmation(
        benchmarks_only,
        drift,
    )
    assert len(inconsistent) == 1


def test_zero_ohl_is_normalized_to_null_without_changing_other_values():
    frame = pd.DataFrame([{
        "open": 0,
        "high": 0.0,
        "low": 0,
        "close": 123.0,
        "volume": 10,
        "trading_value": 1_230,
        "market_cap": 123_000,
    }])
    normalized = _normalize_incomplete_ohlc(frame.copy())
    assert normalized[["open", "high", "low"]].isna().all(axis=None)
    assert normalized.loc[0, "close"] == 123.0
    assert normalized.loc[0, "volume"] == 10
    assert normalized.loc[0, "trading_value"] == 1_230
    assert normalized.loc[0, "market_cap"] == 123_000


def test_konex_is_explicitly_excluded():
    frame = pd.DataFrame([
        _row("005930", "stock"),
        _row("123456", "stock", market="KONEX"),
        _row("1028", "index", market=None),
    ])
    retained, detail = _exclude_unsupported_markets(frame)
    assert set(retained["identifier"]) == {"005930", "1028"}
    assert detail["row_count"] == 1
    assert detail["ticker_count"] == 1
    assert detail["markets"] == {"KONEX": 1}


def test_assets_without_supported_price_history_are_excluded():
    asset_frame = pd.DataFrame([
        {"natural_key": "005930", "asset_type": "stock"},
        {"natural_key": "123456", "asset_type": "stock"},
        {"natural_key": "1028", "asset_type": "index"},
    ])
    identifier_frame = pd.DataFrame([
        {"natural_key": "005930", "source": "KRX", "identifier": "005930"},
        {"natural_key": "005930", "source": "DART", "identifier": "00126380"},
        {"natural_key": "123456", "source": "KRX", "identifier": "123456"},
        {"natural_key": "1028", "source": "KRX", "identifier": "1028"},
    ])
    retained_assets, retained_identifiers = assets.restrict_to_price_universe(
        asset_frame,
        identifier_frame,
        {"005930"},
    )
    assert set(retained_assets["natural_key"]) == {"005930", "1028"}
    assert set(retained_identifiers["natural_key"]) == {"005930", "1028"}


def test_preferred_share_maps_to_unique_common_issuer_name():
    frame = pd.DataFrame([
        {"natural_key": "001520", "name": "동양", "asset_type": "stock"},
        {"natural_key": "001529", "name": "동양3우B", "asset_type": "stock"},
        {"natural_key": "1028", "name": "KOSPI200", "asset_type": "index"},
    ])
    assert assets.preferred_share_issuer_map(frame) == {
        "001529": "001520",
    }


def test_adjusted_close_resets_after_long_ticker_absence():
    frame = pd.DataFrame([
        {
            "ticker": "036220",
            "trade_date": date(2016, 5, 1),
            "close": 100.0,
            "prev_diff": 0.0,
        },
        {
            "ticker": "036220",
            "trade_date": date(2024, 3, 13),
            "close": 500.0,
            "prev_diff": 0.0,
        },
    ])
    adjusted = _with_adj_close(frame)
    assert adjusted["adj_close"].tolist() == [100.0, 500.0]


def test_duplicate_price_blocks():
    frame = _valid_prices()
    frame = pd.concat([frame, frame.iloc[[0]]], ignore_index=True)
    results = check_prices(frame, target_date=DAY)
    duplicate = next(r for r in results if r.rule_code == "COMMON_DUPLICATE_KEY")
    assert duplicate.blocks_publish
    assert duplicate.failed_count == 2


def test_suspended_stock_shape_is_allowed():
    frame = _valid_prices({
        "open": 0.0,
        "high": 0.0,
        "low": 0.0,
        "close": 100.0,
        "adj_close": 100.0,
        "volume": 0,
        "trading_value": 0,
        "market_cap": 100_000.0,
        "prev_diff": 0.0,
        "fluc_rate": 0.0,
    })
    results = check_prices(frame, target_date=DAY)
    assert _failed(results, "PRICE_OHLC_LOGIC") == 0
    no_trade = next(r for r in results if r.rule_code == "SOURCE_NO_TRADE_OHLC")
    assert no_trade.severity.value == "MODIFIED"
    assert no_trade.status.value == "PASS"
    assert "observed_rows=1" in no_trade.actual


def test_partial_zero_ohlc_blocks():
    frame = _valid_prices({"open": 0.0})
    result = next(
        r for r in check_prices(frame, target_date=DAY)
        if r.rule_code == "PRICE_OHLC_LOGIC"
    )
    assert result.blocks_publish


def test_active_close_only_ohlc_is_explained():
    frame = _valid_prices({
        "open": None,
        "high": None,
        "low": None,
        "close": 100.0,
        "adj_close": 100.0,
        "volume": 123,
        "trading_value": 12_300,
        "market_cap": 100_000.0,
    })
    results = check_prices(frame, target_date=DAY)
    assert _failed(results, "PRICE_OHLC_LOGIC") == 0
    explained = next(r for r in results if r.rule_code == "SOURCE_INCOMPLETE_OHLC")
    assert explained.failed_count == 0
    assert explained.severity.value == "MODIFIED"
    assert explained.status.value == "PASS"
    assert "explained_rows=1" in explained.actual


def test_index_market_cap_is_not_required():
    frame = _valid_prices()
    frame.loc[frame["asset_type"].eq("index"), "market_cap"] = None
    assert _failed(check_prices(frame, target_date=DAY), "PRICE_REQUIRED_POSITIVE") == 0


def test_stock_market_cap_is_required():
    frame = _valid_prices({"market_cap": None})
    result = next(
        r for r in check_prices(frame, target_date=DAY)
        if r.rule_code == "PRICE_REQUIRED_POSITIVE"
    )
    assert result.blocks_publish
    assert result.failed_count == 1


def test_corrupted_adj_close_blocks_full_series():
    frame = _valid_prices()
    frame.loc[frame["identifier"].eq("005930"), "adj_close"] = 999.0
    result = next(
        r for r in check_prices(frame)
        if r.rule_code == "ADJ_CLOSE_RECONCILIATION"
    )
    assert result.blocks_publish
    assert result.failed_count == 1


def test_missing_fluc_rate_with_baseline_blocks():
    # A row that HAS a valid prior close (previous>0) but lacks fluc_rate is a
    # genuine gap and must block.
    frame = _valid_prices()
    frame.loc[frame["identifier"].eq("005930"), "fluc_rate"] = None
    result = next(
        r for r in check_prices(frame)
        if r.rule_code == "ADJ_CLOSE_SOURCE_FIELDS"
    )
    assert result.blocks_publish
    assert result.failed_count == 1


def test_missing_prev_diff_is_tolerated_as_no_baseline():
    # No prev_diff -> no usable prior close -> adj_close uses factor 1; treat as
    # no-baseline (MODIFIED), not a blocking error.
    frame = _valid_prices()
    frame.loc[frame["identifier"].eq("005930"), "prev_diff"] = None
    results = check_prices(frame)
    assert _failed(results, "ADJ_CLOSE_SOURCE_FIELDS") == 0
    baseline = next(
        r for r in results if r.rule_code == "PRICE_NO_ADJUSTMENT_BASELINE"
    )
    assert baseline.severity.value == "MODIFIED"


def test_price_spike_is_warning_only():
    frame = _valid_prices({
        "open": 100.0,
        "high": 105.0,
        "low": 95.0,
        "close": 100.0,
        "adj_close": 100.0,
        "market_cap": 100_000.0,
        "prev_diff": 90.0,
        "fluc_rate": 900.0,
    })
    history = pd.DataFrame([{
        "identifier": "005930",
        "trade_date": date(2026, 7, 7),
        "close": 10.0,
    }])
    result = next(
        r for r in check_prices(frame, target_date=DAY, history=history)
        if r.rule_code == "PRICE_RETURN_SPIKE"
    )
    assert result.failed_count == 1
    assert not result.blocks_publish


def test_corporate_action_is_not_a_return_or_scale_warning():
    frame = _valid_prices({
        "open": 1_000.0,
        "high": 1_050.0,
        "low": 950.0,
        "close": 1_000.0,
        "adj_close": 1_000.0,
        "market_cap": 100_000.0,
        "prev_diff": 0.0,
        "fluc_rate": 0.0,
    })
    history = pd.DataFrame([{
        "identifier": "005930",
        "trade_date": date(2026, 7, 7),
        "close": 100.0,
        "adj_close": 100.0,
        "market": "KOSPI",
        "asset_type": "stock",
    }])
    results = check_prices(
        frame,
        target_date=DAY,
        history=history,
        corporate_actions=_action(),
    )
    assert _failed(results, "PRICE_RETURN_SPIKE") == 0
    assert _failed(results, "PRICE_SCALE_JUMP") == 0
    assert _failed(results, "PRICE_ADJUSTMENT_WITHOUT_DART_EVENT") == 0


def test_unconfirmed_krx_adjustment_is_warning():
    frame = _valid_prices({
        "open": 1_000.0,
        "high": 1_050.0,
        "low": 950.0,
        "close": 1_000.0,
        "adj_close": 1_000.0,
        "market_cap": 100_000.0,
        "prev_diff": 0.0,
        "fluc_rate": 0.0,
    })
    history = pd.DataFrame([{
        "identifier": "005930",
        "trade_date": date(2026, 7, 7),
        "close": 100.0,
        "adj_close": 100.0,
        "market": "KOSPI",
        "asset_type": "stock",
    }])
    results = check_prices(frame, target_date=DAY, history=history)
    assert _failed(results, "PRICE_ADJUSTMENT_WITHOUT_DART_EVENT") == 1
    assert _failed(results, "PRICE_SCALE_JUMP") == 1


def _resumption_reset_setup():
    frame = _valid_prices({
        "open": 300.0,
        "high": 310.0,
        "low": 290.0,
        "close": 300.0,
        "adj_close": 300.0,
        "market_cap": 300_000.0,
        "prev_diff": 0.0,
        "fluc_rate": 0.0,
    })
    history = pd.DataFrame([{
        "identifier": "005930",
        "trade_date": date(2026, 7, 7),
        "close": 100.0,
        "adj_close": 100.0,
        "market": "KOSPI",
        "asset_type": "stock",
    }])
    return frame, history


def _result(results, code):
    return next(r for r in results if r.rule_code == code)


def test_resumption_reset_is_explained_not_warning():
    # 거래재개 기준가 리셋(factor 3배)은 economic_return이 0에 가까워
    # 특별거래(30.5% 초과) 근거로는 안 잡히지만, 정지해제 공시로 설명돼
    # PRICE_ADJUSTMENT_WITHOUT_DART_EVENT에서 제외돼야 한다.
    frame, history = _resumption_reset_setup()
    resumption = _action(
        event_type="trading_halt",
        report_name="주권매매거래정지해제(상장적격성 실질심사)",
        effective_date=None,
        expects_price_adjustment=False,
        expected_factor=None,
    )
    results = check_prices(
        frame,
        target_date=DAY,
        history=history,
        corporate_actions=resumption,
    )
    assert _failed(results, "PRICE_ADJUSTMENT_WITHOUT_DART_EVENT") == 0


def test_resumption_reset_detected_by_suspension_signature():
    # 정지해제 공시가 없어도 직전 거래일이 무거래(volume=0)면 거래재개로
    # 결정적으로 식별해 설명한다(공시 비의존 시그니처).
    frame, _ = _resumption_reset_setup()
    history = pd.DataFrame([{
        "identifier": "005930",
        "trade_date": date(2026, 7, 7),
        "close": 100.0,
        "adj_close": 100.0,
        "market": "KOSPI",
        "asset_type": "stock",
        "volume": 0,
    }])
    results = check_prices(frame, target_date=DAY, history=history)
    assert _failed(results, "PRICE_ADJUSTMENT_WITHOUT_DART_EVENT") == 0


def test_reference_reset_without_resumption_stays_warning():
    # 정지해제 공시도 무거래 시그니처도 없으면 여전히 Warning으로 남는다.
    frame, history = _resumption_reset_setup()
    results = check_prices(frame, target_date=DAY, history=history)
    assert _failed(results, "PRICE_ADJUSTMENT_WITHOUT_DART_EVENT") == 1


def _dart_action_frame(reset_offset_days):
    # 005930 주식: 효력일(07-08) 근처엔 리셋이 없고, reset_offset_days만큼
    # 떨어진 날에 KRX 기준가 리셋(source_adjustment_event=True)이 있다.
    eff = date(2026, 7, 8)
    rows = [
        {"identifier": "005930", "asset_type": "stock",
         "trade_date": date(2026, 7, 1),
         "source_adjustment_event": False, "source_adjustment_factor": 1.0},
        {"identifier": "005930", "asset_type": "stock",
         "trade_date": eff,
         "source_adjustment_event": False, "source_adjustment_factor": 1.0},
        {"identifier": "005930", "asset_type": "stock",
         "trade_date": eff + timedelta(days=reset_offset_days),
         "source_adjustment_event": True, "source_adjustment_factor": 0.8},
    ]
    frame = pd.DataFrame(rows)
    actions = pd.DataFrame([{
        "identifier": "005930", "event_type": "rights_detachment",
        "effective_date": eff, "match_window_days": 3,
        "expects_price_adjustment": True, "rcept_no": "r1",
    }])
    candidate_dates = set(frame["trade_date"])
    return _dart_actions_without_krx_adjustment(frame, actions, candidate_dates)


def test_dart_action_misaligned_reset_within_window_is_not_flagged():
    # 실제 KRX 리셋이 효력일에서 며칠 어긋나도 ±15일 창 안이면 반영으로 본다.
    missing = _dart_action_frame(reset_offset_days=8)
    assert missing.empty


def test_dart_action_with_no_nearby_reset_is_flagged():
    # 리셋이 ±15일 창 밖(22일)이면 진짜 조정 누락으로 남긴다.
    assert 22 > ADJUSTMENT_SEARCH_WINDOW_DAYS
    missing = _dart_action_frame(reset_offset_days=22)
    assert len(missing) == 1
    assert missing.iloc[0]["identifier"] == "005930"


def test_future_dart_action_is_not_flagged_before_effective_date():
    effective = date(2026, 8, 4)
    frame = pd.DataFrame([{
        "identifier": "005930",
        "asset_type": "stock",
        "trade_date": date(2026, 7, 31),
        "source_adjustment_event": False,
        "source_adjustment_factor": 1.0,
    }])
    actions = pd.DataFrame([{
        "identifier": "005930",
        "event_type": "bonus_issue",
        "effective_date": effective,
        "match_window_days": 7,
        "expects_price_adjustment": True,
        "rcept_no": "r1",
    }])

    missing = _dart_actions_without_krx_adjustment(
        frame,
        actions,
        {date(2026, 7, 31)},
    )

    assert missing.empty


def test_reciprocal_share_change_explains_scale_jump_as_krx_structure():
    frame = _valid_prices({
        "open": 1_000.0,
        "high": 1_050.0,
        "low": 950.0,
        "close": 1_000.0,
        "adj_close": 1_000.0,
        "shares": 100,
        "market_cap": 100_000.0,
        "prev_diff": 0.0,
        "fluc_rate": 0.0,
    })
    history = pd.DataFrame([{
        "identifier": "005930",
        "trade_date": date(2026, 7, 7),
        "close": 100.0,
        "adj_close": 1_000.0,
        "market": "KOSPI",
        "asset_type": "stock",
        "shares": 1_000,
        "market_cap": 100_000.0,
    }])
    results = check_prices(frame, target_date=DAY, history=history)

    assert _failed(results, "PRICE_SCALE_JUMP") == 0
    # KRX 가격·주식수·시총의 독립 구조 근거가 있으므로 DART 미대사
    # 경고를 중복 발생시키지 않는다.
    assert _failed(results, "PRICE_ADJUSTMENT_WITHOUT_DART_EVENT") == 0


def test_special_trading_event_is_info_not_return_or_scale_warning():
    frame = _valid_prices({
        "open": 10.0,
        "high": 10.0,
        "low": 10.0,
        "close": 10.0,
        "adj_close": 10.0,
        "shares": 1_000,
        "market_cap": 10_000.0,
        "prev_diff": -90.0,
        "fluc_rate": -90.0,
    })
    history = pd.DataFrame([{
        "identifier": "005930",
        "trade_date": date(2026, 7, 7),
        "close": 100.0,
        "adj_close": 100.0,
        "market": "KOSPI",
        "asset_type": "stock",
        "shares": 1_000,
        "market_cap": 100_000.0,
    }])
    action = _action(
        event_type="delisting",
        announcement_date=date(2026, 7, 5),
        effective_date=None,
        match_window_days=0,
        expects_price_adjustment=False,
        report_name="상장폐지에 따른 정리매매 개시",
    )
    results = check_prices(
        frame,
        target_date=DAY,
        history=history,
        corporate_actions=action,
    )

    assert _failed(results, "PRICE_SCALE_JUMP") == 0
    assert _failed(results, "PRICE_RETURN_SPIKE") == 0


def test_special_trading_episode_accepts_notice_up_to_120_days_before():
    frame = _valid_prices({
        "close": 50.0,
        "adj_close": 50.0,
        "open": 50.0,
        "high": 50.0,
        "low": 50.0,
        "prev_diff": -50.0,
        "market_cap": 50_000.0,
    })
    history = pd.DataFrame([{
        "identifier": "005930",
        "trade_date": date(2026, 7, 7),
        "close": 100.0,
        "adj_close": 100.0,
        "market": "KOSPI",
        "asset_type": "stock",
        "shares": 1_000,
        "market_cap": 100_000.0,
    }])
    action = _action(
        event_type="delisting",
        announcement_date=date(2026, 4, 9),
        effective_date=None,
        match_window_days=0,
        expects_price_adjustment=False,
        report_name="상장폐지 관련 정리매매",
    )
    results = check_prices(
        frame,
        target_date=DAY,
        history=history,
        corporate_actions=action,
    )
    assert _failed(results, "PRICE_RETURN_SPIKE") == 0


def test_reviewed_terminal_settlement_spike_is_explained():
    rows = []
    closes = [100.0, 100.0, 100.0, 100.0, 100.0, 200.0, 100.0]
    for offset, close in enumerate(closes):
        previous = closes[offset - 1] if offset else close
        rows.append(_row(
            "000800",
            "stock",
            trade_date=date(2015, 4, 1 + offset),
            open=close,
            high=close,
            low=close,
            close=close,
            adj_close=close,
            prev_diff=close - previous,
            fluc_rate=(close / previous - 1) * 100,
            shares=1_000,
            market_cap=close * 1_000,
        ))
    # 전체 cutoff 이후에도 존재하는 활성 종목이 있어야 000800 시계열이
    # 종료된 과거 episode임을 확정할 수 있다.
    rows.extend([
        _row("005930", "stock", trade_date=DAY),
        _row("035720", "stock", market="KOSDAQ", trade_date=DAY),
        _row("1028", "index", shares=None, market=None, trade_date=DAY),
        _row("2203", "index", shares=None, market=None, trade_date=DAY),
    ])

    results = check_prices(pd.DataFrame(rows))

    assert _failed(results, "PRICE_RETURN_SPIKE") == 0
    assert _failed(results, "PRICE_ROUND_TRIP_SPIKE") == 0


def test_unreviewed_terminal_spike_remains_warning():
    frame = pd.DataFrame([
        _row(
            "123456",
            "stock",
            trade_date=date(2020, 1, 2),
            close=100.0,
            adj_close=100.0,
            prev_diff=0.0,
            shares=1_000,
            market_cap=100_000.0,
        ),
        _row(
            "123456",
            "stock",
            trade_date=date(2020, 1, 3),
            open=200.0,
            high=200.0,
            low=200.0,
            close=200.0,
            adj_close=200.0,
            prev_diff=100.0,
            shares=1_000,
            market_cap=200_000.0,
        ),
        _row("005930", "stock", trade_date=DAY),
        _row("035720", "stock", market="KOSDAQ", trade_date=DAY),
        _row("1028", "index", shares=None, market=None, trade_date=DAY),
        _row("2203", "index", shares=None, market=None, trade_date=DAY),
    ])

    results = check_prices(frame)

    assert _failed(results, "PRICE_RETURN_SPIKE") == 1


def test_capital_reduction_compares_dart_to_actual_share_change():
    frame = _valid_prices({
        "open": 1_000.0,
        "high": 1_050.0,
        "low": 950.0,
        "close": 1_000.0,
        "adj_close": 1_000.0,
        "market_cap": 100_000.0,
        "shares": 100,
        "prev_diff": 800.0,
        "fluc_rate": 0.0,
    })
    history = pd.DataFrame([{
        "identifier": "005930",
        "trade_date": date(2026, 7, 7),
        "close": 100.0,
        "adj_close": 100.0,
        "market": "KOSPI",
        "asset_type": "stock",
        "shares": 1_000,
    }])
    results = check_prices(
        frame,
        target_date=DAY,
        history=history,
        corporate_actions=_action(
            event_type="capital_reduction",
            expected_factor=None,
            share_count_factor=8.0,
            share_count_factor_comparable=True,
            action_method="보통주식 8대 1 무상감자",
            source="DART_STRUCTURED",
        ),
    )
    assert _failed(results, "CORPORATE_ACTION_FACTOR_MISMATCH") == 0
    assert _failed(results, "DART_SHARE_COUNT_FACTOR_MISMATCH") == 1


def test_capital_reduction_matching_actual_share_change_passes():
    frame = _valid_prices({
        "open": 1_000.0,
        "high": 1_050.0,
        "low": 950.0,
        "close": 1_000.0,
        "adj_close": 1_000.0,
        "shares": 100,
        "market_cap": 100_000.0,
        "prev_diff": 800.0,
        "fluc_rate": 0.0,
    })
    history = pd.DataFrame([{
        "identifier": "005930",
        "trade_date": date(2026, 7, 7),
        "close": 100.0,
        "adj_close": 100.0,
        "market": "KOSPI",
        "asset_type": "stock",
        "shares": 800,
        "market_cap": 80_000.0,
    }])
    results = check_prices(
        frame,
        target_date=DAY,
        history=history,
        corporate_actions=_action(
            event_type="capital_reduction",
            expected_factor=None,
            share_count_factor=8.0,
            share_count_factor_comparable=True,
            action_method="보통주식 8대 1 무상감자",
            source="DART_STRUCTURED",
        ),
    )
    assert _failed(results, "DART_SHARE_COUNT_FACTOR_MISMATCH") == 0


def test_non_uniform_reduction_is_explained_not_compared():
    frame = _valid_prices({
        "open": 1_000.0,
        "high": 1_050.0,
        "low": 950.0,
        "close": 1_000.0,
        "adj_close": 1_000.0,
        "shares": 100,
        "market_cap": 100_000.0,
        "prev_diff": 800.0,
        "fluc_rate": 0.0,
    })
    history = pd.DataFrame([{
        "identifier": "005930",
        "trade_date": date(2026, 7, 7),
        "close": 100.0,
        "adj_close": 100.0,
        "market": "KOSPI",
        "asset_type": "stock",
        "shares": 1_000,
        "market_cap": 100_000.0,
    }])
    results = check_prices(
        frame,
        target_date=DAY,
        history=history,
        corporate_actions=_action(
            event_type="capital_reduction",
            expected_factor=None,
            share_count_factor=8.0,
            share_count_factor_comparable=False,
            action_method="최대주주 보유주식만 8대 1 병합",
            source="DART_STRUCTURED",
        ),
    )
    assert _failed(results, "DART_SHARE_COUNT_FACTOR_MISMATCH") == 0
    assert _failed(results, "DART_ACTION_WITHOUT_KRX_ADJUSTMENT") == 0


def test_dart_issued_share_scope_difference_is_not_factor_mismatch():
    frame = _valid_prices({
        "shares": 100,
        "market_cap": 10_500.0,
    })
    history = pd.DataFrame([{
        "identifier": "005930",
        "trade_date": date(2026, 7, 7),
        "close": 100.0,
        "adj_close": 100.0,
        "market": "KOSPI",
        "asset_type": "stock",
        "shares": 800,
        "market_cap": 80_000.0,
    }])
    results = check_prices(
        frame,
        target_date=DAY,
        history=history,
        corporate_actions=_action(
            event_type="capital_reduction",
            share_count_factor=8.0,
            share_count_before=8_000,
            share_count_after=1_000,
            share_count_factor_comparable=True,
            share_count_comparison_reason="UNIFORM_REDUCTION",
            action_method="보통주식 8대 1 무상감자",
            source="DART_STRUCTURED",
        ),
    )

    assert _failed(results, "DART_SHARE_COUNT_FACTOR_MISMATCH") == 0


def test_dart_action_without_krx_adjustment_is_warning():
    frame = _valid_prices()
    history = pd.DataFrame([{
        "identifier": "005930",
        "trade_date": date(2026, 7, 7),
        "close": 100.0,
        "adj_close": 100.0,
        "market": "KOSPI",
        "asset_type": "stock",
    }])
    results = check_prices(
        frame,
        target_date=DAY,
        history=history,
        corporate_actions=_action(
            event_type="bonus_issue",
            expected_factor=0.5,
            source="DART_STRUCTURED",
        ),
    )
    assert _failed(results, "DART_ACTION_WITHOUT_KRX_ADJUSTMENT") == 1


def test_dart_action_accepts_krx_adjustment_anywhere_in_holiday_window():
    frame = _valid_prices()
    history = pd.DataFrame([
        {
            "identifier": "005930",
            "trade_date": date(2026, 7, 6),
            "close": 100.0,
            "adj_close": 50.0,
            "market": "KOSPI",
            "asset_type": "stock",
            "prev_diff": 0.0,
        },
        {
            "identifier": "005930",
            "trade_date": date(2026, 7, 7),
            "close": 50.0,
            "adj_close": 50.0,
            "market": "KOSPI",
            "asset_type": "stock",
            "prev_diff": 0.0,
        },
    ])
    results = check_prices(
        frame,
        target_date=DAY,
        history=history,
        corporate_actions=_action(
            event_type="bonus_issue",
            effective_date=date(2026, 7, 5),
            expected_factor=0.5,
            source="DART_STRUCTURED",
        ),
    )
    assert _failed(results, "DART_ACTION_WITHOUT_KRX_ADJUSTMENT") == 0


def test_wrong_daily_adj_close_continuity_blocks():
    frame = _valid_prices({
        "open": 1_000.0,
        "high": 1_050.0,
        "low": 950.0,
        "close": 1_000.0,
        "adj_close": 900.0,
        "market_cap": 100_000.0,
        "prev_diff": 0.0,
        "fluc_rate": 0.0,
    })
    history = pd.DataFrame([{
        "identifier": "005930",
        "trade_date": date(2026, 7, 7),
        "close": 100.0,
        "adj_close": 100.0,
        "market": "KOSPI",
        "asset_type": "stock",
    }])
    result = next(
        r for r in check_prices(
            frame,
            target_date=DAY,
            history=history,
        )
        if r.rule_code == "ADJ_CLOSE_RETURN_CONTINUITY"
    )
    assert result.blocks_publish
    assert result.failed_count == 1


def test_small_source_adjustment_is_applied_to_daily_continuity():
    frame = _valid_prices({
        "open": 101.0,
        "high": 101.0,
        "low": 101.0,
        "close": 101.0,
        "adj_close": 101.0,
        "market_cap": 101_000.0,
        "prev_diff": 0.9,
        "fluc_rate": 0.8991,
    })
    history = pd.DataFrame([{
        "identifier": "005930",
        "trade_date": date(2026, 7, 7),
        "close": 100.0,
        "adj_close": 100.0,
        "market": "KOSPI",
        "asset_type": "stock",
    }])
    results = check_prices(frame, target_date=DAY, history=history)
    assert _failed(results, "ADJ_CLOSE_RETURN_CONTINUITY") == 0


def test_daily_continuity_is_idempotent_when_history_is_already_rescaled():
    frame = _valid_prices({
        "open": 101.0,
        "high": 101.0,
        "low": 101.0,
        "close": 101.0,
        "adj_close": 101.0,
        "market_cap": 101_000.0,
        "prev_diff": 0.9,
        "fluc_rate": 0.8991,
    })
    history = pd.DataFrame([{
        "identifier": "005930",
        "trade_date": date(2026, 7, 7),
        "close": 100.0,
        "adj_close": 100.1,
        "market": "KOSPI",
        "asset_type": "stock",
    }])

    results = check_prices(frame, target_date=DAY, history=history)

    assert _failed(results, "ADJ_CLOSE_RETURN_CONTINUITY") == 0


class _RescaleCursor:
    def __init__(self):
        self.query = ""
        self.rowcount = 0
        self.update_params = None

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, query, params):
        self.query = query
        if query.startswith("UPDATE"):
            self.update_params = params
            self.rowcount = 1

    def fetchone(self):
        return (date(2026, 7, 7), 100.0, 100.0)


class _RescaleConnection:
    def __init__(self):
        self._cursor = _RescaleCursor()

    def cursor(self):
        return self._cursor


def test_small_source_adjustment_rescales_published_history():
    connection = _RescaleConnection()
    stock = pd.DataFrame([{
        "asset_id": 1,
        "close": 101.0,
        "prev_diff": 0.9,
    }])
    fixed = _rescale_history_for_events(connection, stock, DAY)
    assert fixed == 1
    assert "ROUND(adj_close * %s, 4)" in connection._cursor.query
    factor, asset_id, target_date = connection._cursor.update_params
    assert abs(factor - 1.001) < 1e-12
    assert asset_id == 1
    assert target_date == DAY


class _AlreadyAppliedRescaleCursor(_RescaleCursor):
    def fetchone(self):
        return (date(2026, 7, 7), 100.0, 100.09999999)


class _AlreadyAppliedRescaleConnection:
    def __init__(self):
        self._cursor = _AlreadyAppliedRescaleCursor()

    def cursor(self):
        return self._cursor


def test_already_applied_rescale_normalizes_legacy_extra_decimals():
    connection = _AlreadyAppliedRescaleConnection()
    stock = pd.DataFrame([{
        "asset_id": 1,
        "close": 101.0,
        "prev_diff": 0.9,
    }])

    assert _rescale_history_for_events(connection, stock, DAY) == 0
    assert "SET adj_close = ROUND(adj_close, 4)" in connection._cursor.query
    assert connection._cursor.update_params == (1, DAY)


class _LongGapRescaleCursor(_RescaleCursor):
    def fetchone(self):
        return (date(2024, 1, 1), 100.0, 100.0)


class _LongGapRescaleConnection:
    def __init__(self):
        self._cursor = _LongGapRescaleCursor()

    def cursor(self):
        return self._cursor


def test_long_listing_gap_does_not_rescale_old_issuer_history():
    connection = _LongGapRescaleConnection()
    stock = pd.DataFrame([{
        "asset_id": 1,
        "close": 101.0,
        "prev_diff": 0.9,
    }])
    assert _rescale_history_for_events(connection, stock, DAY) == 0
    assert connection._cursor.update_params is None


class _FakeCursor:
    def __init__(self, current_adj_close):
        self.current_adj_close = current_adj_close
        self.query = ""

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, query, _params):
        self.query = query

    def fetchall(self):
        if "trade_date=%s" in self.query:
            return [(1, 1_000.0, self.current_adj_close)]
        if "trade_date < %s" in self.query:
            return [(1, date(2026, 7, 7), 100.0, 1_000.0)]
        return []


class _FakeConnection:
    def __init__(self, current_adj_close):
        self.current_adj_close = current_adj_close

    def cursor(self):
        return _FakeCursor(self.current_adj_close)


class _LongGapFakeCursor(_FakeCursor):
    def fetchall(self):
        if "trade_date=%s" in self.query:
            return [(1, 1_000.0, self.current_adj_close)]
        if "trade_date < %s" in self.query:
            return [(1, date(2024, 1, 1), 100.0, 100.0)]
        return []


class _LongGapFakeConnection(_FakeConnection):
    def cursor(self):
        return _LongGapFakeCursor(self.current_adj_close)


def test_post_publish_adj_close_verification():
    candidates = pd.DataFrame([{
        "asset_id": 1,
        "asset_type": "stock",
        "prev_diff": 0.0,
    }])
    _verify_adj_close_post_publish(
        _FakeConnection(1_000.0),
        candidates,
        DAY,
    )


def test_post_publish_resets_adj_close_at_new_listing_episode():
    candidates = pd.DataFrame([{
        "asset_id": 1,
        "asset_type": "stock",
        "prev_diff": 900.0,
    }])
    _verify_adj_close_post_publish(
        _LongGapFakeConnection(1_000.0),
        candidates,
        DAY,
    )


def test_post_publish_adj_close_verification_rejects_mismatch():
    candidates = pd.DataFrame([{
        "asset_id": 1,
        "asset_type": "stock",
        "prev_diff": 0.0,
    }])
    try:
        _verify_adj_close_post_publish(
            _FakeConnection(900.0),
            candidates,
            DAY,
        )
    except RuntimeError as exc:
        assert "ADJ_CLOSE_POST_PUBLISH failed" in str(exc)
    else:
        raise AssertionError("expected post-publish verification failure")


# --- historical coverage: date-aware market/benchmark completeness ---------

def test_inception_helpers_have_correct_boundaries():
    assert _required_markets(date(1996, 6, 30)) == {"KOSPI"}
    assert _required_markets(date(1996, 7, 1)) == {"KOSPI", "KOSDAQ"}
    # KRX backfills both KOSPI200(1028) and KOSDAQ150(2203) to the index base
    # date 2010-01-04; before that no benchmark exists.
    assert _expected_benchmarks(date(2010, 1, 3)) == set()
    assert _expected_benchmarks(date(2010, 1, 4)) == {"1028", "2203"}
    assert _expected_benchmarks(date(2015, 7, 13)) == {"1028", "2203"}


def test_market_completeness_allows_kospi_only_before_kosdaq_launch():
    frame = pd.DataFrame([
        _row("000010", "stock", market="KOSPI", trade_date=date(1995, 5, 2)),
    ])
    assert _failed(check_prices(frame), "PRICE_MARKET_COMPLETENESS") == 0


def test_market_completeness_requires_kosdaq_after_launch():
    frame = pd.DataFrame([
        _row("000010", "stock", market="KOSPI", trade_date=date(2020, 5, 4)),
    ])
    assert _failed(check_prices(frame), "PRICE_MARKET_COMPLETENESS") == 1


def test_benchmark_completeness_not_required_before_index_data():
    frame = pd.DataFrame([
        _row("000010", "stock", market="KOSPI", trade_date=date(1998, 3, 2)),
    ])
    assert _failed(check_prices(frame), "PRICE_BENCHMARK_COMPLETENESS") == 0


def test_benchmark_completeness_requires_both_from_2010():
    # KRX provides KOSDAQ150(2203) from 2010; only KOSPI200 is incomplete.
    day = date(2012, 3, 2)
    only_kospi = pd.DataFrame([
        _row("000010", "stock", market="KOSPI", trade_date=day),
        _row("035720", "stock", market="KOSDAQ", trade_date=day),
        _row("1028", "index", shares=None, market=None, trade_date=day),
    ])
    assert _failed(check_prices(only_kospi), "PRICE_BENCHMARK_COMPLETENESS") == 1
    both = pd.concat([
        only_kospi,
        pd.DataFrame([_row("2203", "index", shares=None, market=None, trade_date=day)]),
    ], ignore_index=True)
    assert _failed(check_prices(both), "PRICE_BENCHMARK_COMPLETENESS") == 0


def test_benchmark_completeness_fails_when_benchmark_missing_after_2010():
    day = date(2012, 3, 2)
    frame = pd.DataFrame([
        _row("000010", "stock", market="KOSPI", trade_date=day),
        _row("035720", "stock", market="KOSDAQ", trade_date=day),
    ])
    assert _failed(check_prices(frame), "PRICE_BENCHMARK_COMPLETENESS") == 1


def test_benchmark_completeness_requires_both_after_kosdaq150():
    day = date(2020, 3, 2)
    frame = pd.DataFrame([
        _row("000010", "stock", market="KOSPI", trade_date=day),
        _row("035720", "stock", market="KOSDAQ", trade_date=day),
        _row("1028", "index", shares=None, market=None, trade_date=day),
    ])
    assert _failed(check_prices(frame), "PRICE_BENCHMARK_COMPLETENESS") == 1


def _drift_confirmation_frames(day, benchmark_codes):
    drift = pd.DataFrame([{"trade_date": day, "median_return": -0.10}])
    rows = [
        {"identifier": ident, "asset_type": "stock", "trade_date": day,
         "return": value}
        for ident, value in (
            ("000001", -0.10), ("000002", -0.08),
            ("000003", -0.05), ("000004", 0.01),
        )
    ] + [
        {"identifier": code, "asset_type": "index", "trade_date": day,
         "return": -0.07}
        for code in benchmark_codes
    ]
    return drift, pd.DataFrame(rows)


def test_drift_consistency_both_benchmarks_from_2010_is_consistent():
    # 2010+: both KOSPI200 and KOSDAQ150 exist and confirm the move.
    day = date(2012, 3, 2)
    drift, confirmed = _drift_confirmation_frames(day, ["1028", "2203"])
    _, inconsistent = _distribution_drift_confirmation(confirmed, drift)
    assert inconsistent.empty


def test_drift_consistency_pre_index_relies_on_breadth_only():
    # Before 2010 there is no benchmark; high breadth alone confirms the move.
    day = date(2005, 3, 2)
    drift, confirmed = _drift_confirmation_frames(day, [])
    _, inconsistent = _distribution_drift_confirmation(confirmed, drift)
    assert inconsistent.empty


def test_drift_consistency_flags_missing_expected_benchmark():
    # 2020: both benchmarks expected; supplying only one is inconsistent.
    day = date(2020, 3, 2)
    drift, confirmed = _drift_confirmation_frames(day, ["1028"])
    _, inconsistent = _distribution_drift_confirmation(confirmed, drift)
    assert len(inconsistent) == 1


# --- historical coverage: exclude non-positive price rows ------------------

def _stock_price_row(ticker, close, shares, market_cap, trade_date=None):
    return {
        "ticker": ticker,
        "trade_date": trade_date or date(2001, 3, 2),
        "open": close,
        "high": close,
        "low": close,
        "close": close,
        "volume": 10,
        "trading_value": 1_000,
        "shares": shares,
        "market_cap": market_cap,
        "prev_diff": 0.0,
        "market": "KOSPI",
        "source_file": "marcap-2001.parquet",
    }


def test_nonpositive_price_rows_are_excluded_with_reason():
    frame = pd.DataFrame([
        _stock_price_row("000010", 1000.0, 1_000, 1_000_000.0),   # valid
        _stock_price_row("000020", 0.0, 1_000, 0.0),              # close/mcap 0
        _stock_price_row("000030", 500.0, 0, 0.0),               # shares/mcap 0
        _stock_price_row("000040", 500.0, 1_000, None),          # mcap NaN
    ])
    retained, detail = _exclude_nonpositive_prices(frame)
    assert set(retained["ticker"]) == {"000010"}
    assert detail["row_count"] == 3
    assert detail["ticker_count"] == 3
    assert len(detail["samples"]) == 3


def test_all_valid_prices_are_not_excluded():
    frame = pd.DataFrame([
        _stock_price_row("000010", 1000.0, 1_000, 1_000_000.0),
        _stock_price_row("000020", 2000.0, 2_000, 4_000_000.0),
    ])
    retained, detail = _exclude_nonpositive_prices(frame)
    assert len(retained) == 2
    assert detail["row_count"] == 0
    assert detail["ticker_count"] == 0


def test_no_prior_close_row_is_tolerated_not_flagged():
    # New-listing/resumption style row: 전일대비 == 종가 -> implied prior close 0.
    # adj_close uses factor=1 for such rows, so change-field checks must skip them.
    day = date(2012, 3, 2)
    frame = pd.DataFrame([
        _row("000010", "stock", market="KOSPI", trade_date=day,
             open=1000.0, high=1000.0, low=1000.0, close=1000.0,
             adj_close=1000.0, prev_diff=1000.0, fluc_rate=float("nan"),
             shares=1000, market_cap=1_000_000.0),
        _row("035720", "stock", market="KOSDAQ", trade_date=day),
        _row("1028", "index", shares=None, market=None, trade_date=day),
        _row("2203", "index", shares=None, market=None, trade_date=day),
    ])
    results = check_prices(frame)
    assert _failed(results, "ADJ_CLOSE_SOURCE_FIELDS") == 0
    assert _failed(results, "PRICE_KRX_ARITHMETIC") == 0
    baseline = _result(results, "PRICE_NO_ADJUSTMENT_BASELINE")
    assert baseline.severity.value == "MODIFIED"
    assert "no_baseline_rows=1" in baseline.actual


def test_normalize_nulls_logically_inconsistent_ohlc():
    frame = pd.DataFrame([{
        "open": 100.0, "high": 90.0, "low": 80.0, "close": 105.0,
        "volume": 10, "trading_value": 1000, "market_cap": 1000,
    }])  # high(90) < close(105): inconsistent
    out = _normalize_incomplete_ohlc(frame.copy())
    assert out[["open", "high", "low"]].isna().all(axis=None)
    assert out.loc[0, "close"] == 105.0
