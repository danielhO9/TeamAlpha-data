from datetime import date
from types import SimpleNamespace
from uuid import uuid4

import pandas as pd
import pytest

import pipeline.silver.total_return_rebuild as rebuild
import pipeline.silver.dart_extra_load as dart_extra_load
from pipeline.silver.cash_adjustment_scale_evidence import (
    BoundScaleSourceEvidence,
)
from pipeline.silver.total_return_rebuild import (
    BatchRebuild,
    LocalActionSnapshot,
    RebuildSummary,
    _build_batch,
    _assert_dividend_yields,
    _issuer_dart_actions,
    _prepare_local_action_snapshot,
    _publish_batch,
    parse_args,
    run,
)


def _prices(rows):
    return pd.DataFrame(rows, columns=[
        "asset_id", "identifier", "trade_date", "close", "adj_close",
    ])


def _actions(rows):
    defaults = {
        "asset_id": 1,
        "identifier": "1",
        "source": "DART_DISCLOSURE",
        "action_key": "20260101000001",
        "event_type": "cash_dividend",
        "announcement_date": date(2026, 1, 1),
        "effective_date": None,
        "record_date": date(2026, 1, 6),
        "cash_amount": 10.0,
        "filing_id": "20260101000001",
    }
    return pd.DataFrame([{**defaults, **row} for row in rows])


def test_build_batch_applies_dividend_and_audits_source_action():
    run_id = uuid4()
    prices = _prices([
        (1, "1", date(2026, 1, 2), 100.0, 100.0),
        (1, "1", date(2026, 1, 5), 95.0, 95.0),
        (1, "1", date(2026, 1, 6), 96.0, 96.0),
    ])
    actions = _actions([{}])

    batch = _build_batch(
        prices,
        actions,
        pd.Series(pd.to_datetime([
            "2026-01-02", "2026-01-05", "2026-01-06",
        ])),
        run_id=run_id,
        max_dividend_yield=1.0,
    )

    assert batch.prices["total_return_close"].tolist() == pytest.approx([
        100.0, 105.0, 106.1052631579,
    ])
    audit = batch.audit.iloc[0]
    assert audit["is_canonical"]
    assert audit["excluded_reason"] is None
    assert audit["resolved_ex_date"] == pd.Timestamp("2026-01-05")
    assert audit["applied_trade_date"] == pd.Timestamp("2026-01-05")
    assert audit["quality_run_id"] == run_id


def test_incremental_extension_only_calculates_new_rows_and_resets_after_gap():
    run_id = uuid4()
    rows = [
        (1, date(2026, 1, 5), 110.0, date(2026, 1, 2), 100.0, 150.0),
        (1, date(2026, 1, 6), 121.0, date(2026, 1, 2), 100.0, 150.0),
        (2, date(2026, 1, 5), 80.0, None, None, None),
        (3, date(2026, 1, 5), 70.0, date(2024, 1, 2), 60.0, 90.0),
    ]

    extended = rebuild._extend_unchanged_price_rows(rows, run_id=run_id)

    assert [row["total_return_close"] for row in extended] == pytest.approx([
        165.0, 181.5, 80.0, 70.0,
    ])
    assert all(
        row["total_return_quality_run_id"] == run_id for row in extended
    )


def test_build_batch_uses_database_half_up_cash_rounding():
    prices = _prices([
        (1, "1", date(2026, 1, 2), 100.0, 100.0),
        (1, "1", date(2026, 1, 5), 100.0, 100.0),
    ])
    actions = _actions([{
        "effective_date": date(2026, 1, 5),
        "cash_amount": 0.000000005,
    }])

    batch = _build_batch(
        prices,
        actions,
        pd.Series(pd.to_datetime(["2026-01-02", "2026-01-05"])),
        run_id=None,
        max_dividend_yield=1.0,
    )

    assert batch.audit.iloc[0]["adjusted_cash_amount"] == 0.00000001
    assert batch.resolution_parity_count == 1


def test_future_event_is_not_silently_applied_and_has_explicit_reason():
    prices = _prices([
        (1, "1", date(2026, 1, 2), 100.0, 100.0),
        (1, "1", date(2026, 1, 5), 99.0, 99.0),
    ])
    actions = _actions([{
        "record_date": date(2026, 1, 8),
    }])

    batch = _build_batch(
        prices,
        actions,
        pd.Series(pd.to_datetime([
            "2026-01-02", "2026-01-05", "2026-01-06",
            "2026-01-07", "2026-01-08",
        ])),
        run_id=None,
        max_dividend_yield=1.0,
    )

    assert not batch.audit.iloc[0]["is_canonical"]
    assert (
        batch.audit.iloc[0]["excluded_reason"]
        == "PENDING_FUTURE_TRADE"
    )
    assert batch.prices["total_return_close"].tolist() == [100.0, 99.0]


def test_record_date_after_global_calendar_end_is_pending_not_inferred():
    prices = _prices([
        (1, "1", date(2026, 1, 2), 100.0, 100.0),
        (1, "1", date(2026, 1, 5), 99.0, 99.0),
    ])
    actions = _actions([{
        "record_date": date(2026, 1, 8),
    }])

    batch = _build_batch(
        prices,
        actions,
        pd.Series(pd.to_datetime([
            "2026-01-02", "2026-01-05",
        ])),
        run_id=None,
        max_dividend_yield=1.0,
    )

    assert (
        batch.audit.iloc[0]["excluded_reason"]
        == "PENDING_FUTURE_TRADE"
    )
    assert pd.isna(batch.audit.iloc[0]["resolved_ex_date"])
    assert batch.prices["total_return_close"].tolist() == [100.0, 99.0]


def test_unresolved_canonical_event_fails_closed():
    prices = _prices([
        (1, "1", date(2026, 1, 2), 100.0, 100.0),
    ])
    actions = _actions([{}])

    with pytest.raises(RuntimeError, match="unresolved canonical dividend"):
        _build_batch(
            prices,
            actions,
            pd.Series(pd.to_datetime(["2026-01-02"])),
            run_id=None,
            max_dividend_yield=1.0,
        )


def test_event_before_price_history_is_explicitly_excluded():
    prices = _prices([
        (1, "1", date(2015, 1, 2), 100.0, 100.0),
        (1, "1", date(2015, 1, 5), 101.0, 101.0),
    ])
    actions = _actions([{
        "announcement_date": date(2014, 12, 20),
        "record_date": date(2014, 12, 31),
    }])

    batch = _build_batch(
        prices,
        actions,
        pd.Series(pd.to_datetime([
            "2015-01-02", "2015-01-05",
        ])),
        run_id=None,
        max_dividend_yield=1.0,
    )

    assert not batch.audit.iloc[0]["is_canonical"]
    assert (
        batch.audit.iloc[0]["excluded_reason"]
        == "BEFORE_MARKET_COVERAGE"
    )
    assert pd.isna(batch.audit.iloc[0]["resolved_ex_date"])
    assert batch.prices["total_return_close"].tolist() == [100.0, 101.0]


def test_event_on_first_listing_day_is_v2_explicit_exclusion():
    prices = _prices([
        (1, "1", date(2015, 12, 29), 100.0, 100.0),
        (1, "1", date(2015, 12, 30), 101.0, 101.0),
    ])
    actions = _actions([{
        "announcement_date": date(2015, 12, 20),
        "effective_date": date(2015, 12, 29),
        "record_date": date(2015, 12, 31),
    }])

    batch = _build_batch(
        prices,
        actions,
        pd.Series(pd.to_datetime(["2015-12-29", "2015-12-30"])),
        run_id=None,
        max_dividend_yield=1.0,
    )

    row = batch.audit.iloc[0]
    assert row["resolution_version"] == "krx_dividend_resolution_v2"
    assert row["is_canonical"] is False or not bool(row["is_canonical"])
    assert row["excluded_reason"] == "BEFORE_LISTING_OR_EPISODE_START"
    for column in (
        "applied_trade_date", "previous_trade_date", "previous_close",
        "previous_adj_close", "applied_close", "applied_adj_close",
        "selected_cash_scale", "cash_adjustment_scale_basis",
        "scale_evidence_key", "scale_price_factor_parity",
    ):
        assert pd.isna(row[column])


def test_implausible_cash_yield_fails_closed():
    prices = _prices([
        (1, "1", date(2026, 1, 2), 100.0, 100.0),
        (1, "1", date(2026, 1, 5), 90.0, 90.0),
    ])
    actions = _actions([{
        "effective_date": date(2026, 1, 5),
        "cash_amount": 150.0,
    }])

    with pytest.raises(RuntimeError, match="fail-closed bound"):
        _build_batch(
            prices,
            actions,
            pd.Series(pd.to_datetime([
                "2026-01-02", "2026-01-05",
            ])),
            run_id=None,
            max_dividend_yield=1.0,
        )


def test_aggregate_same_day_cash_yield_fails_closed():
    rebuilt = pd.DataFrame([
        {"identifier": "1", "trade_date": date(2026, 1, 2), "adj_close": 100.0},
        {"identifier": "1", "trade_date": date(2026, 1, 5), "adj_close": 90.0},
    ])
    events = pd.DataFrame([
        {
            "identifier": "1", "dividend_key": "a",
            "application_status": "applied",
            "applied_trade_date": date(2026, 1, 5),
            "adjusted_cash_amount": 60.0,
        },
        {
            "identifier": "1", "dividend_key": "b",
            "application_status": "applied",
            "applied_trade_date": date(2026, 1, 5),
            "adjusted_cash_amount": 60.0,
        },
    ])

    with pytest.raises(RuntimeError, match="aggregate same-day"):
        _assert_dividend_yields(
            rebuilt, events, max_dividend_yield=1.0,
        )


class _RowsCursor:
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


class _RowsConnection:
    def __init__(self, rows):
        self.cursor_instance = _RowsCursor(rows)

    def cursor(self):
        return self.cursor_instance


def test_local_action_snapshot_filters_issuer_and_reports_unmapped(
    tmp_path,
    monkeypatch,
):
    candidates = pd.DataFrame([
        {
            "identifier": "005930",
            "source": "DART_DISCLOSURE",
            "rcept_no": "cash-1",
            "event_type": "cash_dividend",
            "action_scope": "ISSUER",
        },
        {
            "identifier": "999999",
            "source": "DART_DISCLOSURE",
            "rcept_no": "notice-1",
            "event_type": "ex_dividend",
            "action_scope": "ISSUER",
        },
        {
            "identifier": "000001",
            "source": "DART_DISCLOSURE",
            "rcept_no": "related-1",
            "event_type": "cash_dividend",
            "action_scope": "RELATED_COMPANY",
        },
    ])

    def fake_prepare(base, **_kwargs):
        assert base == str(tmp_path.resolve())
        return candidates, {}

    def fake_normalize(scoped):
        assert set(scoped["action_scope"]) == {"ISSUER"}
        assert set(scoped["identifier"]) == {"005930", "999999"}
        return pd.DataFrame([
            {
                "identifier": "005930",
                "asset_id": 1,
                "source": "DART_DISCLOSURE",
                "action_key": "cash-1",
                "action_type": "cash_dividend",
                "announcement_date": date(2026, 1, 1),
                "ex_date": None,
                "record_date": date(2026, 1, 6),
                "cash_amount": 10.0,
                "filing_id": "cash-1",
            },
            {
                "identifier": "999999",
                "asset_id": 2,
                "source": "DART_DISCLOSURE",
                "action_key": "notice-1",
                "action_type": "ex_dividend",
                "announcement_date": date(2026, 1, 2),
                "ex_date": date(2026, 1, 2),
                "record_date": None,
                "cash_amount": None,
                "filing_id": "notice-1",
            },
        ])

    monkeypatch.setattr(rebuild.corporate_actions, "prepare", fake_prepare)
    monkeypatch.setattr(
        rebuild,
        "verify_snapshot_manifest",
        lambda base, **kwargs: SimpleNamespace(
            base=str(tmp_path.resolve()),
            body_digest="f" * 64,
            manifest_sha256="e" * 64,
            body_count=10,
            coverage_start=date(2015, 1, 1),
            coverage_end=date(2026, 1, 2),
        ),
    )
    monkeypatch.setattr(
        rebuild,
        "map_actions_to_pit_assets",
        lambda conn, frame, **kwargs: (
            frame.assign(asset_id=[1, 2]),
            SimpleNamespace(
                out_of_scope_instrument_count=0,
                out_of_scope_market_count=0,
                out_of_scope_market_ticker_count=0,
                out_of_scope_market_classes={},
            ),
            frame.assign(
                asset_id=[1, 2],
                pit_mapping_status="INCLUDED",
                pit_excluded_reason=None,
                pit_event_date=date(2026, 1, 2),
            ),
        ),
    )
    monkeypatch.setattr(
        rebuild.corporate_actions,
        "normalize_for_publish",
        fake_normalize,
    )
    connection = _RowsConnection([])

    snapshot = _prepare_local_action_snapshot(
        connection, str(tmp_path), required_end=date(2026, 1, 2)
    )

    assert snapshot.base == str(tmp_path.resolve())
    assert len(snapshot.fingerprint) == 64
    assert snapshot.unmapped_count == 0
    assert snapshot.actions[["asset_id", "identifier"]].to_dict(
        "records"
    ) == [
        {"asset_id": 1, "identifier": "1"},
        {"asset_id": 2, "identifier": "2"},
    ]
    assert snapshot.manifest_sha256 == "e" * 64


def test_local_preview_fully_binds_nonempty_parent_and_child_evidence(
    tmp_path, monkeypatch,
):
    support_frame = pd.DataFrame([{
        "evidence_key": "scale-1",
        "support_action_source": "DART_DISCLOSURE",
        "support_action_key": "20211224900781",
        "support_action_type": "stock_dividend",
    }])
    scale_evidence = SimpleNamespace(
        frame=pd.DataFrame([{
            "evidence_key": "scale-1",
            "ticker": "005950",
            "cash_receipt_no": "20220214901227",
        }]),
        support_frame=support_frame,
        metadata={"contract": "manifest-evidence"},
    )
    verified_snapshot = SimpleNamespace(
        base=str(tmp_path.resolve()),
        body_digest="f" * 64,
        manifest_sha256="e" * 64,
        body_count=10,
        coverage_start=date(2015, 1, 1),
        coverage_end=date(2026, 1, 2),
        cash_adjustment_scale_source_evidence=scale_evidence.metadata,
    )
    candidates = pd.DataFrame([
        {
            "identifier": "005950", "source": "DART_DISCLOSURE",
            "rcept_no": "20220214901227", "event_type": "cash_dividend",
            "action_scope": "ISSUER",
        },
        {
            "identifier": "005950", "source": "DART_DISCLOSURE",
            "rcept_no": "20211224900781", "event_type": "stock_dividend",
            "action_scope": "ISSUER",
        },
    ])
    normalized = pd.DataFrame([
        {
            "identifier": "005950", "asset_id": 7,
            "source": "DART_DISCLOSURE", "action_key": "20220214901227",
            "action_type": "cash_dividend", "announcement_date": date(2022, 2, 14),
            "ex_date": None, "record_date": date(2021, 12, 31),
            "cash_amount": 100.0, "filing_id": "20220214901227",
        },
        {
            "identifier": "005950", "asset_id": 7,
            "source": "DART_DISCLOSURE", "action_key": "20211224900781",
            "action_type": "stock_dividend", "announcement_date": date(2021, 12, 24),
            "ex_date": None, "record_date": date(2021, 12, 31),
            "cash_amount": None, "filing_id": "20211224900781",
        },
    ])
    bound_parent = pd.DataFrame([{"asset_id": 7, "evidence_key": "scale-1"}])
    bound_child = pd.DataFrame([{"evidence_key": "scale-1", "child": "bound"}])
    bind_calls = []

    monkeypatch.setattr(
        rebuild, "verify_snapshot_manifest", lambda *args, **kwargs: verified_snapshot,
    )
    monkeypatch.setattr(
        rebuild, "verify_source_evidence_manifest",
        lambda *_, **_kwargs: scale_evidence,
    )
    monkeypatch.setattr(
        rebuild.corporate_actions, "prepare",
        lambda *_, **_kwargs: (candidates, {}),
    )
    monkeypatch.setattr(
        rebuild,
        "map_actions_to_pit_assets",
        lambda conn, frame, **kwargs: (
            frame.assign(asset_id=7),
            SimpleNamespace(out_of_scope_instrument_count=0),
            frame.assign(
                asset_id=7,
                pit_mapping_status="INCLUDED",
                pit_excluded_reason=None,
                pit_event_date=date(2021, 12, 31),
            ),
        ),
    )
    monkeypatch.setattr(
        rebuild.corporate_actions,
        "normalize_for_publish",
        lambda *_: normalized.copy(),
    )
    monkeypatch.setattr(
        dart_extra_load,
        "_source_receipt_frame",
        lambda frame, **kwargs: (
            pd.DataFrame([{"receipt_no": "bound-cash"}])
            if {
                "pit_mapping_status", "pit_excluded_reason", "pit_event_date",
            }.issubset(frame.columns)
            else (_ for _ in ()).throw(
                AssertionError("local preview did not preserve PIT audit rows")
            )
        ),
    )

    def fake_bind(verified, **kwargs):
        bind_calls.append((verified, kwargs))
        assert kwargs["action_snapshot_run_id"] is None
        assert kwargs["published_actions"]["quality_run_id"].isna().all()
        return BoundScaleSourceEvidence(bound_parent, bound_child)

    monkeypatch.setattr(rebuild, "bind_source_evidence", fake_bind)
    monkeypatch.setattr(
        rebuild,
        "source_evidence_metadata",
        lambda parent, child, **kwargs: {
            "contract": "bound-evidence",
            "parent_count": len(parent),
            "child_count": len(child),
        },
    )

    snapshot = _prepare_local_action_snapshot(
        object(), str(tmp_path), required_end=date(2026, 1, 2),
    )

    assert len(bind_calls) == 1
    assert snapshot.scale_source_evidence.equals(bound_parent)
    assert snapshot.scale_support_actions.equals(bound_child)
    assert snapshot.cash_scale_evidence == {
        "contract": "bound-evidence", "parent_count": 1, "child_count": 1,
    }


def test_action_reader_is_scoped_to_certified_issuer_dart_only():
    connection = _RowsConnection([])
    snapshot_run_id = uuid4()

    frame = _issuer_dart_actions(connection, [1, 2], snapshot_run_id)

    assert frame.empty
    sql, params = connection.cursor_instance.statements[0]
    compact = " ".join(sql.split())
    assert "ca.source='DART_DISCLOSURE'" in compact
    assert "ca.action_scope='ISSUER'" in compact
    assert "q.status='CERTIFIED'" in compact
    assert "a.asset_type='stock'" in compact
    assert "ca.quality_run_id=%s" in compact
    assert params == ([1, 2], snapshot_run_id)


class _Copy:
    def __init__(self, sink):
        self.sink = sink

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def write_row(self, row):
        self.sink.append(row)


class _PublishCursor:
    def __init__(self, price_count, audit_count):
        self.price_count = price_count
        self.audit_count = audit_count
        self.rowcount = -1
        self.statements = []
        self.copies = []

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def execute(self, sql, params=None):
        self.statements.append((sql, params))
        compact = " ".join(sql.split())
        if compact.startswith("UPDATE price_daily"):
            self.rowcount = self.price_count
        elif compact.startswith("INSERT INTO dividend_event_resolution"):
            self.rowcount = self.audit_count
        else:
            self.rowcount = -1

    def copy(self, sql):
        sink = []
        self.copies.append((sql, sink))
        return _Copy(sink)


class _PublishConnection:
    def __init__(self, price_count, audit_count):
        self.cursor_instance = _PublishCursor(price_count, audit_count)

    def cursor(self):
        return self.cursor_instance


def test_publish_batch_copies_then_updates_price_and_audit_together():
    run_id = uuid4()
    prices = pd.DataFrame([{
        "asset_id": 1,
        "trade_date": date(2026, 1, 2),
        "total_return_close": 100.0,
        "total_return_quality_run_id": run_id,
    }])
    audit = pd.DataFrame([{
        "asset_id": 1,
        "source": "DART_DISCLOSURE",
        "action_key": "a1",
        "resolution_version": "krx_dividend_resolution_v1",
        "is_canonical": True,
        "excluded_reason": None,
        "resolved_ex_date": date(2026, 1, 2),
        "ex_date_basis": "KRX_NOTICE",
        "applied_trade_date": date(2026, 1, 2),
        "raw_cash_amount": 10.0,
        "adjusted_cash_amount": 10.0,
        "quality_run_id": run_id,
    }])
    batch = BatchRebuild(prices, audit, 1, 1, 0)
    connection = _PublishConnection(1, 1)

    assert _publish_batch(connection, batch) == (1, 1)

    statements = " ".join(
        " ".join(sql.split())
        for sql, _ in connection.cursor_instance.statements
    )
    assert "SET total_return_close=s.total_return_close" in statements
    assert (
        "total_return_quality_run_id=s.total_return_quality_run_id"
        in statements
    )
    assert "SET quality_run_id=" not in statements
    assert "q.status='CERTIFIED'" in statements
    assert "ON CONFLICT" not in statements
    assert len(connection.cursor_instance.copies) == 2


def test_cli_is_dry_run_unless_apply_is_explicit():
    assert parse_args([]).apply is False
    assert parse_args(["--apply"]).apply is True
    local = parse_args(["--actions-base", "/tmp/actions"])
    assert local.apply is False
    assert local.actions_base == "/tmp/actions"
    with pytest.raises(SystemExit):
        parse_args(["--apply", "--actions-base", "/tmp/actions"])


def test_cli_help_declares_direct_apply_disabled(capsys):
    with pytest.raises(SystemExit):
        parse_args(["--help"])

    help_text = capsys.readouterr().out
    assert "standalone apply는 비활성화됨" in help_text
    assert "closed orchestrator 전용" in help_text
    assert "Direct ``--apply`` is disabled" in rebuild.__doc__
    assert "uv run python -m pipeline.silver.total_return_rebuild --apply" not in (
        rebuild.__doc__
    )


def test_direct_apply_cli_is_disabled_in_favor_of_closed_orchestrator(monkeypatch):
    monkeypatch.setattr(
        rebuild, "parse_args", lambda: SimpleNamespace(
            apply=True, batch_size=100, max_dividend_yield=1.0,
            actions_base=None,
        ),
    )
    monkeypatch.setattr(
        rebuild, "run", lambda **kwargs: pytest.fail("unsafe apply reached"),
    )

    with pytest.raises(RuntimeError, match="direct total-return --apply"):
        rebuild.main()


def test_local_actions_cannot_reach_apply_or_open_connection(monkeypatch):
    monkeypatch.setattr(
        rebuild.db,
        "connect",
        lambda: pytest.fail("database connection must not be opened"),
    )

    with pytest.raises(ValueError, match="cannot be combined with --apply"):
        run(apply=True, actions_base="/tmp/actions")


def test_rebuild_uses_local_snapshot_and_reports_evidence(monkeypatch):
    local_actions = _actions([{}])
    snapshot = LocalActionSnapshot(
        actions=local_actions,
        base="/tmp/complete-actions",
        fingerprint="f" * 64,
        unmapped_count=7,
    )
    price_output = pd.DataFrame([{
        "asset_id": 1,
        "trade_date": pd.Timestamp("2026-01-02"),
        "total_return_close": 100.0,
        "quality_run_id": None,
    }])
    audit = pd.DataFrame([{"is_canonical": True}])
    batch = BatchRebuild(price_output, audit, 1, 1, 0)
    monkeypatch.setattr(
        rebuild,
        "_prepare_local_action_snapshot",
        lambda conn, base, **kwargs: snapshot,
    )
    monkeypatch.setattr(rebuild, "_certified_asset_ids", lambda conn: [1])
    monkeypatch.setattr(
        rebuild,
        "_global_krx_sessions",
        lambda conn: pd.Series(pd.to_datetime(["2026-01-02"])),
    )
    monkeypatch.setattr(
        rebuild,
        "_source_price_coverage",
        lambda conn: (date(1995, 5, 24), date(2026, 1, 2)),
    )
    identity = SimpleNamespace(
        contract="krx_pit_ticker_asset_v1",
        digest="d" * 64,
        row_count=1,
        asset_count=1,
    )
    monkeypatch.setattr(
        rebuild,
        "krx_common_stock_identity_digest",
        lambda *args, **kwargs: identity,
    )
    monkeypatch.setattr(
        rebuild,
        "_certified_prices",
        lambda conn, asset_ids: _prices([
            (1, "1", date(2026, 1, 2), 100.0, 100.0),
        ]),
    )
    monkeypatch.setattr(
        rebuild,
        "_issuer_dart_actions",
        lambda *args: pytest.fail("RDS actions must not be read"),
    )
    monkeypatch.setattr(rebuild, "_build_batch", lambda *args, **kwargs: batch)

    summary = rebuild._rebuild(
        object(),
        apply=False,
        batch_size=100,
        max_dividend_yield=1.0,
        run_id=None,
        actions_base="/tmp/complete-actions",
    )

    assert summary.action_source == "local_complete_bronze"
    assert summary.action_snapshot_run_id is None
    assert summary.local_actions_base == "/tmp/complete-actions"
    assert summary.local_actions_fingerprint == "f" * 64
    assert summary.unmapped_action_count == 7
    assert summary.cash_action_count == 1


class _ReadOnlyTransaction:
    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False


class _ReadOnlyConnection:
    def __init__(self):
        self.cursor_instance = _RowsCursor([])
        self.commit_count = 0

    def cursor(self):
        return self.cursor_instance

    def commit(self):
        self.commit_count += 1

    def transaction(self):
        return _ReadOnlyTransaction()


def test_local_preview_creates_no_dq_run_or_rds_dml(monkeypatch):
    connection = _ReadOnlyConnection()
    monkeypatch.setattr(rebuild.repository, "assert_schema", lambda conn: None)
    monkeypatch.setattr(rebuild, "_assert_contract_schema", lambda conn: None)
    monkeypatch.setattr(
        rebuild.repository,
        "start_run",
        lambda *args, **kwargs: pytest.fail("dry-run must not create dq_run"),
    )
    monkeypatch.setattr(
        rebuild,
        "_rebuild",
        lambda *args, **kwargs: RebuildSummary(
            apply=False,
            action_source="local_complete_bronze",
            local_actions_base="/tmp/actions",
            local_actions_fingerprint="f" * 64,
            unmapped_action_count=3,
        ),
    )

    summary = run(actions_base="/tmp/actions", conn=connection)

    assert summary.action_source == "local_complete_bronze"
    statements = [
        " ".join(sql.split()).upper()
        for sql, _ in connection.cursor_instance.statements
    ]
    assert statements == [
        "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"
    ]
    assert not any(
        keyword in statement
        for statement in statements
        for keyword in ("INSERT ", "UPDATE ", "DELETE ", "COPY ")
    )
