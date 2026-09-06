import inspect

import pytest

from pipeline import daily_full
from pipeline.daily_full import _fmp_target_day


def test_fmp_target_uses_completed_prior_weekday():
    assert _fmp_target_day("20260804") == "20260803"
    assert _fmp_target_day("20260803") == "20260731"


def test_pending_krx_sessions_excludes_existing_coverage():
    parsed = daily_full.datetime.strptime
    assert daily_full._pending_krx_sessions(
        parsed("20260810", "%Y%m%d").date(),
        parsed("20260811", "%Y%m%d").date(),
    ) == [parsed("20260811", "%Y%m%d").date()]
    assert daily_full._pending_krx_sessions(
        parsed("20260807", "%Y%m%d").date(),
        parsed("20260811", "%Y%m%d").date(),
    ) == [
        parsed("20260810", "%Y%m%d").date(),
        parsed("20260811", "%Y%m%d").date(),
    ]


def test_daily_never_runs_legacy_partial_total_return_writer():
    source = inspect.getsource(daily_full)
    assert "from pipeline.silver import total_return" not in source
    assert "total_return.run_daily" not in source
    assert "prepare_total_return_snapshot" in source
    assert "close_total_return_contract" in source
    assert "freshness.assert_fresh()" in source
    assert "[freshness] WARNING" not in source


def test_daily_requires_v3_disclosure_manifest_and_rejects_v1():
    key = daily_full._action_disclosure_manifest_key("20260721", "20260804")
    assert key == (
        "corporate_actions/dart/manifests/from=20260721/to=20260804/"
        "disclosures_v3.json"
    )
    daily_full._reject_legacy_action_manifests([key])

    legacy = (
        "corporate_actions/dart/manifests/from=20260721/to=20260804/"
        "disclosures.json"
    )
    with pytest.raises(RuntimeError, match="cannot authenticate the v3"):
        daily_full._reject_legacy_action_manifests([legacy])


def _stub_main(monkeypatch, *, events: list[str]) -> None:
    monkeypatch.setenv("S3_BRONZE_BUCKET", "bronze")
    monkeypatch.setenv("PIPELINE_DATE", "20260810")
    monkeypatch.setenv("DART_DIVIDENDS_ENABLED", "false")
    monkeypatch.setattr(daily_full.migrate, "assert_current", lambda: None)
    lock = object()
    monkeypatch.setattr(
        daily_full.dart_silver_backfill_ecs,
        "acquire_daily_certification_lock",
        lambda: lock,
    )
    monkeypatch.setattr(
        daily_full.dart_silver_backfill_ecs,
        "release_daily_certification_lock",
        lambda connection: None,
    )
    monkeypatch.setattr(
        daily_full.dart_silver_backfill_ecs,
        "assert_daily_certification_lock",
        lambda connection: None,
    )
    monkeypatch.setattr(daily_full.stock_krxapi, "run", lambda *a: None)
    monkeypatch.setattr(daily_full.index, "run", lambda *a: None)
    monkeypatch.setattr(
        daily_full.financials, "run_incremental", lambda *a, **k: [],
    )
    monkeypatch.setattr(
        daily_full.repository,
        "certified_target_exists",
        lambda *args, **kwargs: False,
    )

    def action_run(*args, **kwargs):
        return [daily_full._action_disclosure_manifest_key(
            "20260727", "20260810",
        )]

    monkeypatch.setattr(daily_full.corporate_actions, "run", action_run)

    def list_prefix(bucket, prefix):
        if prefix.startswith("stock/"):
            return [
                "stock/krxapi/date=2026-08-10/kospi.parquet",
                "stock/krxapi/date=2026-08-10/kosdaq.parquet",
            ]
        return [
            "index/krxapi/date=2026-08-10/krx.parquet",
            "index/krxapi/date=2026-08-10/kospi.parquet",
            "index/krxapi/date=2026-08-10/kosdaq.parquet",
        ]

    monkeypatch.setattr(daily_full, "_list_prefix", list_prefix)
    monkeypatch.setattr(daily_full, "_download_keys", lambda *a: [])
    monkeypatch.setattr(
        daily_full.dart_silver_backfill_ecs,
        "prepare_total_return_snapshot",
        lambda *a, **k: events.append("prepare"),
    )
    monkeypatch.setattr(
        daily_full.dart_silver_backfill_ecs,
        "preview_total_return_actions",
        lambda *a, **k: events.append("action-preview"),
    )
    contract_readiness = iter((True, False))
    monkeypatch.setattr(
        daily_full.dart_silver_backfill_ecs,
        "total_return_contract_ready",
        lambda **kwargs: next(contract_readiness),
    )
    monkeypatch.setattr(
        daily_full.dart_silver_backfill_ecs,
        "certified_krx_price_coverage_end",
        lambda **kwargs: daily_full.datetime.strptime(
            "20260810", "%Y%m%d",
        ).date(),
    )
    def incremental(*args, **kwargs):
        assert kwargs["action_coverage_start"].isoformat() == "2015-01-01"
        assert kwargs["action_coverage_end"].isoformat() == "2026-08-10"
        events.append("silver-write")

    monkeypatch.setattr(daily_full.load, "incremental", incremental)
    monkeypatch.setattr(
        daily_full.dart_silver_backfill_ecs,
        "close_total_return_contract",
        lambda *a, **k: events.append("close"),
    )
    monkeypatch.setattr(daily_full.fmp_bronze, "run_daily", lambda *a: [])
    monkeypatch.setattr(
        daily_full.fmp_load,
        "run",
        lambda **k: events.append("fmp"),
    )
    monkeypatch.setattr(
        daily_full.freshness,
        "assert_fresh",
        lambda: events.append("freshness") or {"sources": {}},
    )


def test_daily_closes_total_return_before_fmp_and_freshness(monkeypatch):
    events: list[str] = []
    _stub_main(monkeypatch, events=events)

    daily_full.main()

    assert events == [
        "prepare", "action-preview", "silver-write", "close", "fmp",
        "freshness",
    ]


def test_same_day_retry_skips_certified_krx_dart_tr_and_fmp(monkeypatch):
    events: list[str] = []
    _stub_main(monkeypatch, events=events)
    monkeypatch.setattr(
        daily_full.repository,
        "certified_target_exists",
        lambda _conn, mode, _day: mode in {"daily", "fmp_daily"},
    )
    monkeypatch.setattr(
        daily_full.stock_krxapi,
        "run",
        lambda *_args: pytest.fail("KRX collection must be skipped"),
    )
    monkeypatch.setattr(
        daily_full.financials,
        "run_incremental",
        lambda *_args: pytest.fail("DART collection must be skipped"),
    )
    monkeypatch.setattr(
        daily_full.fmp_bronze,
        "run_daily",
        lambda *_args: pytest.fail("FMP collection must be skipped"),
    )

    daily_full.main()

    assert events == ["freshness"]


def test_holiday_financial_refresh_closes_actual_post_publish_invalidation(
    monkeypatch,
):
    events: list[str] = []
    _stub_main(monkeypatch, events=events)
    lock = object()
    monkeypatch.setattr(
        daily_full.dart_silver_backfill_ecs,
        "acquire_daily_certification_lock",
        lambda: lock,
    )
    monkeypatch.setattr(
        daily_full.dart_silver_backfill_ecs,
        "release_daily_certification_lock",
        lambda connection: None,
    )
    monkeypatch.setattr(
        daily_full.financials,
        "run_incremental",
        lambda *args, **kwargs: [
            "s3://bronze/financials/dart/year=2026/corp=005930.json"
        ],
    )
    monkeypatch.setattr(daily_full, "_list_prefix", lambda *args: [])
    monkeypatch.setattr(
        daily_full,
        "_download_keys",
        lambda *args: [
            "/app/data/financials/dart/year=2026/corp=005930.json"
        ],
    )
    contract = {"ready": True}

    def contract_ready(*, conn=None):
        assert conn is lock
        return contract["ready"]

    def financial_only_incremental(*args, **kwargs):
        assert kwargs["market_closed"] is True
        assert kwargs["has_action_change"] is False
        assert kwargs["financial_files"]
        # Model corporate_actions.publish finding a genuine issuer action in
        # the bounded candidate frame and atomically demoting the contract.
        contract["ready"] = False
        events.append("silver-write")

    monkeypatch.setattr(
        daily_full.dart_silver_backfill_ecs,
        "total_return_contract_ready",
        contract_ready,
    )
    monkeypatch.setattr(
        daily_full.load, "incremental", financial_only_incremental,
    )

    daily_full.main()

    assert events == [
        "prepare", "action-preview", "silver-write", "close", "fmp",
        "freshness",
    ]


def test_holiday_financial_refresh_does_not_close_when_contract_stays_ready(
    monkeypatch,
):
    events: list[str] = []
    _stub_main(monkeypatch, events=events)
    monkeypatch.setattr(
        daily_full.financials,
        "run_incremental",
        lambda *args, **kwargs: [
            "s3://bronze/financials/dart/year=2026/corp=005930.json"
        ],
    )
    monkeypatch.setattr(daily_full, "_list_prefix", lambda *args: [])
    monkeypatch.setattr(
        daily_full,
        "_download_keys",
        lambda *args: [
            "/app/data/financials/dart/year=2026/corp=005930.json"
        ],
    )
    monkeypatch.setattr(
        daily_full.dart_silver_backfill_ecs,
        "total_return_contract_ready",
        lambda **kwargs: True,
    )

    daily_full.main()

    assert events == [
        "prepare", "action-preview", "silver-write", "fmp", "freshness",
    ]


def test_daily_holds_common_epoch_lock_before_mutation_and_releases(monkeypatch):
    events: list[str] = []
    _stub_main(monkeypatch, events=events)
    lock = object()
    monkeypatch.setattr(
        daily_full.dart_silver_backfill_ecs,
        "acquire_daily_certification_lock",
        lambda: events.append("lock") or lock,
    )
    original_stock = daily_full.stock_krxapi.run
    monkeypatch.setattr(
        daily_full.stock_krxapi,
        "run",
        lambda *args: events.append("first-mutation") or original_stock(*args),
    )
    monkeypatch.setattr(
        daily_full.dart_silver_backfill_ecs,
        "release_daily_certification_lock",
        lambda connection: events.append(
            "unlock" if connection is lock else "wrong-lock"
        ),
    )

    daily_full.main()

    assert events[0:2] == ["lock", "first-mutation"]
    assert events[-1] == "unlock"


def test_daily_releases_common_epoch_lock_when_first_mutation_fails(monkeypatch):
    events: list[str] = []
    _stub_main(monkeypatch, events=events)
    lock = object()
    monkeypatch.setattr(
        daily_full.dart_silver_backfill_ecs,
        "acquire_daily_certification_lock",
        lambda: events.append("lock") or lock,
    )
    monkeypatch.setattr(
        daily_full.stock_krxapi,
        "run",
        lambda *args: (_ for _ in ()).throw(RuntimeError("KRX failed")),
    )
    monkeypatch.setattr(
        daily_full.dart_silver_backfill_ecs,
        "release_daily_certification_lock",
        lambda connection: events.append(
            "unlock" if connection is lock else "wrong-lock"
        ),
    )

    with pytest.raises(RuntimeError, match="KRX failed"):
        daily_full.main()
    assert events == ["lock", "unlock"]


def test_daily_preflight_failure_happens_before_price_write(monkeypatch):
    events: list[str] = []
    _stub_main(monkeypatch, events=events)

    def fail_preflight(*args, **kwargs):
        events.append("prepare-failed")
        raise RuntimeError("missing current v5 evidence")

    monkeypatch.setattr(
        daily_full.dart_silver_backfill_ecs,
        "prepare_total_return_snapshot",
        fail_preflight,
    )

    with pytest.raises(RuntimeError, match="missing current v5 evidence"):
        daily_full.main()
    assert events == ["prepare-failed"]


def test_daily_refuses_to_skip_multiple_certified_krx_sessions(monkeypatch):
    events: list[str] = []
    _stub_main(monkeypatch, events=events)
    monkeypatch.setenv("PIPELINE_DATE", "20260811")
    monkeypatch.setattr(
        daily_full.dart_silver_backfill_ecs,
        "certified_krx_price_coverage_end",
        lambda **kwargs: daily_full.datetime.strptime(
            "20260807", "%Y%m%d",
        ).date(),
    )
    monkeypatch.setattr(
        daily_full.stock_krxapi,
        "run",
        lambda *args: events.append("unexpected-mutation"),
    )

    with pytest.raises(RuntimeError, match="gap replay first"):
        daily_full.main()
    assert events == []


def test_new_action_invalidates_contract_before_bronze_write_and_preflight(
    monkeypatch,
):
    events: list[str] = []
    _stub_main(monkeypatch, events=events)

    def action_run(*args, **kwargs):
        kwargs["before_change"]("s3://bronze/new-action.json")
        events.append("bronze-action-write")
        kwargs["changed_sink"].append("s3://bronze/new-action.json")
        return [daily_full._action_disclosure_manifest_key(
            "20260727", "20260810",
        )]

    monkeypatch.setattr(daily_full.corporate_actions, "run", action_run)
    monkeypatch.setattr(
        daily_full.dart_silver_backfill_ecs,
        "invalidate_total_return_for_observed_action",
        lambda end, **kwargs: events.append(f"invalidate-{end.isoformat()}"),
    )

    def fail_preflight(*args, **kwargs):
        events.append("prepare-failed")
        raise RuntimeError("new correction family is incomplete")

    monkeypatch.setattr(
        daily_full.dart_silver_backfill_ecs,
        "prepare_total_return_snapshot",
        fail_preflight,
    )

    with pytest.raises(RuntimeError, match="correction family is incomplete"):
        daily_full.main()
    assert events == [
        "invalidate-2026-08-10", "bronze-action-write", "prepare-failed",
    ]


def test_every_new_action_object_rechecks_epoch_but_invalidates_once(
    monkeypatch,
):
    events: list[str] = []
    _stub_main(monkeypatch, events=events)

    def action_run(*args, **kwargs):
        kwargs["before_change"]("s3://bronze/action-1.json")
        kwargs["before_change"]("s3://bronze/action-2.json")
        kwargs["changed_sink"].extend([
            "s3://bronze/action-1.json",
            "s3://bronze/action-2.json",
        ])
        return [daily_full._action_disclosure_manifest_key(
            "20260727", "20260810",
        )]

    monkeypatch.setattr(daily_full.corporate_actions, "run", action_run)
    monkeypatch.setattr(
        daily_full.dart_silver_backfill_ecs,
        "assert_daily_certification_lock",
        lambda _connection: events.append("epoch"),
    )
    monkeypatch.setattr(
        daily_full.dart_silver_backfill_ecs,
        "invalidate_total_return_for_observed_action",
        lambda _end, **_kwargs: events.append("invalidate"),
    )

    daily_full.main()

    assert events[:3] == ["epoch", "invalidate", "epoch"]
    assert events.count("invalidate") == 1


def test_new_price_scale_failure_is_fatal_after_price_invalidation(monkeypatch):
    events: list[str] = []
    _stub_main(monkeypatch, events=events)

    def fail_close(*args, **kwargs):
        events.append("scale-preview-failed")
        raise RuntimeError("changed-scale cash event has no exact evidence")

    monkeypatch.setattr(
        daily_full.dart_silver_backfill_ecs,
        "close_total_return_contract",
        fail_close,
    )

    with pytest.raises(RuntimeError, match="no exact evidence"):
        daily_full.main()
    assert events == [
        "prepare", "action-preview", "silver-write", "scale-preview-failed",
    ]


def test_preexisting_building_contract_is_closed_before_another_price_day(
    monkeypatch,
):
    events: list[str] = []
    _stub_main(monkeypatch, events=events)
    monkeypatch.setattr(
        daily_full.dart_silver_backfill_ecs,
        "total_return_contract_ready",
        lambda **kwargs: False,
    )

    close_calls = 0

    def close(*args, **kwargs):
        nonlocal close_calls
        close_calls += 1
        events.append(f"close-{close_calls}")

    monkeypatch.setattr(
        daily_full.dart_silver_backfill_ecs,
        "close_total_return_contract",
        close,
    )

    daily_full.main()

    assert events == [
        "prepare", "action-preview", "close-1", "silver-write", "close-2",
        "fmp", "freshness",
    ]


def test_preexisting_building_contract_uses_exact_existing_price_horizon(
    monkeypatch,
):
    events: list[str] = []
    _stub_main(monkeypatch, events=events)
    monkeypatch.setenv("PIPELINE_DATE", "20260811")
    monkeypatch.setattr(
        daily_full.dart_silver_backfill_ecs,
        "total_return_contract_ready",
        lambda **kwargs: False,
    )

    def prepare(end, **kwargs):
        events.append(f"prepare-{end.isoformat()}-{kwargs.get('publish', True)}")

    def preview(end, **kwargs):
        events.append(f"preview-{end.isoformat()}")

    def close(end, **kwargs):
        events.append(f"close-{end.isoformat()}")

    monkeypatch.setattr(
        daily_full.dart_silver_backfill_ecs,
        "prepare_total_return_snapshot",
        prepare,
    )
    monkeypatch.setattr(
        daily_full.dart_silver_backfill_ecs,
        "preview_total_return_actions",
        preview,
    )
    monkeypatch.setattr(
        daily_full.dart_silver_backfill_ecs,
        "close_total_return_contract",
        close,
    )
    monkeypatch.setattr(
        daily_full.load,
        "incremental",
        lambda *args, **kwargs: events.append("silver-write"),
    )

    daily_full.main()

    assert events == [
        "prepare-2026-08-10-False",
        "preview-2026-08-10",
        "close-2026-08-10",
        "prepare-2026-08-11-True",
        "preview-2026-08-11",
        "silver-write",
        "close-2026-08-11",
        "fmp",
        "freshness",
    ]


def test_preexisting_building_repair_failure_prevents_new_price_write(
    monkeypatch,
):
    events: list[str] = []
    _stub_main(monkeypatch, events=events)
    monkeypatch.setattr(
        daily_full.dart_silver_backfill_ecs,
        "total_return_contract_ready",
        lambda **kwargs: False,
    )

    def fail_existing(*args, **kwargs):
        events.append("existing-close-failed")
        raise RuntimeError("existing changed-scale evidence is incomplete")

    monkeypatch.setattr(
        daily_full.dart_silver_backfill_ecs,
        "close_total_return_contract",
        fail_existing,
    )

    with pytest.raises(RuntimeError, match="existing changed-scale"):
        daily_full.main()
    assert events == [
        "prepare", "action-preview", "existing-close-failed",
    ]


def test_daily_freshness_failure_propagates_to_ecs(monkeypatch):
    events: list[str] = []
    _stub_main(monkeypatch, events=events)
    monkeypatch.setattr(
        daily_full.freshness,
        "assert_fresh",
        lambda: (_ for _ in ()).throw(RuntimeError("BUILDING contract")),
    )

    with pytest.raises(RuntimeError, match="BUILDING contract"):
        daily_full.main()
    assert events == [
        "prepare", "action-preview", "silver-write", "close", "fmp",
    ]
