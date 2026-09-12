from pipeline import alternative_data_backfill_ecs as backfill


def test_full_statement_collection_precedes_daily_certification_lock(monkeypatch):
    events: list[str] = []

    monkeypatch.setattr(
        backfill,
        "_acquire_with_retry",
        lambda acquire, _label: acquire(),
    )
    monkeypatch.setattr(
        backfill,
        "_acquire_bootstrap_collection_lock",
        lambda: events.append("collection-lock") or "collection",
    )
    monkeypatch.setattr(
        backfill,
        "_release_bootstrap_collection_lock",
        lambda _lock: events.append("collection-release"),
    )
    monkeypatch.setattr(
        backfill.dart_full_statements,
        "run_bootstrap_batch",
        lambda *_args, **_kwargs: events.append("collect")
        or (["response.json"], 10),
    )
    monkeypatch.setattr(
        backfill.dart_silver_backfill_ecs,
        "acquire_daily_certification_lock",
        lambda: events.append("daily-lock") or "daily",
    )
    monkeypatch.setattr(
        backfill.dart_silver_backfill_ecs,
        "release_daily_certification_lock",
        lambda _lock: events.append("daily-release"),
    )
    monkeypatch.setattr(
        backfill.migrate,
        "assert_current",
        lambda _lock: events.append("migration-check"),
    )
    monkeypatch.setattr(
        backfill.alternative_data,
        "publish_files",
        lambda **_kwargs: events.append("publish")
        or {"published": {"fundamental_statement_line": 1}},
    )
    monkeypatch.setattr(
        backfill.dart_full_statements,
        "mark_bootstrap_batch_certified",
        lambda *_args: events.append("checkpoint-certified"),
    )

    result = backfill.publish_full_statement_batch(2015, 2026, max_scopes=9000)

    assert result["remaining_scopes"] == 10
    assert events == [
        "collection-lock",
        "collect",
        "daily-lock",
        "migration-check",
        "publish",
        "daily-release",
        "checkpoint-certified",
        "collection-release",
    ]


def test_lock_retry_waits_then_succeeds(monkeypatch):
    attempts = 0
    sleeps: list[float] = []

    def acquire():
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise RuntimeError("busy")
        return "lock"

    clock = iter([0.0, 1.0, 2.0, 3.0, 4.0])
    monkeypatch.setenv("DART_BOOTSTRAP_LOCK_WAIT_SECONDS", "10")
    monkeypatch.setenv("DART_BOOTSTRAP_LOCK_RETRY_SECONDS", "1")
    monkeypatch.setattr(backfill.time, "monotonic", lambda: next(clock))
    monkeypatch.setattr(backfill.time, "sleep", sleeps.append)

    assert backfill._acquire_with_retry(acquire, "test lock") == "lock"
    assert attempts == 3
    assert sleeps == [1, 1]
