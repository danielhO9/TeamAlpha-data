from datetime import date
import hashlib
from io import BytesIO
import json
from pathlib import Path
import threading
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from botocore.exceptions import ClientError

from pipeline import dart_silver_backfill_ecs as ecs


def test_prepare_snapshot_downloads_refreshes_builds_then_publishes(
    monkeypatch, tmp_path,
):
    calls = []
    certification_lock = object()
    listed_prefixes = []
    downloads = []
    monkeypatch.setenv("S3_BRONZE_BUCKET", "bronze")
    monkeypatch.setenv("DART_SNAPSHOT_EXPECTED_END", "2026-08-10")
    monkeypatch.setattr(ecs.boto3, "client", lambda service: object())
    monkeypatch.setattr(
        ecs,
        "_restore_published_snapshot",
        lambda *args: calls.append(("restore", args, {})),
    )
    def list_objects(s3, bucket, prefix):
        listed_prefixes.append(prefix)
        return [ecs._S3Object(prefix + "object", '"etag"', 1)]

    monkeypatch.setattr(ecs, "_list_objects", list_objects)
    monkeypatch.setattr(ecs, "DATA_ROOT", tmp_path)
    manifest = (
        tmp_path
        / ecs.cash_adjustment_scale_evidence.MANIFEST_RELATIVE_PATH
    )
    manifest.parent.mkdir(parents=True)
    manifest.write_text(json.dumps({
        "schema_version": (
            ecs.cash_adjustment_scale_evidence.SOURCE_EVIDENCE_CONTRACT
        ),
        "complete": True,
        "evidence": [{
            "previous_price_source_object_key": (
                "stock/marcap/date=2018-12-26/all.parquet"
            ),
            "adjustment_price_source_object_key": (
                "stock/marcap/date=2018-12-27/all.parquet"
            ),
        }],
    }), encoding="utf-8")

    def download(bucket, keys, root):
        downloads.append((bucket, list(keys), root))
        return len(keys)

    monkeypatch.setattr(ecs, "_download", download)
    monkeypatch.setattr(
        ecs,
        "_download_changed",
        lambda bucket, objects, root: (
            downloads.append((bucket, list(objects), root)) or (len(objects), 0)
        ),
    )
    monkeypatch.setattr(
        ecs.dart_viewer_corrections,
        "collect_viewer_corrections",
        lambda *a, **k: calls.append(("viewer", a, k)),
    )
    monkeypatch.setattr(
        ecs.dart_viewer_corrections,
        "verify_viewer_corrections",
        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("stale")),
    )
    monkeypatch.setattr(
        ecs.dart_support_action_families,
        "collect_support_action_families",
        lambda *a, **k: calls.append(("families", a, k)),
    )
    monkeypatch.setattr(
        ecs.dart_support_action_families,
        "verify_support_action_families",
        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("stale")),
    )
    monkeypatch.setattr(
        ecs,
        "_publish_component_checkpoint",
        lambda *a, **k: calls.append(("checkpoint", a, k)),
    )
    snapshot = SimpleNamespace(
        manifest_sha256="a" * 64,
        coverage_end=date(2026, 8, 10),
    )
    monkeypatch.setattr(
        ecs.dart_action_snapshot,
        "build_snapshot_manifest",
        lambda *a, **k: calls.append(("snapshot", a, k)) or snapshot,
    )
    monkeypatch.setattr(
        ecs,
        "_publish_generated_snapshot",
        lambda *a, **k: calls.append(("publish", a, k)) or 1,
    )
    monkeypatch.setattr(
        ecs,
        "assert_daily_certification_lock",
        lambda connection: (
            None
            if connection is certification_lock
            else pytest.fail("wrong certification lock")
        ),
    )

    result = ecs.prepare_total_return_snapshot(
        date(2026, 8, 10),
        bucket="bronze",
        root=tmp_path,
        certification_lock=certification_lock,
    )

    assert result is snapshot
    assert listed_prefixes == [
        "dividends/dart/",
        "corporate_actions/dart/",
        "corporate_actions/krx/",
    ]
    assert downloads[1] == (
        "bronze",
        [
            "stock/marcap/date=2018-12-26/all.parquet",
            "stock/marcap/date=2018-12-27/all.parquet",
        ],
        tmp_path,
    )
    assert [call[0] for call in calls] == [
        "restore", "viewer", "checkpoint", "families", "checkpoint",
        "snapshot", "publish",
    ]


def test_retry_restore_requires_exact_published_coverage(monkeypatch, tmp_path):
    monkeypatch.setattr(ecs.boto3, "client", lambda _service: object())
    monkeypatch.setattr(
        ecs,
        "_restore_published_snapshot",
        lambda *_args: SimpleNamespace(coverage_end=date(2026, 8, 9)),
    )

    with pytest.raises(RuntimeError, match="does not match retry coverage"):
        ecs.restore_published_total_return_snapshot(
            date(2026, 8, 10), bucket="bronze", root=tmp_path,
        )


def test_download_bounds_submitted_future_batches(monkeypatch, tmp_path):
    class FakeS3:
        def download_file(self, bucket, key, destination):
            assert bucket == "bronze"

    submitted_batch_sizes: list[int] = []

    class ImmediateFuture:
        def result(self):
            return None

    class RecordingExecutor:
        def __init__(self, max_workers):
            assert max_workers == 32

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

        def submit(self, function, key):
            function(key)
            return ImmediateFuture()

    def completed(futures):
        submitted_batch_sizes.append(len(futures))
        return iter(futures)

    monkeypatch.setattr(ecs.boto3, "client", lambda service: FakeS3())
    monkeypatch.setattr(ecs, "ThreadPoolExecutor", RecordingExecutor)
    monkeypatch.setattr(ecs, "as_completed", completed)

    assert ecs._download(
        "bronze", [f"object-{index}" for index in range(1200)], tmp_path,
    ) == 1200
    assert submitted_batch_sizes == [512, 512, 176]


def test_persistent_download_cache_fetches_only_new_or_changed_objects(
    monkeypatch, tmp_path,
):
    downloads: list[str] = []

    class FakeS3:
        def download_file(self, bucket, key, destination):
            assert bucket == "bronze"
            downloads.append(key)
            Path(destination).write_bytes(key.encode("utf-8"))

    monkeypatch.setattr(ecs.boto3, "client", lambda service: FakeS3())
    first = [
        ecs._S3Object("immutable/a", '"one"', len("immutable/a")),
        ecs._S3Object("mutable/b", '"two"', len("mutable/b")),
    ]
    assert ecs._download_changed("bronze", first, tmp_path) == (2, 0)
    assert sorted(downloads) == ["immutable/a", "mutable/b"]

    downloads.clear()
    second = [
        ecs._S3Object("immutable/a", '"one"', len("immutable/a")),
        ecs._S3Object("mutable/b", '"changed"', len("mutable/b")),
        ecs._S3Object("new/c", '"three"', len("new/c")),
    ]
    assert ecs._download_changed("bronze", second, tmp_path) == (2, 1)
    assert sorted(downloads) == ["mutable/b", "new/c"]


def test_close_total_return_contract_runs_one_atomic_rebuild_and_audit(
    monkeypatch, tmp_path,
):
    calls = []
    certification_lock = object()
    monkeypatch.setattr(
        ecs,
        "assert_daily_certification_lock",
        lambda connection: (
            None
            if connection is certification_lock
            else pytest.fail("wrong certification lock")
        ),
    )
    monkeypatch.setattr(
        ecs.total_return_rebuild,
        "run",
        lambda **kwargs: calls.append(("rebuild", kwargs)),
    )
    monkeypatch.setattr(
        ecs.dart_extra_load,
        "run",
        lambda **kwargs: calls.append(("dart", kwargs)),
    )
    monkeypatch.setattr(
        ecs.total_return_audit,
        "audit",
        lambda **kwargs: calls.append(("audit", kwargs)) or {
            "safe_for_research": True, "checks": {},
        },
    )

    result = ecs.close_total_return_contract(
        date(2026, 8, 10),
        root=tmp_path,
        certification_lock=certification_lock,
    )

    assert result["safe_for_research"] is True
    assert calls == [
        ("dart", {
            "src": "local", "apply": True,
            "total_return_actions_only": True,
            "expected_coverage_end": date(2026, 8, 10),
            "base_override": str(tmp_path),
            "conn": certification_lock,
        }),
        ("rebuild", {
            "apply": True,
            "batch_size": 500,
            "conn": certification_lock,
        }),
        ("audit", {"conn": certification_lock}),
    ]


def test_dart_ecs_one_off_uses_same_prepare_preview_close_chain(monkeypatch):
    calls = []
    lock = object()
    monkeypatch.setenv("DART_SNAPSHOT_EXPECTED_END", "2026-08-10")
    monkeypatch.setattr(
        ecs,
        "acquire_daily_certification_lock",
        lambda: calls.append(("lock", None)) or lock,
    )
    monkeypatch.setattr(
        ecs,
        "release_daily_certification_lock",
        lambda connection: calls.append(("unlock", connection)),
    )
    monkeypatch.setattr(
        ecs, "assert_daily_certification_lock", lambda connection: None,
    )
    monkeypatch.setattr(
        ecs, "prepare_total_return_snapshot",
        lambda end, **kwargs: calls.append(("prepare", end, kwargs)),
    )
    monkeypatch.setattr(
        ecs, "preview_total_return_actions",
        lambda end, **kwargs: calls.append(("preview", end, kwargs)),
    )
    monkeypatch.setattr(
        ecs, "close_total_return_contract",
        lambda end, **kwargs: calls.append(("close", end, kwargs)),
    )

    ecs.run_dart_extras()

    assert calls == [
        ("lock", None),
        (
            "prepare",
            date(2026, 8, 10),
            {"certification_lock": lock},
        ),
        (
            "preview",
            date(2026, 8, 10),
            {"conn": lock},
        ),
        (
            "close",
            date(2026, 8, 10),
            {"certification_lock": lock},
        ),
        ("unlock", lock),
    ]


def test_dart_ecs_one_off_releases_epoch_lock_on_failure(monkeypatch):
    calls = []
    lock = object()
    monkeypatch.setenv("DART_SNAPSHOT_EXPECTED_END", "2026-08-10")
    monkeypatch.setattr(
        ecs,
        "acquire_daily_certification_lock",
        lambda: calls.append("lock") or lock,
    )
    monkeypatch.setattr(
        ecs,
        "release_daily_certification_lock",
        lambda connection: calls.append(
            "unlock" if connection is lock else "wrong-lock"
        ),
    )
    monkeypatch.setattr(
        ecs, "assert_daily_certification_lock", lambda connection: None,
    )
    monkeypatch.setattr(
        ecs,
        "prepare_total_return_snapshot",
        lambda end, **kwargs: (_ for _ in ()).throw(
            RuntimeError("prepare failed")
        ),
    )

    with pytest.raises(RuntimeError, match="prepare failed"):
        ecs.run_dart_extras()
    assert calls == ["lock", "unlock"]


def test_common_epoch_lock_uses_one_session_and_explicit_unlock(monkeypatch):
    connection = MagicMock()
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchone.return_value = (True,)
    monkeypatch.setattr(ecs.db, "connect", lambda: connection)

    acquired = ecs.acquire_daily_certification_lock()
    ecs.release_daily_certification_lock(acquired)

    assert connection.autocommit is True
    assert cursor.execute.call_args_list == [
        (("SELECT pg_try_advisory_lock(%s)", (ecs.DAILY_CERTIFICATION_LOCK_KEY,)),),
        (("SELECT pg_advisory_unlock(%s)", (ecs.DAILY_CERTIFICATION_LOCK_KEY,)),),
    ]
    connection.close.assert_called_once_with()


def test_common_epoch_lock_fails_fast_when_another_task_owns_it(monkeypatch):
    connection = MagicMock()
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchone.return_value = (False,)
    monkeypatch.setattr(ecs.db, "connect", lambda: connection)

    with pytest.raises(RuntimeError, match="another daily/backfill"):
        ecs.acquire_daily_certification_lock()
    connection.close.assert_called_once_with()


def test_common_epoch_health_check_fails_closed_after_session_lock_loss():
    connection = MagicMock()
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchone.return_value = (False,)

    with pytest.raises(RuntimeError, match="epoch lock was lost"):
        ecs.assert_daily_certification_lock(connection)


def test_common_epoch_health_check_never_releases_or_reenters_lock():
    connection = MagicMock()
    cursor = connection.cursor.return_value.__enter__.return_value
    cursor.fetchone.return_value = (True,)

    ecs.assert_daily_certification_lock(connection)

    sql, params = cursor.execute.call_args.args
    assert "FROM pg_locks" in sql
    assert "pg_backend_pid()" in sql
    assert "pg_advisory_unlock" not in sql
    assert "pg_try_advisory_lock" not in sql
    assert params == (
        (ecs.DAILY_CERTIFICATION_LOCK_KEY >> 32) & 0xFFFF_FFFF,
        ecs.DAILY_CERTIFICATION_LOCK_KEY & 0xFFFF_FFFF,
    )


def test_two_closed_flows_are_serialized_for_the_complete_epoch(monkeypatch):
    mutex = threading.Lock()
    first_inside = threading.Event()
    allow_first_to_finish = threading.Event()
    second_waiting = threading.Event()
    entered: list[str] = []
    errors: list[BaseException] = []
    monkeypatch.setenv("DART_SNAPSHOT_EXPECTED_END", "2026-08-10")

    def acquire():
        if threading.current_thread().name == "second":
            second_waiting.set()
        mutex.acquire()
        return threading.current_thread().name

    def release(_token):
        mutex.release()

    def prepare(_end, **_kwargs):
        name = threading.current_thread().name
        entered.append(name)
        if name == "first":
            first_inside.set()
            assert allow_first_to_finish.wait(timeout=2)

    monkeypatch.setattr(ecs, "acquire_daily_certification_lock", acquire)
    monkeypatch.setattr(ecs, "release_daily_certification_lock", release)
    monkeypatch.setattr(
        ecs, "assert_daily_certification_lock", lambda connection: None,
    )
    monkeypatch.setattr(ecs, "prepare_total_return_snapshot", prepare)
    monkeypatch.setattr(
        ecs, "preview_total_return_actions", lambda end, **kwargs: None,
    )
    monkeypatch.setattr(
        ecs, "close_total_return_contract", lambda end, **kwargs: None,
    )

    def invoke():
        try:
            ecs.run_dart_extras()
        except BaseException as exc:  # pragma: no cover - asserted below
            errors.append(exc)

    first = threading.Thread(target=invoke, name="first")
    second = threading.Thread(target=invoke, name="second")
    first.start()
    assert first_inside.wait(timeout=2)
    second.start()
    assert second_waiting.wait(timeout=2)
    # The second task attempted acquisition but cannot observe/build a
    # snapshot until the first certification epoch releases its session lock.
    assert entered == ["first"]
    allow_first_to_finish.set()
    first.join(timeout=2)
    second.join(timeout=2)

    assert not first.is_alive()
    assert not second.is_alive()
    assert errors == []
    assert entered == ["first", "second"]


@pytest.mark.parametrize("ready", [True, False])
def test_total_return_contract_ready_is_read_only(monkeypatch, ready):
    connection = MagicMock()
    cursor = connection.cursor.return_value.__enter__.return_value
    monkeypatch.setattr(ecs.db, "connect", lambda: connection)
    monkeypatch.setattr(
        ecs.quality_freshness,
        "total_return_contract_report",
        lambda conn: {"ready": ready},
    )

    assert ecs.total_return_contract_ready() is ready
    cursor.execute.assert_called_once_with("SET TRANSACTION READ ONLY")
    connection.close.assert_called_once_with()


def test_contract_ready_and_invalidation_reuse_epoch_connection(monkeypatch):
    connection = MagicMock()
    calls: list[str] = []
    monkeypatch.setattr(
        ecs.db,
        "connect",
        lambda: pytest.fail("must not open an unfenced DB session"),
    )
    monkeypatch.setattr(
        ecs.quality_freshness,
        "total_return_contract_report",
        lambda conn: {"ready": True},
    )
    monkeypatch.setattr(
        ecs.return_contract,
        "acquire_return_writer_transaction_lock",
        lambda conn: calls.append("writer-lock"),
    )
    monkeypatch.setattr(
        ecs.return_contract,
        "invalidate_krx_total_return",
        lambda conn, **kwargs: calls.append("invalidate") or True,
    )

    assert ecs.total_return_contract_ready(conn=connection) is True
    assert ecs.invalidate_total_return_for_observed_action(
        date(2026, 8, 10), conn=connection,
    ) is True
    assert calls == ["writer-lock", "invalidate"]
    connection.close.assert_not_called()


def test_observed_action_invalidation_locks_then_demotes(monkeypatch):
    connection = MagicMock()
    calls: list[tuple] = []
    monkeypatch.setattr(ecs.db, "connect", lambda: connection)
    monkeypatch.setattr(
        ecs.return_contract,
        "acquire_return_writer_transaction_lock",
        lambda conn: calls.append(("lock", conn)),
    )
    monkeypatch.setattr(
        ecs.return_contract,
        "invalidate_krx_total_return",
        lambda conn, **kwargs: calls.append(("invalidate", conn, kwargs)) or True,
    )

    assert ecs.invalidate_total_return_for_observed_action(
        date(2026, 8, 10),
    ) is True
    assert calls[0] == ("lock", connection)
    assert calls[1][0:2] == ("invalidate", connection)
    assert calls[1][2]["quality_run_id"] is None
    assert "2026-08-10" in calls[1][2]["reason"]
    connection.close.assert_called_once_with()


def test_dart_ecs_refuses_success_when_final_audit_fails(monkeypatch, tmp_path):
    certification_lock = object()
    monkeypatch.setattr(
        ecs,
        "assert_daily_certification_lock",
        lambda connection: (
            None
            if connection is certification_lock
            else pytest.fail("wrong certification lock")
        ),
    )
    monkeypatch.setattr(ecs.dart_extra_load, "run", lambda **kwargs: None)
    monkeypatch.setattr(ecs.total_return_rebuild, "run", lambda **kwargs: None)
    monkeypatch.setattr(
        ecs.total_return_audit,
        "audit",
        lambda **kwargs: {
            "safe_for_research": False,
            "checks": {"digest": False},
        },
    )

    with pytest.raises(RuntimeError, match="digest"):
        ecs.close_total_return_contract(
            date(2026, 8, 10),
            root=tmp_path,
            certification_lock=certification_lock,
        )


def test_lock_session_loss_during_close_cannot_reach_rebuild_certification(
    monkeypatch, tmp_path,
):
    certification_lock = object()
    calls: list[tuple[str, object]] = []
    monkeypatch.setattr(
        ecs,
        "assert_daily_certification_lock",
        lambda connection: None,
    )

    def rebuild(**kwargs):
        calls.append(("rebuild", kwargs))

    def fail_action_apply(**kwargs):
        assert kwargs["conn"] is certification_lock
        calls.append(("action", kwargs))
        raise RuntimeError("epoch connection lost")

    monkeypatch.setattr(ecs.total_return_rebuild, "run", rebuild)
    monkeypatch.setattr(ecs.dart_extra_load, "run", fail_action_apply)
    monkeypatch.setattr(
        ecs.total_return_audit,
        "audit",
        lambda **kwargs: pytest.fail("stale certification audit reached"),
    )

    with pytest.raises(RuntimeError, match="epoch connection lost"):
        ecs.close_total_return_contract(
            date(2026, 8, 10),
            root=tmp_path,
            certification_lock=certification_lock,
        )

    assert calls == [("action", {
        "src": "local", "apply": True,
        "total_return_actions_only": True,
        "expected_coverage_end": date(2026, 8, 10),
        "base_override": str(tmp_path),
        "conn": certification_lock,
    })]


def test_krx_gap_is_disabled_before_any_incremental_mutation(monkeypatch):
    with pytest.raises(RuntimeError, match="krx-gap is disabled"):
        ecs.run_krx_gap()


def _write_generated_action_manifest(tmp_path, generated=()):
    entries = sorted(({
        "path": path.relative_to(tmp_path).as_posix(),
        "content_length": path.stat().st_size,
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
    } for path in generated), key=lambda item: item["path"])
    action = tmp_path / ecs.dart_action_snapshot.MANIFEST_RELATIVE_PATH
    action.parent.mkdir(parents=True, exist_ok=True)
    action.write_bytes(ecs._canonical_json({
        "schema_version": ecs.dart_action_snapshot.SCHEMA_VERSION,
        "complete": True,
        "objects": entries,
    }))
    return action


def test_generated_snapshot_publication_orders_bodies_before_manifests(tmp_path):
    viewer_manifest = (
        tmp_path / ecs.dart_viewer_corrections.MANIFEST_RELATIVE_PATH
    )
    viewer_body = viewer_manifest.parent / "year=2026" / "viewer.html"
    family_manifest = (
        tmp_path / ecs.dart_support_action_families.MANIFEST_RELATIVE_PATH
    )
    family_body = family_manifest.parent / "objects" / "sha256=body.html"
    reviewed = (
        tmp_path / ecs.reviewed_dividend_corrections.MANIFEST_RELATIVE_PATH
    )
    for path in (
        viewer_manifest, viewer_body, family_manifest, family_body, reviewed,
    ):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(path.name, encoding="utf-8")
    action = _write_generated_action_manifest(tmp_path, (
        viewer_manifest, viewer_body, family_manifest, family_body, reviewed,
    ))
    orphan = viewer_manifest.parent / "receipt=legacy" / "main.html"
    orphan.parent.mkdir(parents=True)
    orphan.write_text("unused legacy cache", encoding="utf-8")

    paths = ecs._generated_snapshot_paths(tmp_path)

    assert paths[-1] == action
    assert paths.index(viewer_body) < paths.index(viewer_manifest)
    assert paths.index(family_body) < paths.index(family_manifest)
    assert paths.index(reviewed) < paths.index(action)
    assert orphan not in paths


def test_generated_snapshot_paths_reject_referenced_object_corruption(tmp_path):
    viewer_manifest = (
        tmp_path / ecs.dart_viewer_corrections.MANIFEST_RELATIVE_PATH
    )
    viewer_manifest.parent.mkdir(parents=True)
    viewer_manifest.write_text("verified viewer", encoding="utf-8")
    _write_generated_action_manifest(tmp_path, (viewer_manifest,))
    viewer_manifest.write_text("corrupted viewer", encoding="utf-8")

    with pytest.raises(RuntimeError, match="object changed"):
        ecs._generated_snapshot_paths(tmp_path)


def test_generated_snapshot_publishes_immutable_bundle_then_current_pointer(
    tmp_path, monkeypatch,
):
    action = _write_generated_action_manifest(tmp_path)
    action_sha = hashlib.sha256(action.read_bytes()).hexdigest()
    puts: list[dict] = []

    class FakeS3:
        def put_object(self, **kwargs):
            body = kwargs["Body"]
            rendered = body.read() if hasattr(body, "read") else body
            puts.append({**kwargs, "Body": rendered})
            return {"ETag": '"new"'}

        def head_object(self, **_kwargs):
            raise AssertionError("new immutable object unexpectedly existed")

    monkeypatch.setattr(ecs.boto3, "client", lambda service: FakeS3())
    certification_lock = object()
    lock_checks = []
    monkeypatch.setattr(
        ecs,
        "assert_daily_certification_lock",
        lambda connection: lock_checks.append(connection),
    )
    snapshot = SimpleNamespace(
        manifest_sha256=action_sha,
        coverage_end=date(2026, 8, 10),
    )

    assert ecs._publish_generated_snapshot(
        "bronze", tmp_path, snapshot, None,
        certification_lock=certification_lock,
    ) == 1

    assert lock_checks == [certification_lock]
    assert puts[-1]["Key"] == ecs._SNAPSHOT_CURRENT_KEY
    assert puts[-1]["IfNoneMatch"] == "*"
    assert all(
        call["Key"].startswith(
            ecs._SNAPSHOT_PUBLISH_ROOT + "/bundles/"
        )
        for call in puts[:-1]
    )
    assert all(call["IfNoneMatch"] == "*" for call in puts[:-1])
    pointer = json.loads(puts[-1]["Body"])
    assert pointer["action_manifest_sha256"] == action_sha
    assert pointer["coverage_end"] == "2026-08-10"
    assert pointer["objects"][0]["path"] == (
        ecs.dart_action_snapshot.MANIFEST_RELATIVE_PATH.as_posix()
    )


def test_restore_published_snapshot_verifies_bundle_bodies(monkeypatch, tmp_path):
    bodies = {
        ecs.dart_action_snapshot.MANIFEST_RELATIVE_PATH.as_posix(): b"action",
        ecs.dart_viewer_corrections.MANIFEST_RELATIVE_PATH.as_posix(): b"viewer",
    }
    action_sha = hashlib.sha256(bodies[
        ecs.dart_action_snapshot.MANIFEST_RELATIVE_PATH.as_posix()
    ]).hexdigest()
    bundle = (
        f"{ecs._SNAPSHOT_PUBLISH_ROOT}/bundles/"
        f"action-manifest-sha256={action_sha}"
    )
    entries = sorted(({
        "path": relative,
        "content_length": len(body),
        "sha256": hashlib.sha256(body).hexdigest(),
    } for relative, body in bodies.items()), key=lambda item: item["path"])
    pointer = {
        "schema_version": ecs._SNAPSHOT_POINTER_SCHEMA,
        "complete": True,
        "coverage_end": "2026-08-10",
        "action_manifest_sha256": action_sha,
        "bundle_prefix": bundle,
        "object_count": len(entries),
        "object_digest": ecs._snapshot_object_digest(entries),
        "objects": entries,
    }

    class FakeS3:
        def get_object(self, **kwargs):
            assert kwargs["Key"] == ecs._SNAPSHOT_CURRENT_KEY
            return {
                "Body": BytesIO(ecs._canonical_json(pointer)),
                "ETag": '"pointer-etag"',
            }

        def download_file(self, bucket, key, destination):
            assert bucket == "bronze"
            relative = key.removeprefix(bundle + "/")
            with open(destination, "wb") as stream:
                stream.write(bodies[relative])

    restored = ecs._restore_published_snapshot(
        FakeS3(), "bronze", tmp_path.resolve(),
    )

    assert restored is not None
    assert restored.etag == '"pointer-etag"'
    assert restored.coverage_end == date(2026, 8, 10)
    for relative, body in bodies.items():
        assert tmp_path.joinpath(relative).read_bytes() == body


def test_generated_snapshot_pointer_cas_failure_rejects_stale_retry(
    tmp_path, monkeypatch,
):
    action = _write_generated_action_manifest(tmp_path)
    action_sha = hashlib.sha256(action.read_bytes()).hexdigest()

    class FakeS3:
        def put_object(self, **kwargs):
            if kwargs["Key"] == ecs._SNAPSHOT_CURRENT_KEY:
                assert kwargs["IfMatch"] == '"old-etag"'
                raise ClientError(
                    {"Error": {"Code": "PreconditionFailed"}},
                    "PutObject",
                )
            return {"ETag": '"body"'}

        def head_object(self, **_kwargs):
            raise AssertionError("new immutable object unexpectedly existed")

    monkeypatch.setattr(ecs.boto3, "client", lambda service: FakeS3())
    certification_lock = object()
    monkeypatch.setattr(
        ecs,
        "assert_daily_certification_lock",
        lambda connection: None,
    )
    previous = ecs._PublishedSnapshotPointer(
        etag='"old-etag"',
        coverage_end=date(2026, 8, 9),
        action_manifest_sha256="b" * 64,
        bundle_prefix="older",
    )
    snapshot = SimpleNamespace(
        manifest_sha256=action_sha,
        coverage_end=date(2026, 8, 10),
    )

    with pytest.raises(RuntimeError, match="changed concurrently"):
        ecs._publish_generated_snapshot(
            "bronze", tmp_path, snapshot, previous,
            certification_lock=certification_lock,
        )


def test_generated_snapshot_lost_epoch_leaves_current_pointer_unchanged(
    tmp_path, monkeypatch,
):
    action = _write_generated_action_manifest(tmp_path)
    action_sha = hashlib.sha256(action.read_bytes()).hexdigest()
    puts: list[str] = []

    class FakeS3:
        def put_object(self, **kwargs):
            puts.append(kwargs["Key"])
            return {"ETag": '"bundle"'}

        def head_object(self, **_kwargs):
            raise AssertionError("new immutable object unexpectedly existed")

    certification_lock = object()
    monkeypatch.setattr(ecs.boto3, "client", lambda service: FakeS3())

    def lost_after_upload(connection):
        assert connection is certification_lock
        assert puts
        assert all(key != ecs._SNAPSHOT_CURRENT_KEY for key in puts)
        raise RuntimeError("daily certification epoch lock was lost")

    monkeypatch.setattr(
        ecs, "assert_daily_certification_lock", lost_after_upload,
    )

    with pytest.raises(RuntimeError, match="epoch lock was lost"):
        ecs._publish_generated_snapshot(
            "bronze",
            tmp_path,
            SimpleNamespace(
                manifest_sha256=action_sha,
                coverage_end=date(2026, 8, 10),
            ),
            None,
            certification_lock=certification_lock,
        )

    assert puts
    assert ecs._SNAPSHOT_CURRENT_KEY not in puts


def test_generated_snapshot_refuses_coverage_regression_before_upload(
    tmp_path, monkeypatch,
):
    action = _write_generated_action_manifest(tmp_path)
    action_sha = hashlib.sha256(action.read_bytes()).hexdigest()
    client = MagicMock()
    monkeypatch.setattr(ecs.boto3, "client", lambda service: client)
    previous = ecs._PublishedSnapshotPointer(
        etag='"current"',
        coverage_end=date(2026, 8, 10),
        action_manifest_sha256="c" * 64,
        bundle_prefix="current",
    )

    with pytest.raises(RuntimeError, match="refusing to regress"):
        ecs._publish_generated_snapshot(
            "bronze",
            tmp_path,
            SimpleNamespace(
                manifest_sha256=action_sha,
                coverage_end=date(2026, 8, 9),
            ),
            previous,
            certification_lock=object(),
        )
    client.put_object.assert_not_called()


def test_cash_scale_price_keys_reject_manifest_path_escape(tmp_path):
    path = tmp_path / ecs.cash_adjustment_scale_evidence.MANIFEST_RELATIVE_PATH
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps({
        "schema_version": (
            ecs.cash_adjustment_scale_evidence.SOURCE_EVIDENCE_CONTRACT
        ),
        "complete": True,
        "evidence": [{
            "previous_price_source_object_key": "../secret.parquet",
            "adjustment_price_source_object_key": (
                "stock/marcap/date=2018-12-27/all.parquet"
            ),
        }],
    }), encoding="utf-8")

    with pytest.raises(RuntimeError, match="invalid KRX price object key"):
        ecs._cash_scale_price_keys(tmp_path)


def test_prepare_requires_frozen_cash_scale_manifest_before_db_work(tmp_path):
    with pytest.raises(RuntimeError, match="missing frozen cash-scale"):
        ecs._cash_scale_price_keys(tmp_path)
