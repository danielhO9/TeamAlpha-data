"""ECS에서 DART 배당·기업행사 및 누락 KRX 날짜를 Silver에 반영한다."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

from pipeline.bronze import (
    dart_support_action_families,
    dart_viewer_corrections,
)
from pipeline.common import db
from pipeline.silver import (
    cash_adjustment_scale_evidence,
    dart_action_snapshot,
    dart_extra_load,
    reviewed_dividend_corrections,
    return_contract,
    total_return_audit,
    total_return_rebuild,
)
from pipeline.silver_quality import freshness as quality_freshness


DATA_ROOT = Path("/app/data")
_PRICE_EVIDENCE_KEY = re.compile(
    r"^stock/(?:marcap|krxapi)/date=[0-9]{4}-[0-9]{2}-[0-9]{2}/"
    r"[^/]+\.parquet$"
)
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_SNAPSHOT_POINTER_SCHEMA = "dart_total_return_snapshot_pointer_v1"
_SNAPSHOT_PUBLISH_ROOT = "quality/dart-total-return-snapshots"
_SNAPSHOT_CURRENT_KEY = f"{_SNAPSHOT_PUBLISH_ROOT}/current.json"
_S3_CACHE_SCHEMA = "teamalpha_s3_object_cache_v1"
_S3_CACHE_INDEX = Path(".teamalpha/s3_object_cache_v1.json")
# Stable, distinct from RETURN_WRITER_LOCK_KEY.  Hold this session lock across
# the entire Bronze-observation -> snapshot -> Silver rebuild epoch so two ECS
# tasks cannot certify against different action generations.
DAILY_CERTIFICATION_LOCK_KEY = 5_248_954_287_015_002


@dataclass(frozen=True)
class _PublishedSnapshotPointer:
    etag: str
    coverage_end: date
    action_manifest_sha256: str
    bundle_prefix: str


@dataclass(frozen=True)
class _S3Object:
    key: str
    etag: str
    size: int


def acquire_daily_certification_lock():
    """Fail fast unless this task can own the complete certification epoch."""
    connection = db.connect()
    try:
        # Session advisory locks do not need a transaction.  Autocommit avoids
        # leaving an hours-long ``idle in transaction`` session while DART/S3
        # network work runs.
        connection.autocommit = True
        with connection.cursor() as cur:
            cur.execute(
                "SELECT pg_try_advisory_lock(%s)",
                (DAILY_CERTIFICATION_LOCK_KEY,),
            )
            row = cur.fetchone()
        if not row or row[0] is not True:
            raise RuntimeError(
                "another daily/backfill certification epoch is active; "
                "refusing overlapping ECS task"
            )
        print("[dart-silver-ecs] daily certification lock acquired", flush=True)
        return connection
    except BaseException:
        connection.close()
        raise


def assert_daily_certification_lock(connection) -> None:
    """Prove that the owning session is alive and still holds the epoch lock."""
    class_id = (DAILY_CERTIFICATION_LOCK_KEY >> 32) & 0xFFFF_FFFF
    object_id = DAILY_CERTIFICATION_LOCK_KEY & 0xFFFF_FFFF
    with connection.cursor() as cur:
        cur.execute(
            """
            SELECT EXISTS (
                SELECT 1
                FROM pg_locks
                WHERE locktype='advisory'
                  AND pid=pg_backend_pid()
                  AND granted
                  AND classid::bigint=%s
                  AND objid::bigint=%s
                  AND objsubid=1
            )
            """,
            (class_id, object_id),
        )
        held = cur.fetchone()
    if not held or held[0] is not True:
        raise RuntimeError(
            "daily certification epoch lock was lost; refusing mutation"
        )


def release_daily_certification_lock(connection) -> None:
    """Release the epoch lock explicitly, then close its owning session."""
    try:
        with connection.cursor() as cur:
            cur.execute(
                "SELECT pg_advisory_unlock(%s)",
                (DAILY_CERTIFICATION_LOCK_KEY,),
            )
            row = cur.fetchone()
        if not row or row[0] is not True:
            raise RuntimeError("daily certification advisory lock was not held")
        print("[dart-silver-ecs] daily certification lock released", flush=True)
    finally:
        connection.close()


def _list_keys(s3, bucket: str, prefix: str) -> list[str]:
    keys: list[str] = []
    for page in s3.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix=prefix,
    ):
        keys.extend(
            item["Key"] for item in page.get("Contents", [])
            if not item["Key"].endswith("/")
        )
    return keys


def _list_objects(s3, bucket: str, prefix: str) -> list[_S3Object]:
    """List object identities used by the persistent incremental cache."""
    objects: list[_S3Object] = []
    for page in s3.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix=prefix,
    ):
        for item in page.get("Contents", []):
            key = str(item["Key"])
            if key.endswith("/"):
                continue
            objects.append(_S3Object(
                key=key,
                etag=str(item.get("ETag") or ""),
                size=int(item.get("Size", -1)),
            ))
    return objects


def _cache_index_path(root: Path) -> Path:
    return root.resolve() / _S3_CACHE_INDEX


def _read_cache_index(root: Path) -> dict[str, dict]:
    path = _cache_index_path(root)
    if not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_bytes())
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"persistent S3 cache index is invalid: {path}") from exc
    objects = payload.get("objects")
    if (
        payload.get("schema_version") != _S3_CACHE_SCHEMA
        or not isinstance(objects, dict)
    ):
        raise RuntimeError(f"persistent S3 cache index contract is invalid: {path}")
    return objects


def _write_cache_index(root: Path, objects: dict[str, dict]) -> None:
    path = _cache_index_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": _S3_CACHE_SCHEMA,
        "updated_at": datetime.now().astimezone().isoformat(),
        "objects": objects,
    }
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent,
    )
    os.close(descriptor)
    temporary = Path(temporary_name)
    try:
        temporary.write_bytes(_canonical_json(payload))
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _download_changed(
    bucket: str,
    objects: list[_S3Object],
    root: Path,
) -> tuple[int, int]:
    """Download only new or changed S3 objects into a persistent data root.

    The ETag/size pair comes from the same paginated LIST already required to
    discover new Bronze keys. A local cache hit therefore needs no HEAD/GET.
    Snapshot construction still hashes every selected research input, so a
    damaged cached body remains fail-closed at certification time.
    """
    root = root.resolve()
    client = boto3.client("s3")
    index = _read_cache_index(root)
    unique = {item.key: item for item in objects}
    changed: list[_S3Object] = []
    reused = 0
    for key in sorted(unique):
        item = unique[key]
        destination = root / key
        cached = index.get(key)
        if (
            cached == {"etag": item.etag, "size": item.size}
            and destination.is_file()
            and destination.stat().st_size == item.size
        ):
            reused += 1
            continue
        changed.append(item)

    def one(item: _S3Object) -> None:
        destination = root / item.key
        destination.parent.mkdir(parents=True, exist_ok=True)
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{destination.name}.", suffix=".download",
            dir=destination.parent,
        )
        os.close(descriptor)
        temporary = Path(temporary_name)
        try:
            client.download_file(bucket, item.key, str(temporary))
            if temporary.stat().st_size != item.size:
                raise RuntimeError(
                    f"S3 object size changed during download: {item.key}"
                )
            os.replace(temporary, destination)
        finally:
            temporary.unlink(missing_ok=True)

    done = 0
    chunk_size = 512
    with ThreadPoolExecutor(max_workers=32) as executor:
        for start in range(0, len(changed), chunk_size):
            chunk = changed[start:start + chunk_size]
            futures = [executor.submit(one, item) for item in chunk]
            for future in as_completed(futures):
                future.result()
                done += 1
                if done % 500 == 0 or done == len(changed):
                    print(
                        "[dart-silver-ecs] cache delta downloaded="
                        f"{done}/{len(changed)} reused={reused}",
                        flush=True,
                    )
    for item in unique.values():
        destination = root / item.key
        if destination.is_file() and destination.stat().st_size == item.size:
            index[item.key] = {"etag": item.etag, "size": item.size}
    _write_cache_index(root, index)
    print(
        "[dart-silver-ecs] persistent cache complete "
        f"downloaded={done} reused={reused} total={len(unique)}",
        flush=True,
    )
    return done, reused


def _download(bucket: str, keys: list[str], root: Path) -> int:
    client = boto3.client("s3")
    unique = sorted(set(keys))
    def one(key: str) -> None:
        destination = root / key
        destination.parent.mkdir(parents=True, exist_ok=True)
        client.download_file(bucket, key, str(destination))
    done = 0
    # Keep the future set bounded.  A complete action snapshot currently has
    # ~150k objects; submitting all of them at once needlessly consumes ECS
    # memory before the first download completes.
    chunk_size = 512
    with ThreadPoolExecutor(max_workers=32) as executor:
        for start in range(0, len(unique), chunk_size):
            chunk = unique[start:start + chunk_size]
            futures = [executor.submit(one, key) for key in chunk]
            for future in as_completed(futures):
                future.result()
                done += 1
                if done % 500 == 0 or done == len(unique):
                    print(
                        f"[dart-silver-ecs] downloaded={done}/{len(unique)}",
                        flush=True,
                    )
    return done


def _generated_snapshot_paths(root: Path) -> tuple[Path, ...]:
    """Return locally generated evidence that must survive the ECS task.

    Core OpenDART and KRX objects are written to S3 by their own collectors.
    Viewer/family evidence and the enclosing v5 manifest are local, verified
    derivatives. The canonical v5 object list is the sole authority on which
    generated bodies belong to this snapshot: directory membership must never
    publish legacy receipt caches or orphan content-addressed objects.
    """
    root = root.resolve()
    action_manifest = root / dart_action_snapshot.MANIFEST_RELATIVE_PATH
    try:
        raw = action_manifest.read_bytes()
        payload = json.loads(raw)
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(
            f"generated DART action manifest is missing/invalid: "
            f"{action_manifest}"
        ) from exc
    if raw != _canonical_json(payload):
        raise RuntimeError("generated DART action manifest is not canonical")
    objects = payload.get("objects")
    if (
        payload.get("schema_version") != dart_action_snapshot.SCHEMA_VERSION
        or payload.get("complete") is not True
        or not isinstance(objects, list)
    ):
        raise RuntimeError("generated DART action manifest contract is invalid")
    candidates: set[Path] = {action_manifest}
    seen: set[str] = set()
    for entry in objects:
        if not isinstance(entry, dict):
            raise RuntimeError("generated DART action object entry is invalid")
        relative = str(entry.get("path") or "")
        if not _generated_relative_allowed(relative):
            continue
        digest = str(entry.get("sha256") or "")
        try:
            length = int(entry.get("content_length", -1))
        except (TypeError, ValueError) as exc:
            raise RuntimeError(
                f"generated DART action object length is invalid: {relative}"
            ) from exc
        if (
            relative in seen
            or _SHA256.fullmatch(digest) is None
            or length < 0
        ):
            raise RuntimeError(
                f"generated DART action object identity is invalid: {relative}"
            )
        seen.add(relative)
        path = (root / relative).resolve()
        if (
            root not in path.parents
            or not path.is_file()
            or path.stat().st_size != length
            or _sha256_path(path) != digest
        ):
            raise RuntimeError(
                f"generated DART action object changed: {relative}"
            )
        candidates.add(path)
    reviewed_manifest = root / reviewed_dividend_corrections.MANIFEST_RELATIVE_PATH
    component_manifests = {
        root / dart_viewer_corrections.MANIFEST_RELATIVE_PATH,
        root / dart_support_action_families.MANIFEST_RELATIVE_PATH,
        reviewed_manifest,
    }

    def publication_order(path: Path) -> tuple[int, str]:
        # Leaf evidence first, component pointers second, enclosing v5 pointer
        # last.  S3 has no multi-object transaction; this order ensures a crash
        # can never publish a new enclosing manifest before its referenced
        # bodies exist remotely.
        if path == action_manifest:
            priority = 2
        elif path in component_manifests:
            priority = 1
        else:
            priority = 0
        return priority, path.relative_to(root).as_posix()

    return tuple(sorted(candidates, key=publication_order))


def _canonical_json(payload: object) -> bytes:
    return json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _sha256_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _snapshot_object_digest(entries: list[dict]) -> str:
    return hashlib.sha256(_canonical_json(entries)).hexdigest()


def _generated_relative_allowed(relative: str) -> bool:
    candidate = Path(relative)
    if (
        not relative
        or candidate.is_absolute()
        or ".." in candidate.parts
        or candidate.as_posix() != relative
    ):
        return False
    exact = {
        dart_action_snapshot.MANIFEST_RELATIVE_PATH.as_posix(),
        reviewed_dividend_corrections.MANIFEST_RELATIVE_PATH.as_posix(),
    }
    roots = (
        dart_viewer_corrections.MANIFEST_RELATIVE_PATH.parent.as_posix()
        + "/",
        dart_support_action_families.MANIFEST_RELATIVE_PATH.parent.as_posix()
        + "/",
    )
    return relative in exact or relative.startswith(roots)


def _client_error_code(exc: ClientError) -> str:
    return str(exc.response.get("Error", {}).get("Code") or "")


def _is_missing_object(exc: ClientError) -> bool:
    return _client_error_code(exc) in {"NoSuchKey", "404", "NotFound"}


def _is_precondition_failure(exc: ClientError) -> bool:
    return _client_error_code(exc) in {
        "PreconditionFailed", "412", "ConditionalRequestConflict", "409",
    }


def _restore_published_snapshot(
    client,
    bucket: str,
    root: Path,
) -> _PublishedSnapshotPointer | None:
    """Overlay the last atomically published generated-evidence bundle."""
    try:
        response = client.get_object(Bucket=bucket, Key=_SNAPSHOT_CURRENT_KEY)
    except ClientError as exc:
        if _is_missing_object(exc):
            return None
        raise
    body = response["Body"]
    try:
        raw = body.read()
    finally:
        close = getattr(body, "close", None)
        if close is not None:
            close()
    try:
        payload = json.loads(raw)
    except (TypeError, json.JSONDecodeError) as exc:
        raise RuntimeError("published DART snapshot pointer is invalid JSON") from exc
    if raw != _canonical_json(payload):
        raise RuntimeError("published DART snapshot pointer is not canonical")
    action_sha = str(payload.get("action_manifest_sha256") or "")
    bundle_prefix = str(payload.get("bundle_prefix") or "")
    expected_bundle = (
        f"{_SNAPSHOT_PUBLISH_ROOT}/bundles/"
        f"action-manifest-sha256={action_sha}"
    )
    try:
        coverage_end = date.fromisoformat(str(payload.get("coverage_end") or ""))
    except ValueError as exc:
        raise RuntimeError("published DART snapshot coverage_end is invalid") from exc
    entries = payload.get("objects")
    if (
        payload.get("schema_version") != _SNAPSHOT_POINTER_SCHEMA
        or payload.get("complete") is not True
        or _SHA256.fullmatch(action_sha) is None
        or bundle_prefix != expected_bundle
        or not isinstance(entries, list)
        or int(payload.get("object_count", -1)) != len(entries)
        or payload.get("object_digest") != _snapshot_object_digest(entries)
    ):
        raise RuntimeError("published DART snapshot pointer contract is invalid")
    normalized: list[dict] = []
    seen: set[str] = set()
    for entry in entries:
        if not isinstance(entry, dict):
            raise RuntimeError("published DART snapshot object is invalid")
        relative = str(entry.get("path") or "")
        digest = str(entry.get("sha256") or "")
        length = int(entry.get("content_length", -1))
        if (
            not _generated_relative_allowed(relative)
            or relative in seen
            or _SHA256.fullmatch(digest) is None
            or length < 0
        ):
            raise RuntimeError(
                f"published DART snapshot object identity is invalid: {relative}"
            )
        seen.add(relative)
        normalized.append({
            "path": relative,
            "content_length": length,
            "sha256": digest,
        })
    if entries != sorted(normalized, key=lambda item: item["path"]):
        raise RuntimeError("published DART snapshot object order is invalid")
    action_entry = next(
        (
            entry for entry in normalized
            if entry["path"]
            == dart_action_snapshot.MANIFEST_RELATIVE_PATH.as_posix()
        ),
        None,
    )
    if action_entry is None or action_entry["sha256"] != action_sha:
        raise RuntimeError("published DART action manifest binding is invalid")

    def download_one(entry: dict) -> None:
        relative = entry["path"]
        destination = (root / relative).resolve()
        if root not in destination.parents:
            raise RuntimeError(
                f"published DART snapshot path escaped root: {relative}"
            )
        if (
            destination.is_file()
            and destination.stat().st_size == entry["content_length"]
            and _sha256_path(destination) == entry["sha256"]
        ):
            return
        destination.parent.mkdir(parents=True, exist_ok=True)
        descriptor, temporary_name = tempfile.mkstemp(
            prefix=f".{destination.name}.", suffix=".download",
            dir=destination.parent,
        )
        os.close(descriptor)
        temporary = Path(temporary_name)
        try:
            key = f"{bundle_prefix}/{relative}"
            client.download_file(bucket, key, str(temporary))
            if (
                temporary.stat().st_size != entry["content_length"]
                or _sha256_path(temporary) != entry["sha256"]
            ):
                raise RuntimeError(
                    f"published DART snapshot object changed: {relative}"
                )
            os.replace(temporary, destination)
        finally:
            temporary.unlink(missing_ok=True)

    with ThreadPoolExecutor(max_workers=32) as executor:
        futures = [executor.submit(download_one, entry) for entry in normalized]
        for future in as_completed(futures):
            future.result()
    etag = str(response.get("ETag") or "")
    if not etag:
        raise RuntimeError("published DART snapshot pointer has no ETag")
    print(
        "[dart-silver-ecs] restored generated snapshot "
        f"coverage_end={coverage_end.isoformat()} objects={len(normalized)}",
        flush=True,
    )
    return _PublishedSnapshotPointer(
        etag=etag,
        coverage_end=coverage_end,
        action_manifest_sha256=action_sha,
        bundle_prefix=bundle_prefix,
    )


def restore_published_total_return_snapshot(
    coverage_end: date,
    *,
    bucket: str | None = None,
    root: Path | None = None,
) -> None:
    """Restore the exact published action bundle for a retry-only recovery.

    A daily retry whose KRX/DART Silver transaction is already certified must
    not repeat source discovery or rebuild the generated evidence bundle.  The
    immutable current pointer already binds every object by length and digest,
    so restoring and checking its coverage is sufficient before the atomic
    action publication and total-return repair.
    """
    bucket = bucket or os.environ["S3_BRONZE_BUCKET"]
    root = (root or DATA_ROOT).resolve()
    pointer = _restore_published_snapshot(
        boto3.client("s3"), bucket, root,
    )
    if pointer is None:
        raise RuntimeError(
            "cannot repair total-return contract without a published "
            "DART action snapshot"
        )
    if pointer.coverage_end != coverage_end:
        raise RuntimeError(
            "published DART action snapshot does not match retry coverage: "
            f"published={pointer.coverage_end.isoformat()} "
            f"required={coverage_end.isoformat()}"
        )
    print(
        "[dart-silver-ecs] retry restored published action snapshot "
        f"coverage_end={coverage_end.isoformat()}",
        flush=True,
    )


def _put_immutable_snapshot_object(
    client,
    bucket: str,
    key: str,
    path: Path,
    entry: dict,
) -> None:
    try:
        with path.open("rb") as body:
            client.put_object(
                Bucket=bucket,
                Key=key,
                Body=body,
                ContentLength=entry["content_length"],
                Metadata={"sha256": entry["sha256"]},
                IfNoneMatch="*",
            )
    except ClientError as exc:
        if not _is_precondition_failure(exc):
            raise
        existing = client.head_object(Bucket=bucket, Key=key)
        if (
            int(existing.get("ContentLength", -1))
            != entry["content_length"]
            or (existing.get("Metadata") or {}).get("sha256")
            != entry["sha256"]
        ):
            raise RuntimeError(
                f"immutable DART snapshot object collision: {key}"
            ) from exc


def _component_paths(root: Path, manifest_relative: Path) -> tuple[Path, ...]:
    """Return one generated component manifest and its owned object paths."""
    manifest = root / manifest_relative
    payload = json.loads(manifest.read_bytes())
    owned_prefix = manifest_relative.parent.as_posix() + "/"
    rendered_paths: set[str] = set()

    def visit(value: object) -> None:
        if isinstance(value, dict):
            for child in value.values():
                visit(child)
        elif isinstance(value, list):
            for child in value:
                visit(child)
        elif isinstance(value, str) and value.startswith(owned_prefix):
            rendered_paths.add(value)

    visit(payload)
    paths = {manifest}
    for relative in rendered_paths:
        candidate = Path(relative)
        if candidate.is_absolute() or ".." in candidate.parts:
            raise RuntimeError(f"generated component path is invalid: {relative}")
        path = root / candidate
        if not path.is_file():
            raise RuntimeError(f"generated component object is missing: {relative}")
        paths.add(path)
    return tuple(sorted(paths, key=lambda item: item == manifest))


def _publish_component_checkpoint(
    client,
    bucket: str,
    root: Path,
    manifest_relative: Path,
    *,
    certification_lock,
) -> None:
    """Publish generated leaves first and CAS the retry checkpoint pointer."""
    manifest = root / manifest_relative
    paths = _component_paths(root, manifest_relative)
    try:
        previous = client.head_object(
            Bucket=bucket, Key=manifest_relative.as_posix(),
        )
    except ClientError as exc:
        if not _is_missing_object(exc):
            raise
        previous = None
    for path in paths:
        if path == manifest:
            continue
        relative = path.relative_to(root).as_posix()
        digest = _sha256_path(path)
        try:
            with path.open("rb") as body:
                client.put_object(
                    Bucket=bucket,
                    Key=relative,
                    Body=body,
                    ContentLength=path.stat().st_size,
                    Metadata={"sha256": digest},
                    IfNoneMatch="*",
                )
        except ClientError as exc:
            if not _is_precondition_failure(exc):
                raise
            existing = client.get_object(Bucket=bucket, Key=relative)["Body"].read()
            if hashlib.sha256(existing).hexdigest() != digest:
                raise RuntimeError(
                    f"generated component object collision: {relative}"
                ) from exc
    assert_daily_certification_lock(certification_lock)
    conditions = (
        {"IfMatch": str(previous["ETag"])}
        if previous is not None else {"IfNoneMatch": "*"}
    )
    client.put_object(
        Bucket=bucket,
        Key=manifest_relative.as_posix(),
        Body=manifest.read_bytes(),
        ContentType="application/json",
        **conditions,
    )
    print(
        "[dart-silver-ecs] published retry checkpoint "
        f"manifest={manifest_relative.as_posix()} objects={len(paths)}",
        flush=True,
    )


def _publish_generated_snapshot(
    bucket: str,
    root: Path,
    snapshot: dart_action_snapshot.VerifiedActionSnapshot,
    previous: _PublishedSnapshotPointer | None,
    *,
    certification_lock,
) -> int:
    """Publish one immutable bundle, then CAS its single current pointer."""
    root = root.resolve()
    client = boto3.client("s3")
    paths = _generated_snapshot_paths(root)
    action_sha = snapshot.manifest_sha256
    if _SHA256.fullmatch(action_sha) is None:
        raise RuntimeError("generated DART action manifest SHA is invalid")
    if snapshot.coverage_end < (
        previous.coverage_end if previous is not None else snapshot.coverage_end
    ):
        raise RuntimeError(
            "refusing to regress published DART snapshot coverage: "
            f"current={previous.coverage_end.isoformat()} "
            f"candidate={snapshot.coverage_end.isoformat()}"
        )
    entries = sorted(({
        "path": path.relative_to(root).as_posix(),
        "content_length": path.stat().st_size,
        "sha256": _sha256_path(path),
    } for path in paths), key=lambda item: item["path"])
    action_entry = next(
        entry for entry in entries
        if entry["path"] == dart_action_snapshot.MANIFEST_RELATIVE_PATH.as_posix()
    )
    if action_entry["sha256"] != action_sha:
        raise RuntimeError("generated DART action manifest SHA changed")
    entries_by_path = {entry["path"]: entry for entry in entries}
    bundle_prefix = (
        f"{_SNAPSHOT_PUBLISH_ROOT}/bundles/"
        f"action-manifest-sha256={action_sha}"
    )

    def upload_one(path: Path) -> None:
        relative = path.relative_to(root).as_posix()
        _put_immutable_snapshot_object(
            client,
            bucket,
            f"{bundle_prefix}/{relative}",
            path,
            entries_by_path[relative],
        )

    with ThreadPoolExecutor(max_workers=16) as executor:
        futures = [executor.submit(upload_one, path) for path in paths]
        for future in as_completed(futures):
            future.result()
    pointer = {
        "schema_version": _SNAPSHOT_POINTER_SCHEMA,
        "complete": True,
        "coverage_end": snapshot.coverage_end.isoformat(),
        "action_manifest_sha256": action_sha,
        "bundle_prefix": bundle_prefix,
        "object_count": len(entries),
        "object_digest": _snapshot_object_digest(entries),
        "objects": entries,
    }
    conditions = (
        {"IfMatch": previous.etag}
        if previous is not None else {"IfNoneMatch": "*"}
    )
    # Immutable bundle objects may take minutes to upload. The K2 session was
    # valid before that work began, but a lost PostgreSQL session releases its
    # advisory lock automatically. Re-prove ownership immediately before the
    # only mutable publication step. Failure leaves harmless orphan bundle
    # objects and the certified current pointer unchanged.
    assert_daily_certification_lock(certification_lock)
    try:
        client.put_object(
            Bucket=bucket,
            Key=_SNAPSHOT_CURRENT_KEY,
            Body=_canonical_json(pointer),
            ContentType="application/json",
            **conditions,
        )
    except ClientError as exc:
        if _is_precondition_failure(exc):
            raise RuntimeError(
                "published DART snapshot pointer changed concurrently; "
                "refusing stale retry"
            ) from exc
        raise
    print(
        "[dart-silver-ecs] published immutable generated snapshot "
        f"objects={len(paths)} manifest={action_sha}",
        flush=True,
    )
    return len(paths)


def _cash_scale_price_keys(root: Path) -> list[str]:
    """Read only the exact KRX price objects named by the frozen manifest."""
    path = root / cash_adjustment_scale_evidence.MANIFEST_RELATIVE_PATH
    if not path.is_file():
        raise RuntimeError(
            "missing frozen cash-scale evidence manifest; daily Silver write "
            "is not allowed"
        )
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError("cash-scale evidence manifest is invalid JSON") from exc
    if (
        payload.get("schema_version")
        != cash_adjustment_scale_evidence.SOURCE_EVIDENCE_CONTRACT
        or payload.get("complete") is not True
        or not isinstance(payload.get("evidence"), list)
    ):
        raise RuntimeError("cash-scale evidence manifest is not complete/frozen")
    keys: set[str] = set()
    for index, row in enumerate(payload["evidence"]):
        if not isinstance(row, dict):
            raise RuntimeError(
                f"cash-scale evidence parent {index} is not an object"
            )
        for field in (
            "previous_price_source_object_key",
            "adjustment_price_source_object_key",
        ):
            key = str(row.get(field) or "").strip()
            candidate = Path(key)
            if (
                not key
                or candidate.is_absolute()
                or ".." in candidate.parts
                or _PRICE_EVIDENCE_KEY.fullmatch(key) is None
            ):
                raise RuntimeError(
                    "cash-scale manifest has an invalid KRX price object key: "
                    f"parent={index} field={field} key={key!r}"
                )
            keys.add(key)
    return sorted(keys)


def prepare_total_return_snapshot(
    coverage_end: date,
    *,
    bucket: str | None = None,
    root: Path | None = None,
    publish: bool = True,
    certification_lock=None,
) -> dart_action_snapshot.VerifiedActionSnapshot:
    """Build a current, complete v5 action snapshot before any Silver write.

    The daily task first writes its native OpenDART interval to S3.  This
    preflight syncs the immutable historical source set, extends official
    viewer/family evidence only for newly observed or touched families, and
    builds the v5 manifest through the requested calendar date. If any source
    family or frozen cash-scale input is incomplete, the function raises before
    the daily KRX price transaction.
    """
    bucket = bucket or os.environ["S3_BRONZE_BUCKET"]
    root = (root or DATA_ROOT).resolve()
    s3 = boto3.client("s3")
    prefixes = (
        "dividends/dart/",
        "corporate_actions/dart/",
        # The certified v5 action snapshot binds cash-scale evidence and its
        # content-addressed KIND/KRX bodies below this prefix.  Downloading
        # only DART objects would make local manifest verification incomplete.
        "corporate_actions/krx/",
    )
    objects = [
        item for prefix in prefixes
        for item in _list_objects(s3, bucket, prefix)
    ]
    count, reused = _download_changed(bucket, objects, root)
    previous = _restore_published_snapshot(s3, bucket, root)
    price_keys = _cash_scale_price_keys(root)
    if price_keys:
        count += _download(bucket, price_keys, root)
    if count == 0 and reused == 0:
        raise RuntimeError("no DART dividend/corporate-action Bronze objects")
    try:
        dart_viewer_corrections.verify_viewer_corrections(
            str(root),
            required_start=dart_action_snapshot.DEFAULT_COVERAGE_START,
            required_end=coverage_end,
        )
        print("[dart-silver-ecs] reused viewer retry checkpoint", flush=True)
    except RuntimeError:
        dart_viewer_corrections.collect_viewer_corrections(
            str(root),
            coverage_start=dart_action_snapshot.DEFAULT_COVERAGE_START,
            coverage_end=coverage_end,
            apply=True,
        )
        _publish_component_checkpoint(
            s3, bucket, root,
            dart_viewer_corrections.MANIFEST_RELATIVE_PATH,
            certification_lock=certification_lock,
        )
    try:
        dart_support_action_families.verify_support_action_families(
            root,
            required_start=dart_action_snapshot.DEFAULT_COVERAGE_START,
            required_end=coverage_end,
        )
        print("[dart-silver-ecs] reused support retry checkpoint", flush=True)
    except RuntimeError:
        dart_support_action_families.collect_support_action_families(
            root,
            coverage_start=dart_action_snapshot.DEFAULT_COVERAGE_START,
            coverage_end=coverage_end,
            apply=True,
        )
        _publish_component_checkpoint(
            s3, bucket, root,
            dart_support_action_families.MANIFEST_RELATIVE_PATH,
            certification_lock=certification_lock,
        )
    snapshot = dart_action_snapshot.build_snapshot_manifest(
        str(root), coverage_end=coverage_end,
    )
    if publish:
        if certification_lock is None:
            raise RuntimeError(
                "publishing a generated action snapshot requires the daily "
                "certification epoch lock"
            )
        assert_daily_certification_lock(certification_lock)
        _publish_generated_snapshot(
            bucket,
            root,
            snapshot,
            previous,
            certification_lock=certification_lock,
        )
    print(
        "[dart-silver-ecs] v5 action snapshot prepared "
        f"coverage_end={coverage_end.isoformat()} "
        f"manifest={snapshot.manifest_sha256}",
        flush=True,
    )
    return snapshot


def preview_total_return_actions(
    coverage_end: date,
    *,
    root: Path | None = None,
    conn=None,
) -> None:
    """Read-only DB preview of the freshly prepared action snapshot."""
    root = (root or DATA_ROOT).resolve()
    dart_extra_load.run(
        src="local",
        apply=False,
        total_return_actions_only=True,
        expected_coverage_end=coverage_end,
        base_override=str(root),
        conn=conn,
    )


def total_return_contract_ready(*, conn=None) -> bool:
    """Read the exact research-readiness contract without mutating RDS."""
    owns_connection = conn is None
    connection = conn or db.connect()
    try:
        with connection.transaction():
            with connection.cursor() as cur:
                cur.execute("SET TRANSACTION READ ONLY")
            report = quality_freshness.total_return_contract_report(connection)
        return bool(report["ready"])
    finally:
        if owns_connection:
            connection.close()


def certified_krx_price_coverage_end(*, conn=None) -> date:
    """Return the exact raw KRX coverage that an inherited rebuild must close."""
    owns_connection = conn is None
    connection = conn or db.connect()
    try:
        _, coverage_end = total_return_rebuild._source_price_coverage(connection)
        return coverage_end
    finally:
        if owns_connection:
            connection.close()


def invalidate_total_return_for_observed_action(
    coverage_end: date,
    *,
    conn=None,
) -> bool:
    """Demote the return label before a new Bronze action object is visible.

    S3 and PostgreSQL cannot share one transaction.  The safe publication
    direction is therefore DB invalidation first, immutable S3 source second.
    A failed callback publishes no new source object; a later S3 failure may
    leave BUILDING, which the next daily retry detects and repairs.
    """
    owns_connection = conn is None
    connection = conn or db.connect()
    try:
        with connection.transaction():
            return_contract.acquire_return_writer_transaction_lock(connection)
            changed = return_contract.invalidate_krx_total_return(
                connection,
                reason=(
                    "DART_BRONZE_ACTION_CHANGE_OBSERVED_BEFORE_PUBLICATION:"
                    f"{coverage_end.isoformat()}"
                ),
                quality_run_id=None,
            )
        print(
            "[dart-silver-ecs] observed-action contract invalidation "
            f"changed={changed}",
            flush=True,
        )
        return changed
    finally:
        if owns_connection:
            connection.close()


def close_total_return_contract(
    coverage_end: date,
    *,
    root: Path | None = None,
    certification_lock=None,
) -> dict:
    """Publish actions, rebuild once, then run the fatal independent audit.

    Snapshot preparation and the action-only preview have already validated
    the immutable local evidence before this function is reached.  Rebuilding
    the complete price history twice more as read-only previews duplicated the
    exact work performed by the atomic apply run.  The apply run is itself
    fail-closed: any validation error rolls back its price/audit transaction
    and leaves the contract visibly BUILDING.
    """
    root = (root or DATA_ROOT).resolve()
    if certification_lock is None:
        raise RuntimeError(
            "total-return certification requires the daily epoch lock"
        )
    assert_daily_certification_lock(certification_lock)
    dart_extra_load.run(
        src="local",
        apply=True,
        total_return_actions_only=True,
        expected_coverage_end=coverage_end,
        base_override=str(root),
        conn=certification_lock,
    )
    assert_daily_certification_lock(certification_lock)
    rebuild_batch_size = int(os.environ.get(
        "TOTAL_RETURN_REBUILD_BATCH_SIZE", "500",
    ))
    if rebuild_batch_size < 1:
        raise RuntimeError("TOTAL_RETURN_REBUILD_BATCH_SIZE must be positive")
    total_return_rebuild.run(
        apply=True,
        batch_size=rebuild_batch_size,
        conn=certification_lock,
    )
    report = total_return_audit.audit(conn=certification_lock)
    if not report.get("safe_for_research"):
        failed = sorted(
            key for key, passed in (report.get("checks") or {}).items()
            if not passed
        )
        raise RuntimeError(
            "DART ECS total-return audit failed after rebuild: "
            f"{failed}"
        )
    print(
        "[dart-silver-ecs] DART TR actions/rebuild certified",
        flush=True,
    )
    return report


def _run_dart_extras_locked(certification_lock=None) -> None:
    expected_end = os.environ.get("DART_SNAPSHOT_EXPECTED_END")
    if not expected_end:
        raise RuntimeError(
            "DART_SNAPSHOT_EXPECTED_END=YYYY-MM-DD is required for apply"
        )
    coverage_end = date.fromisoformat(expected_end)
    # The one-off and the scheduled daily path share the exact same closed
    # workflow.  A successful process therefore always ends at an independently
    # audited CERTIFIED contract; no partial writer can return exit code zero.
    prepare_total_return_snapshot(
        coverage_end,
        certification_lock=certification_lock,
    )
    preview_total_return_actions(coverage_end, conn=certification_lock)
    if certification_lock is not None:
        assert_daily_certification_lock(certification_lock)
    close_total_return_contract(
        coverage_end,
        certification_lock=certification_lock,
    )


def run_dart_extras() -> None:
    certification_lock = acquire_daily_certification_lock()
    try:
        _run_dart_extras_locked(certification_lock)
    finally:
        release_daily_certification_lock(certification_lock)


def run_krx_gap() -> None:
    raise RuntimeError(
        "krx-gap is disabled: incremental gap publication leaves the "
        "total-return contract BUILDING; replay each date through the closed "
        "daily snapshot -> action -> rebuild -> audit orchestrator"
    )


def _run_krx_gap_locked() -> None:
    raise RuntimeError("unsafe krx-gap implementation is disabled")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", choices=("dart-extras", "krx-gap"), required=True)
    args = parser.parse_args()
    if args.phase == "dart-extras":
        run_dart_extras()
    else:
        run_krx_gap()


if __name__ == "__main__":
    main()
