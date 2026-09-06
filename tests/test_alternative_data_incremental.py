import json

from pipeline import alternative_data_incremental as incremental


def test_incremental_publishes_only_changed_and_unseen_sources(monkeypatch):
    monkeypatch.setenv("S3_BRONZE_BUCKET", "bronze")
    objects: dict[str, bytes] = {}
    monkeypatch.setattr(incremental, "read_bytes", lambda uri: objects.get(uri))
    monkeypatch.setattr(
        incremental,
        "write_text_if_changed",
        lambda text, uri: objects.__setitem__(uri, text.encode()) or True,
    )
    monkeypatch.setattr(
        incremental.dart_full_statements,
        "run_incremental_day",
        lambda day, dest: ["s3://bronze/full-new.json"],
    )
    monkeypatch.setattr(
        incremental.dart_ownership,
        "run_incremental",
        lambda day, dest: [],
    )
    monkeypatch.setattr(
        incremental.dart_company_profiles,
        "run_incremental",
        lambda day, dest, shard_count: ["s3://bronze/industry-new.json"],
    )
    monkeypatch.setattr(
        incremental,
        "_list_authorized_sources",
        lambda _bucket, prefix: [
            "s3://bronze/flow-old.csv" if prefix.startswith("investor")
            else "s3://bronze/short-new.csv"
        ],
    )
    state_uri = incremental._state_uri()
    objects[state_uri] = json.dumps({
        "schema_version": "alternative-input-state-v1",
        "krx_files": ["s3://bronze/flow-old.csv"],
    }).encode()
    captured = {}

    def publish_files(**kwargs):
        captured.update(kwargs)
        return {"published": {"fundamental_statement_line": 3}}

    monkeypatch.setattr(incremental.alternative_data, "publish_files", publish_files)
    conn = object()
    result = incremental.run("20260901", conn=conn)

    assert captured["conn"] is conn
    assert captured["investor_flow_files"] == []
    assert captured["short_balance_files"] == ["s3://bronze/short-new.csv"]
    assert result["changed_files"]["full_statements"] == 1
    assert "s3://bronze/short-new.csv" in json.loads(objects[state_uri])["krx_files"]


def test_completed_increment_skips_all_collection(monkeypatch):
    monkeypatch.setenv("S3_BRONZE_BUCKET", "bronze")
    completion = incremental._day_uri("20260901")
    expected = {"day": "20260901", "published": {}}
    monkeypatch.setattr(
        incremental,
        "read_bytes",
        lambda uri: json.dumps(expected).encode() if uri == completion else None,
    )
    monkeypatch.setattr(
        incremental.dart_full_statements,
        "run_incremental_day",
        lambda *_args: (_ for _ in ()).throw(AssertionError("must skip")),
    )
    assert incremental.run("20260901", conn=object()) == expected
