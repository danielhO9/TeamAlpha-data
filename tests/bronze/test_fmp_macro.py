import hashlib
import json
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from pipeline.bronze import fmp_macro as m
from pipeline.bronze.fmp import FMPClient, FMPError, RawResponse


def event(name="Inflation Rate YoY (Jan)", country="KR", day="2015-02-02 23:00:00", **extra):
    return {"date": day, "country": country, "event": name, "currency": "KRW",
            "actual": 0.8, "previous": 0.8, "estimate": 0.9, "unit": "%", **extra}


class Client:
    def __init__(self, rows):
        self.rows = rows
        self.calls = []

    def get(self, endpoint, params):
        self.calls.append((endpoint, params))
        rows = self.rows(params) if callable(self.rows) else self.rows
        body = rows if isinstance(rows, bytes) else json.dumps(rows, indent=2).encode() + b"\n"
        return RawResponse(endpoint, params, 200, "application/json",
                           datetime(2026, 9, 19, tzinfo=timezone.utc), body)


def run_local(tmp_path, monkeypatch, client, **kwargs):
    monkeypatch.setattr(m, "base_uri", lambda dest: str(tmp_path))
    return m.run(date(2015, 2, 1), date(2015, 2, 28), client=client, **kwargs)


def test_scope_only_contains_history_approved_korea_and_china():
    assert len(m.SERIES) == 14
    assert sum(country == "KR" for country, _, _ in m.SERIES.values()) == 13
    rows = [event(name, country) for country, name, _ in m.SERIES.values()]
    rejected = [event("Exports YoY (Jan)"), event("Imports YoY (Jan)"),
                event("M2 Money Supply (Jan)"), event("S&P Global Manufacturing PMI (Jan)"),
                event("CPI (Jan)"), event("Foreign Exchange Reserves (Jan)"),
                event(country="US"), event("NBS Non Manufacturing PMI (Jan)", "CN"),
                event("Inflation Rate YoY Flash (Jan)"), event("Inflation Rate YoY (Q1)")]
    selected, stats = m.select(rows + rejected)
    assert {x["series_id"] for x in selected} == set(m.SERIES)
    assert stats["excluded_row_count"] == len(rejected)


def test_projection_retains_nulls_duplicates_unverified_times_and_units():
    a = event("Interest Rate Decision (Jun)", day="2026-07-16 01:00:00", actual=None)
    b = {**a, "event": "Interest Rate Decision (Jul)", "actual": 2.75}
    selected, stats = m.select([a, b, b, event("Business Confidence (Jan)", actual=73)])
    assert [x["payload"] for x in selected] == [a, b, b, event("Business Confidence (Jan)", actual=73)]
    assert selected[3]["payload"]["unit"] == "%"  # deliberately not fixed in Bronze
    assert stats["null_actual_by_series"] == {"KR_POLICY_RATE": 1}
    assert stats["duplicate_series_timestamp_rows"] == 2
    assert [x["source_row_index"] for x in selected] == list(range(4))


@pytest.mark.parametrize("value", [True, "1.5", float("nan"), float("inf"), {}])
def test_invalid_selected_values_are_rejected(value):
    with pytest.raises(FMPError):
        m.select([event(actual=value)])


@pytest.mark.parametrize("body", [b'{"Error Message":"not authorized"}', b"not json", b"[null]",
    json.dumps([event(day="2015-03-01 00:00:00")]).encode(),
    json.dumps([event(day="2015-02-02")]).encode()])
def test_invalid_envelopes_dates_and_out_of_range(body):
    with pytest.raises(FMPError):
        m._rows(body, date(2015, 2, 1), date(2015, 2, 28))


def test_raw_evidence_selection_checkpoint_and_new_snapshot(tmp_path, monkeypatch):
    rows = [event(), event("Exports YoY (Jan)"), event(country="US")]
    c = Client(rows)
    result = run_local(tmp_path, monkeypatch, c)
    leaf = result["partitions"][0]
    manifest_path = Path(leaf["selection_manifest_uri"])
    meta = json.loads(manifest_path.read_bytes())
    raw = Path(meta["source_object_uri"]).read_bytes()
    selected = json.loads(Path(meta["object_uri"]).read_bytes())
    assert raw == json.dumps(rows, indent=2).encode() + b"\n"
    assert meta["source_sha256"] == hashlib.sha256(raw).hexdigest()
    assert len(selected) == 1 and selected[0]["payload"] == rows[0]
    assert meta["received_at"] == "2026-09-19T00:00:00+00:00"
    assert meta["pit_approved"] is False and meta["silver_publish_allowed"] is False
    assert result["rows_by_series"] == {"KR_CPI_YOY": 1}
    original = {p: p.read_bytes() for p in tmp_path.rglob("*.json")}
    assert run_local(tmp_path, monkeypatch, c) == result
    assert len(c.calls) == 1
    assert all(p.read_bytes() == b for p, b in original.items())
    # Failure after selected payload but before its manifest: recover from raw,
    # no API calls, and preserve original receipt time.
    manifest_path.unlink()
    run_local(tmp_path, monkeypatch, c)
    assert len(c.calls) == 1
    assert json.loads(manifest_path.read_bytes())["received_at"] == meta["received_at"]
    run_local(tmp_path, monkeypatch, c, snapshot="refresh-2")
    assert len(c.calls) == 2


@pytest.mark.parametrize("kind", ["raw", "selected"])
def test_complete_corrupt_object_is_not_silently_overwritten(tmp_path, monkeypatch, kind):
    c = Client([event()])
    result = run_local(tmp_path, monkeypatch, c)
    meta = json.loads(Path(result["partitions"][0]["selection_manifest_uri"]).read_bytes())
    p = Path(meta["source_object_uri"] if kind == "raw" else meta["object_uri"])
    p.write_bytes(b"corrupt")
    with pytest.raises(FMPError, match="corrupt"):
        run_local(tmp_path, monkeypatch, c)
    assert p.read_bytes() == b"corrupt" and len(c.calls) == 1


def test_empty_month_has_no_selection_or_run_completion_marker(tmp_path, monkeypatch):
    with pytest.raises(FMPError, match="no allowlisted"):
        run_local(tmp_path, monkeypatch, Client([]))
    assert list(tmp_path.rglob("response.json"))  # evidence, not successful ingestion
    assert not list(tmp_path.rglob("selection_manifest.json"))
    assert not list(tmp_path.glob("**/runs/**/manifest.json"))


def test_disappearing_series_fails_long_run(tmp_path, monkeypatch):
    monkeypatch.setattr(m, "base_uri", lambda dest: str(tmp_path))
    c = Client(lambda params: [event(day=params["from"] + " 00:00:00")])
    with pytest.raises(FMPError, match="without actual"):
        m.run(date(2015, 1, 1), date(2015, 3, 31), client=c)
    assert not list(tmp_path.glob("**/runs/**/manifest.json"))


def test_row_cap_bisects_retains_parent_raw_and_counts_only_leaves(tmp_path, monkeypatch):
    monkeypatch.setattr(m, "CALENDAR_ROW_LIMIT", 3)
    def response(params):
        row = event(day=params["from"] + " 00:00:00")
        return [row] * (3 if params["to"] == "2015-02-28" and params["from"] == "2015-02-01" else 1)
    c = Client(response)
    result = run_local(tmp_path, monkeypatch, c)
    assert result["selected_row_count"] == 2 and len(result["partitions"]) == 2
    assert len(list(tmp_path.rglob("response.json"))) == 3
    assert len(list(tmp_path.rglob("selection_manifest.json"))) == 2
    run_local(tmp_path, monkeypatch, c)
    assert len(c.calls) == 3


def test_one_day_cap_fails(tmp_path, monkeypatch):
    monkeypatch.setattr(m, "base_uri", lambda dest: str(tmp_path))
    monkeypatch.setattr(m, "CALENDAR_ROW_LIMIT", 1)
    with pytest.raises(FMPError, match="one-day"):
        m.run(date(2015, 2, 2), date(2015, 2, 2), client=Client([event()]))


def test_s3_upload_header_auth_retry_and_resume(monkeypatch):
    import boto3
    from botocore.exceptions import ClientError
    objects = {}
    class S3:
        def get_object(self, Bucket, Key):
            import io
            if (Bucket, Key) not in objects:
                raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
            return {"Body": io.BytesIO(objects[(Bucket, Key)])}
        def put_object(self, Bucket, Key, Body):
            objects[(Bucket, Key)] = Body
        def head_object(self, Bucket, Key):
            return {"ContentLength": len(objects[(Bucket, Key)])}
    monkeypatch.setattr(boto3, "client", lambda service: S3())
    monkeypatch.setenv("S3_BRONZE_BUCKET", "test-bronze")
    class Response:
        def __init__(self, code, body):
            self.status_code, self.content = code, body
            self.headers = {"Content-Type": "application/json", "Retry-After": "0"}
    class Session:
        calls = []
        def get(self, url, **kwargs):
            self.calls.append((url, kwargs))
            return Response(429, b"rate limited") if len(self.calls) == 1 else Response(200, json.dumps([event()]).encode())
    session, waits = Session(), []
    c = FMPClient(api_key="unit-test-secret", session=session, sleeper=waits.append)
    result = m.run(date(2015, 2, 1), date(2015, 2, 28), client=c, dest="s3")
    assert result["manifest_uri"].startswith("s3://test-bronze/macro/")
    assert len(objects) == 5 and waits == [0.0]
    assert all(b"unit-test-secret" not in b for b in objects.values())
    assert session.calls[0][1]["headers"]["apikey"] == "unit-test-secret"
    assert "unit-test-secret" not in session.calls[0][0]
    m.run(date(2015, 2, 1), date(2015, 2, 28), client=c, dest="s3")
    assert len(session.calls) == 2


@pytest.mark.parametrize("kwargs", [{"start": date(2014, 1, 1)}, {"end": date(2014, 1, 1)},
    {"end": date(2099, 1, 1)}, {"snapshot": "../escape"}, {"dest": "typo"}])
def test_bad_args_do_not_call_api_or_sink(monkeypatch, kwargs):
    monkeypatch.setattr(m, "base_uri", lambda d: pytest.fail("must validate first"))
    with pytest.raises(ValueError):
        m.run(**{"start": date(2015, 1, 1), "end": date(2015, 2, 1), **kwargs})


def test_daily_uses_korean_processing_day_and_overlap(monkeypatch):
    calls = []
    monkeypatch.setattr(m, "run", lambda *a, **kw: calls.append((a, kw)))
    m.run_daily("20260919")
    args, kwargs = calls[0]
    assert args == (date(2026, 6, 18), date(2026, 9, 18))
    assert kwargs == {"dest": "s3", "snapshot": "daily-20260919"}


def test_daily_collection_runs_even_when_equities_already_certified(monkeypatch):
    from pipeline import daily_full as d
    calls = []
    monkeypatch.setattr(d.dart_silver_backfill_ecs, "assert_daily_certification_lock", lambda c: None)
    monkeypatch.setattr(d.repository, "certified_target_exists", lambda *a: True)
    monkeypatch.setattr(d.fmp_macro, "run_daily", lambda day: calls.append(("macro", day)))
    monkeypatch.setattr(d.fmp_external, "run_daily", lambda day: calls.append(("external", day)))
    monkeypatch.setattr(d.fmp_regime, "run_daily", lambda day: calls.append(("market", day)))
    d._run_fmp_incremental("bucket", None, "20260919", certification_lock=object())
    assert calls == [("macro", "20260919"), ("external", "20260919"), ("market", "20260918")]


def test_daily_macro_failure_propagates(monkeypatch):
    from pipeline import daily_full as d
    monkeypatch.setattr(d.dart_silver_backfill_ecs, "assert_daily_certification_lock", lambda c: None)
    def fail(day):
        raise FMPError("source failed")
    monkeypatch.setattr(d.fmp_macro, "run_daily", fail)
    monkeypatch.setattr(d.fmp_regime, "run_daily", lambda day: pytest.fail("must stop"))
    with pytest.raises(FMPError, match="source failed"):
        d._run_fmp_incremental("bucket", None, "20260919", certification_lock=object())
