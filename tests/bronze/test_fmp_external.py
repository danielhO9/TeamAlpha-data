from datetime import date, datetime, timezone
import json
from pathlib import Path

import pytest

from pipeline.bronze import fmp_external as m
from pipeline.bronze.fmp import FMPError, RawResponse

RECEIVED = "2026-09-19T00:00:00+00:00"
START, END = date(2015, 2, 1), date(2015, 2, 28)


def row(symbol="USDCNH", day="2015-02-03"):
    data = {"symbol": symbol, "date": day}
    spec = {**m.SERIES, **m.RISK_SERIES}[symbol]
    if spec["kind"] == "cot":
        data.update(date=day + " 00:00:00", cftcContractMarketCode=spec["cftc_code"])
        data.update({f: 20 for f in m.COT_FIELDS})
    else:
        data.update(open=3, high=4, low=2, close=3.5, volume=0)
    return data


class Client:
    def __init__(self):
        self.calls = []

    def get(self, endpoint, params):
        self.calls.append((endpoint, params))
        rows = [row(params["symbol"], params["from"])]
        return RawResponse(endpoint, params, 200, "application/json",
                           datetime.fromisoformat(RECEIVED), json.dumps(rows, indent=2).encode() + b"\n")


def local(tmp_path, monkeypatch, client, **kwargs):
    monkeypatch.setattr(m, "base_uri", lambda dest: str(tmp_path))
    return m.run(START, END, client=client, snapshot="test", **kwargs)


def test_scope_and_priority():
    assert list(m.SERIES) == ["USDCNH", "USDCNY", "USDJPY", "AUDUSD", "EURUSD",
                              "HG", "CL", "DX", "J6", "GC", "VX", "EWY", "EEM", "FXI"]
    assert all(value is False for value in m.PIT.values())


@pytest.mark.parametrize("symbol", list(m.SERIES))
def test_never_backdates_or_guesses_historical_availability(symbol):
    data = row(symbol)
    selected, stats = m.project(json.dumps([data]).encode(), symbol, START, END, RECEIVED)
    got = selected[0]
    assert got["payload"] == data and got["source_row_index"] == 0
    assert got["provider_date"] == data["date"]
    assert got["observed_at"] == got["system_known_at"] == RECEIVED
    assert got["available_at"] is None and got["released_at"] is None and got["vintage"] is None
    assert all(got[k] is False for k in m.PIT)
    assert got["reference_date"] == ("2015-02-03" if m.SERIES[symbol]["kind"] == "cot" else None)
    assert stats["row_count"] == 1


def test_preserves_null_weekend_and_invalid_ohlc_without_cleaning():
    rows = [row(day="2015-02-01"), row(day="2015-02-02")]
    rows[0]["close"] = None
    rows[1]["low"] = 8
    result, stats = m.project(json.dumps(rows).encode(), "USDCNH", START, END, RECEIVED)
    assert [x["payload"] for x in result] == rows
    assert stats["quality_flag_counts"] == {"null_close": 1, "provider_weekend_date": 1, "invalid_ohlc": 1}


@pytest.mark.parametrize("bad", [b"{}", b"not json", b"[null]", b"[]",
    json.dumps([row("EWY")]).encode(),
    json.dumps([row(day="2015-03-01")]).encode(),
    json.dumps([row(), row()]).encode(),
    json.dumps([{**row(), "close": True}]).encode(),
    json.dumps([{**row(), "close": float("nan")}]).encode()])
def test_bad_response_never_completes(bad):
    with pytest.raises(FMPError):
        m.project(bad, "USDCNH", START, END, RECEIVED)


def test_cot_contract_identity_and_monday_date():
    r = row("HG", "2015-02-02")
    result, _ = m.project(json.dumps([r]).encode(), "HG", START, END, RECEIVED)
    assert result[0]["reference_date"] == "2015-02-02"
    r["cftcContractMarketCode"] = "WRONG"
    with pytest.raises(FMPError, match="contract mismatch"):
        m.project(json.dumps([r]).encode(), "HG", START, END, RECEIVED)


def test_raw_byte_evidence_idempotence_and_cache_promotion(tmp_path, monkeypatch):
    src, dst = tmp_path / "src", tmp_path / "dst"
    c = Client()
    result = local(src, monkeypatch, c)
    assert result["row_count"] == 14 and len(c.calls) == 14
    original = {p: p.read_bytes() for p in src.rglob("*.json")}
    assert local(src, monkeypatch, c) == result and len(c.calls) == 14
    assert all(p.read_bytes() == body for p, body in original.items())
    promoted = local(dst, monkeypatch, c, cache_root=str(src))
    assert promoted["row_count"] == result["row_count"] and len(c.calls) == 14
    for part in promoted["partitions"]:
        manifest = json.loads(Path(part["manifest_uri"]).read_bytes())
        raw = Path(manifest["source_object_uri"]).read_bytes()
        raw_meta = json.loads(Path(manifest["source_object_uri"]).with_name("manifest.json").read_bytes())
        assert raw_meta["received_at"] == RECEIVED
        assert raw_meta["copied_from"]["object_uri"].startswith(str(src))
        assert raw == Path(raw_meta["copied_from"]["object_uri"]).read_bytes()
        assert raw.endswith(b"\n")
        assert manifest["silver_publish_allowed"] is False


@pytest.mark.parametrize("kind", ["raw", "observations", "raw_manifest", "manifest"])
def test_corruption_is_not_overwritten(tmp_path, monkeypatch, kind):
    c = Client()
    result = local(tmp_path, monkeypatch, c)
    meta_path = Path(result["partitions"][0]["manifest_uri"])
    meta = json.loads(meta_path.read_bytes())
    target = {"raw": Path(meta["source_object_uri"]), "observations": Path(meta["object_uri"]),
              "raw_manifest": Path(meta["source_object_uri"]).with_name("manifest.json"),
              "manifest": meta_path}[kind]
    target.write_bytes(b"corrupt")
    with pytest.raises(FMPError):
        local(tmp_path, monkeypatch, c)
    assert target.read_bytes() == b"corrupt" and len(c.calls) == 14


@pytest.mark.parametrize("field,value", [("received_at", "2015-01-01"),
    ("received_at", "2099-01-01T00:00:00+00:00"), ("endpoint", "wrong"),
    ("request_params", {}), ("status_code", 429)])
def test_receipt_provenance_mismatch_fails(tmp_path, monkeypatch, field, value):
    c = Client()
    result = local(tmp_path, monkeypatch, c)
    meta = json.loads(Path(result["partitions"][0]["manifest_uri"]).read_bytes())
    p = Path(meta["source_object_uri"]).with_name("manifest.json")
    raw_meta = json.loads(p.read_bytes())
    raw_meta[field] = value
    p.write_bytes(json.dumps(raw_meta).encode())
    with pytest.raises(FMPError, match="receipt corrupt"):
        local(tmp_path, monkeypatch, c)
    assert len(c.calls) == 14


def test_resume_after_projection_manifest_interruption(tmp_path, monkeypatch):
    c = Client()
    result = local(tmp_path, monkeypatch, c)
    p = Path(result["partitions"][0]["manifest_uri"])
    old = p.read_bytes()
    p.unlink()
    local(tmp_path, monkeypatch, c)
    assert p.read_bytes() == old and len(c.calls) == 14


def test_no_manifest_when_missing_full_month(tmp_path, monkeypatch):
    monkeypatch.setattr(m, "base_uri", lambda dest: str(tmp_path))
    with pytest.raises(FMPError, match="empty complete calendar month"):
        m.run(date(2015, 1, 1), date(2015, 3, 31), client=Client())
    assert list(tmp_path.rglob("response.json"))
    assert not list(tmp_path.glob("**/runs/**/manifest.json"))


def test_year_partition_and_daily_overlap(monkeypatch):
    assert list(m.year_windows(date(2015, 12, 1), date(2016, 1, 15))) == [
        (date(2015, 12, 1), date(2015, 12, 31)), (date(2016, 1, 1), date(2016, 1, 15))]
    calls = []
    monkeypatch.setattr(m, "run", lambda *a, **kw: calls.append((a, kw)))
    m.run_daily("20260920")
    assert calls == [((date(2026, 5, 23), date(2026, 9, 19)),
                      {"dest": "s3", "snapshot": "daily-20260920", "bundle": bundle})
                     for bundle in ("core", "risk")]


@pytest.mark.parametrize("kw", [{"start": date(2014, 1, 1)}, {"end": date(2014, 1, 1)},
    {"end": date(2099, 1, 1)}, {"snapshot": "../x"}, {"dest": "typo"}, {"bundle": "typo"}])
def test_bad_args_fail_before_io(monkeypatch, kw):
    monkeypatch.setattr(m, "base_uri", lambda dest: pytest.fail("I/O forbidden"))
    with pytest.raises(ValueError):
        m.run(**{"start": START, "end": END, **kw})


def test_cached_raw_promotes_to_s3_without_api_or_backdated_receipt(tmp_path, monkeypatch):
    import io
    import boto3
    from botocore.exceptions import ClientError
    c = Client()
    local(tmp_path, monkeypatch, c)
    objects = {}
    class S3:
        def get_object(self, Bucket, Key):
            if Key not in objects:
                raise ClientError({"Error": {"Code": "NoSuchKey"}}, "GetObject")
            return {"Body": io.BytesIO(objects[Key])}
        def put_object(self, Bucket, Key, Body):
            objects[Key] = Body
    monkeypatch.setattr(boto3, "client", lambda service: S3())
    monkeypatch.setattr(m, "base_uri", lambda dest: "s3://test-bronze")
    r = m.run(START, END, snapshot="test", dest="s3", cache_root=str(tmp_path), client=c)
    assert len(c.calls) == 14 and len(objects) == 57
    assert r["row_count"] == 14 and r["historical_backtest_allowed"] is False
    for key, body in objects.items():
        if key.endswith("/observations.json"):
            for observation in json.loads(body):
                assert observation["available_at"] is None
                assert observation["system_known_at"] == RECEIVED
        if key.endswith("/raw/manifest.json"):
            assert json.loads(body)["received_at"] == RECEIVED
    original = dict(objects)
    m.run(START, END, snapshot="test", dest="s3", cache_root=str(tmp_path), client=c)
    assert objects == original and len(c.calls) == 14


def test_orphan_raw_is_not_overwritten(tmp_path, monkeypatch):
    c = Client()
    r = local(tmp_path, monkeypatch, c)
    meta = json.loads(Path(r["partitions"][0]["manifest_uri"]).read_bytes())
    raw_path = Path(meta["source_object_uri"])
    before = raw_path.read_bytes()
    raw_path.with_name("manifest.json").unlink()
    with pytest.raises(FMPError, match="orphan"):
        local(tmp_path, monkeypatch, c)
    assert raw_path.read_bytes() == before and len(c.calls) == 14


def test_daily_external_failure_stops_publication(monkeypatch):
    from pipeline import daily_full as d
    monkeypatch.setattr(d.dart_silver_backfill_ecs, "assert_daily_certification_lock", lambda c: None)
    monkeypatch.setattr(d.fmp_macro, "run_daily", lambda day: None)
    def fail(day):
        raise FMPError("external failed")
    monkeypatch.setattr(d.fmp_external, "run_daily", fail)
    monkeypatch.setattr(d.fmp_regime, "run_daily", lambda day: pytest.fail("must stop"))
    with pytest.raises(FMPError, match="external failed"):
        d._run_fmp_incremental("bucket", None, "20260920", certification_lock=object())


def test_risk_scope_is_separate_from_core():
    assert list(m.RISK_SERIES) == ["EWT", "USDTWD", "^VVIX", "^VIX3M", "^VIX9D",
                                  "ZT", "ZN", "ZB", "ZQ"]
    assert not set(m.RISK_SERIES).intersection(m.SERIES)
    assert m.contract()["contract_version"] == "korea-external-v1"
    assert m.contract("risk")["contract_version"] == "korea-risk-v1"
    assert m.contract("risk")["scope"] == m.RISK_SERIES


@pytest.mark.parametrize("symbol", list(m.RISK_SERIES))
def test_risk_never_fabricates_release_or_research_availability(symbol):
    data = row(symbol)
    selected, stats = m.project(json.dumps([data]).encode(), symbol, START, END,
                                RECEIVED, bundle="risk")
    got = selected[0]
    assert got["payload"] == data
    assert got["observed_at"] == got["system_known_at"] == RECEIVED
    assert all(got[k] is None for k in ("released_at", "available_at", "vintage"))
    assert all(got[k] is False for k in m.PIT)
    assert got["reference_date"] == ("2015-02-03" if m.RISK_SERIES[symbol]["kind"] == "cot" else None)
    assert stats["row_count"] == 1


@pytest.mark.parametrize("symbol", ["ZT", "ZN", "ZB", "ZQ"])
def test_rates_cot_identity_is_checked(symbol):
    data = row(symbol)
    data["cftcContractMarketCode"] = "WRONG"
    with pytest.raises(FMPError, match="contract mismatch"):
        m.project(json.dumps([data]).encode(), symbol, START, END, RECEIVED, bundle="risk")


def test_both_bundles_coexist_and_risk_cache_keeps_receipt(tmp_path, monkeypatch):
    c = Client()
    src, dst = tmp_path / "src", tmp_path / "dst"
    core = local(src, monkeypatch, c)
    core_objects = {p: p.read_bytes() for p in src.rglob("*.json")}
    risk = local(src, monkeypatch, c, bundle="risk")
    assert core["row_count"] == 14 and risk["row_count"] == 9
    assert len(c.calls) == 23
    assert all(p.read_bytes() == body for p, body in core_objects.items())
    assert local(src, monkeypatch, c, bundle="risk") == risk
    promoted = local(dst, monkeypatch, c, bundle="risk", cache_root=str(src))
    assert len(c.calls) == 23 and promoted["row_count"] == 9
    for part in promoted["partitions"]:
        meta = json.loads(Path(part["manifest_uri"]).read_bytes())
        assert meta["contract"]["contract_version"] == m.RISK_VERSION
        source = json.loads(Path(meta["source_object_uri"]).with_name("manifest.json").read_bytes())
        assert source["received_at"] == RECEIVED
        assert Path(source["object_uri"]).read_bytes() == Path(source["copied_from"]["object_uri"]).read_bytes()


def test_risk_daily_failure_propagates(monkeypatch):
    calls = []
    def run(*args, **kwargs):
        calls.append(kwargs["bundle"])
        if kwargs["bundle"] == "risk":
            raise FMPError("risk failed")
        return {}
    monkeypatch.setattr(m, "run", run)
    with pytest.raises(FMPError, match="risk failed"):
        m.run_daily("20260920")
    assert calls == ["core", "risk"]
