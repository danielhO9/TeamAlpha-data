import hashlib
import json
import threading
from pathlib import Path

import pandas as pd
import pytest

from pipeline.bronze import (
    dart_company_profiles,
    dart_full_statements,
    dart_ownership,
    kis_market_flows,
    krx_investor_flows,
    krx_short_balances,
)


def test_full_statement_scope_discovery_uses_only_real_major_account_scopes(
    tmp_path: Path,
):
    path = (
        tmp_path / "financials/dart/year=2025/corp=005930/11011.json"
    )
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps([
        {"fs_div": "CFS"},
        {"fs_div": "OFS"},
        {"fs_div": "CFS"},
        {"fs_div": "UNKNOWN"},
    ]), encoding="utf-8")

    assert dart_full_statements.discover_scopes(
        str(tmp_path), 2025, 2025,
    ) == [
        ("005930", 2025, "11011", "CFS"),
        ("005930", 2025, "11011", "OFS"),
    ]


def test_full_statement_run_promotes_legacy_bronze_without_dart_calls(
    tmp_path: Path, monkeypatch,
):
    major_path = (
        tmp_path / "financials/dart/year=2025/corp=005930/11011.json"
    )
    major_path.parent.mkdir(parents=True)
    major_path.write_text(json.dumps([{"fs_div": "CFS"}]), encoding="utf-8")

    rows = [{
        "stock_code": "005930",
        "bsns_year": "2025",
        "reprt_code": "11011",
        "fs_div": "CFS",
        "rcept_no": "20260331000001",
        "sj_div": "BS",
        "account_id": "ifrs-full:Assets",
        "thstrm_amount": "100",
    }]
    raw = json.dumps(rows, ensure_ascii=False).encode("utf-8")
    legacy_path = (
        tmp_path / "financials/dart_full/year=2025/corp=005930/11011-CFS.json"
    )
    legacy_path.parent.mkdir(parents=True)
    legacy_path.write_bytes(raw)

    monkeypatch.setattr(dart_full_statements, "base_uri", lambda _dest: str(tmp_path))
    monkeypatch.setattr(
        dart_full_statements.financials,
        "ensure_corp_code_xml",
        lambda _base: pytest.fail("corp-code lookup must not run"),
    )
    monkeypatch.setattr(
        dart_full_statements,
        "_request_scope",
        lambda *_args, **_kwargs: pytest.fail("OpenDART API must not run"),
    )

    responses = dart_full_statements.run(2025, 2025, "local")

    digest = hashlib.sha256(raw).hexdigest()
    expected = (
        tmp_path / "financials/dart_statement_lines/year=2025/corp=005930/"
        f"report=11011/fs_type=CFS/sha256={digest}/response.json"
    )
    assert responses == [str(expected)]
    assert expected.read_bytes() == raw
    pointer = json.loads((expected.parents[1] / "latest.json").read_text())
    assert pointer["response_uri"] == str(expected)
    assert pointer["source_uri"] == str(legacy_path)
    assert pointer["source_format"] == "legacy-financials-dart-full-list-v1"


def test_full_statement_does_not_promote_mismatched_legacy_scope(tmp_path: Path):
    legacy_path = (
        tmp_path / "financials/dart_full/year=2025/corp=005930/11011-CFS.json"
    )
    legacy_path.parent.mkdir(parents=True)
    legacy_path.write_text(json.dumps([{
        "stock_code": "005930",
        "bsns_year": "2024",
        "reprt_code": "11011",
        "fs_div": "CFS",
    }]), encoding="utf-8")

    assert dart_full_statements._promote_legacy_response(
        str(tmp_path), "005930", 2025, "11011", "CFS",
    ) is None


def test_full_statement_incremental_discovers_only_explicit_changed_files(
    tmp_path: Path, monkeypatch,
):
    changed = tmp_path / "financials/dart/year=2026/corp=005930/11012.json"
    changed.parent.mkdir(parents=True)
    changed.write_text(json.dumps([{"fs_div": "CFS"}]), encoding="utf-8")
    unrelated = tmp_path / "financials/dart/year=2026/corp=000660/11012.json"
    unrelated.parent.mkdir(parents=True)
    unrelated.write_text(json.dumps([{"fs_div": "OFS"}]), encoding="utf-8")

    assert dart_full_statements.discover_scopes_from_files([str(changed)]) == [
        ("005930", 2026, "11012", "CFS"),
    ]


def test_full_statement_bootstrap_selects_recent_pending_scopes(
    monkeypatch, tmp_path: Path,
):
    scopes = [
        ("005930", 2024, "11011", "CFS"),
        ("000660", 2026, "11012", "CFS"),
        ("035420", 2025, "11014", "OFS"),
    ]
    monkeypatch.setattr(
        dart_full_statements, "discover_scopes", lambda *_args: scopes,
    )
    monkeypatch.setattr(
        dart_full_statements, "base_uri", lambda _dest: str(tmp_path),
    )
    monkeypatch.setattr(dart_full_statements, "_s3_inventory", lambda _base: set())
    captured = {}

    def collect(base, selected, **kwargs):
        captured["scopes"] = selected
        return ["one.json", "two.json"]

    monkeypatch.setattr(dart_full_statements, "_collect_scopes", collect)
    files, remaining = dart_full_statements.run_bootstrap_batch(
        2015, 2026, "local", max_scopes=2,
    )

    assert captured["scopes"] == [scopes[1], scopes[2]]
    assert files == ["one.json", "two.json"]
    assert remaining == 1
    state = json.loads((
        tmp_path / dart_full_statements.BOOTSTRAP_STATE_KEY
    ).read_text())
    assert state["status"] == "COLLECTED"

    dart_full_statements.mark_bootstrap_batch_certified("local", files)
    state = json.loads((
        tmp_path / dart_full_statements.BOOTSTRAP_STATE_KEY
    ).read_text())
    assert state["status"] == "CERTIFIED"


def test_full_statement_bootstrap_resumes_unpublished_batch(
    monkeypatch, tmp_path: Path,
):
    selected = [["005930", 2026, "11012", "CFS"]]
    state_path = tmp_path / dart_full_statements.BOOTSTRAP_STATE_KEY
    state_path.parent.mkdir(parents=True)
    state_path.write_text(json.dumps({
        "schema_version": dart_full_statements.BOOTSTRAP_STATE_SCHEMA,
        "status": "COLLECTING",
        "batch_id": "batch",
        "from_year": 2015,
        "to_year": 2026,
        "scopes": selected,
        "remaining_scopes": 12,
    }))
    monkeypatch.setattr(
        dart_full_statements, "base_uri", lambda _dest: str(tmp_path),
    )
    monkeypatch.setattr(
        dart_full_statements, "discover_scopes",
        lambda *_args: pytest.fail("must resume before discovering a new batch"),
    )
    monkeypatch.setattr(dart_full_statements, "_s3_inventory", lambda _base: set())
    captured = {}

    def collect(_base, scopes, **_kwargs):
        captured["scopes"] = scopes
        return ["response.json"]

    monkeypatch.setattr(dart_full_statements, "_collect_scopes", collect)
    files, remaining = dart_full_statements.run_bootstrap_batch(
        2015, 2026, "local", max_scopes=9000,
    )

    assert captured["scopes"] == [tuple(selected[0])]
    assert files == ["response.json"]
    assert remaining == 12
    assert json.loads(state_path.read_text())["status"] == "COLLECTED"


def test_full_statement_requests_overlap_with_bounded_workers(
    monkeypatch, tmp_path: Path,
):
    scopes = [
        (f"{index:06d}", 2026, "11012", "CFS") for index in range(3)
    ]
    monkeypatch.setenv("DART_FULL_STATEMENT_WORKERS", "3")
    monkeypatch.setattr(dart_full_statements.financials, "CALL_GAP_SEC", 0)
    monkeypatch.setattr(
        dart_full_statements.financials,
        "ensure_corp_code_xml",
        lambda _base: [(f"corp-{index}", f"{index:06d}") for index in range(3)],
    )
    barrier = threading.Barrier(3)
    thread_ids: set[int] = set()

    def request(corp_code, *_args, before_request=None, **_kwargs):
        if before_request:
            before_request()
        thread_ids.add(threading.get_ident())
        barrier.wait(timeout=2)
        body = json.dumps({"list": []}).encode()
        return body, {"status": "000", "list": []}

    monkeypatch.setattr(dart_full_statements, "_request_scope", request)
    responses = dart_full_statements._collect_scopes(
        str(tmp_path), scopes, dest="local", refresh_existing=False,
        changed_only=False, known_objects=set(),
    )

    assert len(responses) == 3
    assert len(thread_ids) == 3


def test_ownership_snapshot_fetches_every_page(monkeypatch):
    calls: list[int] = []

    def request(_name, _url, _corp, *, page_no=1, tries=4):
        calls.append(page_no)
        return b"raw", {
            "status": "000",
            "page_no": page_no,
            "total_page": 2,
            "list": [{"rcept_no": str(page_no)}],
        }

    monkeypatch.setattr(dart_ownership, "_request", request)
    monkeypatch.setattr(dart_ownership.time, "sleep", lambda _seconds: None)
    body, payload = dart_ownership._request_all("api", "url", "corp")

    assert calls == [1, 2]
    assert payload["list"] == [{"rcept_no": "1"}, {"rcept_no": "2"}]
    assert json.loads(body)["total_count"] == 2


def test_ownership_incremental_routes_disclosure_to_one_endpoint(
    monkeypatch, tmp_path: Path,
):
    monkeypatch.setattr(dart_ownership, "base_uri", lambda _dest: str(tmp_path))
    monkeypatch.setattr(
        dart_ownership.financials,
        "_incremental_disclosure_days",
        lambda _day: ["20260901"],
    )
    monkeypatch.setattr(
        dart_ownership,
        "_disclosures",
        lambda _base, _day: [{
            "corp_code": "00126380",
            "report_nm": "주식등의대량보유상황보고서",
        }],
    )
    calls: list[dict] = []

    def run(_dest, **kwargs):
        calls.append(kwargs)
        return ["changed.json"]

    monkeypatch.setattr(dart_ownership, "run", run)
    assert dart_ownership.run_incremental("20260901", "local") == ["changed.json"]
    assert calls == [{
        "refresh_existing": True,
        "disclosure_types": ("FIVE_PERCENT",),
        "corp_codes": {"00126380"},
        "changed_only": True,
    }]


def test_company_profile_incremental_refreshes_one_shard(monkeypatch, tmp_path: Path):
    corps = [(f"corp-{index}", f"{index:06d}") for index in range(40)]
    monkeypatch.setattr(dart_company_profiles, "base_uri", lambda _dest: str(tmp_path))
    monkeypatch.setattr(
        dart_company_profiles.financials,
        "ensure_corp_code_xml",
        lambda _base: corps,
    )
    captured: dict = {}

    def run(_dest, **kwargs):
        captured.update(kwargs)
        return []

    monkeypatch.setattr(dart_company_profiles, "run", run)
    dart_company_profiles.run_incremental("20260901", "local", shard_count=20)
    assert captured["refresh_existing"] is True
    assert captured["changed_only"] is True
    assert len(captured["corp_codes"]) == 2


def test_krx_export_requires_provenance_and_flow_columns(tmp_path: Path):
    valid = pd.DataFrame([{
        "일자": "2026-08-31",
        "종목코드": "005930",
        "시장": "코스피",
        "투자자구분": "외국인",
        "매도거래량": 10,
        "매수거래량": 12,
    }]).to_csv(index=False).encode("utf-8")
    shape = krx_investor_flows.validate_export(valid, ".csv")
    assert shape["row_count"] == 1

    path = tmp_path / "flow.csv"
    path.write_bytes(valid)
    with pytest.raises(ValueError, match="authorization_id"):
        krx_investor_flows.ingest(
            str(path), "local", authorization_id="",
        )

    missing_flow = pd.DataFrame([{
        "일자": "2026-08-31",
        "종목코드": "005930",
        "시장": "코스피",
        "투자자구분": "외국인",
    }]).to_csv(index=False).encode("utf-8")
    with pytest.raises(ValueError, match="volume_or_value_fields"):
        krx_investor_flows.validate_export(missing_flow, ".csv")


@pytest.mark.parametrize(
    "module",
    [dart_full_statements, dart_ownership, dart_company_profiles],
)
def test_dart_collectors_fail_before_request_without_api_key(
    monkeypatch, module,
):
    monkeypatch.setenv("DART_API_KEY", "  ")
    with pytest.raises(RuntimeError, match="DART_API_KEY is required"):
        module._api_key()


def test_short_balance_export_requires_market_and_balance_fields():
    raw = pd.DataFrame([{
        "일자": "2026-08-31",
        "종목코드": "005930",
        "시장": "코스피",
        "공매도순보유잔고수량": 100,
        "공매도순보유잔고비중": 0.01,
    }]).to_csv(index=False).encode("utf-8")

    assert krx_short_balances.validate_export(raw, ".csv")["row_count"] == 1

    missing_market = pd.DataFrame([{
        "일자": "2026-08-31",
        "종목코드": "005930",
        "공매도순보유잔고수량": 100,
    }]).to_csv(index=False).encode("utf-8")
    with pytest.raises(ValueError, match="market"):
        krx_short_balances.validate_export(missing_market, ".csv")


class _KisResponse:
    def __init__(
        self, payload: dict, *, headers: dict | None = None, status_code: int = 200,
    ):
        self._payload = payload
        self.content = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        self.status_code = status_code
        self.headers = headers or {"content-type": "application/json"}

    def json(self):
        return self._payload


class _KisSession:
    def __init__(self, payload: dict):
        self.payload = payload
        self.calls = []

    def get(self, url, *, headers, params, timeout):
        self.calls.append((url, headers, params, timeout))
        return _KisResponse(self.payload, headers={"tr_cont": ""})


class _KisSequenceSession:
    def __init__(self, responses):
        self.responses = iter(responses)
        self.calls = []

    def get(self, url, *, headers, params, timeout):
        self.calls.append((url, headers, params, timeout))
        return next(self.responses)


def test_kis_investor_flow_preserves_real_response_and_provenance(
    tmp_path: Path, monkeypatch,
):
    monkeypatch.setenv("KIS_APP_KEY", "test-app-key")
    monkeypatch.setenv("KIS_APP_SECRET", "test-app-secret")
    monkeypatch.setattr(kis_market_flows, "base_uri", lambda _: str(tmp_path))
    payload = {
        "rt_cd": "0",
        "msg_cd": "MCA00000",
        "output1": {},
        "output2": [{
            "stck_bsop_date": "20250812",
            "frgn_ntby_qty": "123",
            "orgn_ntby_qty": "-45",
            "prsn_ntby_qty": "-78",
        }],
    }
    session = _KisSession(payload)

    result = kis_market_flows.collect_investor_flow(
        "005930", "20250812", "local",
        session=session, access_token="test-token",
    )

    response_path = Path(result["uri"])
    manifest = json.loads(response_path.with_name("manifest.json").read_text())
    assert response_path.read_bytes() == _KisResponse(payload).content
    assert manifest["source"] == "KIS_SECURITIES_OPEN_API"
    assert manifest["coverage"]["row_count"] == 1
    assert manifest["coverage"]["asset_count"] == 1
    assert manifest["coverage"]["date_min"] == "20250812"
    assert manifest["coverage"]["categories"] == [
        "foreign", "individual", "institution_total",
    ]
    assert manifest["pagination"]["response_complete"] is True
    assert manifest["availability_contract"]["row_level_available_at"] == (
        "not_supplied_by_endpoint"
    )
    assert "test-app-key" not in json.dumps(manifest)
    assert "test-app-secret" not in json.dumps(manifest)
    assert "test-token" not in json.dumps(manifest)
    assert session.calls[0][2]["FID_INPUT_ISCD"] == "005930"
    frozen_manifest = response_path.with_name("manifest.json").read_bytes()
    repeated = kis_market_flows.collect_investor_flow(
        "005930", "20250812", "local",
        session=session, access_token="test-token",
    )
    assert repeated["uri"] == result["uri"]
    assert response_path.with_name("manifest.json").read_bytes() == frozen_manifest


def test_kis_short_sale_is_distinct_from_short_balance(
    tmp_path: Path, monkeypatch,
):
    monkeypatch.setenv("KIS_APP_KEY", "test-app-key")
    monkeypatch.setenv("KIS_APP_SECRET", "test-app-secret")
    monkeypatch.setattr(kis_market_flows, "base_uri", lambda _: str(tmp_path))
    payload = {
        "rt_cd": "0",
        "msg_cd": "MCA00000",
        "output1": {},
        "output2": [{
            "stck_bsop_date": "20240328",
            "ssts_cntg_qty": "100",
            "ssts_vol_rlim": "1.25",
            "ssts_tr_pbmn": "7654321",
        }],
    }

    result = kis_market_flows.collect_short_sale(
        "005930", "20240301", "20240328", "local",
        session=_KisSession(payload), access_token="test-token",
    )

    manifest = json.loads(Path(result["uri"]).with_name("manifest.json").read_text())
    assert manifest["dataset"] == "short-sale"
    assert manifest["units"]["ratio"] == "percent"
    assert "not short balance" in manifest["semantic_note"]


def test_kis_collector_fails_closed_without_credentials(monkeypatch):
    monkeypatch.delenv("KIS_APP_KEY", raising=False)
    monkeypatch.delenv("KIS_APP_SECRET", raising=False)
    with pytest.raises(RuntimeError, match="KIS_APP_KEY is required"):
        kis_market_flows.issue_token(_KisSession({}))


def test_kis_short_sale_run_reuses_one_token(monkeypatch):
    calls = []
    monkeypatch.setattr(kis_market_flows, "issue_token", lambda _: "one-token")
    monkeypatch.setattr(
        kis_market_flows,
        "collect_short_sale",
        lambda ticker, from_date, to_date, dest, *, session, access_token: (
            calls.append((ticker, from_date, to_date, dest, access_token))
            or {"ticker": ticker}
        ),
    )

    results = kis_market_flows.run(
        dataset="short-sale",
        tickers=["005930"],
        dest="local",
        from_date="20240301",
        to_date="20240328",
    )

    assert results == [{"ticker": "005930"}]
    assert calls == [("005930", "20240301", "20240328", "local", "one-token")]


def test_kis_request_retries_documented_rate_limit_without_leaking_secret(
    monkeypatch,
):
    monkeypatch.setenv("KIS_APP_KEY", "test-app-key")
    monkeypatch.setenv("KIS_APP_SECRET", "test-app-secret")
    sleeps = []
    monkeypatch.setattr(kis_market_flows.time, "sleep", sleeps.append)
    session = _KisSequenceSession([
        _KisResponse(
            {"rt_cd": "1", "msg_cd": "EGW00201", "msg1": "rate limit"},
            status_code=500,
        ),
        _KisResponse({"rt_cd": "0", "output2": []}),
    ])

    raw, payload, _ = kis_market_flows._request(
        session=session,
        access_token="test-token",
        path=kis_market_flows.SHORT_SALE_PATH,
        tr_id=kis_market_flows.SHORT_SALE_TR_ID,
        params={"FID_INPUT_ISCD": "005930"},
    )

    assert payload["rt_cd"] == "0"
    assert json.loads(raw)["rt_cd"] == "0"
    assert len(session.calls) == 2
    assert sleeps == [kis_market_flows.REQUEST_INTERVAL_SECONDS]
