import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

from pipeline.bronze import (
    dart_company_profiles,
    dart_full_statements,
    dart_ownership,
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


def test_full_statement_bootstrap_selects_recent_pending_scopes(monkeypatch):
    scopes = [
        ("005930", 2024, "11011", "CFS"),
        ("000660", 2026, "11012", "CFS"),
        ("035420", 2025, "11014", "OFS"),
    ]
    monkeypatch.setattr(
        dart_full_statements, "discover_scopes", lambda *_args: scopes,
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
