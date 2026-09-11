import json
from pathlib import Path

from pipeline.bronze import financials


def test_regular_report_scopes_include_corrections_and_both_quarters():
    assert financials._regular_report_scopes(
        "[기재정정]분기보고서 (2026.03)"
    ) == (2026, ("11013", "11014"))
    assert financials._regular_report_scopes(
        "사업보고서 (2025.12)"
    ) == (2025, ("11011",))
    assert financials._regular_report_scopes("주요사항보고서") is None


def test_monday_increment_includes_weekend_disclosure_dates():
    assert financials._incremental_disclosure_days("20260907") == [
        "20260905", "20260906", "20260907",
    ]
    assert financials._incremental_disclosure_days("20260908") == ["20260908"]


def test_regular_disclosure_checkpoint_prevents_repeat_list_call(
    tmp_path: Path, monkeypatch,
):
    calls: list[int] = []

    def fetch(_day: str, page_no: int):
        calls.append(page_no)
        return {
            "status": "000",
            "total_page": 1,
            "list": [{"rcept_no": "20260901000001"}],
        }

    monkeypatch.setattr(financials, "_fetch_regular_disclosure_page", fetch)
    first = financials._regular_disclosures(str(tmp_path), "20260901")
    second = financials._regular_disclosures(str(tmp_path), "20260901")

    assert first == second == [{"rcept_no": "20260901000001"}]
    assert calls == [1]


def test_incremental_financials_refresh_only_disclosed_listed_companies(
    tmp_path: Path, monkeypatch,
):
    monkeypatch.setattr(financials, "base_uri", lambda _dest: str(tmp_path))
    monkeypatch.setattr(
        financials,
        "ensure_corp_code_xml",
        lambda _base: [("00126380", "005930"), ("00999999", "000001")],
    )
    monkeypatch.setattr(
        financials,
        "_regular_disclosures",
        lambda _base, _day: [
            {
                "corp_code": "00126380",
                "report_nm": "[기재정정]사업보고서 (2025.12)",
            },
            {
                "corp_code": "88888888",
                "report_nm": "사업보고서 (2025.12)",
            },
        ],
    )
    calls: list[tuple[list[str], int, str]] = []

    def fetch(corp_codes: list[str], year: int, report_code: str):
        calls.append((corp_codes, year, report_code))
        return "000", {"list": [{
            "stock_code": "005930",
            "fs_div": "CFS",
            "account_nm": "자산총계",
            "reprt_code": report_code,
            "bsns_year": str(year),
        }]}

    monkeypatch.setattr(financials, "_fetch_multi", fetch)
    changed = financials.run_incremental("20260901", "s3")

    assert calls == [(["00126380"], 2025, "11011")]
    assert changed == [
        str(tmp_path / "financials/dart/year=2025/corp=005930/11011.json")
    ]
    assert json.loads(Path(changed[0]).read_text())[0]["stock_code"] == "005930"


def test_incremental_financials_make_no_financial_api_call_without_filings(
    tmp_path: Path, monkeypatch,
):
    monkeypatch.setattr(financials, "base_uri", lambda _dest: str(tmp_path))
    monkeypatch.setattr(
        financials, "ensure_corp_code_xml", lambda _base: [("00126380", "005930")],
    )
    monkeypatch.setattr(
        financials, "_regular_disclosures", lambda _base, _day: [],
    )
    monkeypatch.setattr(
        financials,
        "_fetch_multi",
        lambda *_args: (_ for _ in ()).throw(
            AssertionError("financial API must not be called")
        ),
    )

    assert financials.run_incremental("20260901", "s3") == []


def test_incremental_financials_defers_when_filed_report_is_not_available(
    tmp_path: Path, monkeypatch,
):
    monkeypatch.setattr(financials, "base_uri", lambda _dest: str(tmp_path))
    monkeypatch.setattr(
        financials, "ensure_corp_code_xml", lambda _base: [("00126380", "005930")],
    )
    monkeypatch.setattr(
        financials,
        "_regular_disclosures",
        lambda _base, _day: [{
            "corp_code": "00126380",
            "report_nm": "사업보고서 (2025.12)",
        }],
    )
    monkeypatch.setattr(
        financials, "_fetch_multi", lambda *_args: ("013", {"status": "013"}),
    )

    assert financials.run_incremental("20260901", "s3") == []
    deferred = json.loads(
        (tmp_path / financials.DEFERRED_REQUIREMENTS_PATH).read_text()
    )
    assert deferred == [{
        "corp_code": "00126380",
        "report_codes": ["11011"],
        "year": 2025,
    }]


def test_incremental_financials_retries_deferred_scope_on_a_later_day(
    tmp_path: Path, monkeypatch,
):
    monkeypatch.setattr(financials, "base_uri", lambda _dest: str(tmp_path))
    monkeypatch.setattr(
        financials, "ensure_corp_code_xml", lambda _base: [("00126380", "005930")],
    )
    deferred_path = tmp_path / financials.DEFERRED_REQUIREMENTS_PATH
    deferred_path.parent.mkdir(parents=True)
    deferred_path.write_text(json.dumps([{
        "corp_code": "00126380",
        "report_codes": ["11011"],
        "year": 2025,
    }]))
    monkeypatch.setattr(financials, "_regular_disclosures", lambda *_args: [])
    monkeypatch.setattr(
        financials,
        "_fetch_multi",
        lambda _corps, year, report: ("000", {"list": [{
            "stock_code": "005930",
            "fs_div": "CFS",
            "account_nm": "자산총계",
            "reprt_code": report,
            "bsns_year": str(year),
        }]}),
    )

    changed = financials.run_incremental("20260902", "s3")

    assert changed == [
        str(tmp_path / "financials/dart/year=2025/corp=005930/11011.json")
    ]
    assert json.loads(deferred_path.read_text()) == []
