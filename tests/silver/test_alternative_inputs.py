import hashlib
import json
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from pipeline.silver import (
    full_statements,
    industry_classifications,
    investor_flows,
    ownership,
    short_balances,
)


def _write_json(path: Path, payload) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return str(path)


def test_full_statement_transform_retains_every_statement_family(tmp_path: Path):
    path = (
        tmp_path
        / "financials/dart_statement_lines/year=2025/corp=005930/"
        "report=11011/fs_type=CFS/"
        f"sha256={'a' * 64}/response.json"
    )
    rows = []
    for order, statement_type in enumerate(("BS", "IS", "CIS", "CF", "SCE"), 1):
        rows.append({
            "rcept_no": "20260331000001",
            "reprt_code": "11011",
            "bsns_year": "2025",
            "fs_div": "CFS",
            "sj_div": statement_type,
            "account_id": f"ifrs-full:{statement_type}",
            "account_nm": statement_type,
            "thstrm_dt": "2025.01.01 ~ 2025.12.31",
            "thstrm_amount": f"{order},000",
            "currency": "KRW",
            "ord": str(order),
        })
    uri = _write_json(path, {"status": "000", "list": rows})

    frame, stats = full_statements.prepare(files=[uri])

    assert set(frame["statement_type"]) == {"BS", "IS", "CIS", "CF", "SCE"}
    assert set(frame["fs_type"]) == {"CFS"}
    assert set(frame["available_date"]) == {date(2026, 4, 1)}
    assert frame.loc[frame["statement_type"].eq("CF"), "current_amount"].item() == 4000
    assert stats == {
        "file_count": 1,
        "input_rows": 5,
        "transformed_rows": 5,
        "excluded_rows": 0,
        "rejected_rows": 0,
    }


@pytest.mark.parametrize(
    ("disclosure_type", "row", "expected"),
    [
        (
            "EXECUTIVE_MAJOR_SHAREHOLDER",
            {
                "corp_code": "00126380",
                "rcept_no": "20260831000001",
                "repror": "홍길동",
                "isu_exctv_rgist_at": "등기임원",
                "isu_exctv_ofcps": "대표이사",
                "isu_main_shrholdr": "-",
                "sp_stock_lmp_cnt": "1,000",
                "sp_stock_lmp_irds_cnt": "100",
                "sp_stock_lmp_rate": "0.01",
                "sp_stock_lmp_irds_rate": "0.001",
            },
            {"shares": 1000, "shares_change": 100},
        ),
        (
            "FIVE_PERCENT",
            {
                "corp_code": "00126380",
                "rcept_no": "20260831000002",
                "repror": "기관 A",
                "report_tp": "신규",
                "report_resn": "장내매수",
                "stkqy": "2,000",
                "stkqy_irds": "200",
                "stkrt": "5.5",
                "stkrt_irds": "0.5",
                "ctr_stkqy": "2,100",
                "ctr_stkrt": "5.7",
            },
            {"shares": 2000, "control_shares": 2100},
        ),
    ],
)
def test_ownership_transform_is_next_day_pit(
    tmp_path: Path, disclosure_type: str, row: dict, expected: dict,
):
    path = (
        tmp_path
        / f"ownership/dart/disclosure_type={disclosure_type}/corp=005930/"
        f"sha256={'b' * 64}/response.json"
    )
    uri = _write_json(path, {"status": "000", "list": [row]})

    frame, stats = ownership.prepare(files=[uri])

    assert stats["rejected_rows"] == 0
    assert frame.iloc[0]["natural_key"] == "00126380"
    assert frame.iloc[0]["available_date"] == date(2026, 9, 1)
    for column, value in expected.items():
        assert frame.iloc[0][column] == value


def _write_authorized_flow(tmp_path: Path, frame: pd.DataFrame) -> str:
    raw = frame.to_csv(index=False).encode("utf-8")
    digest = hashlib.sha256(raw).hexdigest()
    root = tmp_path / f"investor_flows/krx/sha256={digest}"
    root.mkdir(parents=True)
    source = root / "source.csv"
    source.write_bytes(raw)
    (root / "manifest.json").write_text(json.dumps({
        "schema_version": "krx-investor-flow-export-v1",
        "source": "KRX_DATA_MARKETPLACE_AUTHORIZED_EXPORT",
        "authorization_id": "contract-123",
        "sha256": digest,
    }), encoding="utf-8")
    return str(source)


def test_investor_flow_transform_computes_net_and_normalizes(tmp_path: Path):
    uri = _write_authorized_flow(tmp_path, pd.DataFrame([{
        "일자": "2026-08-31",
        "종목코드": "5930",
        "시장": "코스피",
        "투자자구분": "외국인",
        "매도거래량": "1,000",
        "매수거래량": "1,250",
    }]))

    frame, _ = investor_flows.prepare(files=[uri])

    assert frame.iloc[0]["natural_key"] == "005930"
    assert frame.iloc[0]["market"] == "KOSPI"
    assert frame.iloc[0]["investor_type"] == "FOREIGN"
    assert frame.iloc[0]["net_volume"] == 250


def test_investor_flow_rejects_nonmatching_arithmetic(tmp_path: Path):
    uri = _write_authorized_flow(tmp_path, pd.DataFrame([{
        "trade_date": "2026-08-31",
        "ticker": "005930",
        "market": "KOSPI",
        "investor_type": "FOREIGN",
        "sell_volume": 1000,
        "buy_volume": 1250,
        "net_volume": 249,
    }]))

    with pytest.raises(ValueError, match="arithmetic mismatch"):
        investor_flows.prepare(files=[uri])


def test_alternative_schema_has_all_pit_tables():
    migration = (
        Path(__file__).resolve().parents[2]
        / "pipeline/silver_quality/migrations/013_alternative_research_inputs.sql"
    ).read_text(encoding="utf-8")

    assert "CREATE TABLE IF NOT EXISTS fundamental_statement_line" in migration
    assert "statement_type IN ('BS', 'IS', 'CIS', 'CF', 'SCE')" in migration
    assert "fs_type IN ('CFS', 'OFS')" in migration
    assert "CREATE TABLE IF NOT EXISTS ownership_disclosure_event" in migration
    assert "CREATE TABLE IF NOT EXISTS investor_flow_daily" in migration
    assert "CHECK (available_date = filed + 1)" in migration

    migration_014 = (
        Path(__file__).resolve().parents[2]
        / "pipeline/silver_quality/migrations/014_industry_short_balance.sql"
    ).read_text(encoding="utf-8")
    assert "CREATE TABLE IF NOT EXISTS industry_classification_observation" in migration_014
    assert "CREATE TABLE IF NOT EXISTS short_position_balance_observation" in migration_014
    assert migration_014.count("CHECK (available_at = observed_at)") == 2


def test_industry_observation_is_not_backdated(tmp_path: Path):
    payload = {
        "status": "000",
        "corp_code": "00126380",
        "stock_code": "005930",
        "induty_code": "264",
    }
    raw = json.dumps(payload).encode("utf-8")
    digest = hashlib.sha256(raw).hexdigest()
    root = (
        tmp_path / "company_profiles/dart/corp=005930" / f"sha256={digest}"
        / "observed_at=2026-09-03T120000p0000"
    )
    root.mkdir(parents=True)
    response = root / "response.json"
    response.write_bytes(raw)
    (root / "manifest.json").write_text(json.dumps({
        "schema_version": "dart-company-profile-observation-v1",
        "sha256": digest,
        "observed_at": "2026-09-03T12:00:00+00:00",
    }), encoding="utf-8")

    frame, stats = industry_classifications.prepare(files=[str(response)])

    assert stats["rejected_rows"] == 0
    assert frame.iloc[0]["natural_key"] == "00126380"
    assert frame.iloc[0]["industry_code"] == "264"
    assert frame.iloc[0]["effective_from"] is None
    assert frame.iloc[0]["available_at"].isoformat() == "2026-09-03T12:00:00+00:00"


def test_short_balance_uses_first_observed_vintage(tmp_path: Path):
    source_frame = pd.DataFrame([{
        "일자": "2020-01-02",
        "종목코드": "005930",
        "시장": "코스피",
        "공매도순보유잔고수량": "1,000",
        "상장주식수": "10,000",
        "공매도순보유잔고금액": "50,000,000",
        "시가총액": "500,000,000",
        "공매도순보유잔고비중": "10.0%",
    }])
    raw = source_frame.to_csv(index=False).encode("utf-8")
    digest = hashlib.sha256(raw).hexdigest()
    root = tmp_path / f"short_balances/krx/sha256={digest}"
    root.mkdir(parents=True)
    source = root / "source.csv"
    source.write_bytes(raw)
    (root / "manifest.json").write_text(json.dumps({
        "schema_version": "krx-short-balance-export-v1",
        "source": "KRX_DATA_MARKETPLACE_AUTHORIZED_EXPORT",
        "authorization_id": "contract-456",
        "sha256": digest,
        "observed_at": "2026-09-03T12:00:00+00:00",
    }), encoding="utf-8")

    frame, _ = short_balances.prepare(files=[str(source)])

    assert frame.iloc[0]["position_date"] == date(2020, 1, 2)
    assert frame.iloc[0]["available_at"].isoformat() == "2026-09-03T12:00:00+00:00"
    assert frame.iloc[0]["short_balance_quantity"] == 1000
    assert frame.iloc[0]["short_balance_ratio"] == 10
