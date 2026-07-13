"""fundamental 적재: DART 주요계정 JSON → long 정규화 + PIT(available_date).

계정명(account_nm)을 표준지표로 매핑(주요계정만, 나머지 스킵). thstrm_amount(당기값) 사용.
period_end/fiscal_period 는 bsns_year + reprt 로, available_date 는 rcept_no 접수일 +1일
(접수일 못 구하면 법정기한+1일). 소스 fnlttMultiAcnt 라 source='DART'.
"""
from __future__ import annotations

import glob
import json
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from pipeline.common import db

# DART 주요계정 account_nm → 표준지표 (매핑 없는 계정은 스킵)
METRIC_MAP = {
    "자산총계": "total_assets", "유동자산": "current_assets", "비유동자산": "noncurrent_assets",
    "부채총계": "total_liabilities", "유동부채": "current_liabilities", "비유동부채": "noncurrent_liabilities",
    "자본총계": "total_equity", "자본금": "capital_stock", "이익잉여금": "retained_earnings",
    "매출액": "revenue", "영업이익": "operating_income", "영업이익(손실)": "operating_income",
    "법인세차감전 순이익": "pretax_income", "당기순이익(손실)": "net_income", "총포괄손익": "comprehensive_income",
}
# reprt_code → (fiscal_period, 회계기간 종료 월, 일)
REPRT = {"11011": ("FY", 12, 31), "11013": ("Q1", 3, 31), "11012": ("Q2", 6, 30), "11014": ("Q3", 9, 30)}
COLS = ["asset_id", "source", "period_end", "fiscal_period", "fs_type",
        "filing_id", "filed", "available_date", "metric", "value"]


def _available_date(period_end: date, fiscal_period: str, filed: date | None) -> date:
    if filed is not None:                       # 접수일 있으면 +1일 (PIT)
        return filed + timedelta(days=1)
    d = period_end + timedelta(days=90 if fiscal_period == "FY" else 45)  # 법정 제출기한
    if d.weekday() >= 5:                         # 주말이면 다음 월요일
        d += timedelta(days=7 - d.weekday())
    return d + timedelta(days=1)


def _amount(s) -> float | None:
    s = (s or "").replace(",", "").strip()
    if not s or s == "-":
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _filed_from_rcept(rcept: str) -> date | None:
    if len(rcept) >= 8 and rcept[:8].isdigit():
        try:
            return date(int(rcept[:4]), int(rcept[4:6]), int(rcept[6:8]))
        except ValueError:
            return None
    return None


def _iter_files(base: str, years: set[int] | None, files: list[str] | None) -> list[str]:
    if files is not None:
        return files
    out = []
    for f in glob.glob(f"{base}/financials/dart/year=*/corp=*/*.json"):
        year = int(f.split("year=")[1].split("/")[0])
        if years is None or year in years:
            out.append(f)
    return out


def _file_meta(path: str) -> tuple[str, str]:
    ticker = path.split("corp=")[1].split("/")[0]
    reprt = Path(path).name[:5]
    return ticker, reprt


def run(conn, base: str, krx_map: dict[str, int], years: set[int] | None = None,
        files: list[str] | None = None, replace_existing: bool = False) -> None:
    recs = []
    skipped_ticker = 0
    for f in _iter_files(base, years, files):
        ticker, reprt = _file_meta(f)
        aid = krx_map.get(ticker)
        if aid is None:
            skipped_ticker += 1
            continue
        fp, mm, dd = REPRT[reprt]
        with open(f, encoding="utf-8") as fh:
            rows = json.load(fh)
        for r in rows:
            metric = METRIC_MAP.get(r.get("account_nm"))
            if not metric:
                continue
            val = _amount(r.get("thstrm_amount"))
            if val is None:
                continue
            period_end = date(int(r["bsns_year"]), mm, dd)
            rcept = r.get("rcept_no", "") or ""
            filed = _filed_from_rcept(rcept)
            recs.append((aid, "DART", period_end, fp, r.get("fs_div"),
                         rcept or None, filed, _available_date(period_end, fp, filed),
                         metric, val))

    df = pd.DataFrame(recs, columns=COLS).drop_duplicates(
        ["asset_id", "source", "period_end", "fiscal_period", "fs_type", "metric"], keep="last")
    if replace_existing and not df.empty:
        scopes = list(df[["asset_id", "source", "period_end", "fiscal_period", "fs_type"]]
                      .drop_duplicates().itertuples(index=False, name=None))
        with conn.cursor() as cur:
            for scope in scopes:
                cur.execute(
                    "DELETE FROM fundamental WHERE asset_id=%s AND source=%s AND period_end=%s "
                    "AND fiscal_period=%s AND fs_type=%s",
                    scope,
                )
        conn.commit()
    rows = list(df.itertuples(index=False, name=None))
    n = db.upsert(conn, "fundamental", COLS, rows,
                  conflict=["asset_id", "source", "period_end", "fiscal_period", "fs_type", "metric"],
                  update=["filing_id", "filed", "available_date", "value"])
    print(f"[financials] fundamental upsert {n}행 (미매핑 티커 파일 {skipped_ticker} 스킵)")
