"""OpenDART 전체 재무제표 Bronze를 숫자 원계정 Silver 후보로 변환한다."""
from __future__ import annotations

import glob
import hashlib
import json
import re
from datetime import date, datetime, time, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
from psycopg.types.json import Jsonb

from pipeline.common import db
from pipeline.common.sink import read_bytes

KST = ZoneInfo("Asia/Seoul")
REPRT = {
    "11011": ("FY", 12, 31),
    "11013": ("Q1", 3, 31),
    "11012": ("Q2", 6, 30),
    "11014": ("Q3", 9, 30),
}
STATEMENT_TYPES = frozenset({"BS", "IS", "CIS", "CF", "SCE"})
AMOUNT_FIELDS = (
    "current_amount",
    "current_cumulative_amount",
    "prior_amount",
    "prior_quarter_amount",
    "prior_cumulative_amount",
    "prior_year_amount",
)
_DATE_RE = re.compile(r"(\d{4})[.\-/](\d{2})[.\-/](\d{2})")
_NEW_PATH_RE = re.compile(
    r"year=(?P<year>\d{4})/corp=(?P<ticker>[0-9A-Z]{6})/"
    r"report=(?P<report>11011|11012|11013|11014)/"
    r"fs_type=(?P<fs_type>CFS|OFS)/"
)
_LEGACY_PATH_RE = re.compile(
    r"year=(?P<year>\d{4})/corp=(?P<ticker>[0-9A-Z]{6})/"
    r"(?P<report>11011|11012|11013|11014)-(?P<fs_type>CFS|OFS)\.json$"
)


def _decimal(value) -> Decimal | None:
    rendered = str(value or "").replace(",", "").strip()
    if not rendered or rendered == "-":
        return None
    negative = rendered.startswith("(") and rendered.endswith(")")
    if negative:
        rendered = rendered[1:-1].strip()
    try:
        amount = Decimal(rendered)
    except InvalidOperation:
        return None
    return -amount if negative else amount


def _filed(filing_id: str) -> date | None:
    if len(filing_id) < 8 or not filing_id[:8].isdigit():
        return None
    try:
        return date.fromisoformat(
            f"{filing_id[:4]}-{filing_id[4:6]}-{filing_id[6:8]}"
        )
    except ValueError:
        return None


def _period_end(value: str | None, business_year: int, report_code: str) -> date:
    hits = _DATE_RE.findall(str(value or ""))
    if hits:
        year, month, day = hits[-1]
        try:
            return date(int(year), int(month), int(day))
        except ValueError:
            pass
    _, month, day = REPRT[report_code]
    return date(business_year, month, day)


def _path_meta(uri: str) -> tuple[str, int, str, str]:
    normalized = uri.replace("\\", "/")
    match = _NEW_PATH_RE.search(normalized) or _LEGACY_PATH_RE.search(normalized)
    if match is None:
        raise ValueError(f"unrecognized DART full-statement path: {uri}")
    return (
        match.group("ticker"),
        int(match.group("year")),
        match.group("report"),
        match.group("fs_type"),
    )


def _iter_files(base: str) -> list[str]:
    return sorted(set(
        glob.glob(
            f"{base}/financials/dart_statement_lines/year=*/corp=*/"
            "report=*/fs_type=*/sha256=*/response.json"
        )
        + glob.glob(f"{base}/financials/dart_full/year=*/corp=*/*.json")
    ))


def prepare(
    base: str | None = None,
    *,
    files: list[str] | None = None,
) -> tuple[pd.DataFrame, dict]:
    """Parse every numeric line without semantic account-name mapping."""
    selected = sorted(set(files if files is not None else _iter_files(str(base))))
    records: list[dict] = []
    input_rows = excluded_rows = rejected_rows = 0
    for uri in selected:
        ticker, path_year, path_report, path_fs_type = _path_meta(uri)
        raw = read_bytes(uri)
        if raw is None:
            raise RuntimeError(f"full-statement Bronze object missing: {uri}")
        payload = json.loads(raw.decode("utf-8"))
        rows = payload if isinstance(payload, list) else payload.get("list", [])
        if not isinstance(rows, list):
            raise ValueError(f"full-statement list is invalid: {uri}")
        for row in rows:
            input_rows += 1
            if not isinstance(row, dict):
                rejected_rows += 1
                continue
            report_code = str(row.get("reprt_code") or path_report).strip()
            fs_type = str(row.get("fs_div") or path_fs_type).strip()
            statement_type = str(row.get("sj_div") or "").strip()
            if (
                report_code not in REPRT
                or fs_type != path_fs_type
                or statement_type not in STATEMENT_TYPES
            ):
                rejected_rows += 1
                continue
            business_year_raw = str(row.get("bsns_year") or path_year).strip()
            if not business_year_raw.isdigit():
                rejected_rows += 1
                continue
            business_year = int(business_year_raw)
            filing_id = str(row.get("rcept_no") or "").strip()
            filed = _filed(filing_id)
            if filed is None:
                rejected_rows += 1
                continue
            period_end = _period_end(
                row.get("thstrm_dt"), business_year, report_code,
            )
            if filed < period_end:
                rejected_rows += 1
                continue
            amounts = {
                "current_amount": _decimal(row.get("thstrm_amount")),
                "current_cumulative_amount": _decimal(
                    row.get("thstrm_add_amount")
                ),
                "prior_amount": _decimal(row.get("frmtrm_amount")),
                "prior_quarter_amount": _decimal(row.get("frmtrm_q_amount")),
                "prior_cumulative_amount": _decimal(
                    row.get("frmtrm_add_amount")
                ),
                "prior_year_amount": _decimal(row.get("bfefrmtrm_amount")),
            }
            if all(value is None for value in amounts.values()):
                excluded_rows += 1
                continue
            account_name = str(row.get("account_nm") or "").strip()
            account_id = str(row.get("account_id") or "").strip()
            if not account_id:
                account_id = f"name:{account_name}"
            account_detail = str(row.get("account_detail") or "").strip()
            try:
                line_order = int(str(row.get("ord") or "").strip())
            except ValueError:
                line_order = None
            canonical = json.dumps(
                row,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            line_key = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
            available_date = filed + timedelta(days=1)
            records.append({
                "natural_key": ticker,
                "source": "DART",
                "filing_id": filing_id,
                "report_code": report_code,
                "business_year": business_year,
                "period_end": period_end,
                "fiscal_period": REPRT[report_code][0],
                "fs_type": fs_type,
                "statement_type": statement_type,
                "account_id": account_id,
                "account_name": account_name or None,
                "account_detail": account_detail,
                "line_order": line_order,
                "current_period_label": str(row.get("thstrm_nm") or "").strip() or None,
                **amounts,
                "prior_period_label": str(row.get("frmtrm_nm") or "").strip() or None,
                "prior_quarter_label": str(row.get("frmtrm_q_nm") or "").strip() or None,
                "prior_year_label": str(row.get("bfefrmtrm_nm") or "").strip() or None,
                "currency": str(row.get("currency") or "").strip().upper() or None,
                "filed": filed,
                "accepted_at": None,
                "available_date": available_date,
                "available_at": datetime.combine(available_date, time.min, KST),
                "revision_key": filing_id,
                "line_key": line_key,
                "raw_line": row,
                "source_file": uri,
            })
    frame = pd.DataFrame(records)
    if not frame.empty:
        keys = ["natural_key", "source", "filing_id", "fs_type", "line_key"]
        frame = frame.sort_values(keys).drop_duplicates(keys, keep="last")
        frame = frame.reset_index(drop=True)
    return frame, {
        "file_count": len(selected),
        "input_rows": input_rows,
        "transformed_rows": len(frame),
        "excluded_rows": excluded_rows,
        "rejected_rows": rejected_rows,
    }


def publish(conn, frame: pd.DataFrame, asset_map: dict[str, int], run_id) -> int:
    if frame.empty:
        return 0
    columns = [
        "asset_id", "source", "filing_id", "report_code", "business_year",
        "period_end", "fiscal_period", "fs_type", "statement_type",
        "account_id", "account_name", "account_detail", "line_order",
        "current_period_label", "current_amount", "current_cumulative_amount",
        "prior_period_label", "prior_amount", "prior_quarter_label",
        "prior_quarter_amount", "prior_cumulative_amount", "prior_year_label",
        "prior_year_amount", "currency", "filed", "accepted_at",
        "available_date", "available_at", "revision_key", "line_key",
        "raw_line", "quality_run_id",
    ]
    rows = []
    for row in frame.itertuples(index=False):
        values = row._asdict()
        values["asset_id"] = asset_map[str(row.natural_key)]
        values["raw_line"] = Jsonb(row.raw_line)
        values["quality_run_id"] = run_id
        rows.append(tuple(values[column] for column in columns))
    conflict = ["asset_id", "source", "filing_id", "fs_type", "line_key"]
    update = [column for column in columns if column not in conflict]
    return db.upsert(
        conn,
        "fundamental_statement_line",
        columns,
        rows,
        conflict,
        update,
        temp_name="_stg_full_statement_lines",
    )
