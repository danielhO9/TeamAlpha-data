"""OpenDART 지분공시 Bronze를 point-in-time Silver 이벤트로 변환한다."""
from __future__ import annotations

import glob
import hashlib
import json
import re
from datetime import date, datetime, time, timedelta
from decimal import Decimal, InvalidOperation
from zoneinfo import ZoneInfo

import pandas as pd
from psycopg.types.json import Jsonb

from pipeline.common import db
from pipeline.common.sink import read_bytes

KST = ZoneInfo("Asia/Seoul")
DISCLOSURE_TYPES = {
    "EXECUTIVE_MAJOR_SHAREHOLDER",
    "FIVE_PERCENT",
}
_PATH_RE = re.compile(
    r"ownership/dart/disclosure_type=(?P<disclosure_type>[^/]+)/"
    r"corp=(?P<ticker>[0-9A-Z]{6})/sha256=[0-9a-f]{64}/response\.json$"
)


def _decimal(value) -> Decimal | None:
    rendered = str(value or "").replace(",", "").replace("%", "").strip()
    if not rendered or rendered == "-":
        return None
    try:
        return Decimal(rendered)
    except InvalidOperation:
        return None


def _filed(filing_id: str, explicit: str | None = None) -> date | None:
    rendered = str(explicit or "").replace("-", "").strip()
    if len(rendered) >= 8 and rendered[:8].isdigit():
        rendered = rendered[:8]
    else:
        rendered = filing_id[:8]
    if len(rendered) != 8 or not rendered.isdigit():
        return None
    try:
        return date.fromisoformat(
            f"{rendered[:4]}-{rendered[4:6]}-{rendered[6:8]}"
        )
    except ValueError:
        return None


def _iter_files(base: str) -> list[str]:
    return sorted(glob.glob(
        f"{base}/ownership/dart/disclosure_type=*/corp=*/"
        "sha256=*/response.json"
    ))


def prepare(
    base: str | None = None,
    *,
    files: list[str] | None = None,
) -> tuple[pd.DataFrame, dict]:
    selected = sorted(set(files if files is not None else _iter_files(str(base))))
    records: list[dict] = []
    input_rows = rejected_rows = 0
    for uri in selected:
        match = _PATH_RE.search(uri.replace("\\", "/"))
        if match is None:
            raise ValueError(f"unrecognized DART ownership path: {uri}")
        disclosure_type = match.group("disclosure_type")
        if disclosure_type not in DISCLOSURE_TYPES:
            raise ValueError(f"unknown DART ownership type: {disclosure_type}")
        raw = read_bytes(uri)
        if raw is None:
            raise RuntimeError(f"ownership Bronze object missing: {uri}")
        payload = json.loads(raw.decode("utf-8"))
        rows = payload.get("list") or []
        if not isinstance(rows, list):
            raise ValueError(f"ownership list is invalid: {uri}")
        for row in rows:
            input_rows += 1
            if not isinstance(row, dict):
                rejected_rows += 1
                continue
            corp_code = str(row.get("corp_code") or "").strip()
            filing_id = str(row.get("rcept_no") or "").strip()
            reporter = str(row.get("repror") or "").strip()
            filed = _filed(filing_id, row.get("rcept_dt"))
            if not corp_code or not filing_id or not reporter or filed is None:
                rejected_rows += 1
                continue
            if disclosure_type == "EXECUTIVE_MAJOR_SHAREHOLDER":
                values = {
                    "officer_registered": str(
                        row.get("isu_exctv_rgist_at") or ""
                    ).strip() or None,
                    "officer_position": str(
                        row.get("isu_exctv_ofcps") or ""
                    ).strip() or None,
                    "major_shareholder": str(
                        row.get("isu_main_shrholdr") or ""
                    ).strip() or None,
                    "report_type": None,
                    "report_reason": None,
                    "shares": _decimal(row.get("sp_stock_lmp_cnt")),
                    "shares_change": _decimal(
                        row.get("sp_stock_lmp_irds_cnt")
                    ),
                    "ownership_pct": _decimal(row.get("sp_stock_lmp_rate")),
                    "ownership_pct_change": _decimal(
                        row.get("sp_stock_lmp_irds_rate")
                    ),
                    "control_shares": None,
                    "control_pct": None,
                }
            else:
                values = {
                    "officer_registered": None,
                    "officer_position": None,
                    "major_shareholder": None,
                    "report_type": str(row.get("report_tp") or "").strip() or None,
                    "report_reason": str(row.get("report_resn") or "").strip() or None,
                    "shares": _decimal(row.get("stkqy")),
                    "shares_change": _decimal(row.get("stkqy_irds")),
                    "ownership_pct": _decimal(row.get("stkrt")),
                    "ownership_pct_change": _decimal(row.get("stkrt_irds")),
                    "control_shares": _decimal(row.get("ctr_stkqy")),
                    "control_pct": _decimal(row.get("ctr_stkrt")),
                }
            canonical = json.dumps(
                row,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            event_key = hashlib.sha256(
                f"{disclosure_type}\0{canonical}".encode("utf-8")
            ).hexdigest()
            available_date = filed + timedelta(days=1)
            records.append({
                "natural_key": corp_code,
                "source": "DART",
                "disclosure_type": disclosure_type,
                "filing_id": filing_id,
                "filed": filed,
                "available_date": available_date,
                "available_at": datetime.combine(available_date, time.min, KST),
                "reporter": reporter,
                **values,
                "event_key": event_key,
                "raw_row": row,
                "source_file": uri,
            })
    frame = pd.DataFrame(records)
    if not frame.empty:
        keys = ["natural_key", "source", "event_key"]
        frame = frame.sort_values(keys).drop_duplicates(keys, keep="last")
        duplicate_business_key = [
            "natural_key", "source", "disclosure_type", "filing_id", "reporter",
        ]
        conflicting = frame.duplicated(duplicate_business_key, keep=False)
        if conflicting.any():
            sample = frame.loc[conflicting, duplicate_business_key].head(10)
            raise ValueError(
                "ownership filing/reporter has multiple non-identical rows: "
                f"{sample.to_dict(orient='records')}"
            )
        frame = frame.reset_index(drop=True)
    return frame, {
        "file_count": len(selected),
        "input_rows": input_rows,
        "transformed_rows": len(frame),
        "rejected_rows": rejected_rows,
    }


def publish(conn, frame: pd.DataFrame, asset_map: dict[str, int], run_id) -> int:
    if frame.empty:
        return 0
    columns = [
        "asset_id", "source", "disclosure_type", "filing_id", "filed",
        "available_date", "available_at", "reporter", "officer_registered",
        "officer_position", "major_shareholder", "report_type", "report_reason",
        "shares", "shares_change", "ownership_pct", "ownership_pct_change",
        "control_shares", "control_pct", "event_key", "raw_row", "quality_run_id",
    ]
    rows = []
    for row in frame.itertuples(index=False):
        values = row._asdict()
        values["asset_id"] = asset_map[str(row.natural_key)]
        values["raw_row"] = Jsonb(row.raw_row)
        values["quality_run_id"] = run_id
        rows.append(tuple(values[column] for column in columns))
    conflict = ["asset_id", "source", "event_key"]
    update = [column for column in columns if column not in conflict]
    return db.upsert(
        conn,
        "ownership_disclosure_event",
        columns,
        rows,
        conflict,
        update,
        temp_name="_stg_ownership_events",
    )
