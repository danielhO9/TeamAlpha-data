"""승인된 KRX 투자자별 거래실적 export를 PIT-safe Silver로 변환한다."""
from __future__ import annotations

import glob
import hashlib
import io
import json
import re
from datetime import datetime, time, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

from pipeline.common import db
from pipeline.common.sink import read_bytes

KST = ZoneInfo("Asia/Seoul")
COLUMN_ALIASES = {
    "trade_date": ("trade_date", "일자", "거래일"),
    "ticker": ("ticker", "종목코드", "단축코드"),
    "market": ("market", "시장", "시장구분"),
    "investor_type": ("investor_type", "투자자구분", "투자자"),
    "sell_volume": ("sell_volume", "매도거래량"),
    "buy_volume": ("buy_volume", "매수거래량"),
    "net_volume": ("net_volume", "순매수거래량"),
    "sell_value": ("sell_value", "매도거래대금"),
    "buy_value": ("buy_value", "매수거래대금"),
    "net_value": ("net_value", "순매수거래대금"),
}
INVESTOR_TYPES = {
    "기관합계": "INSTITUTION",
    "금융투자": "FINANCIAL_INVESTMENT",
    "보험": "INSURANCE",
    "투신": "INVESTMENT_TRUST",
    "사모": "PRIVATE_FUND",
    "은행": "BANK",
    "기타금융": "OTHER_FINANCE",
    "연기금등": "PENSION",
    "연기금 등": "PENSION",
    "기타법인": "OTHER_CORPORATION",
    "개인": "INDIVIDUAL",
    "외국인": "FOREIGN",
    "외국인합계": "FOREIGN",
    "외국인 합계": "FOREIGN",
    "기타외국인": "OTHER_FOREIGN",
}
MARKETS = {
    "유가증권": "KOSPI",
    "코스피": "KOSPI",
    "KOSPI": "KOSPI",
    "코스닥": "KOSDAQ",
    "KOSDAQ": "KOSDAQ",
    "코넥스": "KONEX",
    "KONEX": "KONEX",
}
NUMERIC_COLUMNS = (
    "sell_volume", "buy_volume", "net_volume",
    "sell_value", "buy_value", "net_value",
)
_KRX_TICKER = re.compile(r"^[0-9A-Z]{6}$")


def _decimal(value) -> Decimal | None:
    if pd.isna(value):
        return None
    rendered = str(value).replace(",", "").strip()
    if not rendered or rendered == "-":
        return None
    try:
        return Decimal(rendered)
    except InvalidOperation as exc:
        raise ValueError(f"invalid KRX investor-flow number: {value!r}") from exc


def _read_frame(raw: bytes, suffix: str) -> pd.DataFrame:
    separator = "\t" if suffix.lower() in {".tsv", ".txt"} else ","
    last_error: Exception | None = None
    for encoding in ("utf-8-sig", "cp949"):
        try:
            return pd.read_csv(io.BytesIO(raw), sep=separator, encoding=encoding)
        except UnicodeDecodeError as exc:
            last_error = exc
    raise ValueError("KRX investor-flow file encoding is not UTF-8/CP949") from last_error


def _canonical_columns(frame: pd.DataFrame) -> pd.DataFrame:
    rendered = {str(column).strip(): column for column in frame.columns}
    rename = {}
    for canonical, aliases in COLUMN_ALIASES.items():
        matches = [rendered[name] for name in aliases if name in rendered]
        if len(matches) > 1:
            raise ValueError(
                f"multiple columns map to {canonical}: {[str(v) for v in matches]}"
            )
        if matches:
            rename[matches[0]] = canonical
    out = frame.rename(columns=rename).copy()
    missing = [
        column for column in ("trade_date", "ticker", "market", "investor_type")
        if column not in out
    ]
    if missing:
        raise ValueError(f"KRX investor-flow export missing columns: {missing}")
    if not set(NUMERIC_COLUMNS).intersection(out.columns):
        raise ValueError("KRX investor-flow export has no volume/value fields")
    for column in NUMERIC_COLUMNS:
        if column not in out:
            out[column] = None
    return out


def _iter_files(base: str) -> list[str]:
    return sorted(
        path for path in glob.glob(
            f"{base}/investor_flows/krx/sha256=*/source.*"
        )
        if Path(path).suffix.lower() in {".csv", ".tsv", ".txt"}
    )


def _verified_manifest(uri: str, raw: bytes) -> dict:
    manifest_uri = str(Path(uri).with_name("manifest.json"))
    if uri.startswith("s3://"):
        manifest_uri = uri.rsplit("/", 1)[0] + "/manifest.json"
    manifest_raw = read_bytes(manifest_uri)
    if manifest_raw is None:
        raise ValueError(f"KRX investor-flow manifest missing: {manifest_uri}")
    manifest = json.loads(manifest_raw.decode("utf-8"))
    digest = hashlib.sha256(raw).hexdigest()
    if (
        manifest.get("schema_version") != "krx-investor-flow-export-v1"
        or manifest.get("source") != "KRX_DATA_MARKETPLACE_AUTHORIZED_EXPORT"
        or manifest.get("sha256") != digest
        or not str(manifest.get("authorization_id") or "").strip()
    ):
        raise ValueError(f"KRX investor-flow manifest verification failed: {uri}")
    return manifest


def prepare(
    base: str | None = None,
    *,
    files: list[str] | None = None,
) -> tuple[pd.DataFrame, dict]:
    selected = sorted(set(files if files is not None else _iter_files(str(base))))
    records: list[dict] = []
    input_rows = 0
    for uri in selected:
        raw = read_bytes(uri)
        if raw is None:
            raise RuntimeError(f"KRX investor-flow Bronze object missing: {uri}")
        manifest = _verified_manifest(uri, raw)
        frame = _canonical_columns(_read_frame(raw, Path(uri).suffix))
        source_digest = str(manifest["sha256"])
        input_rows += len(frame)
        for row in frame.itertuples(index=False):
            values = row._asdict()
            trade_date = pd.Timestamp(values["trade_date"]).date()
            ticker_raw = str(values["ticker"]).strip().upper()
            if ticker_raw.endswith(".0") and ticker_raw[:-2].isdigit():
                ticker_raw = ticker_raw[:-2]
            ticker = ticker_raw.zfill(6) if ticker_raw.isdigit() else ticker_raw
            if _KRX_TICKER.fullmatch(ticker) is None:
                raise ValueError(f"invalid KRX ticker: {values['ticker']!r}")
            market_raw = str(values["market"]).strip().upper()
            market = MARKETS.get(market_raw)
            if market is None:
                raise ValueError(f"unknown KRX market: {values['market']!r}")
            investor_raw = str(values["investor_type"]).strip()
            investor_type = INVESTOR_TYPES.get(investor_raw, investor_raw.upper())
            amounts = {column: _decimal(values[column]) for column in NUMERIC_COLUMNS}
            if (
                amounts["net_volume"] is None
                and amounts["buy_volume"] is not None
                and amounts["sell_volume"] is not None
            ):
                amounts["net_volume"] = (
                    amounts["buy_volume"] - amounts["sell_volume"]
                )
            if (
                amounts["net_value"] is None
                and amounts["buy_value"] is not None
                and amounts["sell_value"] is not None
            ):
                amounts["net_value"] = (
                    amounts["buy_value"] - amounts["sell_value"]
                )
            if (
                None not in (
                    amounts["sell_volume"], amounts["buy_volume"], amounts["net_volume"],
                )
                and amounts["net_volume"]
                != amounts["buy_volume"] - amounts["sell_volume"]
            ):
                raise ValueError(
                    f"KRX volume arithmetic mismatch: {trade_date} {ticker} {investor_type}"
                )
            if (
                None not in (
                    amounts["sell_value"], amounts["buy_value"], amounts["net_value"],
                )
                and amounts["net_value"]
                != amounts["buy_value"] - amounts["sell_value"]
            ):
                raise ValueError(
                    f"KRX value arithmetic mismatch: {trade_date} {ticker} {investor_type}"
                )
            available_date = trade_date + timedelta(days=1)
            records.append({
                "natural_key": ticker,
                "source": "KRX",
                "trade_date": trade_date,
                "market": market,
                "investor_type": investor_type,
                **amounts,
                "currency": "KRW",
                "available_at": datetime.combine(available_date, time.min, KST),
                "source_file_sha256": source_digest,
                "source_file": uri,
            })
    frame = pd.DataFrame(records)
    if not frame.empty:
        keys = ["natural_key", "source", "trade_date", "market", "investor_type"]
        duplicates = frame.duplicated(keys, keep=False)
        if duplicates.any():
            sample = frame.loc[duplicates, keys].head(10)
            raise ValueError(
                "duplicate KRX investor-flow business keys: "
                f"{sample.to_dict(orient='records')}"
            )
        frame = frame.sort_values(keys).reset_index(drop=True)
    return frame, {
        "file_count": len(selected),
        "input_rows": input_rows,
        "transformed_rows": len(frame),
    }


def publish(conn, frame: pd.DataFrame, asset_map: dict[str, int], run_id) -> int:
    if frame.empty:
        return 0
    columns = [
        "asset_id", "source", "trade_date", "market", "investor_type",
        "sell_volume", "buy_volume", "net_volume", "sell_value", "buy_value",
        "net_value", "currency", "available_at", "source_file_sha256",
        "quality_run_id",
    ]
    rows = []
    for row in frame.itertuples(index=False):
        values = row._asdict()
        values["asset_id"] = asset_map[str(row.natural_key)]
        values["quality_run_id"] = run_id
        rows.append(tuple(values[column] for column in columns))
    conflict = ["asset_id", "source", "trade_date", "market", "investor_type"]
    update = [column for column in columns if column not in conflict]
    return db.upsert(
        conn,
        "investor_flow_daily",
        columns,
        rows,
        conflict,
        update,
        temp_name="_stg_investor_flows",
    )
