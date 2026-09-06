"""승인된 KRX 공매도 순보유잔고 export를 vintage-aware Silver로 변환한다."""
from __future__ import annotations

import glob
import hashlib
import io
import json
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

import pandas as pd

from pipeline.common import db
from pipeline.common.sink import read_bytes

COLUMN_ALIASES = {
    "position_date": ("position_date", "일자", "기준일"),
    "ticker": ("ticker", "종목코드", "단축코드"),
    "market": ("market", "시장", "시장구분"),
    "short_balance_quantity": (
        "short_balance_quantity", "공매도순보유잔고수량", "순보유잔고수량",
    ),
    "listed_shares": ("listed_shares", "상장주식수"),
    "short_balance_value": (
        "short_balance_value", "공매도순보유잔고금액", "순보유잔고금액",
    ),
    "market_cap": ("market_cap", "시가총액"),
    "short_balance_ratio": (
        "short_balance_ratio", "공매도순보유잔고비중", "순보유잔고비중", "비중",
    ),
}
NUMERIC_COLUMNS = (
    "short_balance_quantity", "listed_shares", "short_balance_value",
    "market_cap", "short_balance_ratio",
)
MARKETS = {
    "유가증권": "KOSPI",
    "코스피": "KOSPI",
    "KOSPI": "KOSPI",
    "코스닥": "KOSDAQ",
    "KOSDAQ": "KOSDAQ",
    "코넥스": "KONEX",
    "KONEX": "KONEX",
}
_KRX_TICKER = re.compile(r"^[0-9A-Z]{6}$")


def _decimal(value) -> Decimal | None:
    if pd.isna(value):
        return None
    rendered = str(value).replace(",", "").replace("%", "").strip()
    if not rendered or rendered == "-":
        return None
    try:
        return Decimal(rendered)
    except InvalidOperation as exc:
        raise ValueError(f"invalid KRX short-balance number: {value!r}") from exc


def _read_frame(raw: bytes, suffix: str) -> pd.DataFrame:
    separator = "\t" if suffix.lower() in {".tsv", ".txt"} else ","
    last_error: Exception | None = None
    for encoding in ("utf-8-sig", "cp949"):
        try:
            return pd.read_csv(io.BytesIO(raw), sep=separator, encoding=encoding)
        except UnicodeDecodeError as exc:
            last_error = exc
    raise ValueError("KRX short-balance encoding is not UTF-8/CP949") from last_error


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
        column for column in ("position_date", "ticker", "market")
        if column not in out
    ]
    if missing:
        raise ValueError(f"KRX short-balance export missing columns: {missing}")
    if not set(NUMERIC_COLUMNS).intersection(out.columns):
        raise ValueError("KRX short-balance export has no balance fields")
    for column in NUMERIC_COLUMNS:
        if column not in out:
            out[column] = None
    return out


def _iter_files(base: str) -> list[str]:
    return sorted(
        path for path in glob.glob(f"{base}/short_balances/krx/sha256=*/source.*")
        if Path(path).suffix.lower() in {".csv", ".tsv", ".txt"}
    )


def _manifest(uri: str, raw: bytes) -> dict:
    manifest_uri = str(Path(uri).with_name("manifest.json"))
    if uri.startswith("s3://"):
        manifest_uri = uri.rsplit("/", 1)[0] + "/manifest.json"
    manifest_raw = read_bytes(manifest_uri)
    if manifest_raw is None:
        raise ValueError(f"KRX short-balance manifest missing: {manifest_uri}")
    manifest = json.loads(manifest_raw.decode("utf-8"))
    digest = hashlib.sha256(raw).hexdigest()
    if (
        manifest.get("schema_version") != "krx-short-balance-export-v1"
        or manifest.get("source") != "KRX_DATA_MARKETPLACE_AUTHORIZED_EXPORT"
        or manifest.get("sha256") != digest
        or not str(manifest.get("authorization_id") or "").strip()
    ):
        raise ValueError(f"KRX short-balance manifest verification failed: {uri}")
    observed_at = datetime.fromisoformat(str(manifest.get("observed_at") or ""))
    if observed_at.tzinfo is None:
        raise ValueError(f"KRX short-balance observed_at needs timezone: {uri}")
    manifest["parsed_observed_at"] = observed_at
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
            raise RuntimeError(f"KRX short-balance Bronze object missing: {uri}")
        manifest = _manifest(uri, raw)
        frame = _canonical_columns(_read_frame(raw, Path(uri).suffix))
        digest = str(manifest["sha256"])
        observed_at = manifest["parsed_observed_at"]
        input_rows += len(frame)
        for source_row in frame.itertuples(index=False):
            values = source_row._asdict()
            position_date = pd.Timestamp(values["position_date"]).date()
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
            amounts = {column: _decimal(values[column]) for column in NUMERIC_COLUMNS}
            if all(amounts[column] is None for column in (
                "short_balance_quantity", "short_balance_value", "short_balance_ratio",
            )):
                raise ValueError(
                    f"empty KRX short balance: {position_date} {ticker}"
                )
            canonical = json.dumps(
                {key: str(value) for key, value in sorted(values.items())},
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            observation_key = hashlib.sha256(
                f"{digest}\0{canonical}".encode("utf-8")
            ).hexdigest()
            records.append({
                "natural_key": ticker,
                "source": "KRX",
                "position_date": position_date,
                "market": market,
                **amounts,
                "observed_at": observed_at,
                "available_at": observed_at,
                "observation_key": observation_key,
                "source_file_sha256": digest,
                "source_file": uri,
            })
    frame = pd.DataFrame(records)
    if not frame.empty:
        exact_keys = [
            "natural_key", "source", "position_date", "market", "observation_key",
        ]
        frame = frame.sort_values(exact_keys).drop_duplicates(exact_keys)
        business_keys = [
            "natural_key", "source", "position_date", "market",
            "source_file_sha256",
        ]
        conflicting = frame.duplicated(business_keys, keep=False)
        if conflicting.any():
            sample = frame.loc[conflicting, business_keys].head(10)
            raise ValueError(
                "KRX short-balance source has conflicting business keys: "
                f"{sample.to_dict(orient='records')}"
            )
        frame = frame.reset_index(drop=True)
    return frame, {
        "file_count": len(selected),
        "input_rows": input_rows,
        "transformed_rows": len(frame),
        "rejected_rows": 0,
    }


def publish(conn, frame: pd.DataFrame, asset_map: dict[str, int], run_id) -> int:
    if frame.empty:
        return 0
    columns = [
        "asset_id", "source", "position_date", "market",
        "short_balance_quantity", "listed_shares", "short_balance_value",
        "market_cap", "short_balance_ratio", "observed_at", "available_at",
        "observation_key", "source_file_sha256", "quality_run_id",
    ]
    rows = []
    for row in frame.itertuples(index=False):
        values = row._asdict()
        values["asset_id"] = asset_map[str(row.natural_key)]
        values["quality_run_id"] = run_id
        rows.append(tuple(values[column] for column in columns))
    conflict = ["asset_id", "source", "position_date", "market", "observation_key"]
    update = [
        "short_balance_quantity", "listed_shares", "short_balance_value",
        "market_cap", "short_balance_ratio", "source_file_sha256", "quality_run_id",
    ]
    return db.upsert(
        conn,
        "short_position_balance_observation",
        columns,
        rows,
        conflict,
        update,
        temp_name="_stg_short_balance_observations",
    )
