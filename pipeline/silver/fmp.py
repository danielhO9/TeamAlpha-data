"""FMP Bronze raw responses -> Silver candidates and publishers."""
from __future__ import annotations

import glob
import hashlib
import io
import json
import math
import re
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from functools import lru_cache
from pathlib import Path
from uuid import UUID

import exchange_calendars as xcals
import pandas as pd

from pipeline.common import db
from pipeline.common.sink import read_bytes
from pipeline.fmp_commodities import COMMODITY_BY_SYMBOL, COMMODITY_SPECS
from pipeline.silver_quality.models import CandidateBundle
from pipeline.silver import research_observations

SUPPORTED_EXCHANGES = {"NASDAQ", "NYSE", "AMEX"}
NON_EQUITY_NAME = re.compile(
    r"(?:\bETF\b|\bETN\b|\bWarrants?\b|\bWts?\b|\bUnits?\b|\bRights?\b|"
    r"\bSenior Notes?\b|\bSubordinated Notes?\b|\bBonds?\b)",
    re.IGNORECASE,
)
PREFERRED_NAME = re.compile(r"\bPreferred\b|\bPreference\b", re.IGNORECASE)
REIT_NAME = re.compile(r"\bREIT\b|Real Estate Investment Trust", re.IGNORECASE)
NON_EQUITY_SUFFIXES = {
    "W": "WARRANT_SUFFIX",
    "U": "UNIT_SUFFIX",
    "R": "RIGHT_SUFFIX",
    "Z": "DERIVATIVE_SUFFIX",
}
ISSUER_NAME_NOISE = re.compile(
    r"\b(?:INCORPORATED|INC|CORPORATION|CORP|COMPANY|CO|LIMITED|LTD|PLC|"
    r"HOLDINGS?|GROUP|COMMON|STOCK|ORDINARY|SHARES?|CLASS|WARRANTS?|WTS?|"
    r"UNITS?|RIGHTS?)\b",
    re.IGNORECASE,
)

FINANCIAL_METRICS = {
    "income": {
        "revenue": ("revenue", "currency"),
        "costOfRevenue": ("cost_of_revenue", "currency"),
        "grossProfit": ("gross_profit", "currency"),
        "operatingIncome": ("operating_income", "currency"),
        "incomeBeforeTax": ("pretax_income", "currency"),
        "netIncome": ("net_income", "currency"),
        "eps": ("eps", "per_share"),
        "epsDiluted": ("eps_diluted", "per_share"),
        "weightedAverageShsOut": ("weighted_average_shares", "shares"),
        "weightedAverageShsOutDil": ("weighted_average_shares_diluted", "shares"),
    },
    "balance": {
        "cashAndCashEquivalents": ("cash_and_equivalents", "currency"),
        "totalCurrentAssets": ("current_assets", "currency"),
        "totalAssets": ("total_assets", "currency"),
        "totalCurrentLiabilities": ("current_liabilities", "currency"),
        "totalLiabilities": ("total_liabilities", "currency"),
        "totalStockholdersEquity": ("total_equity", "currency"),
        "totalDebt": ("total_debt", "currency"),
        "netDebt": ("net_debt", "currency"),
    },
    "cashflow": {
        "netCashProvidedByOperatingActivities": ("operating_cash_flow", "currency"),
        "operatingCashFlow": ("operating_cash_flow", "currency"),
        "capitalExpenditure": ("capital_expenditure", "currency"),
        "freeCashFlow": ("free_cash_flow", "currency"),
        "commonStockIssued": ("common_stock_issued", "currency"),
        "commonStockRepurchased": ("common_stock_repurchased", "currency"),
        "dividendsPaid": ("dividends_paid", "currency"),
    },
}
STATEMENT_TYPES = {"income": "IS", "balance": "BS", "cashflow": "CF"}


def _raw_frame(path: str) -> pd.DataFrame:
    payload = read_bytes(path)
    if payload is None or not payload.strip():
        return pd.DataFrame()
    stripped = payload.lstrip()
    if stripped[:1] in {b"[", b"{"}:
        try:
            value = json.loads(payload)
        except (TypeError, ValueError):
            return pd.DataFrame()
        if isinstance(value, list):
            return pd.DataFrame(item for item in value if isinstance(item, dict))
        if isinstance(value, dict):
            for key in ("data", "historical", "results"):
                rows = value.get(key)
                if isinstance(rows, list):
                    return pd.DataFrame(
                        item for item in rows if isinstance(item, dict)
                    )
            return pd.DataFrame([value])
        return pd.DataFrame()
    try:
        return pd.read_csv(io.BytesIO(payload))
    except (pd.errors.EmptyDataError, UnicodeDecodeError):
        return pd.DataFrame()


def _as_bool(value) -> bool | None:
    if value is None or (not isinstance(value, bool) and pd.isna(value)):
        return None
    if isinstance(value, bool):
        return value
    rendered = str(value).strip().lower()
    if rendered in {"true", "1", "yes"}:
        return True
    if rendered in {"false", "0", "no"}:
        return False
    return None


def _text(value) -> str | None:
    if value is None or (not isinstance(value, str) and pd.isna(value)):
        return None
    rendered = str(value).strip()
    return rendered or None


def _parse_date(value) -> date | None:
    rendered = _text(value)
    if not rendered:
        return None
    try:
        return date.fromisoformat(rendered[:10])
    except ValueError:
        return None


def _parse_timestamp(value) -> datetime | None:
    rendered = _text(value)
    if not rendered:
        return None
    parsed = pd.to_datetime(rendered, errors="coerce")
    if pd.isna(parsed):
        return None
    timestamp = pd.Timestamp(parsed)
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize(
            "America/New_York", ambiguous="raise", nonexistent="shift_forward",
        )
    return timestamp.tz_convert("UTC").to_pydatetime()


def _number(value) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return None if pd.isna(parsed) else parsed


def _integer(value) -> int | None:
    parsed = _number(value)
    return None if parsed is None else int(parsed)


def _exchange(row: dict) -> str | None:
    raw = _text(
        row.get("exchangeShortName")
        or row.get("exchange")
        or row.get("exchangeFullName")
    )
    if not raw:
        return None
    normalized = re.sub(r"[^A-Z]", "", raw.upper())
    if normalized.startswith("NASDAQ"):
        return "NASDAQ"
    if normalized in {"AMEX", "NYSEAMERICAN", "AMERICANSTOCKEXCHANGE"}:
        return "AMEX"
    if normalized in {"NYSE", "NEWYORKSTOCKEXCHANGE"}:
        return "NYSE"
    return None


def _country_code(value) -> str | None:
    rendered = (_text(value) or "").upper()
    aliases = {"US": "US", "USA": "US", "UNITED STATES": "US"}
    return aliases.get(rendered, rendered[:2] or None)


def _issuer_key(value) -> str:
    """Normalize provider labels only for base/suffix sibling comparison."""
    rendered = ISSUER_NAME_NOISE.sub(" ", _text(value) or "")
    return re.sub(r"[^A-Z0-9]", "", rendered.upper())


def _non_equity_suffix_reason(
    symbol: str,
    symbol_names: dict[str, set[str]],
) -> str | None:
    """Identify Nasdaq-style W/U/R/Z instruments when the base sibling exists.

    A suffix by itself is not enough: legitimate company tickers can end with
    these letters.  Requiring a same-issuer base symbol avoids that false
    positive while catching provider rows whose company name omits "Warrant"
    or "Unit".
    """
    normalized = symbol.upper()
    if not re.fullmatch(r"[A-Z0-9]+", normalized) or len(normalized) < 2:
        return None
    reason = NON_EQUITY_SUFFIXES.get(normalized[-1])
    base = normalized[:-1]
    if reason is None or base not in symbol_names:
        return None
    names = {_issuer_key(name) for name in symbol_names.get(normalized, set())}
    base_names = {_issuer_key(name) for name in symbol_names.get(base, set())}
    names.discard("")
    base_names.discard("")
    return reason if names & base_names else None


@lru_cache(maxsize=1)
def _xnys_calendar():
    return xcals.get_calendar("XNYS")


@lru_cache(maxsize=8192)
def _is_xnys_session(value: date) -> bool:
    return bool(_xnys_calendar().is_session(pd.Timestamp(value)))


def _latest_snapshot_files(pattern: str, target_date: date | None) -> list[str]:
    paths = sorted(glob.glob(pattern))
    by_snapshot: dict[date, list[str]] = defaultdict(list)
    for path in paths:
        match = re.search(r"snapshot_date=(\d{4}-\d{2}-\d{2})", path)
        if not match:
            continue
        snapshot = date.fromisoformat(match.group(1))
        if target_date is None or snapshot <= target_date:
            by_snapshot[snapshot].append(path)
    if not by_snapshot:
        return []
    return by_snapshot[max(by_snapshot)]


def _all_snapshot_files(pattern: str, target_date: date | None) -> list[str]:
    """Return files from ALL snapshots (<= target_date), not just the latest.

    For monotonic cumulative sets like delisted companies: a partial recent
    snapshot (e.g. a daily run that fetched only page 0) must not shadow an
    earlier complete snapshot. A delisted company never un-delists, so unioning
    every snapshot's pages (deduped downstream by symbol) is complete and safe.
    """
    selected: list[str] = []
    for path in sorted(glob.glob(pattern)):
        match = re.search(r"snapshot_date=(\d{4}-\d{2}-\d{2})", path)
        if not match:
            continue
        snapshot = date.fromisoformat(match.group(1))
        if target_date is None or snapshot <= target_date:
            selected.append(path)
    return selected


def _universe_files(base: str, target_date: date | None) -> tuple[list[str], list[str], list[str]]:
    screener = _latest_snapshot_files(
        f"{base}/stock/fmp/universe/company-screener/snapshot_date=*/response.*",
        target_date,
    )
    screener += _latest_snapshot_files(
        f"{base}/stock/fmp/universe/company-screener/snapshot_date=*/page=*/response.*",
        target_date,
    )
    profiles = _latest_snapshot_files(
        f"{base}/stock/fmp/universe/profile-bulk/snapshot_date=*/part=*/response.*",
        target_date,
    )
    # delisted is cumulative: union across ALL snapshots so a partial recent
    # daily snapshot doesn't hide the full backfill snapshot (survivorship).
    delisted = _all_snapshot_files(
        f"{base}/stock/fmp/universe/delisted/snapshot_date=*/page=*/response.*",
        target_date,
    )
    symbol_changes = _latest_snapshot_files(
        f"{base}/stock/fmp/universe/symbol-change/snapshot_date=*/response.*",
        target_date,
    )
    symbol_changes += _latest_snapshot_files(
        f"{base}/stock/fmp/universe/symbol-change/snapshot_date=*/limit=*/response.*",
        target_date,
    )
    return screener + profiles, delisted, symbol_changes


def prepare_universe(
    base: str,
    target_date: date | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    source_files, delisted_files, change_files = _universe_files(base, target_date)
    merged: dict[str, dict] = {}
    profile_observations = []
    for path in source_files:
        frame = _raw_frame(path)
        observed_at = _manifest_received_at(path)
        for raw in frame.to_dict("records"):
            symbol = _text(raw.get("symbol"))
            if not symbol:
                continue
            if observed_at is not None:
                # Wait for the merged universe admission decision before
                # cleaning/hashing profiles for non-equity/foreign symbols.
                profile_observations.append((symbol, raw, path, observed_at))
            current = merged.setdefault(symbol, {"symbol": symbol, "_files": []})
            for key, value in raw.items():
                if _text(value) is not None or isinstance(value, bool):
                    current[key] = value
            current["_files"].append(path)

    delisted_by_symbol: dict[str, dict] = {}
    for path in delisted_files:
        for raw in _raw_frame(path).to_dict("records"):
            symbol = _text(raw.get("symbol"))
            if not symbol:
                continue
            entry = delisted_by_symbol.setdefault(symbol, {})
            entry.update(raw)
            entry["_source_file"] = path
            current = merged.setdefault(symbol, {"symbol": symbol, "_files": []})
            for key, value in raw.items():
                if _text(value) is not None:
                    current.setdefault(key, value)
            current["_files"].append(path)

    changes: list[dict] = []
    for path in change_files:
        for raw in _raw_frame(path).to_dict("records"):
            raw["_source_file"] = path
            changes.append(raw)

    symbol_names: dict[str, set[str]] = defaultdict(set)
    for symbol, row in merged.items():
        name = _text(row.get("companyName") or row.get("name"))
        if name:
            symbol_names[symbol.upper()].add(name)
    for raw in changes:
        name = _text(raw.get("companyName") or raw.get("name"))
        if not name:
            continue
        for key in ("oldSymbol", "newSymbol"):
            symbol = _text(raw.get(key))
            if symbol:
                symbol_names[symbol.upper()].add(name)

    exclusions: Counter[str] = Counter()
    samples: list[dict] = []
    admitted: dict[str, dict] = {}
    for symbol, row in sorted(merged.items()):
        exchange = _exchange(row)
        is_etf = _as_bool(row.get("isEtf"))
        is_fund = _as_bool(row.get("isFund"))
        is_active = _as_bool(row.get("isActivelyTrading"))
        name = _text(row.get("companyName") or row.get("name")) or symbol
        reason = None
        if exchange not in SUPPORTED_EXCHANGES:
            reason = "UNSUPPORTED_EXCHANGE"
        elif is_etf is True:
            reason = "ETF"
        elif is_fund is True:
            reason = "FUND"
        elif NON_EQUITY_NAME.search(name):
            reason = "NON_EQUITY_NAME"
        elif exchange == "NASDAQ" and (
            suffix_reason := _non_equity_suffix_reason(symbol, symbol_names)
        ) is not None:
            reason = suffix_reason
        elif is_active is False and symbol not in delisted_by_symbol:
            # profile-bulk retains stale, renamed tickers without a delisting
            # date. Only the dedicated delisted endpoint is accepted as
            # evidence that an inactive symbol belongs in the history.
            reason = "INACTIVE_UNDATED"
        elif (is_etf is None or is_fund is None) and symbol not in delisted_by_symbol:
            reason = "AMBIGUOUS_CLASSIFICATION"
        if reason:
            exclusions[reason] += 1
            if len(samples) < 30:
                samples.append({
                    "symbol": symbol,
                    "name": name,
                    "exchange": exchange,
                    "isEtf": is_etf,
                    "isFund": is_fund,
                    "reason": reason,
                })
            continue
        industry = _text(row.get("industry")) or ""
        if _as_bool(row.get("isAdr")) is True:
            instrument_type = "adr"
        elif PREFERRED_NAME.search(name):
            instrument_type = "preferred_stock"
        elif REIT_NAME.search(name) or REIT_NAME.search(industry):
            instrument_type = "reit"
        else:
            instrument_type = "common_stock"
        currency = (_text(row.get("currency")) or "USD").upper()
        admitted[symbol] = {
            **row,
            "name": name,
            "exchange_norm": exchange,
            "instrument_type": instrument_type,
            "currency_norm": currency,
            "listed_from_norm": _parse_date(row.get("ipoDate")),
            "listed_to_norm": _parse_date(row.get("delistedDate")),
        }

    # Symbol changes identify one security across ticker episodes.  The current
    # symbol owns the asset; old tickers become bounded identifiers.
    canonical = {symbol: symbol for symbol in admitted}
    change_by_new: dict[str, list[tuple[str, date]]] = defaultdict(list)
    excluded_change_identifiers = 0
    excluded_change_samples: list[dict] = []
    for raw in sorted(changes, key=lambda item: str(item.get("date") or "")):
        old = _text(raw.get("oldSymbol"))
        new = _text(raw.get("newSymbol"))
        changed = _parse_date(raw.get("date"))
        if not old or not new or not changed or new not in admitted:
            continue
        # FMP occasionally emits a historical no-op row whose old and new
        # symbols are identical (for example GOAI -> GOAI dated 1969-12-31).
        # It is not a ticker transition and must not turn the current ticker
        # into an impossible bounded interval.
        if old == new:
            continue
        suffix_reason = _non_equity_suffix_reason(old, symbol_names)
        if suffix_reason:
            excluded_change_identifiers += 1
            if len(excluded_change_samples) < 30:
                excluded_change_samples.append({
                    "oldSymbol": old,
                    "newSymbol": new,
                    "date": changed.isoformat(),
                    "reason": suffix_reason,
                })
            continue
        canonical[old] = canonical.get(new, new)
        change_by_new[new].append((old, changed))
        if old in admitted and old != new:
            admitted.pop(old)

    asset_rows: list[dict] = []
    identifier_rows: list[dict] = []
    for symbol, row in admitted.items():
        natural_key = f"FMP:{canonical.get(symbol, symbol)}"
        listed_from = row["listed_from_norm"]
        listed_to = row["listed_to_norm"]
        asset_rows.append({
            "natural_key": natural_key,
            "name": row["name"],
            "asset_type": "stock",
            "instrument_type": row["instrument_type"],
            "exchange": row["exchange_norm"],
            "currency": row["currency_norm"],
            "country_code": _country_code(row.get("country")),
            "base_currency": row["currency_norm"],
            "listed_from": listed_from,
            "listed_to": listed_to,
            "is_etf": False,
            "is_fund": False,
            "source_file": (row.get("_files") or [None])[-1],
        })
        current_valid_from = listed_from or date.min
        for old, changed in change_by_new.get(symbol, []):
            identifier_rows.append({
                "natural_key": natural_key,
                "source": "FMP",
                "identifier": old,
                "identifier_type": "ticker",
                "valid_from": date.min,
                "valid_to": changed - timedelta(days=1),
                "source_file": None,
            })
            current_valid_from = max(current_valid_from, changed)
        identifier_rows.append({
            "natural_key": natural_key,
            "source": "FMP",
            "identifier": symbol,
            "identifier_type": "ticker",
            "valid_from": current_valid_from,
            "valid_to": listed_to,
            "source_file": (row.get("_files") or [None])[-1],
        })
        for identifier_type, key in (
            ("cik", "cik"), ("cusip", "cusip"), ("isin", "isin"),
        ):
            value = _text(row.get(key))
            if value:
                identifier_rows.append({
                    "natural_key": natural_key,
                    "source": "FMP",
                    "identifier": value,
                    "identifier_type": identifier_type,
                    "valid_from": listed_from or date.min,
                    "valid_to": listed_to,
                    "source_file": (row.get("_files") or [None])[-1],
                })

    assets = pd.DataFrame(asset_rows)
    identifiers = pd.DataFrame(identifier_rows)
    ambiguous_identifier_samples: list[dict] = []
    ambiguous_identifier_rows_removed = 0
    if not identifiers.empty:
        identifiers = identifiers.drop_duplicates(
            ["natural_key", "source", "identifier_type", "identifier", "valid_from"],
            keep="last",
        ).reset_index(drop=True)
        # FMP occasionally assigns the same current CUSIP/ISIN to multiple
        # active symbols. Keep the raw values in Bronze, but omit every
        # ambiguous mapping from Silver so point-in-time joins stay safe.
        current_security_id = (
            identifiers["valid_to"].isna()
            & identifiers["identifier_type"].isin(["cusip", "isin"])
        )
        ambiguous = current_security_id & identifiers.duplicated(
            ["source", "identifier_type", "identifier"], keep=False,
        )
        ambiguous_identifier_rows_removed = int(ambiguous.sum())
        ambiguous_identifier_samples = identifiers.loc[
            ambiguous,
            ["natural_key", "identifier_type", "identifier"],
        ].head(30).to_dict("records")
        identifiers = identifiers.loc[~ambiguous].reset_index(drop=True)
    admitted_symbols = set(identifiers.loc[
        identifiers["identifier_type"].eq("ticker"), "identifier",
    ]) if not identifiers.empty else set()
    assets.attrs["research_observations"] = [
        research_observations.observation(
            identifier=symbol, source="FMP", dataset="FMP_PROFILE",
            raw_row=raw, source_file=path, available_at=observed_at,
            observed_at=observed_at,
        )
        for symbol, raw, path, observed_at in profile_observations
        if symbol in admitted_symbols
    ]
    return assets, identifiers, {
        "raw_symbol_count": len(merged),
        "admitted_symbol_count": len(admitted),
        "excluded_symbol_count": sum(exclusions.values()),
        "excluded_by_reason": dict(exclusions),
        "excluded_samples": samples,
        "ambiguous_identifier_rows_removed": ambiguous_identifier_rows_removed,
        "ambiguous_identifier_samples": ambiguous_identifier_samples,
        "excluded_symbol_change_identifier_count": excluded_change_identifiers,
        "excluded_symbol_change_identifier_samples": excluded_change_samples,
        "source_file_count": len(source_files) + len(delisted_files) + len(change_files),
    }


def _manifest_received_at(path: str) -> datetime | None:
    manifest_path = (
        path.rsplit("/", 1)[0] + "/manifest.json"
        if path.startswith("s3://") else str(Path(path).with_name("manifest.json"))
    )
    raw = read_bytes(manifest_path)
    if raw is None:
        return None
    try:
        return _parse_timestamp(json.loads(raw).get("received_at"))
    except (TypeError, ValueError):
        return None


def _split_events(base: str) -> dict[str, list[tuple[date, float]]]:
    unique: dict[str, set[tuple[date, float]]] = defaultdict(set)
    for path in glob.glob(f"{base}/corporate_actions/fmp/splits/**/response.*", recursive=True):
        for row in _raw_frame(path).to_dict("records"):
            symbol = _text(row.get("symbol"))
            event_date = _parse_date(row.get("date"))
            numerator = _number(row.get("numerator"))
            denominator = _number(row.get("denominator"))
            if symbol and event_date and numerator and denominator and denominator != 0:
                unique[symbol].add((event_date, numerator / denominator))
    return {
        symbol: sorted(events)
        for symbol, events in unique.items()
    }


def _ticker_metadata(assets: pd.DataFrame, identifiers: pd.DataFrame) -> dict[str, dict]:
    if assets.empty or identifiers.empty:
        return {}
    by_key = assets.set_index("natural_key").to_dict("index")
    return {
        str(row.identifier): {
            **by_key[str(row.natural_key)],
            "natural_key": str(row.natural_key),
            "valid_from": row.valid_from,
            "valid_to": row.valid_to,
        }
        for row in identifiers.itertuples(index=False)
        if row.identifier_type == "ticker" and str(row.natural_key) in by_key
    }


def prepare_prices(
    base: str,
    assets: pd.DataFrame,
    identifiers: pd.DataFrame,
    target_date: date | None = None,
    year: int | None = None,
) -> tuple[pd.DataFrame, dict]:
    ticker_meta = _ticker_metadata(assets, identifiers)
    splits = _split_events(base)
    paths = sorted(glob.glob(f"{base}/stock/fmp/eod-bulk/date=*/response.*"))
    rows: list[dict] = []
    input_rows = 0
    non_session_count = 0
    non_session_samples: list[dict] = []
    for path in paths:
        match = re.search(r"/date=(\d{4}-\d{2}-\d{2})/", path.replace("\\", "/"))
        if not match:
            continue
        partition_date = date.fromisoformat(match.group(1))
        if target_date is not None and partition_date != target_date:
            continue
        if year is not None and partition_date.year != year:
            continue
        frame = _raw_frame(path)
        input_rows += len(frame)
        received_at = _manifest_received_at(path)
        for raw in frame.to_dict("records"):
            symbol = _text(raw.get("symbol"))
            meta = ticker_meta.get(symbol or "")
            trade_date = _parse_date(raw.get("date")) or partition_date
            if (
                meta is None
                or trade_date < meta["valid_from"]
                or (
                    meta["valid_to"] is not None
                    and trade_date > meta["valid_to"]
                )
            ):
                continue
            if not _is_xnys_session(trade_date):
                non_session_count += 1
                if len(non_session_samples) < 20:
                    non_session_samples.append({
                        "symbol": symbol,
                        "trade_date": trade_date.isoformat(),
                        "source_file": path,
                    })
                continue
            factor = 1.0
            for split_date, split_ratio in splits.get(symbol, []):
                if split_date > trade_date:
                    factor *= split_ratio
            adjusted_close = _number(raw.get("close"))
            total_return = _number(raw.get("adjClose"))
            rows.append({
                "natural_key": meta["natural_key"],
                "identifier": symbol,
                "source": "FMP",
                "trade_date": trade_date,
                "open": None if _number(raw.get("open")) is None else _number(raw.get("open")) * factor,
                "high": None if _number(raw.get("high")) is None else _number(raw.get("high")) * factor,
                "low": None if _number(raw.get("low")) is None else _number(raw.get("low")) * factor,
                "close": None if adjusted_close is None else adjusted_close * factor,
                "adj_close": adjusted_close,
                "total_return_close": total_return,
                "currency": meta["base_currency"],
                "vwap": _number(raw.get("vwap")),
                "available_at": received_at,
                "volume": _integer(raw.get("volume")),
                "trading_value": None,
                "shares": None,
                "market_cap": None,
                "market": meta["exchange"],
                "asset_type": "stock",
                "source_file": path,
            })
    frame = pd.DataFrame(rows)
    invalid_samples: list[dict] = []
    invalid_count = 0
    duplicate_samples: list[dict] = []
    duplicate_count = 0
    if not frame.empty:
        numeric = frame[["open", "high", "low", "close"]].apply(
            pd.to_numeric, errors="coerce",
        )
        invalid_mask = (
            numeric["high"].lt(numeric[["open", "close"]].max(axis=1))
            | numeric["low"].gt(numeric[["open", "close"]].min(axis=1))
            | numeric[["open", "high", "low", "close"]].le(0).any(axis=1)
        )
        invalid = frame[invalid_mask]
        invalid_count = len(invalid)
        invalid_samples = invalid.head(20).to_dict("records")
        frame = frame[~invalid_mask].copy()
        key = ["natural_key", "source", "trade_date"]
        duplicates = frame[frame.duplicated(key, keep=False)].sort_values(key)
        duplicate_count = len(duplicates) - duplicates[key].drop_duplicates().shape[0]
        duplicate_samples = duplicates.head(20).to_dict("records")
        frame = frame.drop_duplicates(key, keep="last").reset_index(drop=True)
    return frame, {
        "input_rows": input_rows,
        "transformed_rows": len(frame),
        "excluded_rows": input_rows - len(frame),
        "rejected_rows": 0,
        "invalid_ohlc_excluded": {
            "row_count": invalid_count,
            "samples": invalid_samples,
        },
        "non_session_rows_excluded": {
            "row_count": non_session_count,
            "samples": non_session_samples,
        },
        "duplicate_price_rows_removed": {
            "row_count": duplicate_count,
            "samples": duplicate_samples,
        },
        "price_semantics": {
            "close": "raw reconstructed from split-adjusted close",
            "adj_close": "FMP close (split-adjusted)",
            "total_return_close": "FMP adjClose (dividend-adjusted)",
        },
    }


def prepare_fx(
    base: str,
    target_date: date | None = None,
    year: int | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict]:
    asset_rows = [{
        "natural_key": "FMP:FX:USDKRW",
        "name": "USD/KRW",
        "asset_type": "fx",
        "instrument_type": "fx",
        "exchange": "FX",
        "currency": "KRW",
        "country_code": None,
        "base_currency": "KRW",
        "listed_from": None,
        "listed_to": None,
        "is_etf": False,
        "is_fund": False,
        "source_file": None,
    }]
    identifier_rows = [{
        "natural_key": "FMP:FX:USDKRW",
        "source": "FMP",
        "identifier": "USDKRW",
        "identifier_type": "fx_pair",
        "valid_from": date.min,
        "valid_to": None,
        "source_file": None,
    }]
    paths = sorted(glob.glob(f"{base}/fx/fmp/pair=USDKRW/**/response.*", recursive=True))
    rows = []
    input_rows = 0
    for path in paths:
        frame = _raw_frame(path)
        input_rows += len(frame)
        received_at = _manifest_received_at(path)
        for raw in frame.to_dict("records"):
            trade_date = _parse_date(raw.get("date"))
            if trade_date is None or (
                target_date is not None and trade_date != target_date
            ):
                continue
            if year is not None and trade_date.year != year:
                continue
            close = _number(raw.get("close"))
            rows.append({
                "natural_key": "FMP:FX:USDKRW",
                "identifier": "USDKRW",
                "source": "FMP_FX",
                "trade_date": trade_date,
                "open": _number(raw.get("open")),
                "high": _number(raw.get("high")),
                "low": _number(raw.get("low")),
                "close": close,
                "adj_close": close,
                "total_return_close": close,
                "currency": "KRW",
                "vwap": _number(raw.get("vwap")),
                "available_at": received_at,
                "volume": _integer(raw.get("volume")),
                "trading_value": None,
                "shares": None,
                "market_cap": None,
                "market": "FX",
                "asset_type": "fx",
                "source_file": path,
            })
    invalid_samples: list[dict] = []
    invalid_count = 0
    if rows:
        frame = (
            pd.DataFrame(rows)
            .sort_values("source_file")
            .drop_duplicates(["identifier", "source", "trade_date"], keep="last")
        )
        numeric = frame[["open", "high", "low", "close"]].apply(
            pd.to_numeric, errors="coerce",
        )
        invalid_mask = (
            numeric["high"].lt(numeric[["open", "close"]].max(axis=1))
            | numeric["low"].gt(numeric[["open", "close"]].min(axis=1))
            | numeric[["open", "high", "low", "close"]].le(0).any(axis=1)
        )
        invalid_count = int(invalid_mask.sum())
        invalid_samples = frame.loc[invalid_mask].head(20).to_dict("records")
        rows = frame.loc[~invalid_mask].to_dict("records")
    if not rows:
        asset_rows = []
        identifier_rows = []
    return (
        pd.DataFrame(asset_rows),
        pd.DataFrame(identifier_rows),
        pd.DataFrame(rows),
        {
            "input_rows": input_rows,
            "transformed_rows": len(rows),
            "excluded_rows": input_rows - len(rows),
            "invalid_ohlc_excluded": {
                "row_count": invalid_count,
                "samples": invalid_samples,
            },
        },
    )


def prepare_commodities(
    base: str,
    target_date: date | None = None,
    year: int | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict]:
    """Normalize the 28 allowlisted FMP continuous-futures series."""
    list_paths = _latest_snapshot_files(
        f"{base}/commodities/fmp/list/snapshot_date=*/response.*",
        target_date,
    )
    provider_symbols: set[str] = set()
    provider_currencies: dict[str, str] = {}
    for path in list_paths:
        listed = _raw_frame(path)
        if "symbol" in listed:
            provider_symbols.update(listed["symbol"].dropna().astype(str))
            for row in listed.to_dict("records"):
                symbol = _text(row.get("symbol"))
                currency = (_text(row.get("currency")) or "").upper()
                if symbol:
                    provider_currencies[symbol] = currency
    list_source = list_paths[-1] if list_paths else None

    asset_rows = []
    identifier_rows = []
    for spec in COMMODITY_SPECS:
        natural_key = f"FMP:COMMODITY:{spec.symbol}"
        asset_rows.append({
            "natural_key": natural_key,
            "name": spec.name,
            "asset_type": "commodity",
            "instrument_type": "commodity_future_continuous",
            "exchange": "COMMODITY",
            "currency": "USD",
            "country_code": None,
            "base_currency": "USD",
            "price_unit": spec.price_unit,
            "listed_from": None,
            "listed_to": None,
            "is_etf": False,
            "is_fund": False,
            "source_file": list_source,
        })
        identifier_rows.append({
            "natural_key": natural_key,
            "source": "FMP",
            "identifier": spec.symbol,
            "identifier_type": "commodity_symbol",
            "valid_from": date.min,
            "valid_to": None,
            "source_file": list_source,
        })

    rows: list[dict] = []
    input_rows = 0
    pattern = f"{base}/commodities/fmp/eod/symbol=*/**/response.*"
    for path in sorted(glob.glob(pattern, recursive=True)):
        match = re.search(r"/symbol=([^/]+)/", path.replace("\\", "/"))
        path_symbol = match.group(1) if match else ""
        spec = COMMODITY_BY_SYMBOL.get(path_symbol)
        if spec is None:
            continue
        received_at = _manifest_received_at(path)
        for raw in _raw_frame(path).to_dict("records"):
            input_rows += 1
            symbol = _text(raw.get("symbol")) or path_symbol
            if symbol != path_symbol:
                continue
            trade_date = _parse_date(raw.get("date"))
            if trade_date is None:
                continue
            if target_date is not None:
                # A Monday daily load owns both the Sunday-evening futures
                # session and Monday.  Equity/FX candidates remain restricted
                # to target_date by their own normalizers.
                accepted_dates = {target_date}
                if target_date.weekday() == 0:
                    accepted_dates.add(target_date - timedelta(days=1))
                if trade_date not in accepted_dates:
                    continue
            if year is not None and trade_date.year != year:
                continue
            scale = spec.price_scale

            def scaled(field: str) -> float | None:
                value = _number(raw.get(field))
                return None if value is None else value * scale

            close = scaled("close")
            rows.append({
                "natural_key": f"FMP:COMMODITY:{symbol}",
                "identifier": symbol,
                "source": "FMP_COMMODITY",
                "trade_date": trade_date,
                "open": scaled("open"),
                "high": scaled("high"),
                "low": scaled("low"),
                "close": close,
                "adj_close": close,
                "total_return_close": None,
                "currency": "USD",
                "vwap": scaled("vwap"),
                "available_at": received_at,
                "volume": _integer(raw.get("volume")),
                "trading_value": None,
                "shares": None,
                "market_cap": None,
                "market": None,
                "asset_type": "commodity",
                "price_unit": spec.price_unit,
                "raw_currency": spec.raw_currency,
                "source_file": path,
            })

    frame = pd.DataFrame(rows)
    non_session_samples: list[dict] = []
    non_session_count = 0
    invalid_samples: list[dict] = []
    invalid_count = 0
    duplicate_samples: list[dict] = []
    duplicate_count = 0
    roll_count = 0
    roll_samples: list[dict] = []
    if not frame.empty:
        # Commodity futures commonly open on Sunday evening, and FMP keeps
        # those observations under the Sunday calendar date.  Saturday is the
        # only universally closed session day; preserve such provider rows in
        # Bronze but do not expose them as tradable Silver observations.
        parsed_dates = pd.to_datetime(frame["trade_date"], errors="coerce")
        non_session_mask = parsed_dates.dt.weekday.eq(5)
        non_session = frame[non_session_mask]
        non_session_count = len(non_session)
        non_session_samples = non_session.head(20).to_dict("records")
        frame = frame[~non_session_mask].copy()
        numeric = frame[["open", "high", "low", "close"]].apply(
            pd.to_numeric, errors="coerce",
        )
        invalid_mask = (
            numeric.isna().any(axis=1)
            | numeric["high"].lt(numeric[["open", "close"]].max(axis=1))
            | numeric["low"].gt(numeric[["open", "close"]].min(axis=1))
        )
        invalid = frame[invalid_mask]
        invalid_count = len(invalid)
        invalid_samples = invalid.head(20).to_dict("records")
        frame = frame[~invalid_mask].copy()
        frame = frame.sort_values(
            ["identifier", "trade_date", "available_at", "source_file"],
            na_position="first",
        )
        key = ["natural_key", "source", "trade_date"]
        duplicates = frame[frame.duplicated(key, keep=False)]
        duplicate_count = len(duplicates) - duplicates[key].drop_duplicates().shape[0]
        duplicate_samples = duplicates.head(20).to_dict("records")
        frame = frame.drop_duplicates(key, keep="last").reset_index(drop=True)

        ordered = frame.sort_values(["identifier", "trade_date"]).copy()
        previous = ordered.groupby("identifier")["close"].shift(1)
        move = ordered["close"] / previous - 1
        possible_roll = ordered[previous.ne(0) & move.abs().gt(0.20)].copy()
        roll_count = len(possible_roll)
        if roll_count:
            possible_roll["previous_close"] = previous[possible_roll.index]
            possible_roll["return"] = move[possible_roll.index]
            roll_samples = possible_roll.head(20).to_dict("records")

    missing_from_provider = (
        sorted(set(COMMODITY_BY_SYMBOL) - provider_symbols)
        if list_paths else []
    )
    currency_mismatches = [
        {
            "symbol": spec.symbol,
            "expected": spec.raw_currency,
            "actual": provider_currencies.get(spec.symbol),
        }
        for spec in COMMODITY_SPECS
        if (
            spec.symbol in provider_symbols
            and provider_currencies.get(spec.symbol) != spec.raw_currency
        )
    ]
    return (
        pd.DataFrame(asset_rows),
        pd.DataFrame(identifier_rows),
        frame,
        {
            "expected_series": len(COMMODITY_SPECS),
            "provider_list_present": bool(list_paths),
            "provider_symbol_count": len(provider_symbols),
            "missing_from_provider": missing_from_provider,
            "provider_currency_mismatches": currency_mismatches,
            "input_rows": input_rows,
            "transformed_rows": len(frame),
            "normalized_usx_rows": int(
                frame.get("raw_currency", pd.Series(dtype=str)).eq("USX").sum()
            ),
            "non_session_rows_excluded": {
                "row_count": non_session_count,
                "samples": non_session_samples,
            },
            "invalid_ohlc_excluded": {
                "row_count": invalid_count,
                "samples": invalid_samples,
            },
            "duplicate_price_rows_removed": {
                "row_count": duplicate_count,
                "samples": duplicate_samples,
            },
            "possible_roll": {
                "row_count": roll_count,
                "samples": roll_samples,
            },
        },
    )


def _financial_kind(path: str) -> str | None:
    normalized = path.replace("\\", "/")
    for kind in FINANCIAL_METRICS:
        if f"/financials/fmp/{kind}/" in normalized:
            return kind
    if "/financials/fmp/by-symbol/" in normalized:
        name = Path(path).parent.name
        return {"income-statement": "income", "balance-sheet-statement": "balance", "cash-flow-statement": "cashflow"}.get(name)
    return None


def prepare_fundamentals(
    base: str,
    identifiers: pd.DataFrame,
    year: int | None = None,
    research_target_date: date | None = None,
) -> tuple[pd.DataFrame, dict]:
    ticker_rows = (
        identifiers.loc[identifiers["identifier_type"].eq("ticker")]
        if not identifiers.empty else identifiers
    )
    symbol_to_key = {
        str(row.identifier): str(row.natural_key)
        for row in ticker_rows.itertuples(index=False)
    }
    symbols = set(symbol_to_key)
    paths = sorted(set(
        glob.glob(f"{base}/financials/fmp/income/**/response.*", recursive=True)
        + glob.glob(f"{base}/financials/fmp/balance/**/response.*", recursive=True)
        + glob.glob(f"{base}/financials/fmp/cashflow/**/response.*", recursive=True)
        + glob.glob(f"{base}/financials/fmp/by-symbol/**/response.*", recursive=True)
    ))
    rows: list[dict] = []
    input_rows = 0
    nonfinite_values = 0
    missing_available_at_values = 0
    statement_observations = []
    for path in paths:
        kind = _financial_kind(path)
        if kind is None:
            continue
        year_partition = re.search(r"/year=(\d{4})/", path.replace("\\", "/"))
        if year is not None and year_partition and int(year_partition.group(1)) != year:
            continue
        preserve_research = (
            research_target_date is None
            or f"/snapshot_date={research_target_date.isoformat()}/" in path.replace("\\", "/")
        )
        frame = _raw_frame(path)
        input_rows += len(frame)
        for raw in frame.to_dict("records"):
            symbol = _text(raw.get("symbol"))
            if symbol not in symbols:
                continue
            period_end = _parse_date(raw.get("date"))
            fiscal_period = (_text(raw.get("period")) or "").upper()
            if not period_end or fiscal_period not in {"FY", "Q1", "Q2", "Q3", "Q4"}:
                continue
            if year is not None and period_end.year != year:
                continue
            filed = _parse_date(raw.get("filingDate"))
            accepted_at = _parse_timestamp(raw.get("acceptedDate"))
            available_at = accepted_at
            if available_at is None and filed is not None:
                available_at = datetime.combine(
                    filed + timedelta(days=1), datetime.min.time(), timezone.utc,
                )
            available_date = available_at.date() if available_at else filed
            revision_key = (
                _text(raw.get("acceptedDate"))
                or _text(raw.get("filingDate"))
                or f"{symbol}:{period_end}:{fiscal_period}:{kind}"
            )
            reported_currency = (_text(raw.get("reportedCurrency")) or "").upper() or None
            if preserve_research and available_at is not None and available_at.date() > period_end:
                statement_observations.append(research_observations.observation(
                    identifier=symbol, source="FMP", dataset="FMP_STATEMENT",
                    raw_row=raw, source_file=path, available_at=available_at,
                    metadata={"statement_type": STATEMENT_TYPES[kind],
                              "period_end": period_end.isoformat()},
                ))
            for source_metric, (metric, unit_type) in FINANCIAL_METRICS[kind].items():
                value = _number(raw.get(source_metric))
                if value is None:
                    continue
                if not math.isfinite(value):
                    nonfinite_values += 1
                    continue
                if available_at is None:
                    # A Silver fundamental must be point-in-time safe. Keep
                    # the undated provider row in Bronze, but do not expose it
                    # to research queries where its availability is unknown.
                    missing_available_at_values += 1
                    continue
                rows.append({
                    "natural_key": symbol_to_key[symbol],
                    "identifier": symbol,
                    "source": "FMP",
                    "statement_type": STATEMENT_TYPES[kind],
                    "data_basis": "STANDARDIZED",
                    "period_end": period_end,
                    "fiscal_period": fiscal_period,
                    "fs_type": "UNKNOWN",
                    "filing_id": None,
                    "filed": filed,
                    "accepted_at": accepted_at,
                    "available_date": available_date,
                    "available_at": available_at,
                    "metric": metric,
                    "value": value,
                    "currency": reported_currency if unit_type != "shares" else None,
                    "unit_type": unit_type,
                    "revision_key": revision_key,
                    "source_file": path,
                })
    frame = pd.DataFrame(rows)
    before = len(frame)
    key = [
        "natural_key", "source", "statement_type", "data_basis", "period_end",
        "fiscal_period", "fs_type", "revision_key", "metric",
    ]
    if not frame.empty:
        frame = frame.sort_values("source_file").drop_duplicates(key, keep="last").reset_index(drop=True)
    frame.attrs["research_observations"] = statement_observations
    return frame, {
        "input_rows": input_rows,
        "transformed_rows": len(frame),
        "excluded_rows": input_rows - len(frame),
        "rejected_rows": 0,
        "duplicate_rows_removed": before - len(frame),
        "nonfinite_values_excluded": nonfinite_values,
        "missing_available_at_values_excluded": missing_available_at_values,
    }


def prepare_actions(
    base: str,
    identifiers: pd.DataFrame,
    year: int | None = None,
) -> tuple[pd.DataFrame, dict]:
    ticker_rows = (
        identifiers.loc[identifiers["identifier_type"].eq("ticker")]
        if not identifiers.empty else identifiers
    )
    symbol_to_key = {
        str(row.identifier): str(row.natural_key)
        for row in ticker_rows.itertuples(index=False)
    }
    symbols = set(symbol_to_key)
    records: list[dict] = []
    input_rows = 0
    patterns = {
        "split": f"{base}/corporate_actions/fmp/splits/**/response.*",
        "dividend": f"{base}/corporate_actions/fmp/dividends/**/response.*",
    }
    for kind, pattern in patterns.items():
        for path in glob.glob(pattern, recursive=True):
            year_partition = re.search(
                r"/year=(\d{4})/", path.replace("\\", "/"),
            )
            if year is not None and year_partition and int(year_partition.group(1)) != year:
                continue
            frame = _raw_frame(path)
            input_rows += len(frame)
            for raw in frame.to_dict("records"):
                symbol = _text(raw.get("symbol"))
                event_date = _parse_date(raw.get("date"))
                if symbol not in symbols or event_date is None:
                    continue
                if year is not None and event_date.year != year:
                    continue
                if kind == "split":
                    numerator = _number(raw.get("numerator"))
                    denominator = _number(raw.get("denominator"))
                    price_factor = (
                        denominator / numerator
                        if numerator and denominator else None
                    )
                    share_factor = (
                        numerator / denominator
                        if numerator and denominator else None
                    )
                    action_type = "stock_split"
                    cash_amount = None
                    adjusted_cash_amount = None
                    frequency = None
                    record_date = payment_date = announcement_date = None
                    source = "FMP_SPLIT"
                else:
                    numerator = denominator = price_factor = share_factor = None
                    action_type = "cash_dividend"
                    cash_amount = _number(raw.get("dividend"))
                    adjusted_cash_amount = _number(raw.get("adjDividend"))
                    frequency = _text(raw.get("frequency")) or None
                    announcement_date = _parse_date(raw.get("declarationDate"))
                    record_date = _parse_date(raw.get("recordDate"))
                    payment_date = _parse_date(raw.get("paymentDate"))
                    source = "FMP_DIVIDEND"
                canonical_material = {
                    **raw,
                    "symbol": symbol_to_key[symbol],
                }
                material = json.dumps(
                    canonical_material,
                    sort_keys=True,
                    default=str,
                    separators=(",", ":"),
                )
                records.append({
                    "natural_key": symbol_to_key[symbol],
                    "identifier": symbol,
                    "source": source,
                    "action_key": hashlib.sha256(material.encode()).hexdigest(),
                    "action_type": action_type,
                    "announcement_date": announcement_date,
                    "ex_date": event_date,
                    "record_date": record_date,
                    "payment_date": payment_date,
                    "cash_amount": cash_amount,
                    "adjusted_cash_amount": adjusted_cash_amount,
                    "frequency": frequency,
                    "currency": "USD",
                    "ratio_numerator": numerator,
                    "ratio_denominator": denominator,
                    "expected_price_factor": price_factor,
                    "share_count_factor": share_factor,
                    "status": "confirmed",
                    "confidence": "FMP_CALENDAR",
                    "filing_id": None,
                    "source_file": path,
                })
    frame = pd.DataFrame(records)
    before = len(frame)
    if not frame.empty:
        frame = frame.drop_duplicates(
            ["natural_key", "source", "action_key"], keep="last",
        ).reset_index(drop=True)
    return frame, {
        "input_rows": input_rows,
        "transformed_rows": len(frame),
        "excluded_rows": input_rows - len(frame),
        "rejected_rows": 0,
        "duplicate_rows_removed": before - len(frame),
    }


def market_closed(base: str, target_date: date) -> bool:
    if target_date.weekday() >= 5:
        return True
    paths = _latest_snapshot_files(
        f"{base}/market/fmp/holidays-by-exchange/snapshot_date=*/exchange=*/response.*",
        target_date,
    )
    closed_exchanges = set()
    for path in paths:
        match = re.search(r"/exchange=([^/]+)/", path.replace("\\", "/"))
        exchange = match.group(1) if match else None
        for row in _raw_frame(path).to_dict("records"):
            if _parse_date(row.get("date")) != target_date:
                continue
            is_closed = _as_bool(row.get("isClosed"))
            if is_closed is not False and exchange:
                closed_exchanges.add(exchange)
    return SUPPORTED_EXCHANGES.issubset(closed_exchanges)


def build_candidates(base: str, target_date: date | None = None) -> CandidateBundle:
    assets, identifiers, universe_stats = prepare_universe(base, target_date)
    research_records = assets.attrs.pop("research_observations", [])
    prices, price_stats = prepare_prices(base, assets, identifiers, target_date)
    fx_assets, fx_identifiers, fx_prices, fx_stats = prepare_fx(base, target_date)
    commodity_assets, commodity_identifiers, commodity_prices, commodity_stats = (
        prepare_commodities(base, target_date)
    )
    assets = pd.concat(
        [assets, fx_assets, commodity_assets], ignore_index=True,
    )
    identifiers = pd.concat(
        [identifiers, fx_identifiers, commodity_identifiers], ignore_index=True,
    )
    prices = pd.concat(
        [prices, fx_prices, commodity_prices], ignore_index=True,
    )
    fundamentals, fundamental_stats = prepare_fundamentals(
        base, identifiers, research_target_date=target_date,
    )
    research_records.extend(fundamentals.attrs.pop("research_observations", []))
    actions, action_stats = prepare_actions(base, identifiers)
    return CandidateBundle(
        assets=assets,
        identifiers=identifiers,
        prices=prices,
        fundamentals=fundamentals,
        actions=actions,
        research_observations=research_records,
        stats={
            "asset": universe_stats,
            "price_daily": price_stats,
            "fx": fx_stats,
            "commodity": commodity_stats,
            "fundamental": fundamental_stats,
            "corporate_action": action_stats,
            "_source": "FMP",
            "_target_date": target_date,
            "_market_closed": (
                market_closed(base, target_date)
                or not _is_xnys_session(target_date)
                if target_date is not None else False
            ),
        },
    )


def publish_assets(
    conn,
    asset_candidates: pd.DataFrame,
    identifier_candidates: pd.DataFrame,
    quality_run_id: UUID,
) -> dict[str, int]:
    asset_candidates = asset_candidates.copy()
    if "price_unit" not in asset_candidates:
        asset_candidates["price_unit"] = None
    natural_to_id: dict[str, int] = {}
    with conn.cursor() as cur:
        for natural_key, group in identifier_candidates.groupby("natural_key", sort=False):
            current_asset_id = None
            historical_asset_id = None
            bridge_asset_id = None
            lookup_rows = sorted(
                group.itertuples(index=False),
                key=lambda row: (
                    0
                    if row.identifier_type == "ticker"
                    and pd.isna(row.valid_to)
                    else 1
                    if row.identifier_type == "ticker"
                    else 2
                    if pd.isna(row.valid_to)
                    else 3
                ),
            )
            for row in lookup_rows:
                # Asset identity is anchored only by the validated ticker
                # change chain. FMP historical CUSIP/ISIN values can be stale
                # or recycled across unrelated securities.
                if row.identifier_type not in {
                    "ticker", "fx_pair", "commodity_symbol",
                }:
                    continue
                if pd.isna(row.valid_to):
                    # A ticker may be reused by an unrelated issuer after a
                    # closed historical episode (for example COIN and CSR).
                    # A current candidate can therefore anchor only to a
                    # current stored mapping.
                    cur.execute(
                        "SELECT asset_id FROM asset_identifier "
                        "WHERE source='FMP' AND identifier_type=%s "
                        "AND identifier=%s AND valid_to IS NULL "
                        "ORDER BY valid_from DESC LIMIT 1",
                        (row.identifier_type, str(row.identifier)),
                    )
                    found = cur.fetchone()
                    if found and current_asset_id is None:
                        current_asset_id = found[0]
                else:
                    # Delisted primary tickers are stable only when the full
                    # validity interval matches the already-published episode.
                    cur.execute(
                        "SELECT asset_id FROM asset_identifier "
                        "WHERE source='FMP' AND identifier_type=%s "
                        "AND identifier=%s AND valid_from=%s AND valid_to=%s "
                        "LIMIT 1",
                        (
                            row.identifier_type, str(row.identifier),
                            row.valid_from, row.valid_to,
                        ),
                    )
                    found = cur.fetchone()
                    if found and historical_asset_id is None:
                        historical_asset_id = found[0]
                    if found is None and row.identifier_type == "ticker":
                        # A declared ticker change first arrives while the old
                        # ticker is still the database's open identifier.  The
                        # bounded old leg is the authoritative bridge to that
                        # existing asset; close it below instead of creating a
                        # second asset that will collide on CUSIP/ISIN.
                        cur.execute(
                            "SELECT asset_id FROM asset_identifier "
                            "WHERE source='FMP' AND identifier_type='ticker' "
                            "AND identifier=%s AND valid_to IS NULL "
                            "ORDER BY valid_from DESC LIMIT 1",
                            (str(row.identifier),),
                        )
                        found = cur.fetchone()
                        if found:
                            bridge_asset_id = found[0]
                            break
            # A still-open old leg is a newly declared transition and is the
            # strongest identity evidence. Otherwise the established current
            # ticker wins over potentially duplicated ancient history.
            asset_id = (
                bridge_asset_id
                if bridge_asset_id is not None
                else current_asset_id
                if current_asset_id is not None
                else historical_asset_id
            )
            asset_row = asset_candidates[
                asset_candidates["natural_key"].eq(natural_key)
            ].iloc[0]
            price_unit = (
                None
                if pd.isna(asset_row["price_unit"])
                else str(asset_row["price_unit"])
            )
            values = (
                asset_row["name"], asset_row["asset_type"],
                asset_row["instrument_type"], asset_row["exchange"],
                asset_row["currency"], asset_row["country_code"],
                asset_row["base_currency"], asset_row["listed_from"],
                asset_row["listed_to"], price_unit, quality_run_id,
            )
            if asset_id is None:
                cur.execute(
                    """
                    INSERT INTO asset(
                        name,asset_type,instrument_type,exchange,currency,
                        country_code,base_currency,listed_from,listed_to,price_unit,
                        quality_run_id,loaded_at
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now())
                    RETURNING asset_id
                    """,
                    values,
                )
                asset_id = cur.fetchone()[0]
            else:
                cur.execute(
                    """
                    UPDATE asset SET
                        name=%s,asset_type=%s,instrument_type=%s,exchange=%s,
                        currency=%s,country_code=%s,base_currency=%s,
                        listed_from=%s,listed_to=%s,price_unit=%s,
                        quality_run_id=%s,loaded_at=now()
                    WHERE asset_id=%s
                    """,
                    values + (asset_id,),
                )
            natural_to_id[str(natural_key)] = asset_id

        for row in identifier_candidates.itertuples(index=False):
            asset_id = natural_to_id[str(row.natural_key)]
            if row.identifier_type == "ticker" and not pd.isna(row.valid_to):
                cur.execute(
                    "SELECT asset_id,valid_from FROM asset_identifier "
                    "WHERE source=%s AND identifier_type='ticker' "
                    "AND identifier=%s AND valid_to IS NULL "
                    "ORDER BY valid_from DESC LIMIT 1",
                    (row.source, str(row.identifier)),
                )
                existing_open = cur.fetchone()
                if existing_open:
                    if (
                        existing_open[0] != asset_id
                        or existing_open[1] > row.valid_to
                    ):
                        raise RuntimeError(
                            "FMP ticker-change bridge conflict: "
                            f"identifier={row.identifier}"
                        )
                    cur.execute(
                        "UPDATE asset_identifier SET valid_to=%s, "
                        "quality_run_id=%s, loaded_at=now() "
                        "WHERE source=%s AND identifier_type='ticker' "
                        "AND identifier=%s AND valid_to IS NULL",
                        (
                            row.valid_to, quality_run_id, row.source,
                            str(row.identifier),
                        ),
                    )
                    continue
            if row.identifier_type != "cik" and pd.isna(row.valid_to):
                cur.execute(
                    "SELECT asset_id FROM asset_identifier "
                    "WHERE source=%s AND identifier_type=%s AND identifier=%s "
                    "AND valid_to IS NULL",
                    (row.source, row.identifier_type, str(row.identifier)),
                )
                existing = cur.fetchone()
                if existing and existing[0] != asset_id:
                    # A validated ticker-change chain can move into a ticker
                    # that was previously used by another issuer. Close that
                    # older open episode immediately before the new episode;
                    # the bounded old leg selected above remains the identity
                    # anchor for the changing security.
                    if row.identifier_type != "ticker" or pd.isna(row.valid_from):
                        raise RuntimeError(
                            "FMP identifier conflict: "
                            f"type={row.identifier_type} identifier={row.identifier}"
                        )
                    new_valid_from = row.valid_from
                    if new_valid_from == date.min:
                        raise RuntimeError(
                            "FMP identifier conflict: "
                            f"type={row.identifier_type} identifier={row.identifier}"
                        )
                    cur.execute(
                        "SELECT valid_from FROM asset_identifier "
                        "WHERE source=%s AND identifier_type=%s AND identifier=%s "
                        "AND valid_to IS NULL AND asset_id=%s",
                        (
                            row.source, row.identifier_type,
                            str(row.identifier), existing[0],
                        ),
                    )
                    existing_valid_from = cur.fetchone()
                    close_to = new_valid_from - timedelta(days=1)
                    if (
                        existing_valid_from is None
                        or existing_valid_from[0] > close_to
                    ):
                        raise RuntimeError(
                            "FMP identifier conflict: "
                            f"type={row.identifier_type} identifier={row.identifier}"
                        )
                    cur.execute(
                        "UPDATE asset_identifier SET valid_to=%s, "
                        "quality_run_id=%s, loaded_at=now() "
                        "WHERE source=%s AND identifier_type=%s "
                        "AND identifier=%s AND valid_to IS NULL AND asset_id=%s",
                        (
                            close_to, quality_run_id, row.source,
                            row.identifier_type, str(row.identifier), existing[0],
                        ),
                    )
                    existing = None
                if existing:
                    # The partial unique index allows one current mapping,
                    # while the history PK also includes valid_from. Reusing
                    # the same current mapping must update it in place rather
                    # than insert a second current interval.
                    cur.execute(
                        "UPDATE asset_identifier SET quality_run_id=%s, "
                        "loaded_at=now() WHERE source=%s AND identifier_type=%s "
                        "AND identifier=%s AND valid_to IS NULL",
                        (
                            quality_run_id, row.source, row.identifier_type,
                            str(row.identifier),
                        ),
                    )
                    continue
            cur.execute(
                """
                INSERT INTO asset_identifier(
                    asset_id,source,identifier,identifier_type,valid_from,valid_to,
                    quality_run_id,loaded_at
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,now())
                ON CONFLICT(
                    asset_id,source,identifier_type,identifier,valid_from
                ) DO UPDATE SET valid_to=EXCLUDED.valid_to,
                    quality_run_id=EXCLUDED.quality_run_id,loaded_at=now()
                """,
                (
                    asset_id, row.source, str(row.identifier), row.identifier_type,
                    row.valid_from,
                    None if pd.isna(row.valid_to) else row.valid_to,
                    quality_run_id,
                ),
            )
    identifier_map = {
        str(row.identifier): natural_to_id[str(row.natural_key)]
        for row in identifier_candidates.itertuples(index=False)
        if row.identifier_type in {"ticker", "fx_pair", "commodity_symbol"}
    }
    identifier_map.update(natural_to_id)
    return identifier_map


def _publish_frame(
    conn,
    table: str,
    candidates: pd.DataFrame,
    identifier_map: dict[str, int],
    quality_run_id: UUID,
    columns: list[str],
    conflict: list[str],
    update: list[str],
) -> int:
    if candidates.empty:
        return 0
    frame = candidates.copy()
    mapping_column = "natural_key" if "natural_key" in frame else "identifier"
    frame["asset_id"] = frame[mapping_column].map(identifier_map)
    if frame["asset_id"].isna().any():
        missing = sorted(frame.loc[frame["asset_id"].isna(), "identifier"].unique())
        raise RuntimeError(f"unmapped FMP identifiers: {missing[:20]}")
    frame["asset_id"] = frame["asset_id"].astype("int64")
    frame["quality_run_id"] = quality_run_id
    duplicate_publish_key = frame.duplicated(conflict, keep=False)
    if duplicate_publish_key.any():
        samples = frame.loc[
            duplicate_publish_key, conflict + ["identifier"]
        ].head(20).to_dict("records")
        raise RuntimeError(
            f"duplicate FMP publish key table={table} samples={samples}"
        )
    rows = list(
        frame[columns].astype(object).where(pd.notna(frame[columns]), None)
        .itertuples(index=False, name=None)
    )
    if table == "fundamental" and set(frame["source"].astype(str)) == {"FMP"}:
        first_period = frame["period_end"].min()
        last_period = frame["period_end"].max()
        statement_types = sorted(frame["statement_type"].astype(str).unique())
        fiscal_periods = sorted(frame["fiscal_period"].astype(str).unique())
        with conn.cursor() as cur:
            cur.execute(
                "SELECT EXISTS(SELECT 1 FROM fundamental "
                "WHERE source='FMP' AND period_end BETWEEN %s AND %s "
                "AND statement_type=ANY(%s) AND fiscal_period=ANY(%s))",
                (
                    first_period,
                    last_period,
                    statement_types,
                    fiscal_periods,
                ),
            )
            period_already_loaded = bool(cur.fetchone()[0])
            if not period_already_loaded:
                # Historical partitions are immutable once certified. A direct
                # COPY avoids writing the same rows to a staging table before
                # the indexed target, which is material on small RDS classes.
                with cur.copy(
                    f"COPY {table} ({', '.join(columns)}) FROM STDIN"
                ) as copy:
                    for row in rows:
                        copy.write_row(row)
                print(
                    "[silver-fmp] direct-copy "
                    f"table={table} periods={first_period}:{last_period} "
                    f"rows={len(rows)}",
                    flush=True,
                )
                return len(rows)
    return db.upsert(
        conn, table, columns, rows, conflict, update,
        temp_name=f"_stg_fmp_{table}",
    )


def publish_prices(conn, candidates, identifier_map, quality_run_id) -> int:
    columns = [
        "asset_id", "source", "trade_date", "open", "high", "low", "close",
        "adj_close", "total_return_close", "currency", "vwap", "available_at",
        "volume", "trading_value", "shares", "market_cap", "market",
        "quality_run_id",
    ]
    return _publish_frame(
        conn, "price_daily", candidates, identifier_map, quality_run_id, columns,
        ["asset_id", "source", "trade_date"],
        [column for column in columns if column not in {"asset_id", "source", "trade_date"}],
    )


def publish_fundamentals(conn, candidates, identifier_map, quality_run_id) -> int:
    columns = [
        "asset_id", "source", "statement_type", "data_basis", "period_end",
        "fiscal_period", "fs_type", "filing_id", "filed", "accepted_at",
        "available_date", "available_at", "metric", "value", "currency",
        "unit_type", "revision_key", "quality_run_id",
    ]
    conflict = [
        "asset_id", "source", "statement_type", "data_basis", "period_end",
        "fiscal_period", "fs_type", "revision_key", "metric",
    ]
    return _publish_frame(
        conn, "fundamental", candidates, identifier_map, quality_run_id, columns,
        conflict, [column for column in columns if column not in set(conflict)],
    )


def publish_actions(conn, candidates, identifier_map, quality_run_id) -> int:
    columns = [
        "asset_id", "source", "action_key", "action_type", "announcement_date",
        "ex_date", "record_date", "payment_date", "cash_amount",
        "adjusted_cash_amount", "currency", "frequency",
        "ratio_numerator", "ratio_denominator", "expected_price_factor",
        "share_count_factor", "status", "confidence", "filing_id",
        "quality_run_id",
    ]
    return _publish_frame(
        conn, "corporate_action", candidates, identifier_map, quality_run_id,
        columns, ["asset_id", "source", "action_key"],
        [column for column in columns if column not in {"asset_id", "source", "action_key"}],
    )
