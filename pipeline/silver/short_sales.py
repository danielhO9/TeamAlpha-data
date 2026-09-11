"""Compute daily executed-short ratios using independently checked raw volume.

This local preparation step does not publish to Silver DB/Gold or certify PIT
availability. KIS adjusted acml_vol, supplied ssts_vol_rlim and cumulative
stnd_vol_smtn are retained as source fields, never used as the denominator.
"""
from __future__ import annotations

import argparse
from decimal import Decimal, InvalidOperation
from pathlib import Path
import re

import pandas as pd


def _key(ticker, trade_date) -> tuple[str, str]:
    ticker = str(ticker).strip().upper()
    if re.fullmatch(r"[0-9A-Z]{6}", ticker) is None:
        raise ValueError("ticker must be a six-character string")
    rendered = str(trade_date).strip()
    if re.fullmatch(r"\d{8}|\d{4}-\d{2}-\d{2}", rendered) is None:
        raise ValueError("trade_date must be YYYYMMDD or YYYY-MM-DD")
    date = pd.to_datetime(rendered, format="%Y%m%d" if len(rendered) == 8 else "%Y-%m-%d")
    return ticker, date.strftime("%Y-%m-%d")


def _quantity(value) -> int | None:
    if value is None or pd.isna(value) or str(value).strip() in {"", "-"}:
        return None
    try:
        number = Decimal(str(value).replace(",", "").strip())
    except InvalidOperation as exc:
        raise ValueError("invalid share quantity") from exc
    if not number.is_finite() or number < 0 or number != number.to_integral_value():
        raise ValueError("share quantity must be a finite nonnegative integer")
    return int(number)


def _verified(value) -> bool:
    # In particular, bool('False') is not a verification flag.
    return str(value).strip().lower() == "true"


def _text(value) -> str:
    return "" if value is None or pd.isna(value) else str(value).strip()


def _records(frame: pd.DataFrame, required: set[str]) -> list[dict]:
    if missing := required - set(frame.columns):
        raise ValueError(f"missing columns: {sorted(missing)}")
    records = frame.to_dict("records")
    seen = set()
    for row in records:
        key = _key(row["ticker"], row["trade_date"])
        if key in seen:
            raise ValueError(f"duplicate ticker/date; select one observation vintage: {key}")
        seen.add(key)
        row["ticker"], row["trade_date"] = key
    return records


def prepare(short_sales: pd.DataFrame, raw_volumes: pd.DataFrame) -> pd.DataFrame:
    """Join exact ticker/date keys; fail closed for unverified denominators.

    raw_volumes requires volume_basis='UNADJUSTED_SHARES', raw_volume_verified
    and a volume_evidence reference. These are caller-supplied evidence claims,
    not inferred from a plausible ratio. Short quantity verification is separate.
    Source input frames and KIS raw fields are never mutated.
    """
    sales = _records(short_sales, {"ticker", "trade_date", "ssts_cntg_qty"})
    volumes = _records(raw_volumes, {
        "ticker", "trade_date", "raw_total_volume", "volume_basis",
        "raw_volume_verified", "volume_evidence",
    })
    by_key = {(r["ticker"], r["trade_date"]): r for r in volumes}
    results = []
    for sale in sales:
        volume_row = by_key.get((sale["ticker"], sale["trade_date"]))
        quantity = _quantity(sale["ssts_cntg_qty"])
        volume = _quantity(volume_row["raw_total_volume"]) if volume_row else None
        volume_ok = bool(volume_row) and (
            volume_row["volume_basis"] == "UNADJUSTED_SHARES"
            and _verified(volume_row["raw_volume_verified"])
            and bool(_text(volume_row["volume_evidence"]))
        )
        quantity_ok = (
            _verified(sale.get("short_qty_verified"))
            and bool(_text(sale.get("short_qty_evidence")))
        )
        ratio = None
        if quantity is None:
            status = "MISSING_SHORT_QUANTITY"
        elif volume is None:
            status = "MISSING_RAW_VOLUME"
        elif not volume_ok:
            status = "UNVERIFIED_RAW_VOLUME"
        elif volume == 0:
            status = "ZERO_RAW_VOLUME" if quantity == 0 else "QUANTITY_EXCEEDS_RAW_VOLUME"
        elif quantity > volume:
            status = "QUANTITY_EXCEEDS_RAW_VOLUME"
        else:
            ratio = float(Decimal(quantity) * 100 / Decimal(volume))
            status = "COMPUTED"
        results.append({
            **sale,
            "raw_total_volume": volume,
            "volume_basis": volume_row["volume_basis"] if volume_row else None,
            "volume_evidence": volume_row["volume_evidence"] if volume_row else None,
            "raw_volume_verified": volume_ok,
            "short_qty_verified": quantity_ok,
            "short_sale_ratio_pct": ratio,
            "ratio_status": status,
            "ratio_value_verified": status == "COMPUTED" and quantity_ok,
        })
    return pd.DataFrame(results, columns=list(dict.fromkeys([
        *short_sales.columns, "raw_total_volume", "volume_basis", "volume_evidence",
        "raw_volume_verified", "short_qty_verified", "short_sale_ratio_pct",
        "ratio_status", "ratio_value_verified",
    ])))


def _read(path: str) -> pd.DataFrame:
    if Path(path).suffix == ".parquet":
        return pd.read_parquet(path)
    return pd.read_csv(path, dtype=str, keep_default_na=False)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--short-sales", required=True, help="CSV/Parquet with ticker, trade_date, ssts_cntg_qty")
    parser.add_argument("--raw-volumes", required=True, help="CSV/Parquet with verified unadjusted volume and evidence")
    parser.add_argument("--output", required=True, help="new local CSV/Parquet, never a DB destination")
    args = parser.parse_args()
    output = Path(args.output)
    if output.exists():
        raise FileExistsError(f"refusing to overwrite: {output}")
    frame = prepare(_read(args.short_sales), _read(args.raw_volumes))
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.suffix == ".parquet":
        frame.to_parquet(output, index=False)
    else:
        frame.to_csv(output, index=False)
    print(frame["ratio_status"].value_counts().to_dict())


if __name__ == "__main__":
    main()
