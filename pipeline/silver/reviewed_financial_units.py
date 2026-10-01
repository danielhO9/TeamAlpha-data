"""Receipt-specific OpenDART API scale defects; never winsorize fundamentals.

Most XML filings display KRW amounts while their API receipts multiply amounts
by 1,000,000; one omits the million-KRW unit instead. Anchors below are exact API
total-assets amounts. Unknown revisions
and non-KRW statements are never changed. Bronze/raw statement lines stay raw.
"""
from decimal import Decimal

# receipt -> (ticker, original API assets by scope). Evidence: document.xml for
# the SAME receipt, not a later restatement. See tools/repair_financial_units.py.
REVIEWED = {
    "20230323001157": ("032680", {"CFS": "122129876850000000", "OFS": "95028499373000000"}),
    "20241114002786": ("007720", {"CFS": "159266074177000000", "OFS": "159016488957000000"}),
    "20221011000544": ("039230", {"CFS": "50245047414000000", "OFS": "35055947590000000"}),
    "20191114000246": ("032080", {"OFS": "114055541787000000"}),
    "20230209000202": ("060310", {"CFS": "72650807169000000", "OFS": "70218084025000000"}),
    "20200601000502": ("069330", {"CFS": "59321781418000000", "OFS": "34046750676000000"}),
    "20170814002311": ("102260", {"CFS": "842787340982000000", "OFS": "377958792330000000"}),
    "20200715000093": ("144620", {"OFS": "127175438579000000"}),
    "20260703000402": ("160600", {"CFS": "148625362188000000", "OFS": "146301383003000000"}),
    "20240813000596": ("323350", {"OFS": "23641465260000000"}),
    "20240513000154": ("439250", {"OFS": "37280655578000000"}),
    "20250515000236": ("484130", {"OFS": "9898940774000000"}),
    "20171114002669": ("001740", {"CFS": "7814615", "OFS": "7092608"}),
}
# SK Networks' report is explicitly in millions of KRW; its API omitted the
# multiplication (the opposite defect). All other reviewed receipts over-scale.
DIVISORS = {"20171114002669": Decimal("0.000001")}
DOCUMENT_UNITS = {"20171114002669": Decimal("1000000")}


def correct_candidates(frame):
    """Scale only a complete, exact known defective receipt/scope; idempotent.

Mixed/changed anchors fail closed. Raw lines are NOT passed to this function:
they include per-share amounts, shares, and ratios that must never be scaled.
"""
    frame = frame.copy()
    modified = []
    for receipt, (ticker, anchors) in REVIEWED.items():
        divisor = DIVISORS.get(receipt, Decimal(1_000_000))
        for scope, original in anchors.items():
            mask = (frame["revision_key"].eq(receipt)
                    & frame["identifier"].eq(ticker)
                    & frame["fs_type"].eq(scope)
                    & frame["source"].eq("DART")
                    & frame["currency"].eq("KRW"))
            group = frame.loc[mask]
            if group.empty:
                continue
            assets = group.loc[group["metric"].eq("total_assets"), "value"]
            if assets.empty:
                raise ValueError(f"reviewed scale correction lacks assets anchor: {receipt}/{scope}")
            # prepare historically uses float64. Compare at that same precision.
            raw = float(original)
            corrected = float(Decimal(original) / divisor)
            if (assets == corrected).all():
                continue
            if not (assets == raw).all():
                raise ValueError(f"reviewed scale anchor changed: {receipt}/{scope}")
            frame.loc[mask, "value"] = frame.loc[mask, "value"] / float(divisor)
            modified.append({"revision_key": receipt, "identifier": ticker,
                             "fs_type": scope, "row_count": len(group),
                             "divisor": str(divisor)})
    return frame, modified
