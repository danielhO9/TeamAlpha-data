"""Evidence-bound economic price references, without modifying Bronze quotes.

KRX quoted changes need not adjust share units during liquidation trading.
Never infer a correction from a large return or a share-count ratio alone.
Keep original quote fields and fail closed if the reviewed observation changes.
"""
from datetime import date
import math

import pandas as pd

CHEIL_REVIEW_ID = "cheil-052670-consolidation-20260209-v1"
CHEIL_EVIDENCE_URL = (
    "https://cdn.financialreports.eu/financialreports/media/filings/17635/2026/"
    "RNS/17635_rns_2026-01-27_63def361-0e11-4695-8fbc-bb2891c75d38.html"
)


def apply_reviewed_references(frame: pd.DataFrame) -> pd.DataFrame:
    """Normalize the one verified event for both batch and daily price paths.

    2026-01-27 issuer filing: 1500 old shares become one new share, listed
    2026-02-09. Old shares 29,129,064; new shares 19,419 (fractional shares
    are handled separately). Thus use the explicit 1500 ratio, NOT the
    approximate share-count quotient. This is price return, not cash proceeds.
    """
    if frame.empty:
        return frame
    selected = (frame.ticker.astype(str).eq("052670")
                & pd.to_datetime(frame.trade_date).dt.date.eq(date(2026, 2, 9)))
    if not selected.any():
        return frame
    result = frame.copy()
    reference = 2080.0 * 1500
    for index in result.index[selected]:
        row = result.loc[index]
        close, shares = float(row.close), float(row.shares)
        baseline = close-float(row.prev_diff)
        if not (math.isclose(close, 625000, abs_tol=1e-6, rel_tol=0)
                and math.isclose(shares, 19419, abs_tol=1e-6, rel_tol=0)
                and any(math.isclose(baseline, v, abs_tol=1e-6, rel_tol=0)
                        for v in (2080.0, reference))):
            raise ValueError(f"Reviewed price reference changed: {CHEIL_REVIEW_ID}")
        # Reapplying to an already normalized candidate must retain raw evidence.
        if pd.isna(row.get("reviewed_price_reference_id")):
            result.loc[index, "quoted_prev_diff"] = row.prev_diff
            result.loc[index, "quoted_fluc_rate"] = row.fluc_rate
        elif row.reviewed_price_reference_id != CHEIL_REVIEW_ID:
            raise ValueError("Conflicting reviewed price reference")
        result.loc[index, "prev_diff"] = close-reference
        result.loc[index, "fluc_rate"] = (close/reference-1)*100
        result.loc[index, "reviewed_price_reference_id"] = CHEIL_REVIEW_ID
        result.loc[index, "reviewed_price_reference_evidence"] = CHEIL_EVIDENCE_URL
    return result
