"""Evidence-backed lifecycle candidates; never infer listing dates from prices.

This module prepares metadata, not a publication or a coverage certificate.
Delisting is exclusive; asset.listed_to is inclusive. Market transfers and
relistings remain in the evidence ledger instead of being erased by min/max.
"""
from __future__ import annotations

from datetime import date, timedelta


def candidate(asset_id, ticker, facts, *, research_start="2015-01-01"):
    """Summarize exact-security facts, retaining unresolved lifecycle risks."""
    selected = [dict(f) for f in facts if f["ticker"] == ticker]
    for fact in selected:
        date.fromisoformat(fact["date"])
        if not fact.get("source_file") or not fact.get("source_sha256"):
            raise ValueError("lifecycle fact has no source evidence")
        if fact["event"] not in {"LISTING", "DELISTING", "CURRENT_LISTING"}:
            raise ValueError("unsupported lifecycle event")
    listings = [f for f in selected if f["event"] in {"LISTING", "CURRENT_LISTING"}
                and f.get("market") in {"KOSPI", "KOSDAQ"}]
    current = [f for f in selected if f["event"] == "CURRENT_LISTING"]
    exits = [f for f in selected if f["event"] == "DELISTING" and not f.get("market_transfer")]
    # A KONEX-only episode is outside the requested KOSPI/KOSDAQ scope. Its
    # exit is still preserved in facts, but must not terminate a new supported
    # listing on the same day (e.g. a SPAC merger transfer).
    exits = [f for f in exits if any(x["date"] < f["date"] for x in listings)]
    start = min((f["date"] for f in listings), default=None)
    last_exit = max((f["date"] for f in exits), default=None)
    issues = []
    # A historical exit followed by a new listing cannot be flattened into a
    # single continuously listed period. Pre-research episodes may be dropped
    # only when an explicit subsequent supported-market listing is available.
    for exit_fact in sorted(exits, key=lambda f: f["date"]):
        later = [f["date"] for f in listings if f["date"] > exit_fact["date"]]
        if later:
            if exit_fact["date"] >= research_start:
                issues.append("RELISTING_REQUIRES_INTERVAL_REVIEW")
            else:
                start = min(later)
    if not start:
        issues.append("MISSING_SUPPORTED_MARKET_LISTING")
    if current:
        end = None
        latest_listing = max((f["date"] for f in listings), default=None)
        if last_exit and latest_listing and last_exit >= latest_listing:
            issues.append("CURRENT_LISTING_WITH_LATER_TERMINAL_EXIT")
    elif last_exit:
        end = (date.fromisoformat(last_exit) - timedelta(days=1)).isoformat()
    else:
        end = None
        issues.append("CURRENT_OR_TERMINAL_STATUS_UNKNOWN")
    if start and end and end < start:
        issues.append("INVALID_INTERVAL")
    # Separate relisting episodes; no synthetic coverage through an unlisted
    # gap. A downstream publisher must use these intervals, not flat bounds.
    periods = []
    if start:
        period_start = start
        for exit_day in sorted({f["date"] for f in exits if f["date"] >= start}):
            period_end = (date.fromisoformat(exit_day) - timedelta(days=1)).isoformat()
            if period_end >= research_start:
                periods.append({"start": period_start, "end": period_end})
            later = sorted({f["date"] for f in listings if f["date"] > exit_day})
            period_start = later[0] if later else None
            if period_start is None:
                break
        if period_start is not None and current:
            periods.append({"start": period_start, "end": None})
    return {"asset_id": int(asset_id), "ticker": ticker,
            "proposed_listed_from": start, "proposed_listed_to": end,
            "delisted_on_exclusive": last_exit if not current else None,
            "status": "REVIEW_REQUIRED" if issues else "PENDING_RDS_VALIDATION",
            "issues": sorted(set(issues)), "periods": periods,
            "facts": selected}


def check_observed_bounds(row, first_day, last_day):
    """Check independent observations; observations do not set lifecycle dates."""
    issues = list(row["issues"])
    if first_day and row["proposed_listed_from"] and first_day < row["proposed_listed_from"]:
        issues.append("OBSERVATION_BEFORE_PROPOSED_LISTING")
    if last_day and row["proposed_listed_to"] and last_day > row["proposed_listed_to"]:
        issues.append("OBSERVATION_AFTER_PROPOSED_DELISTING")
    return sorted(set(issues))
