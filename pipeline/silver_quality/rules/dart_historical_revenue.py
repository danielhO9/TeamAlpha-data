"""Independent checks for bounded original-document revenue, never all DART."""
from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

from pipeline.silver_quality.models import CheckResult, CheckStatus, Severity


def check_document_revenue(prepared) -> list[CheckResult]:
    bad = defaultdict(list)
    keys = set()
    groups = defaultdict(list)
    for r in prepared.rows:
        key = r["observation_key"]
        if key in keys:
            bad["DOCUMENT_REVENUE_KEYS"].append(key)
        keys.add(key)
        filed, available, until = (date.fromisoformat(r[k]) for k in ("filed", "available_date", "valid_until_exclusive"))
        if not (date.fromisoformat(r["period_start"]) <= date.fromisoformat(r["period_end"]) <= filed
                and available == filed + timedelta(days=1) and available < until):
            bad["DOCUMENT_REVENUE_PIT_WINDOW"].append(key)
        at = datetime.fromisoformat(r["available_at"])
        if at.tzinfo is None or at.astimezone(ZoneInfo("Asia/Seoul")).isoformat() != available.isoformat() + "T00:00:00+09:00":
            bad["DOCUMENT_REVENUE_PIT_WINDOW"].append(key)
        if (r["quantity_semantics"] != "FLOW_CURRENT_QUARTER" or r["metric"] != "revenue"
                or r["fiscal_period"] not in {"Q1", "Q2", "Q3"} or r["fs_type"] not in {"CFS", "OFS"}
                or r["currency"] != "KRW" or not Decimal(r["value"]).is_finite() or Decimal(r["value"]) < 0
                or r["revision_key"] != r["filing_id"]):
            bad["DOCUMENT_REVENUE_SEMANTICS"].append(key)
        if not all(r["evidence"].get(k) for k in ("stage_row_sha256", "xml_sha256", "zip_sha256", "identity", "current_ytd_evidence")):
            bad["DOCUMENT_REVENUE_PROVENANCE"].append(key)
        groups[(r["asset_id"], r["period_end"], r["fs_type"])].append((available, until, key))
    for intervals in groups.values():
        intervals.sort()
        for before, after in zip(intervals, intervals[1:]):
            if before[1] > after[0]:
                bad["DOCUMENT_REVENUE_PIT_WINDOW"].append(after[2])
    rules = ("DOCUMENT_REVENUE_KEYS", "DOCUMENT_REVENUE_PIT_WINDOW", "DOCUMENT_REVENUE_SEMANTICS", "DOCUMENT_REVENUE_PROVENANCE")
    results = [CheckResult(code, "fundamental_document_revenue", Severity.CRITICAL,
                           CheckStatus.FAIL if bad[code] else CheckStatus.PASS,
                           "no invalid/overlapping source observations", f"rows={len(prepared.rows)}; failed={len(bad[code])}",
                           len(bad[code]), [{"observation_key": k} for k in bad[code][:20]], prepared.dataset_version)
               for code in rules]
    results.append(CheckResult("DOCUMENT_REVENUE_BOUNDED_COVERAGE", "fundamental_document_revenue", Severity.WARNING,
                               CheckStatus.FAIL if prepared.blocking_windows else CheckStatus.PASS,
                               "unresolved intervals are explicitly unavailable, never forward-filled",
                               f"blocking_windows={len(prepared.blocking_windows)}; certification is source/window-specific",
                               len(prepared.blocking_windows), list(prepared.blocking_windows[:20]), prepared.dataset_version))
    return results
