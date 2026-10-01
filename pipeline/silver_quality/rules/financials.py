"""fundamental PIT, revision, currency, accounting 규칙."""
from __future__ import annotations

import pandas as pd

from pipeline.silver_quality.models import CheckResult, CheckStatus, Severity
from pipeline.silver_quality.rules.common import duplicate_keys, null_keys, result

FUNDAMENTAL_KEYS = [
    "identifier", "source", "statement_type", "data_basis", "period_end", "fiscal_period",
    "fs_type", "revision_key", "metric",
]


def check_financials(
    df: pd.DataFrame,
    partition_key: str | None = None,
    source_inconsistency: dict | None = None,
) -> list[CheckResult]:
    # Older unit-test fixtures and persisted candidate artifacts predate v2
    # discriminator columns. Normalize them before applying the v1.18 rules.
    df = df.copy()
    if "statement_type" not in df:
        bs_metrics = {
            "total_assets", "current_assets", "noncurrent_assets",
            "total_liabilities", "current_liabilities",
            "noncurrent_liabilities", "total_equity", "capital_stock",
            "retained_earnings",
        }
        df["statement_type"] = df["metric"].map(
            lambda metric: "BS" if metric in bs_metrics else "IS"
        )
    if "data_basis" not in df:
        df["data_basis"] = "STANDARDIZED"
    if "unit_type" not in df:
        df["unit_type"] = "currency"
    checks = [
        null_keys(
            df, "fundamental",
            FUNDAMENTAL_KEYS + ["available_date", "currency", "value"],
            partition_key,
        ),
        duplicate_keys(df, "fundamental", FUNDAMENTAL_KEYS, partition_key),
    ]
    if df.empty:
        return checks

    dividend = df["statement_type"].eq("DIVIDEND")
    enum_bad = df[
        ~df["fiscal_period"].isin(["FY", "Q1", "Q2", "Q3", "Q4"])
        | ~df["fs_type"].isin(["CFS", "OFS"])
        & ~(dividend & df["fs_type"].eq("UNKNOWN"))
        | ~df["currency"].astype(str).str.fullmatch(r"[A-Z]{3}")
    ]
    checks.append(result(
        "FUNDAMENTAL_ENUM_CURRENCY", "fundamental", Severity.ERROR, enum_bad,
        "valid fiscal_period/fs_type and 3-letter currency", partition_key=partition_key,
    ))

    filed = pd.to_datetime(df["filed"], errors="coerce")
    period = pd.to_datetime(df["period_end"], errors="coerce")
    available = pd.to_datetime(df["available_date"], errors="coerce")
    pit_bad = df[
        (filed.notna() & (period > filed))
        | (filed.notna() & (available != filed + pd.offsets.Day(1)))
        | (available <= period)
    ]
    checks.append(result(
        "FUNDAMENTAL_PIT_ORDER", "fundamental", Severity.CRITICAL, pit_bad,
        "period_end <= filed < available_date", partition_key=partition_key,
    ))

    filing_scope = [
        "identifier", "source", "period_end", "fiscal_period",
        "fs_type", "revision_key",
    ]
    currency_count = df.groupby(filing_scope, dropna=False)["currency"].transform("nunique")
    checks.append(result(
        "FUNDAMENTAL_CURRENCY_CONSISTENCY", "fundamental", Severity.ERROR,
        df[currency_count.gt(1)], "one currency per filing revision",
        partition_key=partition_key,
    ))

    value_num = pd.to_numeric(df["value"], errors="coerce")
    # Review gate, not a cap: no value is fabricated or silently truncated.
    scale_bad = df[df["source"].eq("DART") & df["currency"].eq("KRW")
                   & df["metric"].eq("total_assets") & value_num.abs().gt(1e16)]
    checks.append(result(
        "FUNDAMENTAL_SCALE_REVIEW_REQUIRED", "fundamental", Severity.ERROR,
        scale_bad, "KRW total assets above 10 quadrillion require source-document review",
        partition_key=partition_key,
    ))
    implausible = df[
        (df["metric"].isin({"total_assets"}) & value_num.le(0))
        | (df["metric"].isin({"revenue", "total_liabilities"}) & value_num.lt(0))
    ]
    checks.append(result(
        "FUNDAMENTAL_VALUE_PLAUSIBILITY", "fundamental", Severity.ERROR,
        implausible,
        "total_assets > 0; revenue and total_liabilities >= 0",
        partition_key=partition_key,
    ))

    accounting_df = df[~dividend]
    pivot = accounting_df.pivot_table(
        index=filing_scope,
        columns="metric",
        values="value",
        aggfunc="first",
    )
    required = {"total_assets", "total_liabilities", "total_equity"}
    if required.issubset(pivot.columns):
        base = pivot["total_assets"].abs().replace(0, pd.NA)
        rel = (
            pivot["total_assets"]
            - pivot["total_liabilities"]
            - pivot["total_equity"]
        ).abs() / base
        all_bad_idx = rel[rel.gt(0.01)].index
        confirmed_keys = set(
            (source_inconsistency or {}).get("scope_keys", [])
        )
        bad_idx = pd.MultiIndex.from_tuples(
            [
                key
                for key in all_bad_idx
                if tuple(key) not in confirmed_keys
            ],
            names=all_bad_idx.names,
        )
        source_index = pd.MultiIndex.from_frame(accounting_df[filing_scope])
        accounting_bad = accounting_df[source_index.isin(bad_idx)]
        accounting_scopes = pivot.loc[bad_idx].reset_index()
        accounting_scopes["relative_error"] = rel.loc[bad_idx].to_numpy()
    else:
        accounting_bad = df.iloc[0:0]
        accounting_scopes = pd.DataFrame()
    accounting_samples = (
        accounting_scopes.head(20).astype(object)
        .where(pd.notna(accounting_scopes.head(20)), None)
        .to_dict("records")
    )
    checks.append(CheckResult(
        rule_code="FUNDAMENTAL_ACCOUNTING_EQUATION",
        dataset="fundamental",
        severity=Severity.WARNING,
        status=(
            CheckStatus.FAIL
            if len(accounting_scopes)
            else CheckStatus.PASS
        ),
        expected="Assets ≈ Liabilities + Equity within 1% per filing revision",
        actual=(
            f"failed_scopes={len(accounting_scopes)}, "
            f"affected_rows={len(accounting_bad)}"
        ),
        failed_count=len(accounting_scopes),
        samples=accounting_samples,
        partition_key=partition_key,
    ))
    # Gross imbalance (>10%) is not a presentation quirk — the statement itself is
    # untrustworthy — so it blocks. Load-time exclusion drops these scopes before
    # publish, so this fires only if a gross scope somehow reaches evaluation.
    if len(accounting_scopes) and "relative_error" in accounting_scopes:
        gross_scopes = accounting_scopes[
            accounting_scopes["relative_error"] > 0.10
        ]
    else:
        gross_scopes = accounting_scopes.iloc[0:0]
    gross_samples = (
        gross_scopes.head(20).astype(object)
        .where(pd.notna(gross_scopes.head(20)), None)
        .to_dict("records")
    )
    checks.append(CheckResult(
        rule_code="FUNDAMENTAL_ACCOUNTING_EQUATION_GROSS",
        dataset="fundamental",
        severity=Severity.ERROR,
        status=CheckStatus.FAIL if len(gross_scopes) else CheckStatus.PASS,
        expected="Assets ≈ Liabilities + Equity within 10% per filing revision",
        actual=f"gross_scopes={len(gross_scopes)}",
        failed_count=len(gross_scopes),
        samples=gross_samples,
        partition_key=partition_key,
    ))
    confirmed = source_inconsistency or {}
    confirmed_scopes = int(confirmed.get("scope_count", 0))
    confirmed_rows = int(confirmed.get("row_count", 0))
    checks.append(CheckResult(
        rule_code="DART_SOURCE_ACCOUNTING_INCONSISTENCY",
        dataset="fundamental",
        severity=Severity.WARNING,
        status=(
            CheckStatus.FAIL
            if confirmed_scopes
            else CheckStatus.PASS
        ),
        expected=(
            "DART major-account values satisfy Assets ≈ Liabilities + Equity; "
            "if not, preserve source values and confirm against the same-revision "
            "full-statement API without deriving a correction"
        ),
        actual=(
            f"confirmed_source_scopes={confirmed_scopes}, "
            f"affected_account_rows={confirmed_rows}"
        ),
        failed_count=confirmed_scopes,
        samples=list(confirmed.get("samples", []))[:20],
        partition_key=partition_key,
    ))

    major = {"total_assets", "revenue", "net_income"}
    if accounting_df.empty:
        scope_metrics = pd.DataFrame(
            columns=[*filing_scope, "present_metrics"],
        )
    else:
        scope_metrics = (
            accounting_df.groupby(filing_scope, dropna=False)["metric"]
            .agg(lambda values: sorted(set(values)))
            .rename("present_metrics")
            .reset_index()
        )
    covered_major = scope_metrics["present_metrics"].map(
        lambda values: bool(set(values) & major)
    ).astype(bool)
    missing_major_scopes = scope_metrics.loc[~covered_major]
    source_index = pd.MultiIndex.from_frame(accounting_df[filing_scope])
    missing_index = pd.MultiIndex.from_frame(
        missing_major_scopes[filing_scope]
    )
    affected_rows = int(source_index.isin(missing_index).sum())
    major_samples = (
        missing_major_scopes.head(20).astype(object)
        .where(pd.notna(missing_major_scopes.head(20)), None)
        .to_dict("records")
    )
    checks.append(CheckResult(
        rule_code="FUNDAMENTAL_MAJOR_METRIC_COVERAGE",
        dataset="fundamental",
        severity=Severity.WARNING,
        status=(
            CheckStatus.FAIL
            if len(missing_major_scopes)
            else CheckStatus.PASS
        ),
        expected=(
            "at least one of total_assets/revenue/net_income "
            "per filing revision"
        ),
        actual=(
            f"failed_scopes={len(missing_major_scopes)}, "
            f"affected_rows={affected_rows}"
        ),
        failed_count=len(missing_major_scopes),
        samples=major_samples,
        partition_key=partition_key,
    ))
    if dividend.any():
        dividend_rows = df[dividend]
        specs = {
            "total_cash_dividend": "currency",
            "payout_ratio": "percent",
            "dividend_yield": "percent",
            "cash_dividend_per_share": "per_share",
            "stock_dividend_per_share": "shares",
        }
        bad_dividend = dividend_rows[
            ~dividend_rows["metric"].isin(specs)
            | dividend_rows.apply(
                lambda row: specs.get(row["metric"]) != row["unit_type"],
                axis=1,
            )
        ]
        checks.append(result(
            "DIVIDEND_METRIC_UNIT", "fundamental", Severity.ERROR,
            bad_dividend, "known dividend metric with its canonical unit_type",
            partition_key=partition_key,
        ))
        nonnegative = dividend_rows[
            dividend_rows["metric"].isin({
                "total_cash_dividend", "dividend_yield",
                "cash_dividend_per_share", "stock_dividend_per_share",
            })
            & pd.to_numeric(dividend_rows["value"], errors="coerce").lt(0)
        ]
        checks.append(result(
            "DIVIDEND_NONNEGATIVE", "fundamental", Severity.ERROR,
            nonnegative, "cash amount, yield and per-share dividend >= 0",
            partition_key=partition_key,
        ))
    return checks
