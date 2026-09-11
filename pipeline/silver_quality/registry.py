"""규칙 registry와 실행 순서."""
from __future__ import annotations

from pipeline.silver_quality.models import (
    CandidateBundle,
    CheckResult,
    CheckStatus,
    Severity,
)
from pipeline.silver_quality.rules.common import result
from pipeline.silver_quality.rules.assets import check_assets
from pipeline.silver_quality.rules.actions import check_actions
from pipeline.silver_quality.rules.financials import check_financials
from pipeline.silver_quality.rules.prices import (
    check_market_coverage_floor,
    check_prices,
)
from pipeline.silver_quality.rules.reconciliation import check_reconciliation


def run_registered_rules(
    bundle: CandidateBundle,
    *,
    target_date=None,
    history=None,
    partition_key: str | None = None,
    changed_action_receipts: set[str] | None = None,
) -> list[CheckResult]:
    results: list[CheckResult] = []
    quality_actions = bundle.actions
    if changed_action_receipts is not None:
        receipts = {str(value) for value in changed_action_receipts}
        if bundle.actions.empty or "rcept_no" not in bundle.actions:
            quality_actions = bundle.actions.iloc[0:0]
        else:
            quality_actions = bundle.actions[
                bundle.actions["rcept_no"].astype(str).isin(receipts)
            ].reset_index(drop=True)
    results.extend(check_assets(bundle.assets, bundle.identifiers, partition_key))
    krx = set(bundle.stats.get("_existing_krx_identifiers", set()))
    if not bundle.identifiers.empty:
        krx.update(
            bundle.identifiers.loc[
                bundle.identifiers["source"].eq("KRX"), "identifier"
            ].astype(str)
        )
    if not bundle.prices.empty:
        missing = bundle.prices[
            ~bundle.prices["identifier"].astype(str).isin(krx)
        ]
        results.append(result(
            "PRICE_IDENTIFIER_MAPPING", "price_daily", Severity.ERROR,
            missing, "every price identifier maps to a KRX asset",
            partition_key=partition_key,
        ))
    if not bundle.fundamentals.empty:
        missing = bundle.fundamentals[
            ~bundle.fundamentals["identifier"].astype(str).isin(krx)
        ]
        results.append(result(
            "FUNDAMENTAL_IDENTIFIER_MAPPING", "fundamental", Severity.ERROR,
            missing, "every fundamental ticker maps to a KRX asset",
            partition_key=partition_key,
        ))
    if not quality_actions.empty:
        missing = quality_actions[
            ~quality_actions["identifier"].astype(str).isin(krx)
        ]
        results.append(result(
            "ACTION_IDENTIFIER_MAPPING", "corporate_action", Severity.ERROR,
            missing, "every corporate action maps to a KRX asset",
            partition_key=partition_key,
        ))
    unsupported_market = bundle.stats.get("price_daily", {}).get(
        "unsupported_market",
        {"row_count": 0, "ticker_count": 0, "markets": {}, "samples": []},
    )
    unsupported_rows = int(unsupported_market.get("row_count", 0))
    unsupported_tickers = int(unsupported_market.get("ticker_count", 0))
    if unsupported_rows > 0:
        results.append(CheckResult(
            rule_code="UNSUPPORTED_MARKET_EXCLUDED",
            dataset="price_daily",
            severity=Severity.MODIFIED,
            status=CheckStatus.PASS,
            expected="KONEX rows are explicitly excluded from the Silver universe",
            actual=(
                f"excluded_rows={unsupported_rows}, "
                f"excluded_tickers={unsupported_tickers}, "
                f"markets={unsupported_market.get('markets', {})}"
            ),
            failed_count=0,
            samples=list(unsupported_market.get("samples", []))[:20],
            partition_key=partition_key,
        ))
    nonpositive_price = bundle.stats.get("price_daily", {}).get(
        "nonpositive_price",
        {"row_count": 0, "ticker_count": 0, "samples": []},
    )
    nonpositive_rows = int(nonpositive_price.get("row_count", 0))
    if nonpositive_rows > 0:
        results.append(CheckResult(
            rule_code="NONPOSITIVE_PRICE_EXCLUDED",
            dataset="price_daily",
            severity=Severity.MODIFIED,
            status=CheckStatus.PASS,
            expected=(
                "close/shares/market_cap 비양수 주식 행은 Bronze 보존, "
                "Silver에서 제외"
            ),
            actual=(
                f"excluded_rows={nonpositive_rows}, "
                f"excluded_tickers={int(nonpositive_price.get('ticker_count', 0))}"
            ),
            failed_count=0,
            samples=list(nonpositive_price.get("samples", []))[:20],
            partition_key=partition_key,
        ))
    unsupported_asset = bundle.stats.get("fundamental", {}).get(
        "unsupported_market_asset",
        {"row_count": 0, "ticker_count": 0, "samples": []},
    )
    unsupported_asset_rows = int(unsupported_asset.get("row_count", 0))
    unsupported_asset_tickers = int(
        unsupported_asset.get("ticker_count", 0)
    )
    if unsupported_asset_rows > 0:
        results.append(CheckResult(
            rule_code="UNSUPPORTED_MARKET_ASSET_EXCLUDED",
            dataset="fundamental",
            severity=Severity.MODIFIED,
            status=CheckStatus.PASS,
            expected=(
                "fundamentals for assets with only KONEX price history "
                "are explicitly excluded"
            ),
            actual=(
                f"excluded_rows={unsupported_asset_rows}, "
                f"excluded_tickers={unsupported_asset_tickers}"
            ),
            failed_count=0,
            samples=list(unsupported_asset.get("samples", []))[:20],
            partition_key=partition_key,
        ))
    nontradable = bundle.stats.get("fundamental", {}).get(
        "no_tradable_price_asset",
        {"row_count": 0, "ticker_count": 0, "samples": []},
    )
    nontradable_rows = int(nontradable.get("row_count", 0))
    nontradable_tickers = int(nontradable.get("ticker_count", 0))
    if nontradable_rows > 0:
        results.append(CheckResult(
            rule_code="NO_TRADABLE_PRICE_ASSET",
            dataset="fundamental",
            severity=Severity.MODIFIED,
            status=CheckStatus.PASS,
            expected=(
                "DART ticker occurs at least once in the complete KRX price universe; "
                "otherwise exclude explicitly"
            ),
            actual=(
                f"excluded_rows={nontradable_rows}, "
                f"excluded_tickers={nontradable_tickers}"
            ),
            failed_count=0,
            samples=list(nontradable.get("samples", []))[:20],
            partition_key=partition_key,
        ))
    for stat_key, code, expected in (
        (
            "unsupported_market_action",
            "UNSUPPORTED_MARKET_ACTION_EXCLUDED",
            "corporate actions for KONEX-only assets are excluded from Silver",
        ),
        (
            "no_tradable_price_action",
            "NO_TRADABLE_PRICE_ACTION",
            "corporate actions require a ticker in the complete KRX price universe",
        ),
    ):
        detail = bundle.stats.get("corporate_action", {}).get(
            stat_key,
            {"row_count": 0, "ticker_count": 0, "samples": []},
        )
        row_count = int(detail.get("row_count", 0))
        if row_count > 0:
            results.append(CheckResult(
                rule_code=code,
                dataset="corporate_action",
                severity=Severity.MODIFIED,
                status=CheckStatus.PASS,
                expected=expected,
                actual=(
                    f"excluded_rows={row_count}, "
                    f"excluded_tickers={int(detail.get('ticker_count', 0))}"
                ),
                failed_count=0,
                samples=list(detail.get("samples", []))[:20],
                partition_key=partition_key,
            ))
    full_statement = bundle.stats.get("fundamental", {}).get(
        "full_statement_supplement",
        {"row_count": 0, "file_count": 0},
    )
    full_statement_rows = int(full_statement.get("row_count", 0))
    if full_statement_rows > 0:
        results.append(CheckResult(
            rule_code="DART_FULL_STATEMENT_SUPPLEMENT",
            dataset="fundamental",
            severity=Severity.MODIFIED,
            status=CheckStatus.PASS,
            expected=(
                "full-statement source fills only business keys absent from "
                "the DART major-account source"
            ),
            actual=(
                f"supplemented_rows={full_statement_rows}, "
                f"source_files={int(full_statement.get('file_count', 0))}"
            ),
            failed_count=0,
            partition_key=partition_key,
        ))
    presentation_conflict = bundle.stats.get("fundamental", {}).get(
        "presentation_conflict_resolved",
        {"row_count": 0, "group_count": 0, "samples": []},
    )
    presentation_conflict_rows = int(presentation_conflict.get("row_count", 0))
    if presentation_conflict_rows > 0:
        results.append(CheckResult(
            rule_code="DART_PRESENTATION_CONFLICT_RESOLVED",
            dataset="fundamental",
            severity=Severity.MODIFIED,
            status=CheckStatus.PASS,
            expected=(
                "when one filing presents a metric twice with different values "
                "(dual income-statement formats), keep the first-presented "
                "(min ord) line and drop the rest"
            ),
            actual=(
                f"resolved_rows={presentation_conflict_rows}, "
                f"groups={int(presentation_conflict.get('group_count', 0))}"
            ),
            failed_count=0,
            samples=list(presentation_conflict.get("samples", []))[:20],
            partition_key=partition_key,
        ))
    negative_dividend = bundle.stats.get("fundamental", {}).get(
        "negative_dividend_excluded",
        {"row_count": 0, "samples": []},
    )
    negative_dividend_rows = int(negative_dividend.get("row_count", 0))
    if negative_dividend_rows > 0:
        results.append(CheckResult(
            rule_code="NEGATIVE_DIVIDEND_EXCLUDED",
            dataset="fundamental",
            severity=Severity.MODIFIED,
            status=CheckStatus.PASS,
            expected=(
                "cash amount, yield and per-share dividend are non-negative; "
                "negative source values are excluded before publish"
            ),
            actual=f"excluded_rows={negative_dividend_rows}",
            failed_count=0,
            samples=list(negative_dividend.get("samples", []))[:20],
            partition_key=partition_key,
        ))
    implausible_value = bundle.stats.get("fundamental", {}).get(
        "implausible_value_excluded",
        {"row_count": 0, "samples": []},
    )
    implausible_value_rows = int(implausible_value.get("row_count", 0))
    if implausible_value_rows > 0:
        results.append(CheckResult(
            rule_code="FUNDAMENTAL_VALUE_EXCLUDED",
            dataset="fundamental",
            severity=Severity.MODIFIED,
            status=CheckStatus.PASS,
            expected=(
                "physically impossible values (total_assets<=0, revenue<0, "
                "total_liabilities<0) are excluded before publish"
            ),
            actual=f"excluded_rows={implausible_value_rows}",
            failed_count=0,
            samples=list(implausible_value.get("samples", []))[:20],
            partition_key=partition_key,
        ))
    gross_excluded = bundle.stats.get("fundamental", {}).get(
        "accounting_equation_gross_excluded",
        {"row_count": 0, "scope_count": 0, "samples": []},
    )
    gross_excluded_rows = int(gross_excluded.get("row_count", 0))
    if gross_excluded_rows > 0:
        results.append(CheckResult(
            rule_code="ACCOUNTING_EQUATION_GROSS_EXCLUDED",
            dataset="fundamental",
            severity=Severity.MODIFIED,
            status=CheckStatus.PASS,
            expected=(
                "filing scopes whose assets != liabilities + equity by >10% are "
                "excluded before publish (statement untrustworthy)"
            ),
            actual=(
                f"excluded_rows={gross_excluded_rows}, "
                f"scopes={int(gross_excluded.get('scope_count', 0))}"
            ),
            failed_count=0,
            samples=list(gross_excluded.get("samples", []))[:20],
            partition_key=partition_key,
        ))
    replacement = bundle.stats.get("fundamental", {}).get(
        "accounting_equation_supplement_replacement",
        {"row_count": 0, "scope_count": 0, "samples": []},
    )
    replacement_rows = int(replacement.get("row_count", 0))
    replacement_scopes = int(replacement.get("scope_count", 0))
    if replacement_rows > 0:
        results.append(CheckResult(
            rule_code="DART_ACCOUNTING_EQUATION_SUPPLEMENT_REPLACEMENT",
            dataset="fundamental",
            severity=Severity.MODIFIED,
            status=CheckStatus.PASS,
            expected=(
                "replace assets/liabilities/equity atomically only when the "
                "same-revision DART full statement balances within 1%"
            ),
            actual=(
                f"replaced_scopes={replacement_scopes}, "
                f"replaced_rows={replacement_rows}"
            ),
            failed_count=0,
            samples=list(replacement.get("samples", []))[:20],
            partition_key=partition_key,
        ))
    results.extend(check_market_coverage_floor(
        bundle.prices,
        bundle.stats,
        target_date,
        bool(bundle.stats.get("_market_closed", False)),
    ))
    if not bundle.prices.empty:
        results.extend(check_prices(
            bundle.prices,
            target_date=target_date,
            history=history,
            corporate_actions=bundle.actions,
            partition_key=partition_key,
        ))
    if not bundle.fundamentals.empty:
        results.extend(check_financials(
            bundle.fundamentals,
            partition_key,
            source_inconsistency=bundle.stats.get(
                "fundamental",
                {},
            ).get("source_accounting_inconsistency"),
        ))
    if changed_action_receipts is not None or not quality_actions.empty:
        results.extend(check_actions(quality_actions, partition_key))
    results.extend(check_reconciliation(bundle.stats, partition_key))
    return results
