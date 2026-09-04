"""DART 배당·기업행사 Bronze의 source-scoped Silver 적재를 준비한다.

Standalone CLI apply는 총수익 rebuild와 audit까지 원자적으로 닫을 수 없어
비활성화되어 있다. 실제 쓰기는 ``pipeline.daily_full`` 또는
``pipeline.dart_silver_backfill_ecs`` closed orchestrator에서만 수행한다.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path

import pandas as pd

from pipeline.common import db
from pipeline.common.paths import base_uri
from pipeline.silver import corporate_actions, dividends, financials
from pipeline.silver.dividend_evidence import (
    assert_verified_cash_evidence,
    included_cash_parity_digest,
    published_action_digest,
    source_receipt_digest,
    terminal_source_receipt_digest,
)
from pipeline.silver.cash_adjustment_scale_evidence import (
    SUPPORT_ACTION_COLUMNS,
    SOURCE_EVIDENCE_COLUMNS,
    bind_source_evidence,
    source_evidence_metadata,
    verify_source_evidence_manifest,
)
from pipeline.silver.dart_action_snapshot import (
    DEFAULT_COVERAGE_START,
    SCHEMA_VERSION as ACTION_SNAPSHOT_SCHEMA_VERSION,
    VerifiedActionSnapshot,
    verify_snapshot_manifest,
)
from pipeline.silver.return_identity import (
    PitActionMapStats,
    krx_common_stock_identity_digest,
    map_actions_to_pit_assets,
)
from pipeline.silver.return_contract import (
    acquire_return_writer_transaction_lock,
    is_valid_krx_ticker,
    normalize_krx_ticker,
)
from pipeline.silver.reviewed_cash_scale_exceptions import (
    PAID_RIGHTS_COMPONENT_BODY_SHA256,
    PAID_RIGHTS_IDENTITY,
)
from pipeline.silver_quality import repository
from pipeline.silver_quality.models import CandidateBundle
from pipeline.silver_quality.runner import assert_publishable, evaluate, print_summary


_EMPTY_ROWSET_DIGEST = hashlib.sha256(b"[]").hexdigest()


@dataclass(frozen=True)
class DartExtraSummary:
    apply: bool
    action_count: int
    dividend_count: int
    snapshot_body_digest: str
    snapshot_body_count: int
    snapshot_coverage_start: str
    snapshot_coverage_end: str
    pit_mapped_common_stock_count: int
    pit_before_contract_count: int
    pit_out_of_scope_instrument_count: int
    pit_out_of_scope_market_count: int
    pit_out_of_scope_market_ticker_count: int
    pit_out_of_scope_market_classes: dict[str, int]
    pit_included_corp_cls_counts: dict[str, int]
    pit_excluded_reason_counts: dict[str, int]
    source_cash_receipt_count: int
    economic_decision_count: int
    attachment_correction_count: int
    no_common_cash_dividend_count: int
    withdrawn_or_cancelled_count: int
    pending_record_date_count: int
    unresolved_cash_receipt_count: int
    included_cash_receipt_count: int
    excluded_cash_receipt_count: int
    included_cash_receipts_by_corp_cls: dict[str, int]
    excluded_cash_receipts_by_corp_cls: dict[str, int]
    cash_receipt_exclusion_reasons: dict[str, int]
    source_receipt_row_digest: str
    terminal_economic_receipt_count: int
    terminal_economic_receipt_digest: str
    published_action_count: int
    published_action_row_digest: str
    published_action_scope_contract: str
    included_cash_action_parity_count: int
    included_cash_action_parity_digest: str
    run_id: str | None = None


def _identifier_map(conn) -> dict[str, int]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT identifier, asset_id FROM asset_identifier "
            "WHERE source='KRX' AND valid_to IS NULL"
        )
        return {str(identifier): int(asset_id) for identifier, asset_id in cur.fetchall()}


def _all_krx_identifiers(conn) -> set[str]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT DISTINCT identifier FROM asset_identifier "
            "WHERE source='KRX' AND identifier_type='ticker'"
        )
        return {str(row[0]) for row in cur.fetchall()}


def _exclude_unmapped(frame, allowed: set[str]) -> tuple[object, dict]:
    if frame.empty:
        return frame, {"row_count": 0, "ticker_count": 0, "samples": []}
    identifiers = frame["identifier"].astype(str)
    missing = frame[~identifiers.isin(allowed)]
    retained = frame[identifiers.isin(allowed)].reset_index(drop=True)
    return retained, {
        "row_count": len(missing),
        "ticker_count": int(missing["identifier"].astype(str).nunique()),
        "samples": (
            missing[["identifier", "source_file"]].drop_duplicates()
            .head(20).to_dict("records")
        ),
    }


def _manifest_support_action_candidates(
    scale_evidence,
) -> pd.DataFrame:
    """Create exact manifest-bound actions absent from native DART input.

    KIND rows are official exchange artifacts.  ``DART_VIEWER`` rows are
    official, content-addressed DART viewer bodies used only when a verified
    issuer family's exact structured/disclosure economics are unavailable.
    """
    support = scale_evidence.support_frame
    support = support[support["support_action_source"].isin({
        "KRX_KIND", "DART_VIEWER",
    })]
    if support.empty:
        return pd.DataFrame(columns=corporate_actions.COLUMNS)
    parent_by_key = scale_evidence.frame.set_index("evidence_key")
    records_by_identity: dict[tuple[str, str, str, str], dict] = {}
    support_by_identity: dict[tuple[str, str, str, str], dict] = {}
    reusable_support_fields = (
        "support_action_source", "support_action_key", "support_action_type",
        "support_action_body_path", "support_action_body_sha256",
        "support_announcement_date", "support_ex_date", "support_record_date",
        "support_ratio_numerator", "support_ratio_denominator",
        "support_entitlement_security_class",
        "support_distributed_security_class",
        "support_expected_price_factor", "support_reference_price",
        "support_reason", "support_report_name", "support_action_scope",
        "support_semantic_role",
    )

    def same_value(left, right) -> bool:
        left_missing = left is None or pd.isna(left)
        right_missing = right is None or pd.isna(right)
        if left_missing or right_missing:
            return left_missing and right_missing
        return left == right

    for row in support.to_dict("records"):
        evidence_key = str(row["evidence_key"])
        if evidence_key not in parent_by_key.index:
            raise RuntimeError("synthetic support action has no evidence parent")
        parent = parent_by_key.loc[evidence_key]
        if isinstance(parent, pd.DataFrame):
            raise RuntimeError("synthetic support evidence parent is ambiguous")
        if (
            str(row.get("target_cash_receipt_no") or "")
            != str(parent["cash_receipt_no"])
            or pd.Timestamp(row.get("target_adjustment_date")).date()
            != pd.Timestamp(parent["adjustment_trade_date"]).date()
        ):
            raise RuntimeError("synthetic support target/parent parity failed")
        source = str(row["support_action_source"])
        action_type = str(row["support_action_type"])
        if source == "DART_VIEWER":
            numerator = float(row.get("support_ratio_numerator") or 0)
            denominator = float(row.get("support_ratio_denominator") or 0)
            body_sha = str(row.get("support_action_body_sha256") or "")
            body_path = str(row.get("support_action_body_path") or "")
            report_name = re.sub(
                r"\s+", "", str(row.get("support_report_name") or ""),
            )
            if (
                row.get("support_semantic_role") != "ADJUSTMENT_COMPONENT"
                or row.get("support_entitlement_security_class") != "COMMON"
                or row.get("support_distributed_security_class") != "COMMON"
                or row.get("support_action_scope") != "ISSUER"
                or re.fullmatch(r"[0-9]{14}", str(
                    row.get("support_action_key") or ""
                )) is None
                or not body_path.endswith(f"sha256={body_sha}.html")
                or not body_path.startswith(
                    "corporate_actions/dart/support_action_families/"
                    "objects/sha256="
                )
                or numerator <= 0
                or denominator <= 0
            ):
                raise RuntimeError(
                    "DART_VIEWER synthetic action semantics changed"
                )
            expected = row.get("support_expected_price_factor")
            if action_type == "bonus_issue":
                if (
                    re.fullmatch(
                        r"(?:\[기재정정\])?"
                        r"주요사항보고서\(무상증자결정\)",
                        report_name,
                    ) is None
                    or row.get("support_ex_date") is None
                    or (
                        row.get("support_record_date") is not None
                        and not pd.isna(row.get("support_record_date"))
                    )
                    or expected is None
                    or pd.isna(expected)
                    or not math.isclose(
                        float(expected),
                        1.0 / (1.0 + numerator / denominator),
                        rel_tol=0,
                        abs_tol=5e-13,
                    )
                ):
                    raise RuntimeError(
                        "DART_VIEWER bonus synthetic action semantics changed"
                    )
            elif action_type == "stock_dividend":
                if (
                    re.fullmatch(
                        r"(?:\[기재정정\])?주식배당결정", report_name,
                    ) is None
                    or (
                        row.get("support_ex_date") is not None
                        and not pd.isna(row.get("support_ex_date"))
                    )
                    or row.get("support_record_date") is None
                    or pd.isna(row.get("support_record_date"))
                    or (expected is not None and not pd.isna(expected))
                ):
                    raise RuntimeError(
                        "DART_VIEWER stock-dividend synthetic action semantics "
                        "changed"
                    )
            else:
                raise RuntimeError(
                    "DART_VIEWER synthetic action type changed"
                )
        elif source == "KRX_KIND" and action_type == "paid_increase":
            paid_identity = (
                str(parent["ticker"]).zfill(6),
                str(parent["cash_receipt_no"]),
                pd.Timestamp(parent["adjustment_trade_date"]).date().isoformat(),
                str(row["support_action_key"]),
                pd.Timestamp(row["support_record_date"]).date().isoformat(),
                format(
                    float(row["support_ratio_numerator"])
                    / float(row["support_ratio_denominator"]),
                    ".12g",
                ),
            )
            if (
                paid_identity != PAID_RIGHTS_IDENTITY
                or str(row["support_action_body_sha256"])
                != PAID_RIGHTS_COMPONENT_BODY_SHA256
                or row["support_semantic_role"] != "ADJUSTMENT_COMPONENT"
                or row["support_entitlement_security_class"] != "COMMON"
                or row["support_distributed_security_class"] != "COMMON"
                or (
                    row.get("support_expected_price_factor") is not None
                    and not pd.isna(row.get("support_expected_price_factor"))
                )
            ):
                raise RuntimeError("paid-rights synthetic identity changed")
        identity = (
            source,
            str(row["support_action_key"]),
            action_type,
            str(parent["ticker"]),
        )
        previous_support = support_by_identity.get(identity)
        if previous_support is not None:
            mismatched = [
                field for field in reusable_support_fields
                if not same_value(previous_support.get(field), row.get(field))
            ]
            if mismatched:
                raise RuntimeError(
                    "reused synthetic support action has conflicting immutable "
                    f"semantics: identity={identity} fields={mismatched}"
                )
            # Multiple cash receipts can legitimately reuse one immutable
            # official action.  corporate_action stores it once; evidence
            # children retain each parent-specific reference separately.
            continue
        support_by_identity[identity] = row
        records_by_identity[identity] = {
            "identifier": str(parent["ticker"]),
            "event_type": action_type,
            "announcement_date": row["support_announcement_date"],
            "effective_date": row["support_ex_date"],
            "match_window_days": 0,
            "expected_factor": row["support_expected_price_factor"],
            "share_count_factor": None,
            "share_count_before": None,
            "share_count_after": None,
            "share_count_factor_comparable": False,
            "share_count_comparison_reason": None,
            "action_method": row["support_reason"],
            "record_date": row["support_record_date"],
            "payment_date": None,
            "cash_amount": None,
            "adjusted_cash_amount": None,
            "ratio_numerator": row["support_ratio_numerator"],
            "ratio_denominator": row["support_ratio_denominator"],
            "currency": "KRW",
            "frequency": None,
            "confirms_price_adjustment": row["support_ex_date"] is not None,
            "expects_price_adjustment": True,
            "confidence": "REVIEWED_OFFICIAL_BODY",
            "rcept_no": str(row["support_action_key"]),
            "report_name": row["support_report_name"],
            "dart_rm": None,
            "corp_cls": None,
            "action_scope": row["support_action_scope"],
            "cash_amount_status": None,
            "source_evidence_status": (
                "VERIFIED_DART_VIEWER_BODY"
                if source == "DART_VIEWER" else None
            ),
            "correction_of_action_key": None,
            "revision_root_action_key": None,
            "revision_kind": None,
            "viewer_evidence_sha256": (
                row["support_action_body_sha256"]
                if source == "DART_VIEWER" else None
            ),
            "economic_evidence_sha256": None,
            "reviewed_correction_id": None,
            "payment_date_quality_status": None,
            "source_body_sha256": row["support_action_body_sha256"],
            "source": source,
            "source_file": row["support_action_body_path"],
        }
    records = [records_by_identity[key] for key in sorted(records_by_identity)]
    return pd.DataFrame(records, columns=corporate_actions.COLUMNS)


def _total_return_actions(frame, scale_evidence):
    """Keep the base cash/ex partition plus exact referenced support rows."""
    if frame.empty:
        return frame.copy()
    required = {
        "source", "rcept_no", "event_type", "action_scope",
    }
    missing = required - set(frame.columns)
    if missing:
        raise RuntimeError(
            "total-return corporate-action candidates missing columns: "
            f"{sorted(missing)}"
        )
    base = (
        frame["source"].eq("DART_DISCLOSURE")
        & frame["event_type"].isin(("cash_dividend", "ex_dividend"))
        & frame["action_scope"].eq("ISSUER")
    )
    support_keys = scale_evidence.support_frame[[
        "support_action_source", "support_action_key", "support_action_type",
    ]].drop_duplicates().rename(columns={
        "support_action_source": "source",
        "support_action_key": "rcept_no",
        "support_action_type": "event_type",
    })
    keyed = frame.merge(
        support_keys.assign(_is_scale_support=True),
        on=["source", "rcept_no", "event_type"],
        how="left",
        validate="many_to_one",
    )
    support_mask = keyed["_is_scale_support"].eq(True).to_numpy(dtype=bool)
    selected = keyed[
        (base.to_numpy(dtype=bool) | support_mask)
        & keyed["action_scope"].eq("ISSUER")
    ].drop(columns="_is_scale_support")
    matched_support = selected.merge(
        support_keys,
        on=["source", "rcept_no", "event_type"],
        how="inner",
    )[["source", "rcept_no", "event_type"]].drop_duplicates()
    if len(matched_support) != len(support_keys):
        missing = support_keys.merge(
            matched_support,
            on=["source", "rcept_no", "event_type"],
            how="left", indicator=True,
        )
        raise RuntimeError(
            "cash-scale manifest support action is absent from parser input: "
            f"{missing[missing['_merge'].eq('left_only')].head(20).to_dict('records')}"
        )
    return selected.reset_index(drop=True)


_SOURCE_RECEIPT_COLUMNS = [
    "quality_run_id", "receipt_no", "asset_id", "ticker", "corp_cls",
    "report_name", "dart_rm", "announcement_date", "revision_kind",
    "revision_root_receipt_no", "previous_receipt_no",
    "terminal_receipt_no", "terminal_announcement_date",
    "is_terminal_economic_revision",
    "source_evidence_status", "cash_amount_status", "record_date",
    "payment_date", "cash_amount", "viewer_evidence_sha256",
    "economic_evidence_sha256", "reviewed_correction_id",
    "payment_date_quality_status",
    "pit_event_date", "mapping_status", "excluded_reason",
]


def _source_receipt_frame(
    action_frame: pd.DataFrame,
    *,
    quality_run_id,
) -> pd.DataFrame:
    normalized = corporate_actions.normalize_for_publish(action_frame)
    cash = normalized[
        normalized["source"].eq("DART_DISCLOSURE")
        & normalized["action_type"].eq("cash_dividend")
    ].copy()
    if cash.empty:
        raise RuntimeError("mapped DART action snapshot has no cash receipts")
    cash["quality_run_id"] = quality_run_id
    cash["receipt_no"] = cash["action_key"].astype(str)
    cash["ticker"] = cash["identifier"].map(normalize_krx_ticker)
    cash["revision_root_receipt_no"] = (
        cash["revision_root_action_key"].fillna("").astype(str)
    )
    cash["revision_root_receipt_no"] = cash[
        "revision_root_receipt_no"
    ].where(
        cash["revision_root_receipt_no"].ne(""), cash["receipt_no"]
    )
    cash["previous_receipt_no"] = cash["correction_of_action_key"]
    economic = cash[
        cash["revision_kind"].fillna("").ne("ATTACHMENT_ONLY")
        & cash["cash_amount_status"].ne("ATTACHMENT_ONLY")
    ].copy()
    if economic.empty:
        raise RuntimeError("DART cash receipt families have no economic rows")
    terminal = (
        economic.sort_values(
            ["ticker", "revision_root_receipt_no", "announcement_date",
             "receipt_no"],
            kind="stable",
        )
        .groupby(
            ["ticker", "revision_root_receipt_no"],
            sort=False,
            as_index=False,
        )
        .tail(1)
    )
    terminal_lookup = {
        (str(row.ticker), str(row.revision_root_receipt_no)): (
            str(row.receipt_no), row.announcement_date,
        )
        for row in terminal.itertuples(index=False)
    }
    family_keys = list(zip(
        cash["ticker"].astype(str),
        cash["revision_root_receipt_no"].astype(str),
    ))
    missing_families = sorted(set(family_keys) - set(terminal_lookup))
    if missing_families:
        raise RuntimeError(
            f"DART cash receipt families have no terminal evidence: "
            f"{missing_families[:20]}"
        )
    cash["terminal_receipt_no"] = [
        terminal_lookup[key][0] for key in family_keys
    ]
    cash["terminal_announcement_date"] = [
        terminal_lookup[key][1] for key in family_keys
    ]
    cash["is_terminal_economic_revision"] = cash["receipt_no"].eq(
        cash["terminal_receipt_no"]
    )
    terminal_pending = cash[
        cash["is_terminal_economic_revision"]
        & cash["cash_amount_status"].eq("POSITIVE_PENDING_RECORD_DATE")
    ]
    if not terminal_pending.empty:
        raise RuntimeError(
            "DART cash receipt family has an incomplete terminal revision: "
            f"{terminal_pending['receipt_no'].astype(str).tolist()[:20]}"
        )
    if "rcept_no" not in action_frame:
        raise RuntimeError("DART action audit rows require receipt numbers")
    receipt_keys = action_frame["rcept_no"].fillna("").astype(str)
    if receipt_keys.eq("").any() or receipt_keys.duplicated().any():
        raise RuntimeError("DART action audit receipt numbers are missing/duplicate")
    audit_by_receipt = action_frame.assign(
        _receipt_no=action_frame["rcept_no"].astype(str)
    ).set_index("_receipt_no")
    cash["pit_event_date"] = cash["receipt_no"].map(
        audit_by_receipt["pit_event_date"]
    )
    cash["mapping_status"] = cash["receipt_no"].map(
        audit_by_receipt["pit_mapping_status"]
    )
    cash["excluded_reason"] = cash["receipt_no"].map(
        audit_by_receipt["pit_excluded_reason"]
    )
    assert_verified_cash_evidence(
        cash,
        action_key_column="receipt_no",
        root_key_column="revision_root_receipt_no",
    )
    if not cash["ticker"].map(is_valid_krx_ticker).all():
        raise RuntimeError("DART cash source receipt has an invalid ticker")
    if cash["receipt_no"].duplicated().any():
        raise RuntimeError("DART cash source receipt numbers are not unique")
    included = cash["mapping_status"].eq("INCLUDED")
    excluded = cash["mapping_status"].eq("EXCLUDED")
    invalid_partition = (
        ~(included | excluded)
        | (included & cash["asset_id"].isna())
        | (included & cash["excluded_reason"].notna())
        | (excluded & cash["excluded_reason"].isna())
    )
    if invalid_partition.any():
        sample = cash.loc[
            invalid_partition,
            ["ticker", "receipt_no", "asset_id", "mapping_status", "excluded_reason"],
        ].head(20).to_dict("records")
        raise RuntimeError(f"invalid DART cash receipt mapping partition: {sample}")
    try:
        cash["asset_id"] = cash["asset_id"].astype("Int64")
    except (TypeError, ValueError) as exc:
        raise RuntimeError(
            "DART cash source receipt has a non-integral asset identity"
        ) from exc
    return cash[_SOURCE_RECEIPT_COLUMNS].reset_index(drop=True)


def _source_receipt_stats(frame: pd.DataFrame) -> dict[str, object]:
    attachment = frame["cash_amount_status"].eq("ATTACHMENT_ONLY")
    no_common = frame["cash_amount_status"].eq("NO_COMMON_CASH_DIVIDEND")
    cancelled = frame["cash_amount_status"].eq("NO_ECONOMIC_EVENT")
    pending = frame["cash_amount_status"].eq(
        "POSITIVE_PENDING_RECORD_DATE"
    )
    economic = frame["is_terminal_economic_revision"].fillna(False)
    included = frame["mapping_status"].eq("INCLUDED")
    excluded = frame["mapping_status"].eq("EXCLUDED")
    if not (included | excluded).all():
        raise RuntimeError("cash receipt mapping partition is incomplete")
    reason_counts = {
        str(key): int(value)
        for key, value in frame.loc[
            excluded, "excluded_reason"
        ].value_counts(dropna=False).to_dict().items()
    }
    if sum(reason_counts.values()) != int(excluded.sum()):
        raise RuntimeError("cash receipt exclusion reason parity failed")
    if len(frame) != int(included.sum()) + int(excluded.sum()):
        raise RuntimeError("cash receipt inclusion/exclusion parity failed")
    included_classes = (
        frame.loc[included, "corp_cls"].fillna("UNKNOWN").astype(str)
        .value_counts().to_dict()
    )
    excluded_classes = (
        frame.loc[excluded, "corp_cls"].fillna("UNKNOWN").astype(str)
        .value_counts().to_dict()
    )
    return {
        "source_cash_receipt_count": len(frame),
        "economic_decision_count": int(economic.sum()),
        "attachment_correction_count": int(attachment.sum()),
        "no_common_cash_dividend_count": int(no_common.sum()),
        "withdrawn_or_cancelled_count": int(cancelled.sum()),
        "pending_record_date_count": int(pending.sum()),
        "unresolved_cash_receipt_count": 0,
        "included_cash_receipt_count": int(included.sum()),
        "excluded_cash_receipt_count": int(excluded.sum()),
        "included_cash_receipts_by_corp_cls": {
            str(key): int(value) for key, value in included_classes.items()
        },
        "excluded_cash_receipts_by_corp_cls": {
            str(key): int(value) for key, value in excluded_classes.items()
        },
        "cash_receipt_exclusion_reasons": reason_counts,
        "source_receipt_row_digest": source_receipt_digest(frame),
        "terminal_economic_receipt_count": int(economic.sum()),
        "terminal_economic_receipt_digest": (
            terminal_source_receipt_digest(frame)
        ),
    }


def _published_action_contract(
    action_frame: pd.DataFrame,
    receipt_frame: pd.DataFrame,
) -> dict[str, object]:
    published = corporate_actions.normalize_for_publish(action_frame)
    if published.empty or published["asset_id"].isna().any():
        raise RuntimeError("published TR action partition lacks PIT asset identity")
    if not published["action_scope"].eq("ISSUER").all():
        raise RuntimeError("published TR action partition is not issuer scoped")
    published["asset_id"] = published["asset_id"].astype("int64")
    cash = published[published["action_type"].eq("cash_dividend")].copy()
    cash_parity = cash.rename(columns={
        "action_key": "receipt_no",
        "correction_of_action_key": "previous_receipt_no",
        "revision_root_action_key": "revision_root_receipt_no",
    })
    included_receipts = receipt_frame[
        receipt_frame["mapping_status"].eq("INCLUDED")
    ].copy()
    action_parity_digest = included_cash_parity_digest(cash_parity)
    receipt_parity_digest = included_cash_parity_digest(included_receipts)
    if len(cash_parity) != len(included_receipts) or (
        action_parity_digest != receipt_parity_digest
    ):
        raise RuntimeError(
            "included DART receipt/corporate-action exact parity failed"
        )
    return {
        "published_action_count": len(published),
        "published_action_row_digest": published_action_digest(published),
        "published_action_scope_contract": (
            "issuer_cash_ex_plus_manifest_scale_support_v1"
        ),
        "included_cash_action_parity_count": len(cash_parity),
        "included_cash_action_parity_digest": action_parity_digest,
    }


def _publish_source_receipts(conn, frame: pd.DataFrame) -> int:
    rows = list(
        frame[_SOURCE_RECEIPT_COLUMNS].astype(object).where(
            pd.notna(frame[_SOURCE_RECEIPT_COLUMNS]), None,
        ).itertuples(index=False, name=None)
    )
    return db.upsert(
        conn,
        "dividend_source_receipt",
        _SOURCE_RECEIPT_COLUMNS,
        rows,
        conflict=["quality_run_id", "receipt_no"],
        update=[],
        temp_name="_stg_dividend_source_receipt",
    )


def _bound_scale_evidence(
    scale_evidence,
    *,
    action_frame: pd.DataFrame,
    receipt_frame: pd.DataFrame,
    quality_run_id,
):
    published = corporate_actions.normalize_for_publish(action_frame)
    published["quality_run_id"] = quality_run_id
    return bind_source_evidence(
        scale_evidence,
        receipt_frame=receipt_frame,
        published_actions=published,
        action_snapshot_run_id=quality_run_id,
    )


def _publish_scale_evidence(conn, bound) -> tuple[int, int]:
    parent_rows = list(
        bound.frame[list(SOURCE_EVIDENCE_COLUMNS)].astype(object).where(
            pd.notna(bound.frame[list(SOURCE_EVIDENCE_COLUMNS)]), None,
        ).itertuples(index=False, name=None)
    )
    support_rows = list(
        bound.support_frame[list(SUPPORT_ACTION_COLUMNS)].astype(object).where(
            pd.notna(bound.support_frame[list(SUPPORT_ACTION_COLUMNS)]), None,
        ).itertuples(index=False, name=None)
    )
    parent_count = db.upsert(
        conn,
        "cash_adjustment_scale_source_evidence",
        list(SOURCE_EVIDENCE_COLUMNS),
        parent_rows,
        conflict=["action_snapshot_run_id", "evidence_key"],
        update=[],
        temp_name="_stg_cash_adjustment_scale_source_evidence",
    )
    support_count = db.upsert(
        conn,
        "cash_adjustment_scale_support_action",
        list(SUPPORT_ACTION_COLUMNS),
        support_rows,
        conflict=[
            "action_snapshot_run_id", "evidence_key",
            "support_action_source", "support_action_key",
            "support_action_type",
        ],
        update=[],
        temp_name="_stg_cash_adjustment_scale_support_action",
    )
    return parent_count, support_count


def run(
    *,
    src: str = "local",
    base_override: str | None = None,
    total_return_actions_only: bool = False,
    apply: bool = False,
    expected_coverage_end: date | None = None,
    conn=None,
) -> DartExtraSummary:
    """Publish a complete local DART snapshot into existing KRX Silver.

    ``base_override`` is intended for an immutable temporary download used by
    repair/audit jobs.  It does not change the configured Bronze root.
    """
    if apply and expected_coverage_end is None:
        raise ValueError("--apply requires --expected-coverage-end")
    if apply and not total_return_actions_only:
        raise ValueError(
            "generic DART dividend fundamentals apply is disabled until an "
            "alot-matter content-addressed completeness manifest exists; "
            "use --total-return-actions-only for the certified TR recovery"
        )
    base = str(Path(base_override).resolve()) if base_override else base_uri(src)
    verified = verify_snapshot_manifest(
        base,
        required_start=DEFAULT_COVERAGE_START,
        required_end=expected_coverage_end,
    )
    scale_evidence = verify_source_evidence_manifest(
        base,
        required_start=verified.coverage_start,
        required_end=verified.coverage_end,
    )
    if verified.cash_adjustment_scale_source_evidence != scale_evidence.metadata:
        raise RuntimeError("action snapshot/cash-scale manifest metadata mismatch")
    owns_connection = conn is None
    connection = conn or db.connect()
    identity = krx_common_stock_identity_digest(
        connection,
        coverage_start=DEFAULT_COVERAGE_START,
        coverage_end=verified.coverage_end,
    )
    context = None
    results = []

    def prepare_bundle() -> tuple[
        CandidateBundle,
        pd.DataFrame,
        pd.DataFrame,
        dict[str, int],
        PitActionMapStats,
        pd.DataFrame,
    ]:
        if total_return_actions_only:
            dividend_frame = pd.DataFrame(columns=dividends.COLUMNS)
            dividend_stats = {
                "input_rows": 0,
                "transformed_rows": 0,
                "excluded_rows": 0,
                "rejected_rows": 0,
                "duplicate_rows_removed": 0,
                "source_file_count": 0,
            }
        else:
            dividend_frame, dividend_stats = dividends.prepare(base)
        action_frame, action_stats = corporate_actions.prepare(
            base,
            coverage_start=verified.coverage_start,
            coverage_end=verified.coverage_end,
            verified_snapshot_sha256=verified.manifest_sha256,
        )
        if total_return_actions_only:
            manifest_support = _manifest_support_action_candidates(scale_evidence)
            if not manifest_support.empty:
                action_frame = pd.concat(
                    [action_frame, manifest_support], ignore_index=True,
                )
            before_filter = len(action_frame)
            action_frame = _total_return_actions(action_frame, scale_evidence)
            if (
                action_frame.empty
                or not action_frame["event_type"].eq("cash_dividend").any()
            ):
                raise RuntimeError(
                    "complete DART snapshot has no ISSUER cash-dividend"
                )
            action_frame, pit_stats, source_action_frame = map_actions_to_pit_assets(
                connection,
                action_frame,
                coverage_start=DEFAULT_COVERAGE_START,
                include_audit=True,
                verified_snapshot_sha256=verified.manifest_sha256,
                asset_identity_digest=identity.digest,
            )
            action_stats = dict(action_stats)
            if (
                action_frame.empty
                or not action_frame["event_type"].eq("cash_dividend").any()
            ):
                raise RuntimeError(
                    "DART snapshot has no 2015+ common-stock cash-dividend"
                )
            action_stats.update({
                "row_count": len(action_frame),
                "total_return_action_input_rows": before_filter,
                "total_return_action_excluded_rows": before_filter - len(action_frame),
                "pit_mapping": asdict(pit_stats),
            })
        else:
            pit_stats = PitActionMapStats(0, 0, 0, 0)
            source_action_frame = pd.DataFrame()
        identifier_map = _identifier_map(connection)
        allowed = _all_krx_identifiers(connection)
        dividend_frame, dividend_unmapped = _exclude_unmapped(
            dividend_frame, set(identifier_map),
        )
        if total_return_actions_only:
            action_unmapped = {"row_count": 0, "ticker_count": 0, "samples": []}
        else:
            action_frame, action_unmapped = _exclude_unmapped(
                action_frame, set(identifier_map)
            )
        dividend_stats = dict(dividend_stats)
        excluded_rows = int(dividend_unmapped["row_count"])
        dividend_stats["transformed_rows"] = len(dividend_frame)
        dividend_stats["excluded_rows"] = (
            int(dividend_stats.get("excluded_rows", 0)) + excluded_rows
        )
        bundle = CandidateBundle(
            fundamentals=dividend_frame,
            actions=action_frame,
            stats={
                "fundamental": dividend_stats,
                "corporate_action": action_stats,
                "_existing_krx_identifiers": allowed,
                "_dividend_unmapped": dividend_unmapped,
                "_action_unmapped": action_unmapped,
            },
        )
        return (
            bundle, dividend_frame, action_frame, identifier_map, pit_stats,
            source_action_frame,
        )

    def summary_for(
        dividend_frame: pd.DataFrame,
        action_frame: pd.DataFrame,
        pit_stats: PitActionMapStats,
        *,
        run_id: str | None,
        receipt_stats: dict[str, object],
        published_action_stats: dict[str, object],
    ) -> DartExtraSummary:
        return DartExtraSummary(
            apply=apply,
            action_count=len(action_frame),
            dividend_count=len(dividend_frame),
            snapshot_body_digest=verified.body_digest,
            snapshot_body_count=verified.body_count,
            snapshot_coverage_start=verified.coverage_start.isoformat(),
            snapshot_coverage_end=verified.coverage_end.isoformat(),
            pit_mapped_common_stock_count=pit_stats.mapped_common_stock_count,
            pit_before_contract_count=pit_stats.before_contract_count,
            pit_out_of_scope_instrument_count=(
                pit_stats.out_of_scope_instrument_count
            ),
            pit_out_of_scope_market_count=(
                pit_stats.out_of_scope_market_count
            ),
            pit_out_of_scope_market_ticker_count=(
                pit_stats.out_of_scope_market_ticker_count
            ),
            pit_out_of_scope_market_classes=(
                pit_stats.out_of_scope_market_classes
            ),
            pit_included_corp_cls_counts=pit_stats.included_corp_cls_counts,
            pit_excluded_reason_counts=pit_stats.excluded_reason_counts,
            **receipt_stats,
            **published_action_stats,
            run_id=run_id,
        )

    try:
        if not apply:
            with connection.transaction():
                with connection.cursor() as cur:
                    cur.execute(
                        "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, "
                        "READ ONLY"
                    )
                repository.assert_schema(connection)
                (
                    bundle, dividend_frame, action_frame, _, pit_stats,
                    source_action_frame,
                ) = prepare_bundle()
                repeated = verify_snapshot_manifest(
                    base,
                    required_start=DEFAULT_COVERAGE_START,
                    required_end=expected_coverage_end,
                )
                if repeated != verified:
                    raise RuntimeError("DART snapshot changed during parse")
                results = evaluate(bundle)
                print_summary(results)
                assert_publishable(results)
            receipt_frame = _source_receipt_frame(
                source_action_frame, quality_run_id=None,
            ) if total_return_actions_only else pd.DataFrame()
            receipt_stats = (
                _source_receipt_stats(receipt_frame)
                if total_return_actions_only else {
                    "source_cash_receipt_count": 0,
                    "economic_decision_count": 0,
                    "attachment_correction_count": 0,
                    "no_common_cash_dividend_count": 0,
                    "withdrawn_or_cancelled_count": 0,
                    "pending_record_date_count": 0,
                    "unresolved_cash_receipt_count": 0,
                    "included_cash_receipt_count": 0,
                    "excluded_cash_receipt_count": 0,
                    "included_cash_receipts_by_corp_cls": {},
                    "excluded_cash_receipts_by_corp_cls": {},
                    "cash_receipt_exclusion_reasons": {},
                    "source_receipt_row_digest": _EMPTY_ROWSET_DIGEST,
                    "terminal_economic_receipt_count": 0,
                    "terminal_economic_receipt_digest": _EMPTY_ROWSET_DIGEST,
                }
            )
            if total_return_actions_only:
                _bound_scale_evidence(
                    scale_evidence,
                    action_frame=action_frame,
                    receipt_frame=receipt_frame,
                    quality_run_id=None,
                )
            published_action_stats = (
                _published_action_contract(action_frame, receipt_frame)
                if total_return_actions_only else {
                    "published_action_count": 0,
                    "published_action_row_digest": _EMPTY_ROWSET_DIGEST,
                    "published_action_scope_contract": (
                        "issuer_cash_ex_plus_manifest_scale_support_v1"
                    ),
                    "included_cash_action_parity_count": 0,
                    "included_cash_action_parity_digest": _EMPTY_ROWSET_DIGEST,
                }
            )
            summary = summary_for(
                dividend_frame, action_frame, pit_stats, run_id=None,
                receipt_stats=receipt_stats,
                published_action_stats=published_action_stats,
            )
            print(json.dumps(asdict(summary), ensure_ascii=False, indent=2))
            return summary

        repository.assert_schema(connection)
        connection.commit()
        context = repository.start_run(
            connection,
            mode="dart_dividend_action_backfill",
            input_fingerprint=verified.body_digest,
        )
        with connection.transaction():
            with connection.cursor() as cur:
                cur.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            acquire_return_writer_transaction_lock(connection)
            # Re-verify immediately before parse/publish so an edited local
            # body cannot race the manifest preflight.
            repeated = verify_snapshot_manifest(
                base,
                required_start=DEFAULT_COVERAGE_START,
                required_end=expected_coverage_end,
            )
            if repeated != verified:
                raise RuntimeError("DART snapshot changed before apply")
            (
                bundle, dividend_frame, action_frame, identifier_map,
                pit_stats, source_action_frame,
            ) = prepare_bundle()
            results = evaluate(bundle)
            print_summary(results)
            assert_publishable(results)
            # Re-verify after parsing and immediately before the first DML.
            # This closes the manifest/body TOCTOU window.
            after_parse = verify_snapshot_manifest(
                base,
                required_start=DEFAULT_COVERAGE_START,
                required_end=expected_coverage_end,
            )
            if after_parse != verified:
                raise RuntimeError("DART snapshot changed during parse")
            receipt_frame = (
                _source_receipt_frame(
                    source_action_frame, quality_run_id=context.run_id,
                )
                if total_return_actions_only else pd.DataFrame()
            )
            receipt_stats = (
                _source_receipt_stats(receipt_frame)
                if total_return_actions_only else {
                    "source_cash_receipt_count": 0,
                    "economic_decision_count": 0,
                    "attachment_correction_count": 0,
                    "no_common_cash_dividend_count": 0,
                    "withdrawn_or_cancelled_count": 0,
                    "pending_record_date_count": 0,
                    "unresolved_cash_receipt_count": 0,
                    "included_cash_receipt_count": 0,
                    "excluded_cash_receipt_count": 0,
                    "included_cash_receipts_by_corp_cls": {},
                    "excluded_cash_receipts_by_corp_cls": {},
                    "cash_receipt_exclusion_reasons": {},
                    "source_receipt_row_digest": _EMPTY_ROWSET_DIGEST,
                    "terminal_economic_receipt_count": 0,
                    "terminal_economic_receipt_digest": _EMPTY_ROWSET_DIGEST,
                }
            )
            bound_scale = (
                _bound_scale_evidence(
                    scale_evidence,
                    action_frame=action_frame,
                    receipt_frame=receipt_frame,
                    quality_run_id=context.run_id,
                )
                if total_return_actions_only else None
            )
            scale_metadata = (
                source_evidence_metadata(
                    bound_scale.frame,
                    bound_scale.support_frame,
                    verified=scale_evidence,
                )
                if bound_scale is not None else None
            )
            published_action_stats = (
                _published_action_contract(action_frame, receipt_frame)
                if total_return_actions_only else {
                    "published_action_count": 0,
                    "published_action_row_digest": _EMPTY_ROWSET_DIGEST,
                    "published_action_scope_contract": (
                        "issuer_cash_ex_plus_manifest_scale_support_v1"
                    ),
                    "included_cash_action_parity_count": 0,
                    "included_cash_action_parity_digest": _EMPTY_ROWSET_DIGEST,
                }
            )
            if not total_return_actions_only:
                financials.publish(
                    connection, dividend_frame, identifier_map, context.run_id,
                    replace_scopes=True,
                )
            corporate_actions.publish(
                connection, action_frame, identifier_map, context.run_id,
            )
            if total_return_actions_only:
                published_receipts = _publish_source_receipts(
                    connection, receipt_frame,
                )
                if published_receipts != receipt_stats[
                    "source_cash_receipt_count"
                ]:
                    raise RuntimeError("dividend source receipt publish parity failed")
                with connection.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO dart_action_snapshot_contract (
                            quality_run_id,schema_version,manifest_sha256,
                            body_digest,body_count,coverage_start,coverage_end,
                            action_count,metadata
                        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)
                        """,
                        (
                            context.run_id,
                            ACTION_SNAPSHOT_SCHEMA_VERSION,
                            verified.manifest_sha256,
                            verified.body_digest,
                            verified.body_count,
                            verified.coverage_start,
                            verified.coverage_end,
                            len(action_frame),
                            json.dumps(
                                {
                                    "total_return_actions_only": True,
                                    "markets": ["KOSPI", "KOSDAQ"],
                                    "disclosure_observation_audit": (
                                        verified.disclosure_observation_audit
                                    ),
                                    "pit_scope": {
                                        "contract": (
                                            "event_date_identity_common_stock_"
                                            "certified_kospi_kosdaq_price_episode"
                                        ),
                                        "input_action_count": pit_stats.input_count,
                                        "included_action_count": (
                                            pit_stats.mapped_common_stock_count
                                        ),
                                        "excluded_action_count": sum(
                                            pit_stats.excluded_reason_counts.values()
                                        ),
                                        "included_by_corp_cls": (
                                            pit_stats.included_corp_cls_counts
                                        ),
                                        "excluded_by_corp_cls": (
                                            pit_stats.excluded_corp_cls_counts
                                        ),
                                        "excluded_by_reason": (
                                            pit_stats.excluded_reason_counts
                                        ),
                                    },
                                    "source_receipts": receipt_stats,
                                    "published_actions": (
                                        published_action_stats
                                    ),
                                    "cash_adjustment_scale_evidence": (
                                        scale_metadata
                                    ),
                                },
                                ensure_ascii=False,
                            ),
                        ),
                    )
                assert bound_scale is not None
                parent_count, support_count = _publish_scale_evidence(
                    connection, bound_scale,
                )
                if (
                    parent_count != len(bound_scale.frame)
                    or support_count != len(bound_scale.support_frame)
                ):
                    raise RuntimeError(
                        "cash-scale parent/child evidence publish parity failed"
                    )
            repository.save_metrics(connection, context.run_id, bundle)
            repository.finish_run(
                connection, context, "CERTIFIED", results, commit=False,
            )
        summary = summary_for(
            dividend_frame,
            action_frame,
            pit_stats,
            run_id=str(context.run_id),
            receipt_stats=receipt_stats,
            published_action_stats=published_action_stats,
        )
        print(json.dumps(asdict(summary), ensure_ascii=False, indent=2))
        return summary
    except Exception as exc:
        connection.rollback()
        if context is not None:
            repository.finish_run(
                connection, context, "FAILED", results, error_message=str(exc),
            )
        raise
    finally:
        if owns_connection:
            connection.close()


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--src", choices=("local",), default="local")
    parser.add_argument(
        "--base",
        help="전체 DART snapshot이 있는 로컬 root (기본: repo data/)",
    )
    parser.add_argument(
        "--total-return-actions-only",
        action="store_true",
        help=(
            "총수익 재구축에 필요한 ISSUER cash/ex-dividend action만 적재하고 "
            "fundamental 배당 지표는 변경하지 않음"
        ),
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help=(
            "standalone apply는 비활성화됨; 쓰기는 daily_full 또는 "
            "dart_silver_backfill_ecs closed orchestrator 전용"
        ),
    )
    parser.add_argument(
        "--expected-coverage-end",
        type=date.fromisoformat,
        help="apply가 반드시 포함해야 하는 snapshot 마지막 날짜 (YYYY-MM-DD)",
    )
    return parser.parse_args(argv)


def main() -> None:
    args = parse_args()
    kwargs = {
        "src": args.src,
        "base_override": args.base,
        "total_return_actions_only": args.total_return_actions_only,
        "apply": args.apply,
        "expected_coverage_end": args.expected_coverage_end,
    }
    if not args.apply:
        run(**kwargs)
        return
    raise RuntimeError(
        "direct DART action --apply is disabled because it can stop before "
        "the total-return rebuild/audit; use pipeline.daily_full or "
        "pipeline.dart_silver_backfill_ecs --phase dart-extras"
    )


if __name__ == "__main__":
    main()
