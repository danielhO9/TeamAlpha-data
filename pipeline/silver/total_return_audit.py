"""Read-only audit of the published KRX total-return certification contract."""
from __future__ import annotations

import json
import math
import re
from contextlib import nullcontext
from datetime import date
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

import pandas as pd

from pipeline.common import db
from pipeline.silver.cash_adjustment_scale_evidence import (
    PRE_EVENT_PRICE_SCALE,
    RESOLUTION_DIGEST_COLUMNS,
    RESOLUTION_EVIDENCE_CONTRACT,
    SOURCE_EVIDENCE_COLUMNS,
    SOURCE_EVIDENCE_CONTRACT,
    STABLE_PRICE_SCALE,
    SUPPORT_ACTION_COLUMNS,
    resolution_evidence_digest,
    source_evidence_digest,
    source_manifest_digest,
    support_action_digest,
    support_manifest_digest,
)
from pipeline.silver.dividend_evidence import (
    INCLUDED_CASH_PARITY_COLUMNS,
    PUBLISHED_ACTION_DIGEST_COLUMNS,
    SOURCE_RECEIPT_DIGEST_COLUMNS,
    included_cash_parity_digest,
    published_action_digest,
    source_receipt_digest,
    terminal_source_receipt_digest,
)
from pipeline.silver.krx_kind_reference import (
    KIND_COMPONENT_REPORT_NAME_61474,
    KIND_COMPONENT_REPORT_NAME_11306,
    KIND_REFERENCE_REPORT_NAME_70767,
    KIND_REFERENCE_REPORT_NAME_99311,
)
from pipeline.silver.dart_action_snapshot import (
    SCHEMA_VERSION as ACTION_SNAPSHOT_SCHEMA_VERSION,
)
from pipeline.silver.return_identity import (
    ASSET_IDENTITY_CONTRACT,
    CERTIFIED_MARKETS,
    krx_common_stock_identity_digest,
)
from pipeline.silver.return_contract import CONTRACT_RELEASE
from pipeline.silver.total_returns import stored_price_factor_interval
from pipeline.silver.reviewed_cash_scale_exceptions import (
    PAID_RIGHTS_COMPONENT_BODY_SHA256,
    PAID_RIGHTS_IDENTITY,
)


CONTRACT_START = date(2015, 1, 1)
METHODOLOGY_VERSION = "krx_gross_dividend_reinvested_v3"
RESOLUTION_VERSION = "krx_dividend_resolution_v2"

_DECIMAL_8 = Decimal("0.00000001")
_DECIMAL_12 = Decimal("0.000000000001")


def _source_receipt_contract_frame(conn, run_id) -> pd.DataFrame:
    columns = list(SOURCE_RECEIPT_DIGEST_COLUMNS)
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT {','.join(columns)} FROM dividend_source_receipt "
            "WHERE quality_run_id=%s ORDER BY receipt_no",
            (run_id,),
        )
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=columns)


def _published_action_contract_frame(conn, run_id) -> pd.DataFrame:
    columns = list(PUBLISHED_ACTION_DIGEST_COLUMNS)
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT {','.join('ca.' + column for column in columns)} "
            "FROM corporate_action ca JOIN asset a ON a.asset_id=ca.asset_id "
            "WHERE ca.quality_run_id=%s "
            "AND ca.action_scope='ISSUER' "
            "AND ((ca.source='DART_DISCLOSURE' "
            "      AND ca.action_type IN ('cash_dividend','ex_dividend')) "
            " OR EXISTS (SELECT 1 "
            "      FROM cash_adjustment_scale_support_action se "
            "      WHERE se.action_snapshot_run_id=ca.quality_run_id "
            "        AND se.support_action_source=ca.source "
            "        AND se.support_action_key=ca.action_key "
            "        AND se.support_action_type=ca.action_type)) "
            "AND a.asset_type='stock' "
            "AND a.instrument_type='common_stock' "
            "AND a.exchange='KRX' "
            "ORDER BY ca.asset_id,ca.source,ca.action_key",
            (run_id,),
        )
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=columns)


def _scale_source_contract_frames(
    conn, run_id,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load the immutable source-evidence parent/child row sets."""
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT {','.join(SOURCE_EVIDENCE_COLUMNS)} "
            "FROM cash_adjustment_scale_source_evidence "
            "WHERE action_snapshot_run_id=%s ORDER BY evidence_key",
            (run_id,),
        )
        parent_rows = cur.fetchall()
        cur.execute(
            f"SELECT {','.join(SUPPORT_ACTION_COLUMNS)} "
            "FROM cash_adjustment_scale_support_action "
            "WHERE action_snapshot_run_id=%s "
            "ORDER BY evidence_key,support_action_source,"
            "support_action_key,support_action_type",
            (run_id,),
        )
        support_rows = cur.fetchall()
    return (
        pd.DataFrame(parent_rows, columns=SOURCE_EVIDENCE_COLUMNS),
        pd.DataFrame(support_rows, columns=SUPPORT_ACTION_COLUMNS),
    )


def _resolution_contract_frame(conn, run_id) -> pd.DataFrame:
    """Load all applied resolution-v2 rows plus the ex-date selector input."""
    columns = [*RESOLUTION_DIGEST_COLUMNS, "resolved_ex_date"]
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT {','.join(columns)} FROM dividend_event_resolution "
            "WHERE quality_run_id=%s AND is_canonical "
            "AND excluded_reason IS NULL "
            "ORDER BY asset_id,source,action_key",
            (run_id,),
        )
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=columns)


def _decimal(value: object, places: Decimal) -> Decimal:
    try:
        rendered = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError(f"invalid decimal value: {value!r}") from exc
    if not rendered.is_finite():
        raise ValueError(f"non-finite decimal value: {value!r}")
    return rendered.quantize(places, rounding=ROUND_HALF_UP)


def _positive_float(value: object) -> float:
    rendered = float(value)
    if not math.isfinite(rendered) or rendered <= 0:
        raise ValueError(f"expected a finite positive value, got {value!r}")
    return rendered


def _stored_scale_interval(
    *, close: float, adjusted_close: float,
) -> tuple[float, float]:
    close = _positive_float(close)
    adjusted_close = _positive_float(adjusted_close)
    low = (adjusted_close - 0.00005) / close
    high = (adjusted_close + 0.00005) / close
    ulp = max(math.ulp(low), math.ulp(high))
    return low - ulp, high + ulp


def _canonical_support_groups(value: object) -> tuple[str, ...]:
    if not isinstance(value, str):
        raise ValueError("support semantic groups must be canonical JSON text")
    parsed = json.loads(value)
    if (
        not isinstance(parsed, list)
        or not parsed
        or any(not isinstance(item, str) or not item.strip() for item in parsed)
    ):
        raise ValueError("support semantic groups must be a non-empty string list")
    canonical = sorted(set(parsed))
    if parsed != canonical or value != json.dumps(
        canonical, ensure_ascii=False, separators=(",", ":"),
    ):
        raise ValueError("support semantic groups are not canonical/unique")
    return tuple(canonical)


def _viewer_bonus_group_parity(
    parent: dict[str, object],
    child: dict[str, object],
    groups: tuple[str, ...],
) -> bool:
    """Bind the producer's sole canonical bonus group to DB row semantics."""
    if len(groups) != 1:
        return False
    numerator = _positive_float(child["support_ratio_numerator"])
    denominator = _positive_float(child["support_ratio_denominator"])
    effective = pd.Timestamp(child["support_ex_date"]).date()
    expected = (
        f"{parent['ticker']}|{effective.isoformat()}|BONUS_ISSUE|"
        f"{format(numerator / denominator, '.12g')}"
    )
    return groups == (expected,)


def _viewer_stock_dividend_group_parity(
    parent: dict[str, object],
    child: dict[str, object],
    groups: tuple[str, ...],
) -> bool:
    """Bind the producer's sole canonical stock-dividend group exactly."""
    if len(groups) != 1:
        return False
    numerator = _positive_float(child["support_ratio_numerator"])
    denominator = _positive_float(child["support_ratio_denominator"])
    record = pd.Timestamp(child["support_record_date"]).date()
    expected = (
        f"{parent['ticker']}|{record.isoformat()}|STOCK_DIVIDEND|"
        f"{format(numerator / denominator, '.12g')}"
    )
    return groups == (expected,)


def _validate_scale_source_rows(
    parents: pd.DataFrame,
    supports: pd.DataFrame,
) -> tuple[bool, int]:
    """Independently verify parent/child identity, digests and group semantics."""
    try:
        if parents["evidence_key"].astype(str).duplicated().any():
            return False, 0
        if not parents.empty and (
            parents["action_snapshot_run_id"].astype(str).nunique() != 1
            or not parents["cash_scale_basis"].eq(
                PRE_EVENT_PRICE_SCALE
            ).all()
        ):
            return False, 0
        child_identity = [
            "evidence_key", "support_action_source", "support_action_key",
            "support_action_type",
        ]
        if supports.duplicated(child_identity).any():
            return False, 0
        if parents.empty:
            return supports.empty, 0
        if supports.empty:
            return False, 0
        all_groups: set[str] = set()
        parent_keys = set(parents["evidence_key"].astype(str))
        if not set(supports["evidence_key"].astype(str)).issubset(parent_keys):
            return False, 0
        for parent in parents.to_dict("records"):
            expected_factor = _decimal(
                Decimal(str(parent["raw_reference_price"]))
                / Decimal(str(parent["raw_previous_close"])),
                _DECIMAL_12,
            )
            if (
                parent["previous_trade_date"] >= parent["adjustment_trade_date"]
                or _positive_float(parent["raw_applied_close"]) <= 0
                or _decimal(parent["expected_price_factor"], _DECIMAL_12)
                != expected_factor
            ):
                return False, 0
            children = supports[
                supports["evidence_key"].astype(str).eq(str(parent["evidence_key"]))
            ]
            memberships: dict[str, int] = {}
            for child in children.to_dict("records"):
                groups = _canonical_support_groups(
                    child["support_semantic_group_keys"]
                )
                all_groups.update(groups)
                source_type = (
                    str(child["support_action_source"]),
                    str(child["support_action_type"]),
                )
                if (
                    source_type not in {
                        ("DART_DISCLOSURE", "ex_dividend"),
                        ("DART_DISCLOSURE", "rights_detachment"),
                        ("DART_DISCLOSURE", "stock_dividend"),
                        ("DART_DISCLOSURE", "combined_detachment"),
                        ("DART_STRUCTURED", "bonus_issue"),
                        ("DART_VIEWER", "bonus_issue"),
                        ("DART_VIEWER", "stock_dividend"),
                        ("KRX_KIND", "stock_dividend"),
                        ("KRX_KIND", "paid_increase"),
                        ("KRX_KIND", "ex_dividend"),
                        ("KRX_KIND", "rights_detachment"),
                        ("KRX_KIND", "combined_detachment"),
                    }
                    or child["support_action_scope"] != "ISSUER"
                    or not str(child["support_report_name"] or "").strip()
                    or any(
                        token in str(child["support_report_name"] or "")
                        for token in ("철회", "취소", "부결")
                    )
                ):
                    return False, 0
                role = str(child["support_semantic_role"])
                if role not in {"ADJUSTMENT_COMPONENT", "CORROBORATION"}:
                    return False, 0
                if (
                    str(child["action_snapshot_run_id"])
                    != str(parent["action_snapshot_run_id"])
                    or str(child["support_action_quality_run_id"])
                    != str(parent["action_snapshot_run_id"])
                    or str(child.get("target_cash_receipt_no") or "")
                    != str(parent["cash_receipt_no"])
                    or pd.Timestamp(child.get("target_adjustment_date")).date()
                    != pd.Timestamp(parent["adjustment_trade_date"]).date()
                ):
                    return False, 0
                if role == "ADJUSTMENT_COMPONENT" and len(groups) != 1:
                    return False, 0
                if role == "ADJUSTMENT_COMPONENT":
                    if source_type not in {
                        ("DART_STRUCTURED", "bonus_issue"),
                        ("DART_VIEWER", "bonus_issue"),
                        ("DART_VIEWER", "stock_dividend"),
                        ("DART_DISCLOSURE", "stock_dividend"),
                        ("KRX_KIND", "stock_dividend"),
                        ("KRX_KIND", "paid_increase"),
                    }:
                        return False, 0
                    security_classes = (
                        child["support_entitlement_security_class"],
                        child["support_distributed_security_class"],
                    )
                    if source_type in {
                        ("DART_STRUCTURED", "bonus_issue"),
                        ("DART_VIEWER", "bonus_issue"),
                    }:
                        numerator = _positive_float(
                            child["support_ratio_numerator"]
                        )
                        denominator = _positive_float(
                            child["support_ratio_denominator"]
                        )
                        expected = 1.0 / (1.0 + numerator / denominator)
                        if (
                            security_classes != ("COMMON", "COMMON")
                            or _decimal(
                                child["support_expected_price_factor"],
                                _DECIMAL_12,
                            ) != _decimal(expected, _DECIMAL_12)
                        ):
                            return False, 0
                        if source_type == ("DART_VIEWER", "bonus_issue"):
                            body_sha = str(
                                child["support_action_body_sha256"] or ""
                            )
                            body_path = str(
                                child["support_action_body_path"] or ""
                            )
                            report_name = re.sub(
                                r"\s+", "",
                                str(child["support_report_name"] or ""),
                            )
                            if (
                                re.fullmatch(r"[0-9]{14}", str(
                                    child["support_action_key"] or ""
                                )) is None
                                or re.fullmatch(
                                    r"corporate_actions/dart/"
                                    r"support_action_families/objects/"
                                    r"sha256=([0-9a-f]{64})\.html",
                                    body_path,
                                ) is None
                                or not body_path.endswith(
                                    f"sha256={body_sha}.html"
                                )
                                or re.fullmatch(
                                    r"(?:\[기재정정\])?"
                                    r"주요사항보고서\(무상증자결정\)",
                                    report_name,
                                ) is None
                                or child["support_ex_date"] is None
                                or pd.isna(child["support_ex_date"])
                                or (
                                    child["support_record_date"] is not None
                                    and not pd.isna(
                                        child["support_record_date"]
                                    )
                                )
                                or not _viewer_bonus_group_parity(
                                    parent, child, groups,
                                )
                            ):
                                return False, 0
                    else:
                        _positive_float(child["support_ratio_numerator"])
                        _positive_float(child["support_ratio_denominator"])
                        if security_classes not in {
                            ("COMMON", "COMMON"),
                            ("COMMON_AND_PREFERRED", "NEW_PREFERRED"),
                        }:
                            return False, 0
                        if source_type == ("KRX_KIND", "stock_dividend") and (
                            child["support_report_name"]
                            != KIND_COMPONENT_REPORT_NAME_61474
                        ):
                            return False, 0
                        if source_type == ("KRX_KIND", "paid_increase"):
                            paid_identity = (
                                str(parent["ticker"]).zfill(6),
                                str(parent["cash_receipt_no"]),
                                pd.Timestamp(
                                    parent["adjustment_trade_date"]
                                ).date().isoformat(),
                                str(child["support_action_key"]),
                                pd.Timestamp(
                                    child["support_record_date"]
                                ).date().isoformat(),
                                format(
                                    float(child["support_ratio_numerator"])
                                    / float(child["support_ratio_denominator"]),
                                    ".12g",
                                ),
                            )
                            if (
                                paid_identity != PAID_RIGHTS_IDENTITY
                                or str(child["support_action_body_sha256"])
                                != PAID_RIGHTS_COMPONENT_BODY_SHA256
                                or child["support_report_name"]
                                != KIND_COMPONENT_REPORT_NAME_11306
                                or security_classes != ("COMMON", "COMMON")
                                or (
                                    child["support_expected_price_factor"]
                                    is not None
                                    and not pd.isna(
                                        child["support_expected_price_factor"]
                                    )
                                )
                            ):
                                return False, 0
                        if source_type == ("DART_VIEWER", "stock_dividend"):
                            body_sha = str(
                                child["support_action_body_sha256"] or ""
                            )
                            body_path = str(
                                child["support_action_body_path"] or ""
                            )
                            report_name = re.sub(
                                r"\s+", "",
                                str(child["support_report_name"] or ""),
                            )
                            expected_factor = child.get(
                                "support_expected_price_factor"
                            )
                            if (
                                security_classes != ("COMMON", "COMMON")
                                or re.fullmatch(
                                    r"[0-9]{14}", str(
                                        child["support_action_key"] or ""
                                    ),
                                ) is None
                                or re.fullmatch(
                                    r"corporate_actions/dart/"
                                    r"support_action_families/objects/"
                                    r"sha256=([0-9a-f]{64})\.html",
                                    body_path,
                                ) is None
                                or not body_path.endswith(
                                    f"sha256={body_sha}.html"
                                )
                                or re.fullmatch(
                                    r"(?:\[기재정정\])?주식배당결정",
                                    report_name,
                                ) is None
                                or (
                                    child["support_ex_date"] is not None
                                    and not pd.isna(child["support_ex_date"])
                                )
                                or child["support_record_date"] is None
                                or pd.isna(child["support_record_date"])
                                or (
                                    expected_factor is not None
                                    and not pd.isna(expected_factor)
                                )
                                or not _viewer_stock_dividend_group_parity(
                                    parent, child, groups,
                                )
                            ):
                                return False, 0
                elif source_type not in {
                    ("DART_DISCLOSURE", "ex_dividend"),
                    ("DART_DISCLOSURE", "rights_detachment"),
                    ("DART_DISCLOSURE", "combined_detachment"),
                    ("KRX_KIND", "ex_dividend"),
                    ("KRX_KIND", "rights_detachment"),
                    ("KRX_KIND", "combined_detachment"),
                }:
                    return False, 0
                if source_type in {
                    ("DART_DISCLOSURE", "ex_dividend"),
                    ("DART_DISCLOSURE", "rights_detachment"),
                    ("DART_DISCLOSURE", "combined_detachment"),
                    ("KRX_KIND", "ex_dividend"),
                    ("KRX_KIND", "rights_detachment"),
                    ("KRX_KIND", "combined_detachment"),
                } and child["support_ex_date"] != parent["adjustment_trade_date"]:
                    return False, 0
                if (
                    source_type[0] == "KRX_KIND"
                    and role == "CORROBORATION"
                ):
                    def absent(value: object) -> bool:
                        return value is None or bool(pd.isna(value))

                    reason = re.sub(
                        r"\s+", "", str(child["support_reason"] or "")
                    )
                    reason_matches = (
                        source_type[1] == "ex_dividend"
                        and "주식배당" in reason
                    ) or (
                        source_type[1] == "rights_detachment"
                        and "무상증자" in reason
                    ) or (
                        source_type[1] == "combined_detachment"
                        and "무상증자" in reason
                        and "배당" in reason
                    )
                    if (
                        child["support_entitlement_security_class"]
                        not in {"COMMON", "PREFERRED"}
                        or not absent(
                            child["support_distributed_security_class"]
                        )
                        or not absent(child["support_ratio_numerator"])
                        or not absent(child["support_ratio_denominator"])
                        or not absent(child["support_expected_price_factor"])
                        or not absent(child["support_record_date"])
                        or child["support_report_name"] not in {
                            KIND_REFERENCE_REPORT_NAME_99311,
                            KIND_REFERENCE_REPORT_NAME_70767,
                        }
                        or _decimal(
                            child["support_reference_price"], _DECIMAL_8,
                        ) != _decimal(parent["raw_reference_price"], _DECIMAL_8)
                        or not reason_matches
                    ):
                        return False, 0
                if source_type == (
                    "DART_DISCLOSURE", "combined_detachment"
                ) and (
                    _decimal(
                        child["support_reference_price"], _DECIMAL_8
                    ) != _decimal(parent["raw_reference_price"], _DECIMAL_8)
                    or "무상증자" not in str(child["support_reason"] or "")
                    or "배당" not in str(child["support_reason"] or "")
                ):
                    return False, 0
                for group in groups:
                    memberships[group] = memberships.get(group, 0) + int(
                        role == "ADJUSTMENT_COMPONENT"
                    )
            if not memberships or any(count != 1 for count in memberships.values()):
                return False, 0
            if (
                len(children) != int(parent["support_action_count"])
                or len(memberships) != int(parent["support_semantic_group_count"])
                or support_manifest_digest(children)
                != str(parent["support_action_digest"])
            ):
                return False, 0
        return True, len(all_groups)
    except (KeyError, TypeError, ValueError, RuntimeError, json.JSONDecodeError):
        return False, 0


def _runtime_resolution_evidence(
    resolutions: pd.DataFrame,
    parents: pd.DataFrame,
    *,
    action_snapshot_run_id: object,
) -> tuple[dict[str, object], bool]:
    """Recompute every runtime scale decision without trusting metadata."""
    stable = changed = formula = parity = 0
    used_parent_keys: list[tuple[str, str]] = []
    valid = True
    try:
        parent_lookup = {
            (str(row["action_snapshot_run_id"]), str(row["evidence_key"])): row
            for row in parents.to_dict("records")
        }
        for row in resolutions.to_dict("records"):
            previous_close = _positive_float(row["previous_close"])
            previous_adj = _positive_float(row["previous_adj_close"])
            applied_close = _positive_float(row["applied_close"])
            applied_adj = _positive_float(row["applied_adj_close"])
            previous_scale = previous_adj / previous_close
            applied_scale = applied_adj / applied_close
            observed_factor = previous_scale / applied_scale
            previous_interval = _stored_scale_interval(
                close=previous_close, adjusted_close=previous_adj,
            )
            applied_interval = _stored_scale_interval(
                close=applied_close, adjusted_close=applied_adj,
            )
            interval_stable = (
                previous_interval[0] <= applied_interval[1]
                and applied_interval[0] <= previous_interval[1]
            )
            if (
                _decimal(row["previous_price_scale"], _DECIMAL_12)
                != _decimal(previous_scale, _DECIMAL_12)
                or _decimal(row["applied_price_scale"], _DECIMAL_12)
                != _decimal(applied_scale, _DECIMAL_12)
                or _decimal(row["scale_price_factor_observed"], _DECIMAL_12)
                != _decimal(observed_factor, _DECIMAL_12)
                or bool(row["scale_price_factor_parity"]) is not True
                or bool(row["scale_change_detected"]) == interval_stable
                or row["previous_trade_date"] >= row["applied_trade_date"]
            ):
                valid = False
            evidence_run = row["scale_evidence_action_snapshot_run_id"]
            evidence_key = row["scale_evidence_key"]
            if interval_stable:
                stable += 1
                exact_trade = row["resolved_ex_date"] == row["applied_trade_date"]
                selected = applied_scale if exact_trade else previous_scale
                if (
                    row["cash_adjustment_scale_basis"] != STABLE_PRICE_SCALE
                    or evidence_run is not None
                    or evidence_key is not None
                    or _decimal(row["selected_cash_scale"], _DECIMAL_12)
                    != _decimal(selected, _DECIMAL_12)
                    or _decimal(
                        row["scale_price_factor_reference"], _DECIMAL_12
                    ) != _decimal(1, _DECIMAL_12)
                ):
                    valid = False
            else:
                changed += 1
                identity = (str(evidence_run), str(evidence_key))
                parent = parent_lookup.get(identity)
                if (
                    evidence_run is None
                    or evidence_key is None
                    or str(evidence_run) != str(action_snapshot_run_id)
                    or parent is None
                    or row["cash_adjustment_scale_basis"] != PRE_EVENT_PRICE_SCALE
                    or _decimal(row["selected_cash_scale"], _DECIMAL_12)
                    != _decimal(previous_scale, _DECIMAL_12)
                ):
                    valid = False
                else:
                    used_parent_keys.append(identity)
                    factor_low, factor_high = stored_price_factor_interval(
                        previous_close=previous_close,
                        previous_adj_close=previous_adj,
                        applied_close=applied_close,
                        applied_adj_close=applied_adj,
                    )
                    reference = _positive_float(
                        row["scale_price_factor_reference"]
                    )
                    parent_factor = _positive_float(parent["expected_price_factor"])
                    parent_match = (
                        int(parent["asset_id"]) == int(row["asset_id"])
                        and str(parent["cash_receipt_no"])
                        == str(row["action_key"])
                        and parent["previous_trade_date"]
                        == row["previous_trade_date"]
                        and parent["adjustment_trade_date"]
                        == row["applied_trade_date"]
                        and _decimal(parent["raw_previous_close"], _DECIMAL_8)
                        == _decimal(row["previous_close"], _DECIMAL_8)
                        and _decimal(parent["raw_applied_close"], _DECIMAL_8)
                        == _decimal(row["applied_close"], _DECIMAL_8)
                        and _decimal(parent_factor, _DECIMAL_12)
                        == _decimal(reference, _DECIMAL_12)
                        and factor_low <= reference <= factor_high
                    )
                    if not parent_match:
                        valid = False
            expected_cash = _decimal(
                Decimal(str(row["raw_cash_amount"]))
                * Decimal(str(row["selected_cash_scale"])),
                _DECIMAL_8,
            )
            if _decimal(row["adjusted_cash_amount"], _DECIMAL_8) == expected_cash:
                formula += 1
            else:
                valid = False
            if bool(row["scale_price_factor_parity"]):
                parity += 1
        expected_parent_keys = {
            (str(row["action_snapshot_run_id"]), str(row["evidence_key"]))
            for row in parents.to_dict("records")
        }
        # Exact set equality proves changed=one, stable=zero, and no unused
        # evidence.  The list/set length check also rejects evidence reuse.
        if (
            set(used_parent_keys) != expected_parent_keys
            or len(used_parent_keys) != len(expected_parent_keys)
        ):
            valid = False
        digest_frame = resolutions[list(RESOLUTION_DIGEST_COLUMNS)]
        runtime = {
            "contract": RESOLUTION_EVIDENCE_CONTRACT,
            "row_count": len(resolutions),
            "row_digest": resolution_evidence_digest(digest_frame),
            "applied_event_count": len(resolutions),
            "stable_scale_event_count": stable,
            "changed_scale_event_count": changed,
            "unresolved_count": 0,
            "resolution_parity_count": parity,
            "adjusted_cash_parity_count": formula,
            "adj_close_decimal_places": 4,
            "cash_in_adj_close": False,
        }
        valid = valid and stable + changed == len(resolutions)
        valid = valid and formula == parity == len(resolutions)
        return runtime, valid
    except (
        KeyError, TypeError, ValueError, RuntimeError, InvalidOperation,
        ZeroDivisionError,
    ):
        return {}, False


def _action_cash_parity_frame(actions: pd.DataFrame) -> pd.DataFrame:
    return actions[actions["action_type"].eq("cash_dividend")].rename(
        columns={
            "action_key": "receipt_no",
            "correction_of_action_key": "previous_receipt_no",
            "revision_root_action_key": "revision_root_receipt_no",
        }
    )[list(INCLUDED_CASH_PARITY_COLUMNS)].copy()


def _column_exists(conn, table: str, column: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema='public' AND table_name=%s
                  AND column_name=%s
            )
            """,
            (table, column),
        )
        return bool(cur.fetchone()[0])


def _table_exists(conn, table: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s)", (f"public.{table}",))
        return cur.fetchone()[0] is not None


def audit(conn=None, *, use_existing_transaction: bool = False) -> dict:
    """Return evidence and fail-closed checks without changing DB state."""
    owns_connection = conn is None
    connection = conn or db.connect()
    try:
        transaction = (
            nullcontext()
            if use_existing_transaction
            else connection.transaction()
        )
        with transaction:
            with connection.cursor() as cur:
                if not use_existing_transaction:
                    cur.execute(
                        "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, "
                        "READ ONLY"
                    )
                cur.execute(
                    """
                    SELECT c.status,c.coverage_start,c.coverage_end,
                           c.quality_run_id,c.methodology_version,c.metadata,
                           q.status,q.mode
                    FROM price_return_contract c
                    LEFT JOIN dq_run q ON q.run_id=c.quality_run_id
                    WHERE c.source='KRX' AND c.asset_type='stock'
                      AND c.field_name='total_return_close'
                    """
                )
                contract = cur.fetchone()
            if not contract:
                return {
                    "safe_for_research": False,
                    "checks": {"contract_exists": False},
                }
            (
                status,
                coverage_start,
                coverage_end,
                run_id,
                methodology_version,
                metadata,
                contract_run_status,
                contract_run_mode,
            ) = contract
            metadata = metadata or {}
            if isinstance(metadata, str):
                metadata = json.loads(metadata)
            with connection.cursor() as cur:
                cur.execute(
                    """
                    SELECT count(*),count(DISTINCT p.asset_id),
                           min(p.trade_date),max(p.trade_date),
                           count(*) FILTER (WHERE q.status='CERTIFIED'),
                           count(*) FILTER (
                               WHERE p.total_return_close IS NULL
                                  OR p.total_return_close <= 0
                                  OR p.total_return_close::text IN (
                                      'NaN','Infinity','-Infinity'
                                  )
                           )
                    FROM price_daily p
                    JOIN asset a ON a.asset_id=p.asset_id
                    LEFT JOIN dq_run q ON q.run_id=p.quality_run_id
                    WHERE p.source='KRX'
                      AND a.asset_type='stock'
                      AND a.instrument_type='common_stock'
                      AND a.exchange='KRX'
                      AND p.market IN ('KOSPI','KOSDAQ')
                      AND p.trade_date >= %s
                    """,
                    (CONTRACT_START,),
                )
                (
                    row_count,
                    asset_count,
                    first_trade,
                    last_trade,
                    raw_certified,
                    invalid_total_return_count,
                ) = cur.fetchone()
            has_lineage = _column_exists(
                connection, "price_daily", "total_return_quality_run_id"
            )
            if has_lineage:
                with connection.cursor() as cur:
                    cur.execute(
                        """
                        SELECT count(*)
                        FROM price_daily p
                        JOIN asset a ON a.asset_id=p.asset_id
                        WHERE p.source='KRX'
                          AND a.asset_type='stock'
                          AND a.instrument_type='common_stock'
                          AND a.exchange='KRX'
                          AND p.market IN ('KOSPI','KOSDAQ')
                          AND p.trade_date >= %s
                          AND p.total_return_quality_run_id=%s
                        """,
                        (CONTRACT_START, run_id),
                    )
                    run_parity_count = int(cur.fetchone()[0])
            else:
                run_parity_count = 0

            observed_identity = (
                krx_common_stock_identity_digest(
                    connection,
                    coverage_start=CONTRACT_START,
                    coverage_end=last_trade,
                )
                if last_trade is not None
                else None
            )

            action_snapshot_run_id = metadata.get("action_snapshot_run_id")
            snapshot_table = _table_exists(
                connection, "dart_action_snapshot_contract"
            )
            snapshot_row = None
            if snapshot_table and action_snapshot_run_id:
                with connection.cursor() as cur:
                    cur.execute(
                        """
                        SELECT s.manifest_sha256,s.body_digest,s.body_count,
                               s.coverage_start,s.coverage_end,s.action_count,
                               s.metadata,q.status,q.mode,s.schema_version
                        FROM dart_action_snapshot_contract s
                        LEFT JOIN dq_run q ON q.run_id=s.quality_run_id
                        WHERE s.quality_run_id=%s
                        """,
                        (action_snapshot_run_id,),
                    )
                    snapshot_row = cur.fetchone()

            snapshot_metadata = (
                snapshot_row[6] if snapshot_row is not None else {}
            ) or {}
            if isinstance(snapshot_metadata, str):
                snapshot_metadata = json.loads(snapshot_metadata)

            has_action_corp_cls = _column_exists(
                connection, "corporate_action", "corp_cls"
            )
            persisted_action_count = 0
            persisted_cash_action_count = 0
            if snapshot_row is not None:
                with connection.cursor() as cur:
                    cur.execute(
                        """
                        SELECT count(*), count(*) FILTER (
                                   WHERE ca.action_type='cash_dividend'
                               )
                        FROM corporate_action ca
                        JOIN asset a ON a.asset_id=ca.asset_id
                        WHERE ca.quality_run_id=%s
                          AND ca.action_scope='ISSUER'
                          AND (
                              (ca.source='DART_DISCLOSURE'
                               AND ca.action_type IN (
                                   'cash_dividend','ex_dividend'
                               ))
                              OR EXISTS (
                                  SELECT 1
                                  FROM cash_adjustment_scale_support_action se
                                  WHERE se.action_snapshot_run_id=
                                        ca.quality_run_id
                                    AND se.support_action_source=ca.source
                                    AND se.support_action_key=ca.action_key
                                    AND se.support_action_type=ca.action_type
                              )
                          )
                          AND a.asset_type='stock'
                          AND a.instrument_type='common_stock'
                          AND a.exchange='KRX'
                        """,
                        (action_snapshot_run_id,),
                    )
                    (
                        persisted_action_count,
                        persisted_cash_action_count,
                    ) = [int(value) for value in cur.fetchone()]

            resolution_append_only = False
            resolution_run_count = 0
            resolution_expected_version_count = 0
            resolution_canonical_source_count = 0
            resolution_applied_count = 0
            resolution_excluded_count = 0
            resolution_semantic_count = 0
            resolution_lineage_count = 0
            resolution_first_listing_count = 0
            resolution_explicit_exclusion_count = 0
            resolution_price_lineage_count = 0
            resolution_price_lineage_mismatch_count = 0
            resolution_action_mismatch_count = 0
            resolution_rows = pd.DataFrame(
                columns=[*RESOLUTION_DIGEST_COLUMNS, "resolved_ex_date"]
            )
            if _table_exists(connection, "dividend_event_resolution"):
                with connection.cursor() as cur:
                    cur.execute(
                        """
                        SELECT array_agg(a.attname ORDER BY keys.ordinality)
                        FROM pg_constraint c
                        CROSS JOIN LATERAL unnest(c.conkey)
                            WITH ORDINALITY AS keys(attnum, ordinality)
                        JOIN pg_attribute a
                          ON a.attrelid=c.conrelid
                         AND a.attnum=keys.attnum
                        WHERE c.conrelid=
                              'dividend_event_resolution'::regclass
                          AND c.contype='p'
                        GROUP BY c.conname
                        """
                    )
                    pk = cur.fetchone()
                    cur.execute(
                        """
                        SELECT count(*),
                               count(*) FILTER (
                                   WHERE resolution_version=%s
                               ),
                               count(*) FILTER (
                                   WHERE (
                                       is_canonical
                                       AND excluded_reason IS NULL
                                   ) OR (
                                       NOT is_canonical
                                       AND excluded_reason IN (
                                           'BEFORE_MARKET_COVERAGE',
                                           'PENDING_FUTURE_TRADE',
                                           'BEFORE_LISTING_OR_EPISODE_START',
                                           'LISTING_EPISODE_GAP'
                                       )
                                   )
                               ),
                               count(*) FILTER (
                                   WHERE is_canonical
                                     AND excluded_reason IS NULL
                                     AND resolved_ex_date IS NOT NULL
                                     AND applied_trade_date IS NOT NULL
                                     AND adjusted_cash_amount > 0
                               ),
                               count(*) FILTER (
                                   WHERE NOT is_canonical
                                     AND excluded_reason IN (
                                         'ATTACHMENT_CORRECTION',
                                         'INVALID_CASH_AMOUNT',
                                         'SUPERSEDED_REVISION',
                                         'NO_COMMON_CASH_DIVIDEND',
                                         'NO_ECONOMIC_EVENT',
                                         'BEFORE_MARKET_COVERAGE',
                                         'PENDING_FUTURE_TRADE',
                                         'BEFORE_LISTING_OR_EPISODE_START',
                                         'LISTING_EPISODE_GAP'
                                     )
                                     AND applied_trade_date IS NULL
                                     AND adjusted_cash_amount IS NULL
                                     AND previous_trade_date IS NULL
                                     AND previous_close IS NULL
                                     AND previous_adj_close IS NULL
                                     AND applied_close IS NULL
                                     AND applied_adj_close IS NULL
                                     AND previous_price_scale IS NULL
                                     AND applied_price_scale IS NULL
                                     AND selected_cash_scale IS NULL
                                     AND cash_adjustment_scale_basis IS NULL
                                     AND scale_change_detected IS NULL
                                     AND scale_evidence_action_snapshot_run_id
                                         IS NULL
                                     AND scale_evidence_key IS NULL
                                     AND scale_price_factor_observed IS NULL
                                     AND scale_price_factor_reference IS NULL
                                     AND scale_price_factor_parity IS NULL
                               ),
                               count(*) FILTER (
                                   WHERE (
                                       is_canonical
                                       AND excluded_reason IS NULL
                                       AND resolved_ex_date IS NOT NULL
                                       AND applied_trade_date IS NOT NULL
                                       AND adjusted_cash_amount > 0
                                   ) OR (
                                       NOT is_canonical
                                       AND excluded_reason IN (
                                           'ATTACHMENT_CORRECTION',
                                           'INVALID_CASH_AMOUNT',
                                           'SUPERSEDED_REVISION',
                                           'NO_COMMON_CASH_DIVIDEND',
                                           'NO_ECONOMIC_EVENT',
                                           'BEFORE_MARKET_COVERAGE',
                                           'PENDING_FUTURE_TRADE',
                                           'BEFORE_LISTING_OR_EPISODE_START',
                                           'LISTING_EPISODE_GAP'
                                       )
                                       AND applied_trade_date IS NULL
                                       AND adjusted_cash_amount IS NULL
                                       AND previous_trade_date IS NULL
                                       AND previous_close IS NULL
                                       AND previous_adj_close IS NULL
                                       AND applied_close IS NULL
                                       AND applied_adj_close IS NULL
                                       AND previous_price_scale IS NULL
                                       AND applied_price_scale IS NULL
                                       AND selected_cash_scale IS NULL
                                       AND cash_adjustment_scale_basis IS NULL
                                       AND scale_change_detected IS NULL
                                       AND scale_evidence_action_snapshot_run_id
                                           IS NULL
                                       AND scale_evidence_key IS NULL
                                       AND scale_price_factor_observed IS NULL
                                       AND scale_price_factor_reference IS NULL
                                       AND scale_price_factor_parity IS NULL
                                   )
                               ),
                               count(*) FILTER (
                                   WHERE source_announcement_date IS NOT NULL
                                     AND revision_group_key IS NOT NULL
                                     AND action_key ~ '^[0-9]{14}$'
                                     AND revision_root_action_key ~
                                         '^[0-9]{14}$'
                                     AND (
                                         correction_of_action_key IS NULL
                                         OR correction_of_action_key ~
                                             '^[0-9]{14}$'
                                     )
                                     AND cash_amount_status IN (
                                         'POSITIVE',
                                         'POSITIVE_PENDING_RECORD_DATE',
                                         'NO_COMMON_CASH_DIVIDEND',
                                         'NO_ECONOMIC_EVENT',
                                         'ATTACHMENT_ONLY'
                                     )
                                     AND revision_root_action_key IS NOT NULL
                                     AND revision_kind IS NOT NULL
                                     AND (
                                         (source_evidence_status=
                                              'VERIFIED_OPENDART_DOCUMENT'
                                          AND coalesce(
                                              viewer_evidence_sha256,''
                                          )=''
                                          AND economic_evidence_sha256 ~
                                              '^[0-9a-f]{64}$')
                                         OR
                                         (source_evidence_status=
                                              'VERIFIED_DART_VIEWER_BODY'
                                          AND viewer_evidence_sha256 ~
                                              '^[0-9a-f]{64}$'
                                          AND economic_evidence_sha256 ~
                                              '^[0-9a-f]{64}$'
                                          AND viewer_evidence_sha256=
                                              economic_evidence_sha256)
                                         OR
                                         (source_evidence_status=
                                              'VERIFIED_ATTACHMENT_CORRECTION'
                                          AND viewer_evidence_sha256 ~
                                              '^[0-9a-f]{64}$'
                                          AND economic_evidence_sha256 ~
                                              '^[0-9a-f]{64}$'
                                          AND viewer_evidence_sha256<>
                                              economic_evidence_sha256
                                          AND cash_amount_status=
                                              'ATTACHMENT_ONLY'
                                          AND revision_kind='ATTACHMENT_ONLY'
                                          AND correction_of_action_key
                                              IS NOT NULL)
                                         OR
                                         (source_evidence_status=
                                              'VERIFIED_REVIEWED_SOURCE_ERRATUM'
                                          AND coalesce(
                                              viewer_evidence_sha256,''
                                          )=''
                                          AND economic_evidence_sha256 ~
                                              '^[0-9a-f]{64}$'
                                          AND btrim(coalesce(
                                              reviewed_correction_id,''
                                         ))<>'')
                                     )
                                     AND (
                                         source_evidence_status<>
                                             'VERIFIED_ATTACHMENT_CORRECTION'
                                         OR EXISTS (
                                             SELECT 1
                                             FROM dividend_event_resolution prior
                                             WHERE prior.quality_run_id=
                                                 dividend_event_resolution.quality_run_id
                                               AND prior.asset_id=
                                                 dividend_event_resolution.asset_id
                                               AND prior.source=
                                                 dividend_event_resolution.source
                                               AND prior.action_key=
                                                 dividend_event_resolution.correction_of_action_key
                                               AND prior.revision_root_action_key=
                                                 dividend_event_resolution.revision_root_action_key
                                         )
                                     )
                               ),
                               count(*) FILTER (
                                   WHERE NOT is_canonical
                                     AND excluded_reason=
                                         'BEFORE_LISTING_OR_EPISODE_START'
                               ),
                               count(*) FILTER (
                                   WHERE NOT is_canonical
                                     AND excluded_reason IN (
                                         'ATTACHMENT_CORRECTION',
                                         'INVALID_CASH_AMOUNT',
                                         'SUPERSEDED_REVISION',
                                         'NO_COMMON_CASH_DIVIDEND',
                                         'NO_ECONOMIC_EVENT',
                                         'BEFORE_MARKET_COVERAGE',
                                         'PENDING_FUTURE_TRADE',
                                         'BEFORE_LISTING_OR_EPISODE_START',
                                         'LISTING_EPISODE_GAP'
                                     )
                               )
                        FROM dividend_event_resolution
                        WHERE quality_run_id=%s
                        """,
                        (RESOLUTION_VERSION, run_id),
                    )
                    (
                        resolution_run_count,
                        resolution_expected_version_count,
                        resolution_canonical_source_count,
                        resolution_applied_count,
                        resolution_excluded_count,
                        resolution_semantic_count,
                        resolution_lineage_count,
                        resolution_first_listing_count,
                        resolution_explicit_exclusion_count,
                    ) = [int(value) for value in cur.fetchone()]
                    cur.execute(
                        """
                        WITH applied AS (
                            SELECT r.previous_trade_date,
                                   r.previous_close,r.previous_adj_close,
                                   r.applied_close,r.applied_adj_close,
                                   current_price.asset_id AS current_asset_id,
                                   current_price.close AS current_close,
                                   current_price.adj_close AS current_adj_close,
                                   previous_price.trade_date AS actual_previous_date,
                                   previous_price.close AS actual_previous_close,
                                   previous_price.adj_close AS actual_previous_adj_close
                            FROM dividend_event_resolution r
                            LEFT JOIN price_daily current_price
                              ON current_price.asset_id=r.asset_id
                             AND current_price.source='KRX'
                             AND current_price.trade_date=
                                 r.applied_trade_date
                            LEFT JOIN LATERAL (
                                SELECT p.trade_date,p.close,p.adj_close
                                FROM price_daily p
                                WHERE p.asset_id=r.asset_id
                                  AND p.source='KRX'
                                  AND p.trade_date<r.applied_trade_date
                                ORDER BY p.trade_date DESC
                                LIMIT 1
                            ) previous_price ON true
                            WHERE r.quality_run_id=%s
                              AND r.is_canonical
                              AND r.excluded_reason IS NULL
                        )
                        SELECT count(*),count(*) FILTER (
                            WHERE current_asset_id IS NULL
                               OR actual_previous_date IS NULL
                               OR previous_trade_date IS DISTINCT FROM
                                  actual_previous_date
                               OR previous_close IS DISTINCT FROM
                                  actual_previous_close
                               OR previous_adj_close IS DISTINCT FROM
                                  actual_previous_adj_close
                               OR applied_close IS DISTINCT FROM current_close
                               OR applied_adj_close IS DISTINCT FROM
                                  current_adj_close
                        )
                        FROM applied
                        """,
                        (run_id,),
                    )
                    (
                        resolution_price_lineage_count,
                        resolution_price_lineage_mismatch_count,
                    ) = [int(value) for value in cur.fetchone()]
                    if action_snapshot_run_id:
                        cur.execute(
                            """
                            SELECT count(*)
                            FROM dividend_event_resolution r
                            LEFT JOIN corporate_action ca
                              ON ca.asset_id=r.asset_id
                             AND ca.source=r.source
                             AND ca.action_key=r.action_key
                             AND ca.action_type='cash_dividend'
                             AND ca.quality_run_id=%s
                            WHERE r.quality_run_id=%s
                              AND (
                                  ca.asset_id IS NULL
                                  OR ca.cash_amount IS DISTINCT FROM
                                     r.raw_cash_amount
                                  OR ca.announcement_date IS DISTINCT FROM
                                     r.source_announcement_date
                                  OR ca.source_evidence_status IS DISTINCT FROM
                                     r.source_evidence_status
                                  OR ca.cash_amount_status IS DISTINCT FROM
                                     r.cash_amount_status
                                  OR ca.correction_of_action_key IS DISTINCT FROM
                                     r.correction_of_action_key
                                  OR ca.revision_root_action_key IS DISTINCT FROM
                                     r.revision_root_action_key
                                  OR ca.revision_kind IS DISTINCT FROM
                                     r.revision_kind
                                  OR ca.viewer_evidence_sha256 IS DISTINCT FROM
                                     r.viewer_evidence_sha256
                                  OR ca.economic_evidence_sha256 IS DISTINCT FROM
                                     r.economic_evidence_sha256
                                  OR ca.reviewed_correction_id IS DISTINCT FROM
                                     r.reviewed_correction_id
                                  OR ca.payment_date_quality_status IS DISTINCT FROM
                                     r.payment_date_quality_status
                              )
                            """,
                            (action_snapshot_run_id, run_id),
                        )
                        resolution_action_mismatch_count = int(
                            cur.fetchone()[0]
                        )
                resolution_append_only = bool(
                    pk
                    and tuple(pk[0]) == (
                        "quality_run_id", "asset_id", "source", "action_key",
                        "resolution_version",
                    )
                )
                resolution_rows = _resolution_contract_frame(
                    connection, run_id,
                )

            source_receipt_append_only = False
            source_receipt_count = 0
            source_receipt_distinct_count = 0
            source_receipt_included_count = 0
            source_receipt_excluded_count = 0
            source_receipt_attachment_count = 0
            source_receipt_no_common_count = 0
            source_receipt_cancelled_count = 0
            source_receipt_pending_count = 0
            source_receipt_unresolved_count = 0
            source_receipt_semantic_count = 0
            source_receipt_terminal_family_count = 0
            source_receipt_terminal_pending_count = 0
            source_receipt_exclusion_reasons: dict[str, int] = {}
            source_receipt_included_classes: dict[str, int] = {}
            source_receipt_excluded_classes: dict[str, int] = {}
            source_receipt_rows = pd.DataFrame(
                columns=SOURCE_RECEIPT_DIGEST_COLUMNS
            )
            persisted_action_rows = pd.DataFrame(
                columns=PUBLISHED_ACTION_DIGEST_COLUMNS
            )
            action_cash_parity_rows = pd.DataFrame(
                columns=INCLUDED_CASH_PARITY_COLUMNS
            )
            source_receipt_row_digest = None
            terminal_receipt_row_digest = None
            published_action_row_digest = None
            receipt_cash_parity_digest = None
            action_cash_parity_digest = None
            source_receipt_table = _table_exists(
                connection, "dividend_source_receipt"
            )
            if source_receipt_table and action_snapshot_run_id:
                with connection.cursor() as cur:
                    cur.execute(
                        """
                        SELECT array_agg(a.attname ORDER BY keys.ordinality)
                        FROM pg_constraint c
                        CROSS JOIN LATERAL unnest(c.conkey)
                            WITH ORDINALITY AS keys(attnum, ordinality)
                        JOIN pg_attribute a
                          ON a.attrelid=c.conrelid
                         AND a.attnum=keys.attnum
                        WHERE c.conrelid='dividend_source_receipt'::regclass
                          AND c.contype='p'
                        GROUP BY c.conname
                        """
                    )
                    source_pk = cur.fetchone()
                    cur.execute(
                        """
                        SELECT count(*),count(DISTINCT d.receipt_no),
                               count(*) FILTER (
                                   WHERE d.mapping_status='INCLUDED'
                               ),
                               count(*) FILTER (
                                   WHERE d.mapping_status='EXCLUDED'
                               ),
                               count(*) FILTER (
                                   WHERE d.cash_amount_status='ATTACHMENT_ONLY'
                               ),
                               count(*) FILTER (
                                   WHERE d.cash_amount_status=
                                       'NO_COMMON_CASH_DIVIDEND'
                               ),
                               count(*) FILTER (
                                   WHERE d.cash_amount_status=
                                       'NO_ECONOMIC_EVENT'
                               ),
                               count(*) FILTER (
                                   WHERE d.cash_amount_status=
                                       'POSITIVE_PENDING_RECORD_DATE'
                               ),
                               count(*) FILTER (
                                   WHERE NOT coalesce((
                                       (d.source_evidence_status=
                                            'VERIFIED_OPENDART_DOCUMENT'
                                        AND coalesce(
                                            d.viewer_evidence_sha256,''
                                        )=''
                                        AND d.economic_evidence_sha256 ~
                                            '^[0-9a-f]{64}$')
                                       OR
                                       (d.source_evidence_status=
                                            'VERIFIED_DART_VIEWER_BODY'
                                        AND d.viewer_evidence_sha256 ~
                                            '^[0-9a-f]{64}$'
                                        AND d.economic_evidence_sha256 ~
                                            '^[0-9a-f]{64}$'
                                        AND d.viewer_evidence_sha256=
                                            d.economic_evidence_sha256)
                                       OR
                                       (d.source_evidence_status=
                                            'VERIFIED_ATTACHMENT_CORRECTION'
                                        AND d.viewer_evidence_sha256 ~
                                            '^[0-9a-f]{64}$'
                                        AND d.economic_evidence_sha256 ~
                                            '^[0-9a-f]{64}$'
                                        AND d.viewer_evidence_sha256<>
                                            d.economic_evidence_sha256
                                        AND d.cash_amount_status=
                                            'ATTACHMENT_ONLY'
                                        AND d.revision_kind='ATTACHMENT_ONLY'
                                        AND d.previous_receipt_no IS NOT NULL)
                                       OR
                                       (d.source_evidence_status=
                                            'VERIFIED_REVIEWED_SOURCE_ERRATUM'
                                        AND coalesce(
                                            d.viewer_evidence_sha256,''
                                        )=''
                                        AND d.economic_evidence_sha256 ~
                                            '^[0-9a-f]{64}$'
                                        AND btrim(coalesce(
                                            d.reviewed_correction_id,''
                                        ))<>'')
                                   ),false)
                                      OR d.cash_amount_status NOT IN (
                                          'POSITIVE',
                                          'POSITIVE_PENDING_RECORD_DATE',
                                          'NO_COMMON_CASH_DIVIDEND',
                                          'NO_ECONOMIC_EVENT',
                                          'ATTACHMENT_ONLY'
                                      )
                                      OR (
                                          d.source_evidence_status=
                                              'VERIFIED_ATTACHMENT_CORRECTION'
                                          AND NOT EXISTS (
                                              SELECT 1
                                              FROM dividend_source_receipt prior
                                              WHERE prior.quality_run_id=
                                                  d.quality_run_id
                                                AND prior.receipt_no=
                                                  d.previous_receipt_no
                                                AND prior.ticker=d.ticker
                                                AND prior.revision_root_receipt_no=
                                                  d.revision_root_receipt_no
                                          )
                                      )
                               ),
                               count(*) FILTER (
                                   WHERE d.receipt_no ~ '^[0-9]{14}$'
                                     AND d.revision_root_receipt_no ~
                                         '^[0-9]{14}$'
                                     AND d.terminal_receipt_no ~
                                         '^[0-9]{14}$'
                                     AND d.terminal_announcement_date
                                         IS NOT NULL
                                     AND d.is_terminal_economic_revision =
                                         (d.receipt_no=d.terminal_receipt_no)
                                     AND (
                                         d.previous_receipt_no IS NULL
                                         OR d.previous_receipt_no ~
                                             '^[0-9]{14}$'
                                     )
                                     AND d.ticker ~ '^[0-9A-Z]{6}$'
                                     AND d.pit_event_date IS NOT NULL
                                     AND (
                                         (d.source_evidence_status=
                                              'VERIFIED_OPENDART_DOCUMENT'
                                          AND coalesce(
                                              d.viewer_evidence_sha256,''
                                          )=''
                                          AND d.economic_evidence_sha256 ~
                                              '^[0-9a-f]{64}$')
                                         OR
                                         (d.source_evidence_status=
                                              'VERIFIED_DART_VIEWER_BODY'
                                          AND d.viewer_evidence_sha256 ~
                                              '^[0-9a-f]{64}$'
                                          AND d.economic_evidence_sha256 ~
                                              '^[0-9a-f]{64}$'
                                          AND d.viewer_evidence_sha256=
                                              d.economic_evidence_sha256)
                                         OR
                                         (d.source_evidence_status=
                                              'VERIFIED_ATTACHMENT_CORRECTION'
                                          AND d.viewer_evidence_sha256 ~
                                              '^[0-9a-f]{64}$'
                                          AND d.economic_evidence_sha256 ~
                                              '^[0-9a-f]{64}$'
                                          AND d.viewer_evidence_sha256<>
                                              d.economic_evidence_sha256
                                          AND d.cash_amount_status=
                                              'ATTACHMENT_ONLY'
                                          AND d.revision_kind='ATTACHMENT_ONLY'
                                          AND d.previous_receipt_no IS NOT NULL)
                                         OR
                                         (d.source_evidence_status=
                                              'VERIFIED_REVIEWED_SOURCE_ERRATUM'
                                          AND coalesce(
                                              d.viewer_evidence_sha256,''
                                          )=''
                                          AND d.economic_evidence_sha256 ~
                                              '^[0-9a-f]{64}$'
                                          AND btrim(coalesce(
                                              d.reviewed_correction_id,''
                                          ))<>'')
                                     )
                                     AND (
                                         d.source_evidence_status<>
                                             'VERIFIED_ATTACHMENT_CORRECTION'
                                         OR EXISTS (
                                             SELECT 1
                                             FROM dividend_source_receipt prior
                                             WHERE prior.quality_run_id=
                                                 d.quality_run_id
                                               AND prior.receipt_no=
                                                 d.previous_receipt_no
                                               AND prior.ticker=d.ticker
                                               AND prior.revision_root_receipt_no=
                                                 d.revision_root_receipt_no
                                         )
                                     )
                                     AND (
                                         (d.mapping_status='INCLUDED'
                                          AND d.asset_id IS NOT NULL
                                          AND d.excluded_reason IS NULL)
                                         OR
                                         (d.mapping_status='EXCLUDED'
                                          AND d.excluded_reason IS NOT NULL)
                                     )
                                     AND (
                                         (d.cash_amount_status='POSITIVE'
                                          AND d.record_date IS NOT NULL
                                          AND d.cash_amount > 0)
                                         OR
                                         (d.cash_amount_status=
                                              'POSITIVE_PENDING_RECORD_DATE'
                                          AND d.record_date IS NULL
                                          AND d.cash_amount > 0)
                                         OR
                                         (d.cash_amount_status IN (
                                              'NO_COMMON_CASH_DIVIDEND',
                                              'NO_ECONOMIC_EVENT',
                                              'ATTACHMENT_ONLY'
                                          ) AND d.cash_amount IS NULL)
                                     )
                               ),
                               count(*) FILTER (
                                   WHERE d.is_terminal_economic_revision
                               ),
                               count(*) FILTER (
                                   WHERE d.is_terminal_economic_revision
                                     AND d.cash_amount_status=
                                         'POSITIVE_PENDING_RECORD_DATE'
                               )
                        FROM dividend_source_receipt d
                        WHERE d.quality_run_id=%s
                        """,
                        (action_snapshot_run_id,),
                    )
                    (
                        source_receipt_count,
                        source_receipt_distinct_count,
                        source_receipt_included_count,
                        source_receipt_excluded_count,
                        source_receipt_attachment_count,
                        source_receipt_no_common_count,
                        source_receipt_cancelled_count,
                        source_receipt_pending_count,
                        source_receipt_unresolved_count,
                        source_receipt_semantic_count,
                        source_receipt_terminal_family_count,
                        source_receipt_terminal_pending_count,
                    ) = [int(value) for value in cur.fetchone()]
                    cur.execute(
                        """
                        SELECT excluded_reason,count(*)
                        FROM dividend_source_receipt
                        WHERE quality_run_id=%s
                          AND mapping_status='EXCLUDED'
                        GROUP BY excluded_reason
                        ORDER BY excluded_reason
                        """,
                        (action_snapshot_run_id,),
                    )
                    source_receipt_exclusion_reasons = {
                        str(reason): int(count)
                        for reason, count in cur.fetchall()
                    }
                    cur.execute(
                        """
                        SELECT mapping_status,
                               coalesce(nullif(btrim(corp_cls),''),'UNKNOWN'),
                               count(*)
                        FROM dividend_source_receipt
                        WHERE quality_run_id=%s
                        GROUP BY mapping_status,
                                 coalesce(nullif(btrim(corp_cls),''),'UNKNOWN')
                        ORDER BY mapping_status,2
                        """,
                        (action_snapshot_run_id,),
                    )
                    for mapping_status, corp_cls, count in cur.fetchall():
                        target = (
                            source_receipt_included_classes
                            if mapping_status == "INCLUDED"
                            else source_receipt_excluded_classes
                        )
                        target[str(corp_cls)] = int(count)
                source_receipt_rows = _source_receipt_contract_frame(
                    connection, action_snapshot_run_id,
                )
                persisted_action_rows = _published_action_contract_frame(
                    connection, action_snapshot_run_id,
                )
                included_receipt_rows = source_receipt_rows[
                    source_receipt_rows["mapping_status"].eq("INCLUDED")
                ]
                action_cash_parity_rows = _action_cash_parity_frame(
                    persisted_action_rows
                )
                source_receipt_row_digest = source_receipt_digest(
                    source_receipt_rows
                )
                terminal_receipt_row_digest = terminal_source_receipt_digest(
                    source_receipt_rows
                )
                published_action_row_digest = published_action_digest(
                    persisted_action_rows
                )
                receipt_cash_parity_digest = included_cash_parity_digest(
                    included_receipt_rows
                )
                action_cash_parity_digest = included_cash_parity_digest(
                    action_cash_parity_rows
                )
                source_receipt_append_only = bool(
                    source_pk
                    and tuple(source_pk[0]) == (
                        "quality_run_id", "receipt_no",
                    )
                )

            scale_parent_table = _table_exists(
                connection, "cash_adjustment_scale_source_evidence"
            )
            scale_support_table = _table_exists(
                connection, "cash_adjustment_scale_support_action"
            )
            scale_parents = pd.DataFrame(columns=SOURCE_EVIDENCE_COLUMNS)
            scale_supports = pd.DataFrame(columns=SUPPORT_ACTION_COLUMNS)
            scale_source_structure_valid = False
            scale_source_group_count = 0
            scale_cash_action_mismatch_count = 0
            scale_support_action_mismatch_count = 0
            persisted_parent_digest = None
            persisted_support_digest = None
            manifest_parent_digest = None
            manifest_support_digest = None
            if (
                action_snapshot_run_id
                and scale_parent_table
                and scale_support_table
            ):
                scale_parents, scale_supports = _scale_source_contract_frames(
                    connection, action_snapshot_run_id,
                )
                (
                    scale_source_structure_valid,
                    scale_source_group_count,
                ) = _validate_scale_source_rows(scale_parents, scale_supports)
                try:
                    persisted_parent_digest = source_evidence_digest(
                        scale_parents
                    )
                    persisted_support_digest = support_action_digest(
                        scale_supports
                    )
                    manifest_parent_digest = source_manifest_digest(
                        scale_parents
                    )
                    manifest_support_digest = support_manifest_digest(
                        scale_supports
                    )
                except (KeyError, TypeError, ValueError, RuntimeError):
                    scale_source_structure_valid = False
                with connection.cursor() as cur:
                    cur.execute(
                        """
                        SELECT count(*)
                        FROM cash_adjustment_scale_source_evidence pe
                        LEFT JOIN corporate_action ca
                          ON ca.asset_id=pe.asset_id
                         AND ca.source='DART_DISCLOSURE'
                         AND ca.action_key=pe.cash_receipt_no
                         AND ca.action_type='cash_dividend'
                         AND ca.quality_run_id=pe.action_snapshot_run_id
                        LEFT JOIN dividend_source_receipt dr
                          ON dr.quality_run_id=pe.action_snapshot_run_id
                         AND dr.receipt_no=pe.cash_receipt_no
                        WHERE pe.action_snapshot_run_id=%s
                          AND (
                              ca.asset_id IS NULL
                              OR ca.source_body_sha256 IS DISTINCT FROM
                                 pe.cash_action_body_sha256
                              OR dr.receipt_no IS NULL
                              OR dr.asset_id IS DISTINCT FROM pe.asset_id
                              OR dr.ticker IS DISTINCT FROM pe.ticker
                              OR dr.mapping_status IS DISTINCT FROM 'INCLUDED'
                              OR NOT dr.is_terminal_economic_revision
                              OR dr.economic_evidence_sha256 IS DISTINCT FROM
                                 pe.cash_economic_sha256
                              OR dr.source_evidence_status IS DISTINCT FROM
                                 pe.cash_source_evidence_status
                          )
                        """,
                        (action_snapshot_run_id,),
                    )
                    scale_cash_action_mismatch_count = int(cur.fetchone()[0])
                    cur.execute(
                        """
                        SELECT count(*)
                        FROM cash_adjustment_scale_support_action se
                        JOIN cash_adjustment_scale_source_evidence pe
                          ON pe.action_snapshot_run_id=
                             se.action_snapshot_run_id
                         AND pe.evidence_key=se.evidence_key
                        LEFT JOIN corporate_action ca
                          ON ca.asset_id=pe.asset_id
                         AND ca.source=se.support_action_source
                         AND ca.action_key=se.support_action_key
                         AND ca.action_type=se.support_action_type
                         AND ca.quality_run_id=
                             se.support_action_quality_run_id
                        LEFT JOIN dividend_source_receipt dr
                          ON dr.quality_run_id=pe.action_snapshot_run_id
                         AND dr.receipt_no=pe.cash_receipt_no
                        WHERE se.action_snapshot_run_id=%s
                          AND (
                              se.support_action_quality_run_id IS DISTINCT FROM
                                 se.action_snapshot_run_id
                              OR ca.asset_id IS NULL
                              OR ca.source_body_sha256 IS DISTINCT FROM
                                 se.support_action_body_sha256
                              OR ca.announcement_date IS DISTINCT FROM
                                 se.support_announcement_date
                              OR ca.ex_date IS DISTINCT FROM se.support_ex_date
                              OR ca.record_date IS DISTINCT FROM
                                 se.support_record_date
                              OR ca.ratio_numerator IS DISTINCT FROM
                                 se.support_ratio_numerator
                              OR ca.ratio_denominator IS DISTINCT FROM
                                 se.support_ratio_denominator
                              OR ca.expected_price_factor IS DISTINCT FROM
                                 se.support_expected_price_factor
                              OR ca.report_name IS DISTINCT FROM
                                 se.support_report_name
                              OR ca.action_scope IS DISTINCT FROM
                                 se.support_action_scope
                          )
                        """,
                        (action_snapshot_run_id,),
                    )
                    scale_support_action_mismatch_count = int(cur.fetchone()[0])

            certified_scope = metadata.get("certified_scope") or {}
            input_scope = metadata.get("input_scope") or {}
            action_metadata = metadata.get("action_snapshot") or {}
            action_pit_scope = action_metadata.get("pit_scope") or {}
            snapshot_pit_scope = snapshot_metadata.get("pit_scope") or {}
            action_source_receipts = action_metadata.get(
                "source_receipts"
            ) or {}
            snapshot_source_receipts = snapshot_metadata.get(
                "source_receipts"
            ) or {}
            action_published_actions = action_metadata.get(
                "published_actions"
            ) or {}
            snapshot_published_actions = snapshot_metadata.get(
                "published_actions"
            ) or {}
            action_disclosure_observation_audit = action_metadata.get(
                "disclosure_observation_audit"
            ) or {}
            snapshot_disclosure_observation_audit = snapshot_metadata.get(
                "disclosure_observation_audit"
            ) or {}
            action_scale_source = action_metadata.get(
                "cash_adjustment_scale_evidence"
            ) or {}
            snapshot_scale_source = snapshot_metadata.get(
                "cash_adjustment_scale_evidence"
            ) or {}
            identity_metadata = metadata.get("asset_identity") or {}
            parity_metadata = metadata.get("per_row_run_parity") or {}
            research_role = metadata.get("research_role") or {}
            scale_evidence = metadata.get(
                "cash_adjustment_scale_evidence"
            ) or {}
            expected_action_metadata = (
                {
                    "manifest_sha256": snapshot_row[0],
                    "body_digest": snapshot_row[1],
                    "body_count": int(snapshot_row[2]),
                    "action_count": int(snapshot_row[5]),
                    "coverage_start": str(snapshot_row[3]),
                    "coverage_end": str(snapshot_row[4]),
                    "pit_scope": snapshot_metadata.get("pit_scope") or {},
                    "source_receipts": (
                        snapshot_metadata.get("source_receipts") or {}
                    ),
                    "published_actions": (
                        snapshot_metadata.get("published_actions") or {}
                    ),
                    "disclosure_observation_audit": (
                        snapshot_metadata.get(
                            "disclosure_observation_audit"
                        ) or {}
                    ),
                    "cash_adjustment_scale_evidence": (
                        snapshot_metadata.get(
                            "cash_adjustment_scale_evidence"
                        ) or {}
                    ),
                }
                if snapshot_row is not None else None
            )
            snapshot_content_matches = (
                expected_action_metadata is not None
                and action_metadata == expected_action_metadata
            )
            scale_source_metadata_valid = False
            if action_scale_source == snapshot_scale_source:
                try:
                    expected_scale_source_keys = {
                        "contract", "manifest_sha256",
                        "manifest_parent_row_count",
                        "manifest_parent_row_digest",
                        "manifest_support_action_count",
                        "manifest_support_action_digest",
                        "manifest_support_semantic_group_count",
                        "persisted_parent_row_count",
                        "persisted_parent_row_digest",
                        "persisted_support_action_count",
                        "persisted_support_action_digest",
                        "persisted_support_semantic_group_count",
                        "changed_scale_coverage_count", "unresolved_count",
                    }
                    scale_source_metadata_valid = (
                        scale_source_structure_valid
                        and set(action_scale_source) == expected_scale_source_keys
                        and action_scale_source.get("contract")
                        == SOURCE_EVIDENCE_CONTRACT
                        and int(action_scale_source.get(
                            "manifest_parent_row_count", -1,
                        )) == len(scale_parents)
                        and action_scale_source.get(
                            "manifest_parent_row_digest"
                        ) == manifest_parent_digest
                        and int(action_scale_source.get(
                            "manifest_support_action_count", -1,
                        )) == len(scale_supports)
                        and action_scale_source.get(
                            "manifest_support_action_digest"
                        ) == manifest_support_digest
                        and int(action_scale_source.get(
                            "manifest_support_semantic_group_count", -1,
                        )) == scale_source_group_count
                        and int(action_scale_source.get(
                            "persisted_parent_row_count", -1,
                        )) == len(scale_parents)
                        and action_scale_source.get(
                            "persisted_parent_row_digest"
                        ) == persisted_parent_digest
                        and int(action_scale_source.get(
                            "persisted_support_action_count", -1,
                        )) == len(scale_supports)
                        and action_scale_source.get(
                            "persisted_support_action_digest"
                        ) == persisted_support_digest
                        and int(action_scale_source.get(
                            "persisted_support_semantic_group_count", -1,
                        )) == scale_source_group_count
                        and int(action_scale_source.get(
                            "changed_scale_coverage_count", -1,
                        )) == len(scale_parents)
                        and int(action_scale_source.get(
                            "unresolved_count", -1,
                        )) == 0
                        and re.fullmatch(
                            r"[0-9a-f]{64}",
                            str(action_scale_source.get("manifest_sha256", "")),
                        ) is not None
                        and scale_cash_action_mismatch_count == 0
                        and scale_support_action_mismatch_count == 0
                    )
                except (TypeError, ValueError):
                    scale_source_metadata_valid = False

            runtime_scale_evidence, runtime_scale_valid = (
                _runtime_resolution_evidence(
                    resolution_rows,
                    scale_parents,
                    action_snapshot_run_id=action_snapshot_run_id,
                )
            )
            if runtime_scale_evidence:
                runtime_scale_evidence.update({
                    "first_listing_exclusion_count": (
                        resolution_first_listing_count
                    ),
                    "explicit_exclusion_count": (
                        resolution_explicit_exclusion_count
                    ),
                })
            runtime_scale_valid = (
                runtime_scale_valid
                and resolution_price_lineage_count == len(resolution_rows)
                and resolution_price_lineage_mismatch_count == 0
                and resolution_action_mismatch_count == 0
                and len(resolution_rows) == resolution_applied_count
            )
            pit_included_classes = action_pit_scope.get(
                "included_by_corp_cls"
            ) or {}
            pit_excluded_classes = action_pit_scope.get(
                "excluded_by_corp_cls"
            ) or {}
            pit_excluded_reasons = action_pit_scope.get(
                "excluded_by_reason"
            ) or {}
            pit_input_count = int(action_pit_scope.get(
                "input_action_count", -1,
            ))
            pit_included_count = int(action_pit_scope.get(
                "included_action_count", -1,
            ))
            pit_excluded_count = int(action_pit_scope.get(
                "excluded_action_count", -1,
            ))
            pit_scope_evidence = (
                action_pit_scope == snapshot_pit_scope
                and action_pit_scope.get("contract") == (
                    "event_date_identity_common_stock_"
                    "certified_kospi_kosdaq_price_episode"
                )
                and all(isinstance(value, dict) for value in (
                    pit_included_classes,
                    pit_excluded_classes,
                    pit_excluded_reasons,
                ))
                and pit_input_count
                == pit_included_count + pit_excluded_count
                and pit_included_count == int(snapshot_row[5])
                and sum(int(value) for value in pit_included_classes.values())
                == pit_included_count
                and sum(int(value) for value in pit_excluded_classes.values())
                == pit_excluded_count
                and sum(int(value) for value in pit_excluded_reasons.values())
                == pit_excluded_count
            ) if snapshot_row is not None else False
            expected_source_total = int(action_source_receipts.get(
                "source_cash_receipt_count", -1,
            ))
            expected_source_included = int(action_source_receipts.get(
                "included_cash_receipt_count", -1,
            ))
            expected_source_excluded = int(action_source_receipts.get(
                "excluded_cash_receipt_count", -1,
            ))
            expected_source_reasons = action_source_receipts.get(
                "cash_receipt_exclusion_reasons",
            ) or {}
            expected_source_included_classes = action_source_receipts.get(
                "included_cash_receipts_by_corp_cls",
            ) or {}
            expected_source_excluded_classes = action_source_receipts.get(
                "excluded_cash_receipts_by_corp_cls",
            ) or {}
            source_receipt_evidence = (
                action_source_receipts == snapshot_source_receipts
                and source_receipt_table
                and source_receipt_append_only
                and expected_source_total == source_receipt_count
                and source_receipt_distinct_count == source_receipt_count
                and expected_source_total
                == expected_source_included + expected_source_excluded
                and expected_source_included
                == source_receipt_included_count
                == persisted_cash_action_count
                and expected_source_excluded
                == source_receipt_excluded_count
                and int(action_source_receipts.get(
                    "attachment_correction_count", -1,
                )) == source_receipt_attachment_count
                and int(action_source_receipts.get(
                    "no_common_cash_dividend_count", -1,
                )) == source_receipt_no_common_count
                and int(action_source_receipts.get(
                    "withdrawn_or_cancelled_count", -1,
                )) == source_receipt_cancelled_count
                and int(action_source_receipts.get(
                    "pending_record_date_count", -1,
                )) == source_receipt_pending_count
                and int(action_source_receipts.get(
                    "unresolved_cash_receipt_count", -1,
                )) == source_receipt_unresolved_count == 0
                and source_receipt_semantic_count == source_receipt_count
                and int(action_source_receipts.get(
                    "economic_decision_count", -1,
                )) == source_receipt_terminal_family_count
                and int(action_source_receipts.get(
                    "terminal_economic_receipt_count", -1,
                )) == source_receipt_terminal_family_count
                and source_receipt_terminal_pending_count == 0
                and action_source_receipts.get(
                    "source_receipt_row_digest"
                ) == source_receipt_row_digest
                and action_source_receipts.get(
                    "terminal_economic_receipt_digest"
                ) == terminal_receipt_row_digest
                and expected_source_reasons
                == source_receipt_exclusion_reasons
                and expected_source_included_classes
                == source_receipt_included_classes
                and expected_source_excluded_classes
                == source_receipt_excluded_classes
                and sum(int(value) for value in expected_source_reasons.values())
                == expected_source_excluded
            )
            published_action_evidence = (
                source_receipt_table
                and action_snapshot_run_id is not None
                and action_published_actions == snapshot_published_actions
                and int(action_published_actions.get(
                    "published_action_count", -1,
                )) == len(persisted_action_rows) == persisted_action_count
                and action_published_actions.get(
                    "published_action_scope_contract"
                ) == "issuer_cash_ex_plus_manifest_scale_support_v1"
                and action_published_actions.get(
                    "published_action_row_digest"
                ) == published_action_row_digest
                and int(action_published_actions.get(
                    "included_cash_action_parity_count", -1,
                )) == len(action_cash_parity_rows) == persisted_cash_action_count
                and receipt_cash_parity_digest == action_cash_parity_digest
                and action_published_actions.get(
                    "included_cash_action_parity_digest"
                ) == action_cash_parity_digest
            )
            checks = {
                "contract_exists": True,
                "status_certified": status == "CERTIFIED",
                "contract_run_certified": (
                    contract_run_status == "CERTIFIED"
                    and contract_run_mode == "krx_total_return_rebuild"
                ),
                "methodology_current": (
                    methodology_version == METHODOLOGY_VERSION
                ),
                "contract_release_current": (
                    metadata.get("contract_release")
                    == CONTRACT_RELEASE
                ),
                "coverage_matches_first_trade": coverage_start == first_trade,
                "coverage_matches_latest_price": (
                    coverage_end == last_trade
                ),
                "common_stock_scope": (
                    certified_scope.get("source") == "KRX"
                    and certified_scope.get("asset_type") == "stock"
                    and certified_scope.get("instrument_type") == "common_stock"
                    and certified_scope.get("coverage_start")
                    == CONTRACT_START.isoformat()
                    and certified_scope.get("markets")
                    == list(CERTIFIED_MARKETS)
                ),
                "raw_price_lineage_certified": int(raw_certified) == int(row_count),
                "total_return_values_valid": (
                    int(invalid_total_return_count) == 0
                ),
                "separate_total_return_lineage_column": has_lineage,
                "per_row_total_return_run_parity": (
                    has_lineage and int(run_parity_count) == int(row_count)
                ),
                "append_only_resolution_pk": resolution_append_only,
                "resolution_row_parity": (
                    resolution_run_count
                    == int(metadata.get("cash_action_count", -1))
                ),
                "resolution_version_parity": (
                    resolution_expected_version_count == resolution_run_count
                    and metadata.get("resolution_version")
                    == RESOLUTION_VERSION
                ),
                "resolution_semantic_parity": (
                    resolution_semantic_count == resolution_run_count
                    and resolution_canonical_source_count
                    == int(metadata.get("canonical_event_count", -1))
                    and resolution_applied_count
                    == int(metadata.get("applied_event_count", -1))
                    and resolution_excluded_count
                    == int(metadata.get("excluded_event_count", -1))
                ),
                "resolution_explicit_exclusions": (
                    resolution_explicit_exclusion_count
                    == resolution_excluded_count
                    and resolution_first_listing_count >= 0
                ),
                "resolution_lineage_parity": (
                    resolution_lineage_count == resolution_run_count
                ),
                "content_addressed_action_snapshot": (
                    snapshot_content_matches
                ),
                "disclosure_observation_canonicalization_bound": (
                    action_disclosure_observation_audit
                    == snapshot_disclosure_observation_audit
                    and action_disclosure_observation_audit.get("contract")
                    == "latest_manifest_interval_mutable_list_fields_v2"
                    and re.fullmatch(
                        r"[0-9a-f]{64}",
                        str(action_disclosure_observation_audit.get(
                            "mutable_conflict_digest", "",
                        )),
                    ) is not None
                ),
                "action_snapshot_market_scope": (
                    snapshot_metadata.get("markets")
                    == list(CERTIFIED_MARKETS)
                ),
                "action_snapshot_coverage": (
                    snapshot_row is not None
                    and snapshot_row[3] == CONTRACT_START
                    and last_trade is not None
                    and snapshot_row[4] >= last_trade
                ),
                "action_snapshot_run_certified": (
                    snapshot_row is not None
                    and snapshot_row[7] == "CERTIFIED"
                    and snapshot_row[8]
                    == "dart_dividend_action_backfill"
                    and snapshot_row[9] == ACTION_SNAPSHOT_SCHEMA_VERSION
                ),
                "input_scope_contract": input_scope == {
                    "prices": "CERTIFIED KRX common_stock KOSPI/KOSDAQ",
                    "actions": (
                        "CERTIFIED issuer DART cash/ex actions plus exact "
                        "referenced scale-support corporate_action rows, "
                        "bound by event-date PIT identity and source-body digest"
                    ),
                    "cash_scale_source_evidence": (
                        "append-only content-addressed cash/action and separate "
                        "previous/adjustment KRX source objects; changed scale "
                        "exact 1:1 parent, stable scale no parent"
                    ),
                },
                "action_snapshot_row_parity": (
                    snapshot_row is not None
                    and has_action_corp_cls
                    and persisted_action_count == int(snapshot_row[5])
                ),
                "pit_scope_partition_evidence": pit_scope_evidence,
                "source_receipt_append_only_pk": (
                    source_receipt_append_only
                ),
                "source_receipt_exact_parity": source_receipt_evidence,
                "published_action_exact_parity": published_action_evidence,
                "cash_scale_source_exact_parity": (
                    scale_source_metadata_valid
                ),
                "asset_identity_bound": (
                    observed_identity is not None
                    and identity_metadata.get("contract")
                    == ASSET_IDENTITY_CONTRACT
                    and identity_metadata.get("digest")
                    == observed_identity.digest
                    and int(identity_metadata.get("row_count", -1))
                    == observed_identity.row_count
                    and int(identity_metadata.get("asset_count", -1))
                    == observed_identity.asset_count
                ),
                "contract_declares_row_parity": (
                    parity_metadata.get("passed") is True
                    and int(parity_metadata.get("expected", -1))
                    == int(row_count)
                    and int(parity_metadata.get("actual", -1))
                    == int(row_count)
                ),
                "ex_post_label_role_declared": (
                    research_role.get("role")
                    == "ex_post_realized_forward_return_label"
                    and research_role.get("feature_pit_safe") is False
                    and research_role.get("action_vintage")
                    == "latest_corrected_action_snapshot"
                ),
                "cash_adjustment_scale_verified": (
                    runtime_scale_valid
                    and scale_evidence == runtime_scale_evidence
                ),
            }
            return {
                "safe_for_research": all(checks.values()),
                "checks": checks,
                "contract": {
                    "status": status,
                    "coverage_start": str(coverage_start),
                    "coverage_end": str(coverage_end),
                    "quality_run_id": str(run_id),
                    "methodology_version": methodology_version,
                },
                "observed_scope": {
                    "instrument_type": "common_stock",
                    "markets": list(CERTIFIED_MARKETS),
                    "row_count": int(row_count),
                    "asset_count": int(asset_count),
                    "first_trade": str(first_trade),
                    "last_trade": str(last_trade),
                    "raw_certified_row_count": int(raw_certified),
                    "invalid_total_return_row_count": int(
                        invalid_total_return_count
                    ),
                    "total_return_run_parity_count": int(run_parity_count),
                    "resolution_run_row_count": resolution_run_count,
                    "resolution_applied_row_count": resolution_applied_count,
                    "resolution_excluded_row_count": resolution_excluded_count,
                    "cash_scale_verified_event_count": len(resolution_rows),
                    "cash_scale_mismatch_count": (
                        resolution_price_lineage_mismatch_count
                        + resolution_action_mismatch_count
                    ),
                    "cash_scale_source_parent_count": len(scale_parents),
                    "cash_scale_source_support_count": len(scale_supports),
                    "cash_scale_source_group_count": scale_source_group_count,
                    "cash_scale_first_listing_exclusion_count": (
                        resolution_first_listing_count
                    ),
                    "persisted_action_row_count": persisted_action_count,
                    "persisted_cash_action_row_count": (
                        persisted_cash_action_count
                    ),
                    "source_receipt_row_count": source_receipt_count,
                    "source_receipt_included_count": (
                        source_receipt_included_count
                    ),
                    "source_receipt_excluded_count": (
                        source_receipt_excluded_count
                    ),
                    "source_receipt_terminal_pending_count": (
                        source_receipt_terminal_pending_count
                    ),
                },
                "action_snapshot_contract": (
                    None
                    if snapshot_row is None
                    else {
                        "manifest_sha256": snapshot_row[0],
                        "body_digest": snapshot_row[1],
                        "body_count": int(snapshot_row[2]),
                        "coverage_start": str(snapshot_row[3]),
                        "coverage_end": str(snapshot_row[4]),
                        "action_count": int(snapshot_row[5]),
                        "metadata": snapshot_metadata,
                        "dq_run_status": snapshot_row[7],
                        "dq_run_mode": snapshot_row[8],
                        "schema_version": snapshot_row[9],
                    }
                ),
            }
    finally:
        if owns_connection:
            connection.close()


def main() -> None:
    report = audit()
    print(json.dumps(report, ensure_ascii=False, indent=2))
    if not report["safe_for_research"]:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
