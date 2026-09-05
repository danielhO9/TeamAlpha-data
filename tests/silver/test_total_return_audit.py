from contextlib import nullcontext
import copy
from datetime import date
import hashlib
import json
from uuid import uuid4

import pandas as pd
import pytest

from pipeline.silver.cash_adjustment_scale_evidence import (
    PRE_EVENT_PRICE_SCALE,
    RESOLUTION_DIGEST_COLUMNS,
    RESOLUTION_EVIDENCE_CONTRACT,
    SOURCE_EVIDENCE_COLUMNS,
    SOURCE_EVIDENCE_CONTRACT,
    SUPPORT_ACTION_COLUMNS,
    resolution_evidence_digest,
    source_evidence_digest,
    source_manifest_digest,
    support_action_digest,
    support_manifest_digest,
)
from pipeline.silver.dividend_evidence import (
    PUBLISHED_ACTION_DIGEST_COLUMNS,
    SOURCE_RECEIPT_DIGEST_COLUMNS,
    included_cash_parity_digest,
    published_action_digest,
    source_receipt_digest,
    terminal_source_receipt_digest,
)
from pipeline.silver.return_contract import CONTRACT_RELEASE
from pipeline.silver.total_return_audit import _validate_scale_source_rows, audit
from pipeline.silver.krx_kind_reference import (
    KIND_REFERENCE_REPORT_NAME_70767,
    KIND_REFERENCE_REPORT_NAME_99311,
)


class _Cursor:
    def __init__(self, state):
        self.state = state
        self.sql = ""
        self.params = None

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def execute(self, sql, params=None):
        self.sql = " ".join(sql.split())
        self.params = params

    def fetchone(self):
        if self.sql.startswith("SELECT c.status,c.coverage_start"):
            return self.state["contract"]
        if "count(DISTINCT p.asset_id)" in self.sql:
            return self.state["scope"]
        if "information_schema.columns" in self.sql:
            if self.params[0] == "corporate_action":
                return (self.state["has_corp_cls"],)
            return (self.state["has_lineage"],)
        if self.sql.startswith("SELECT to_regclass"):
            table = self.params[0].split(".")[-1]
            return (table if self.state.get(table, False) else None,)
        if "p.total_return_quality_run_id" in self.sql:
            return (self.state["run_parity"],)
        if self.sql.startswith("SELECT s.manifest_sha256"):
            return self.state["snapshot"]
        if self.sql.startswith("SELECT count(*) FROM dividend_event_resolution r"):
            return (self.state["resolution_action_mismatch_count"],)
        if self.sql.startswith(
            "SELECT count(*) FROM cash_adjustment_scale_source_evidence pe"
        ):
            return (self.state["scale_cash_action_mismatch_count"],)
        if self.sql.startswith(
            "SELECT count(*) FROM cash_adjustment_scale_support_action se"
        ):
            return (self.state["scale_support_action_mismatch_count"],)
        if "FROM corporate_action ca" in self.sql:
            return self.state["persisted_action_counts"]
        if "c.conrelid= 'dividend_event_resolution'" in self.sql:
            return (self.state["resolution_pk"],)
        if "c.conrelid='dividend_source_receipt'" in self.sql:
            return (self.state["source_receipt_pk"],)
        if self.sql.startswith("WITH applied AS"):
            return self.state["scale_counts"]
        if "FROM dividend_event_resolution" in self.sql:
            return self.state["resolution_counts"]
        if self.sql.startswith("SELECT count(*),count(DISTINCT d.receipt_no)"):
            return self.state["source_receipt_counts"]
        raise AssertionError(self.sql)

    def fetchall(self):
        if self.sql.startswith("SELECT ai.asset_id"):
            return self.state["identity_rows"]
        if self.sql.startswith("SELECT excluded_reason,count(*)"):
            return self.state["source_receipt_reason_rows"]
        if self.sql.startswith("SELECT mapping_status,"):
            return self.state["source_receipt_class_rows"]
        if self.sql.startswith("SELECT receipt_no,asset_id,ticker"):
            return self.state["source_receipt_digest_rows"]
        if self.sql.startswith("SELECT ca.asset_id,ca.source,ca.action_key"):
            return self.state["published_action_digest_rows"]
        if self.sql.startswith("SELECT action_snapshot_run_id,evidence_key"):
            if "FROM cash_adjustment_scale_source_evidence" in self.sql:
                return self.state["scale_parent_rows"]
            return self.state["scale_support_rows"]
        if self.sql.startswith("SELECT asset_id,source,action_key"):
            return self.state["resolution_digest_rows"]
        raise AssertionError(self.sql)


class _Connection:
    def __init__(self, state):
        self.state = state

    def cursor(self):
        return _Cursor(self.state)

    def transaction(self):
        return nullcontext()


def _safe_state():
    rebuild_run = uuid4()
    action_run = uuid4()
    identity_rows = [(1, "005930", date.min, None)]
    identity_hash = hashlib.sha256()
    identity_hash.update(
        json.dumps(
            [1, "005930", date.min.isoformat(), None],
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
    )
    identity_hash.update(b"\n")
    pit_scope = {
        "contract": (
            "event_date_identity_common_stock_"
            "certified_kospi_kosdaq_price_episode"
        ),
        "input_action_count": 6,
        "included_action_count": 4,
        "excluded_action_count": 2,
        "included_by_corp_cls": {"Y": 3, "E": 1},
        "excluded_by_corp_cls": {"N": 1, "UNKNOWN": 1},
        "excluded_by_reason": {
            "NO_EVENT_DATE_PIT_IDENTITY": 1,
            "BEFORE_CONTRACT": 1,
        },
    }
    receipt_rows = [
        {
            "receipt_no": "20260102000001", "asset_id": 1,
            "ticker": "005930", "corp_cls": "Y", "report_name": "cash",
            "dart_rm": "", "announcement_date": date(2026, 1, 2),
            "revision_kind": "ORIGINAL_DECISION",
            "revision_root_receipt_no": "20260102000001",
            "previous_receipt_no": None,
            "terminal_receipt_no": "20260102000001",
            "terminal_announcement_date": date(2026, 1, 2),
            "is_terminal_economic_revision": True,
            "source_evidence_status": "VERIFIED_OPENDART_DOCUMENT",
            "cash_amount_status": "POSITIVE",
            "record_date": date(2026, 1, 31), "payment_date": None,
            "cash_amount": 100.0, "viewer_evidence_sha256": None,
            "economic_evidence_sha256": "1" * 64,
            "reviewed_correction_id": None,
            "payment_date_quality_status": None,
            "pit_event_date": date(2026, 1, 31),
            "mapping_status": "INCLUDED", "excluded_reason": None,
        },
        {
            "receipt_no": "20260202000002", "asset_id": 2,
            "ticker": "000660", "corp_cls": "E", "report_name": "cash",
            "dart_rm": "", "announcement_date": date(2026, 2, 2),
            "revision_kind": "ORIGINAL_DECISION",
            "revision_root_receipt_no": "20260202000002",
            "previous_receipt_no": None,
            "terminal_receipt_no": "20260202000002",
            "terminal_announcement_date": date(2026, 2, 2),
            "is_terminal_economic_revision": True,
            "source_evidence_status": "VERIFIED_OPENDART_DOCUMENT",
            "cash_amount_status": "POSITIVE",
            "record_date": date(2026, 2, 28), "payment_date": None,
            "cash_amount": 200.0, "viewer_evidence_sha256": None,
            "economic_evidence_sha256": "2" * 64,
            "reviewed_correction_id": None,
            "payment_date_quality_status": None,
            "pit_event_date": date(2026, 2, 28),
            "mapping_status": "INCLUDED", "excluded_reason": None,
        },
    ]
    receipt_frame = pd.DataFrame(receipt_rows)
    action_defaults = {
        column: None for column in PUBLISHED_ACTION_DIGEST_COLUMNS
    }
    cash_action_body_shas = ["4" * 64, "5" * 64]
    action_rows = []
    for receipt, source_body_sha256 in zip(
        receipt_rows, cash_action_body_shas, strict=True,
    ):
        action_rows.append({**action_defaults,
            "asset_id": receipt["asset_id"], "source": "DART_DISCLOSURE",
            "action_key": receipt["receipt_no"],
            "action_type": "cash_dividend",
            "announcement_date": receipt["announcement_date"],
            "record_date": receipt["record_date"],
            "payment_date": receipt["payment_date"],
            "cash_amount": receipt["cash_amount"], "currency": "KRW",
            "status": "announced", "report_name": receipt["report_name"],
            "dart_rm": receipt["dart_rm"], "corp_cls": receipt["corp_cls"],
            "action_scope": "ISSUER",
            "cash_amount_status": receipt["cash_amount_status"],
            "source_evidence_status": receipt["source_evidence_status"],
            "correction_of_action_key": receipt["previous_receipt_no"],
            "revision_root_action_key": receipt["revision_root_receipt_no"],
            "revision_kind": receipt["revision_kind"],
            "viewer_evidence_sha256": receipt["viewer_evidence_sha256"],
            "economic_evidence_sha256": receipt["economic_evidence_sha256"],
            "reviewed_correction_id": receipt["reviewed_correction_id"],
            "payment_date_quality_status": (
                receipt["payment_date_quality_status"]
            ),
            "source_body_sha256": source_body_sha256,
        })
    action_rows.append({**action_defaults,
        "asset_id": 1, "source": "DART_DISCLOSURE",
        "action_key": "20260302000003", "action_type": "ex_dividend",
        "announcement_date": date(2026, 3, 2),
        "ex_date": date(2026, 3, 30), "currency": "KRW",
        "status": "confirmed", "action_scope": "ISSUER",
    })
    support_action_key = "20260102009999"
    support_body_sha256 = "6" * 64
    action_rows.append({**action_defaults,
        "asset_id": 1, "source": "DART_STRUCTURED",
        "action_key": support_action_key, "action_type": "bonus_issue",
        "announcement_date": date(2026, 1, 2),
        "ex_date": date(2026, 1, 30),
        "ratio_numerator": 1.0, "ratio_denominator": 9.0,
        "expected_price_factor": 0.9,
        "currency": "KRW", "status": "confirmed",
        "report_name": "무상증자결정", "corp_cls": "Y",
        "action_scope": "ISSUER", "source_body_sha256": support_body_sha256,
    })
    action_frame = pd.DataFrame(action_rows)

    evidence_key = "cash-scale-20260102000001"
    support_row = {
        "action_snapshot_run_id": action_run,
        "evidence_key": evidence_key,
        "support_action_source": "DART_STRUCTURED",
        "support_action_key": support_action_key,
        "support_action_type": "bonus_issue",
        "target_cash_receipt_no": "20260102000001",
        "target_adjustment_date": date(2026, 1, 30),
        "support_action_body_path": "corporate_actions/dart/support.json",
        "support_action_body_sha256": support_body_sha256,
        "support_action_quality_run_id": action_run,
        "support_announcement_date": date(2026, 1, 2),
        "support_ex_date": date(2026, 1, 30),
        "support_record_date": None,
        "support_ratio_numerator": 1.0,
        "support_ratio_denominator": 9.0,
        "support_entitlement_security_class": "COMMON",
        "support_distributed_security_class": "COMMON",
        "support_expected_price_factor": 0.9,
        "support_reference_price": 90.0,
        "support_reason": "무상증자",
        "support_report_name": "무상증자결정",
        "support_action_scope": "ISSUER",
        "support_semantic_group_keys": '["bonus-common"]',
        "support_semantic_role": "ADJUSTMENT_COMPONENT",
        "manifest_support_row_sha256": "8" * 64,
    }
    support_frame = pd.DataFrame([support_row], columns=SUPPORT_ACTION_COLUMNS)
    parent_row = {
        "action_snapshot_run_id": action_run,
        "evidence_key": evidence_key,
        "asset_id": 1,
        "ticker": "005930",
        "cash_receipt_no": "20260102000001",
        "cash_source_evidence_status": "VERIFIED_OPENDART_DOCUMENT",
        "cash_action_body_path": "corporate_actions/dart/cash.json",
        "cash_action_body_sha256": cash_action_body_shas[0],
        "cash_economic_body_path": "corporate_actions/dart/document.zip",
        "cash_economic_body_schema": "OPENDART_DOCUMENT_ZIP_V1",
        "cash_economic_sha256": "1" * 64,
        "support_action_count": 1,
        "support_action_digest": support_manifest_digest(support_frame),
        "support_semantic_group_count": 1,
        "price_source": "KRX",
        "previous_price_source_object_key": "krx/2026-01-29.parquet",
        "previous_price_source_content_sha256": "9" * 64,
        "previous_price_source_etag": "a" * 32,
        "previous_price_source_schema": "marcap_parquet_v1",
        "adjustment_price_source_object_key": "krx/2026-01-30.parquet",
        "adjustment_price_source_content_sha256": "b" * 64,
        "adjustment_price_source_etag": "c" * 32,
        "adjustment_price_source_schema": "marcap_parquet_v1",
        "previous_trade_date": date(2026, 1, 29),
        "adjustment_trade_date": date(2026, 1, 30),
        "raw_previous_close": 100.0,
        "raw_applied_close": 90.0,
        "raw_reference_price": 90.0,
        "expected_price_factor": 0.9,
        "cash_scale_basis": PRE_EVENT_PRICE_SCALE,
        "manifest_row_sha256": "d" * 64,
    }
    parent_frame = pd.DataFrame([parent_row], columns=SOURCE_EVIDENCE_COLUMNS)
    scale_source = {
        "contract": SOURCE_EVIDENCE_CONTRACT,
        "manifest_sha256": "e" * 64,
        "manifest_parent_row_count": 1,
        "manifest_parent_row_digest": source_manifest_digest(parent_frame),
        "manifest_support_action_count": 1,
        "manifest_support_action_digest": support_manifest_digest(support_frame),
        "manifest_support_semantic_group_count": 1,
        "persisted_parent_row_count": 1,
        "persisted_parent_row_digest": source_evidence_digest(parent_frame),
        "persisted_support_action_count": 1,
        "persisted_support_action_digest": support_action_digest(support_frame),
        "persisted_support_semantic_group_count": 1,
        "changed_scale_coverage_count": 1,
        "unresolved_count": 0,
    }
    resolution_row = {
        "asset_id": 1,
        "source": "DART_DISCLOSURE",
        "action_key": "20260102000001",
        "resolution_version": "krx_dividend_resolution_v2",
        "applied_trade_date": date(2026, 1, 30),
        "raw_cash_amount": 100.0,
        "adjusted_cash_amount": 90.0,
        "previous_trade_date": date(2026, 1, 29),
        "previous_close": 100.0,
        "previous_adj_close": 90.0,
        "applied_close": 90.0,
        "applied_adj_close": 90.0,
        "previous_price_scale": 0.9,
        "applied_price_scale": 1.0,
        "selected_cash_scale": 0.9,
        "cash_adjustment_scale_basis": PRE_EVENT_PRICE_SCALE,
        "scale_change_detected": True,
        "scale_evidence_action_snapshot_run_id": action_run,
        "scale_evidence_key": evidence_key,
        "scale_price_factor_observed": 0.9,
        "scale_price_factor_reference": 0.9,
        "scale_price_factor_parity": True,
        "resolved_ex_date": date(2026, 1, 30),
    }
    resolution_frame = pd.DataFrame([resolution_row])
    runtime_scale = {
        "contract": RESOLUTION_EVIDENCE_CONTRACT,
        "row_count": 1,
        "row_digest": resolution_evidence_digest(
            resolution_frame[list(RESOLUTION_DIGEST_COLUMNS)]
        ),
        "applied_event_count": 1,
        "stable_scale_event_count": 0,
        "changed_scale_event_count": 1,
        "unresolved_count": 0,
        "resolution_parity_count": 1,
        "adjusted_cash_parity_count": 1,
        "first_listing_exclusion_count": 1,
        "explicit_exclusion_count": 1,
        "adj_close_decimal_places": 4,
        "cash_in_adj_close": False,
    }
    cash_action_parity = action_frame[
        action_frame["action_type"].eq("cash_dividend")
    ].rename(columns={
        "action_key": "receipt_no",
        "correction_of_action_key": "previous_receipt_no",
        "revision_root_action_key": "revision_root_receipt_no",
    })
    source_receipts = {
        "source_cash_receipt_count": 2,
        "economic_decision_count": 2,
        "attachment_correction_count": 0,
        "no_common_cash_dividend_count": 0,
        "withdrawn_or_cancelled_count": 0,
        "pending_record_date_count": 0,
        "unresolved_cash_receipt_count": 0,
        "included_cash_receipt_count": 2,
        "excluded_cash_receipt_count": 0,
        "included_cash_receipts_by_corp_cls": {"Y": 1, "E": 1},
        "excluded_cash_receipts_by_corp_cls": {},
        "cash_receipt_exclusion_reasons": {},
        "source_receipt_row_digest": source_receipt_digest(receipt_frame),
        "terminal_economic_receipt_count": 2,
        "terminal_economic_receipt_digest": (
            terminal_source_receipt_digest(receipt_frame)
        ),
    }
    published_actions = {
        "published_action_count": 4,
        "published_action_row_digest": published_action_digest(action_frame),
        "published_action_scope_contract": (
            "issuer_cash_ex_plus_manifest_scale_support_v1"
        ),
        "included_cash_action_parity_count": 2,
        "included_cash_action_parity_digest": included_cash_parity_digest(
            cash_action_parity
        ),
    }
    disclosure_observation_audit = {
        "contract": "latest_manifest_interval_mutable_list_fields_v3",
        "observation_count": 3,
        "unique_receipt_count": 2,
        "mutable_conflict_digest": "c" * 64,
    }
    metadata = {
        "contract_release": CONTRACT_RELEASE,
        "cash_action_count": 2,
        "canonical_event_count": 2,
        "applied_event_count": 1,
        "excluded_event_count": 1,
        "resolution_version": "krx_dividend_resolution_v2",
        "action_snapshot_run_id": str(action_run),
        "certified_scope": {
            "source": "KRX",
            "asset_type": "stock",
            "instrument_type": "common_stock",
            "markets": ["KOSPI", "KOSDAQ"],
            "coverage_start": "2015-01-01",
        },
        "input_scope": {
            "prices": "CERTIFIED KRX common_stock KOSPI/KOSDAQ",
            "actions": (
                "CERTIFIED issuer DART cash/ex actions plus exact referenced "
                "scale-support corporate_action rows, bound by event-date PIT "
                "identity and source-body digest"
            ),
            "cash_scale_source_evidence": (
                "append-only content-addressed cash/action and separate "
                "previous/adjustment KRX source objects; changed scale exact "
                "1:1 parent, stable scale no parent"
            ),
        },
        "action_snapshot": {
            "manifest_sha256": "a" * 64,
            "body_digest": "b" * 64,
            "body_count": 100,
            "action_count": 4,
            "coverage_start": "2015-01-01",
            "coverage_end": "2026-08-10",
            "pit_scope": pit_scope,
            "source_receipts": source_receipts,
            "published_actions": published_actions,
            "disclosure_observation_audit": disclosure_observation_audit,
            "cash_adjustment_scale_evidence": scale_source,
        },
        "asset_identity": {
            "contract": "krx_pit_ticker_asset_v3_price_scoped",
            "digest": identity_hash.hexdigest(),
            "row_count": 1,
            "asset_count": 1,
        },
        "per_row_run_parity": {
            "passed": True,
            "expected": 10,
            "actual": 10,
        },
        "research_role": {
            "role": "ex_post_realized_forward_return_label",
            "feature_pit_safe": False,
            "action_vintage": "latest_corrected_action_snapshot",
        },
        "cash_adjustment_scale_evidence": runtime_scale,
    }
    return {
        "contract": (
            "CERTIFIED",
            date(2015, 1, 2),
            date(2026, 8, 10),
            rebuild_run,
            "krx_gross_dividend_reinvested_v3",
            metadata,
            "CERTIFIED",
            "krx_total_return_rebuild",
        ),
        "scope": (
            10,
            2,
            date(2015, 1, 2),
            date(2026, 8, 10),
            10,
            0,
        ),
        "has_lineage": True,
        "has_corp_cls": True,
        "run_parity": 10,
        "dart_action_snapshot_contract": True,
        "dividend_event_resolution": True,
        "dividend_source_receipt": True,
        "cash_adjustment_scale_source_evidence": True,
        "cash_adjustment_scale_support_action": True,
        "snapshot": (
            "a" * 64,
            "b" * 64,
            100,
            date(2015, 1, 1),
            date(2026, 8, 10),
            4,
            {
                "markets": ["KOSPI", "KOSDAQ"],
                "pit_scope": pit_scope,
                "source_receipts": source_receipts,
                "published_actions": published_actions,
                "disclosure_observation_audit": (
                    disclosure_observation_audit
                ),
                "cash_adjustment_scale_evidence": scale_source,
            },
            "CERTIFIED",
            "dart_dividend_action_backfill",
            "dart_total_return_action_snapshot_v5",
        ),
        "resolution_pk": [
            "quality_run_id", "asset_id", "source", "action_key",
            "resolution_version",
        ],
        "resolution_counts": (2, 2, 2, 1, 1, 2, 2, 1, 1),
        "scale_counts": (1, 0),
        "resolution_action_mismatch_count": 0,
        "persisted_action_counts": (4, 2),
        "scale_cash_action_mismatch_count": 0,
        "scale_support_action_mismatch_count": 0,
        "source_receipt_pk": ["quality_run_id", "receipt_no"],
        "source_receipt_counts": (
            2, 2, 2, 0,
            0, 0, 0, 0,
            0, 2, 2, 0,
        ),
        "source_receipt_reason_rows": [],
        "source_receipt_class_rows": [
            ("INCLUDED", "E", 1), ("INCLUDED", "Y", 1),
        ],
        "identity_rows": identity_rows,
        "source_receipt_digest_rows": [
            tuple(row[column] for column in SOURCE_RECEIPT_DIGEST_COLUMNS)
            for row in receipt_rows
        ],
        "published_action_digest_rows": [
            tuple(row[column] for column in PUBLISHED_ACTION_DIGEST_COLUMNS)
            for row in action_rows
        ],
        "scale_parent_rows": [
            tuple(parent_row[column] for column in SOURCE_EVIDENCE_COLUMNS)
        ],
        "scale_support_rows": [
            tuple(support_row[column] for column in SUPPORT_ACTION_COLUMNS)
        ],
        "resolution_digest_rows": [
            tuple(
                resolution_row[column]
                for column in (*RESOLUTION_DIGEST_COLUMNS, "resolved_ex_date")
            )
        ],
    }


def test_read_only_audit_accepts_first_trade_after_requested_scope_start():
    report = audit(conn=_Connection(_safe_state()))

    assert report["safe_for_research"] is True
    assert report["checks"]["coverage_matches_first_trade"] is True
    assert report["checks"]["common_stock_scope"] is True


def test_read_only_audit_rejects_legacy_certification_without_lineage():
    state = _safe_state()
    state["has_lineage"] = False
    state["dart_action_snapshot_contract"] = False
    state["resolution_pk"] = [
        "asset_id", "source", "action_key", "resolution_version",
    ]

    report = audit(conn=_Connection(state))

    assert report["safe_for_research"] is False
    assert report["checks"]["separate_total_return_lineage_column"] is False
    assert report["checks"]["content_addressed_action_snapshot"] is False
    assert report["checks"]["append_only_resolution_pk"] is False


def test_audit_rejects_future_contract_bound_and_identity_digest_drift():
    state = _safe_state()
    contract = list(state["contract"])
    contract[2] = date(2027, 1, 1)
    contract[5] = dict(contract[5])
    contract[5]["asset_identity"] = dict(
        contract[5]["asset_identity"], digest="0" * 64
    )
    state["contract"] = tuple(contract)

    report = audit(conn=_Connection(state))

    assert report["safe_for_research"] is False
    assert report["checks"]["coverage_matches_latest_price"] is False
    assert report["checks"]["asset_identity_bound"] is False


@pytest.mark.parametrize(
    ("column", "corrupted"),
    [
        ("previous_receipt_no", "20250101000000"),
        ("viewer_evidence_sha256", "a" * 64),
        ("economic_evidence_sha256", "f" * 64),
    ],
)
def test_audit_digest_rejects_persisted_receipt_lineage_or_sha_drift(
    column, corrupted,
):
    state = _safe_state()
    position = SOURCE_RECEIPT_DIGEST_COLUMNS.index(column)
    row = list(state["source_receipt_digest_rows"][0])
    row[position] = corrupted
    state["source_receipt_digest_rows"][0] = tuple(row)

    report = audit(conn=_Connection(state))

    assert report["safe_for_research"] is False
    assert report["checks"]["source_receipt_exact_parity"] is False


def test_audit_requires_exact_return_to_action_snapshot_metadata_parity():
    state = _safe_state()
    contract = list(state["contract"])
    contract[5] = copy.deepcopy(contract[5])
    contract[5]["action_snapshot"]["unexpected"] = True
    state["contract"] = tuple(contract)

    report = audit(conn=_Connection(state))

    assert report["safe_for_research"] is False
    assert report["checks"]["content_addressed_action_snapshot"] is False


def test_audit_recomputes_parent_child_persisted_digest_and_counts():
    state = _safe_state()
    position = SOURCE_EVIDENCE_COLUMNS.index("raw_reference_price")
    row = list(state["scale_parent_rows"][0])
    row[position] = 80.0
    state["scale_parent_rows"][0] = tuple(row)

    report = audit(conn=_Connection(state))

    assert report["safe_for_research"] is False
    assert report["checks"]["cash_scale_source_exact_parity"] is False


def test_audit_rejects_noncanonical_support_group_semantics():
    state = _safe_state()
    position = SUPPORT_ACTION_COLUMNS.index("support_semantic_group_keys")
    row = list(state["scale_support_rows"][0])
    row[position] = '["z","a"]'
    state["scale_support_rows"][0] = tuple(row)

    report = audit(conn=_Connection(state))

    assert report["safe_for_research"] is False
    assert report["checks"]["cash_scale_source_exact_parity"] is False


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("target_cash_receipt_no", "20260102000002"),
        ("target_adjustment_date", date(2026, 1, 31)),
    ],
)
def test_audit_rejects_support_cross_parent_target_swap(field, value):
    state = _safe_state()
    position = SUPPORT_ACTION_COLUMNS.index(field)
    row = list(state["scale_support_rows"][0])
    row[position] = value
    state["scale_support_rows"][0] = tuple(row)

    report = audit(conn=_Connection(state))

    assert report["safe_for_research"] is False
    assert report["checks"]["cash_scale_source_exact_parity"] is False


def _kind_corroboration_frames(**changes):
    state = _safe_state()
    parent = dict(zip(
        SOURCE_EVIDENCE_COLUMNS, state["scale_parent_rows"][0], strict=True,
    ))
    original_support = dict(zip(
        SUPPORT_ACTION_COLUMNS, state["scale_support_rows"][0], strict=True,
    ))
    parent.update({
        "asset_id": 6800,
        "ticker": "006800",
        "cash_receipt_no": "20260316800587",
        "previous_trade_date": date(2026, 3, 13),
        "adjustment_trade_date": date(2026, 3, 16),
        "raw_previous_close": 69_500.0,
        "raw_applied_close": 70_900.0,
        "raw_reference_price": 69_200.0,
        "expected_price_factor": 69_200.0 / 69_500.0,
    })
    action_type = str(changes.pop("base_action_type", "ex_dividend"))
    reason = {
        "ex_dividend": "주식배당",
        "rights_detachment": "무상증자",
        "combined_detachment": "무상증자 및 주식배당",
    }[action_type]
    group = "stock-common" if action_type == "ex_dividend" else "bonus-common"
    component = {
        **original_support,
        "target_cash_receipt_no": "20260316800587",
        "target_adjustment_date": date(2026, 3, 16),
        "support_semantic_group_keys": json.dumps([group], separators=(",", ":")),
    }
    if action_type == "ex_dividend":
        component.update({
            "support_action_source": "DART_DISCLOSURE",
            "support_action_key": "20260313800897",
            "support_action_type": "stock_dividend",
            "support_action_body_path": (
                "corporate_actions/dart/documents/year=2026/corp=006800/"
                "rcept=20260313800897.zip"
            ),
            "support_action_body_sha256": "6" * 64,
            "support_announcement_date": date(2026, 3, 13),
            "support_ex_date": None,
            "support_record_date": date(2026, 3, 17),
            "support_ratio_numerator": 0.0073206,
            "support_ratio_denominator": 1.0,
            "support_expected_price_factor": None,
            "support_reference_price": None,
            "support_reason": None,
            "support_report_name": "[기재정정]주식배당결정",
        })
    support = {
        **original_support,
        "support_action_source": "KRX_KIND",
        "support_action_key": "20260313001262",
        "support_action_type": action_type,
        "target_cash_receipt_no": "20260316800587",
        "target_adjustment_date": date(2026, 3, 16),
        "support_action_body_path": (
            "corporate_actions/krx/kind/reference/body/"
            "6d/6d24251bbabc1ca2.html"
        ),
        "support_action_body_sha256": (
            "6d24251bbabc1ca2b7f6dba7639d6b448e9a7df6a1ac2ebed44f7139578e6d02"
        ),
        "support_announcement_date": date(2026, 3, 13),
        "support_ex_date": date(2026, 3, 16),
        "support_record_date": None,
        "support_ratio_numerator": None,
        "support_ratio_denominator": None,
        "support_entitlement_security_class": "COMMON",
        "support_distributed_security_class": None,
        "support_expected_price_factor": None,
        "support_reference_price": 69_200.0,
        "support_reason": reason,
        "support_report_name": KIND_REFERENCE_REPORT_NAME_99311,
        "support_semantic_group_keys": json.dumps([group], separators=(",", ":")),
        "support_semantic_role": "CORROBORATION",
    }
    support.update(changes)
    support_frame = pd.DataFrame(
        [component, support], columns=SUPPORT_ACTION_COLUMNS,
    )
    parent["support_action_count"] = 2
    parent["support_semantic_group_count"] = 1
    parent["support_action_digest"] = support_manifest_digest(support_frame)
    parent_frame = pd.DataFrame([parent], columns=SOURCE_EVIDENCE_COLUMNS)
    return parent_frame, support_frame


def _viewer_bonus_frames(**changes):
    state = _safe_state()
    parent = dict(zip(
        SOURCE_EVIDENCE_COLUMNS, state["scale_parent_rows"][0], strict=True,
    ))
    support = dict(zip(
        SUPPORT_ACTION_COLUMNS, state["scale_support_rows"][0], strict=True,
    ))
    body_sha = "6" * 64
    group = "005930|2026-01-30|BONUS_ISSUE|0.111111111111"
    support.update({
        "support_action_source": "DART_VIEWER",
        "support_action_body_path": (
            "corporate_actions/dart/support_action_families/objects/"
            f"sha256={body_sha}.html"
        ),
        "support_action_body_sha256": body_sha,
        "support_report_name": "주요사항보고서(무상증자결정)",
        "support_record_date": None,
        "support_semantic_group_keys": json.dumps(
            [group], separators=(",", ":"),
        ),
    })
    support.update(changes)
    support_frame = pd.DataFrame([support], columns=SUPPORT_ACTION_COLUMNS)
    parent["support_action_digest"] = support_manifest_digest(support_frame)
    parent_frame = pd.DataFrame([parent], columns=SOURCE_EVIDENCE_COLUMNS)
    return parent_frame, support_frame


def _viewer_stock_dividend_frames(**changes):
    parents, supports = _viewer_bonus_frames()
    group = "032960|2015-12-31|STOCK_DIVIDEND|0.05"
    support = supports.iloc[0].to_dict()
    support.update({
        "support_action_key": "20151228900387",
        "support_action_type": "stock_dividend",
        "support_report_name": "[기재정정]주식배당결정",
        "support_ex_date": None,
        "support_record_date": date(2015, 12, 31),
        "support_ratio_numerator": 0.05,
        "support_ratio_denominator": 1.0,
        "support_expected_price_factor": None,
        "support_semantic_group_keys": json.dumps(
            [group], separators=(",", ":"),
        ),
    })
    support.update(changes)
    support_frame = pd.DataFrame([support], columns=SUPPORT_ACTION_COLUMNS)
    parent = parents.iloc[0].to_dict()
    parent["ticker"] = "032960"
    parent["support_action_digest"] = support_manifest_digest(support_frame)
    return (
        pd.DataFrame([parent], columns=SOURCE_EVIDENCE_COLUMNS),
        support_frame,
    )


def test_scale_source_audit_accepts_exact_viewer_bonus_component():
    parents, supports = _viewer_bonus_frames()

    valid, group_count = _validate_scale_source_rows(parents, supports)

    assert valid is True
    assert group_count == 1


def test_scale_source_audit_accepts_exact_viewer_stock_dividend_component():
    parents, supports = _viewer_stock_dividend_frames()

    valid, group_count = _validate_scale_source_rows(parents, supports)

    assert valid is True
    assert group_count == 1


@pytest.mark.parametrize(
    "changes",
    [
        {"support_semantic_role": "CORROBORATION"},
        {"support_distributed_security_class": "PREFERRED"},
        {"support_expected_price_factor": 0.8},
        {"support_report_name": "주식배당"},
        {"support_ex_date": date(2026, 1, 30)},
        {"support_record_date": date(2026, 2, 1)},
        {"support_action_body_path": "corporate_actions/dart/viewer.html"},
        {"support_action_key": "not-a-receipt"},
        {
            "support_semantic_group_keys": (
                '["005930|2026-01-31|STOCK_DIVIDEND|0.1"]'
            ),
        },
    ],
)
def test_scale_source_audit_rejects_viewer_stock_dividend_drift(changes):
    parents, supports = _viewer_stock_dividend_frames(**changes)

    valid, _ = _validate_scale_source_rows(parents, supports)

    assert valid is False


@pytest.mark.parametrize(
    "changes",
    [
        {"support_semantic_role": "CORROBORATION"},
        {"support_distributed_security_class": "PREFERRED"},
        {"support_expected_price_factor": 0.8},
        {"support_report_name": "무상증자결정"},
        {"support_record_date": date(2026, 1, 31)},
        {"support_action_body_path": "corporate_actions/dart/viewer.html"},
        {"support_action_key": "not-a-receipt"},
    ],
)
def test_scale_source_audit_rejects_viewer_bonus_semantic_drift(changes):
    parents, supports = _viewer_bonus_frames(**changes)

    valid, _ = _validate_scale_source_rows(parents, supports)

    assert valid is False


def test_scale_source_audit_rejects_viewer_bonus_effective_date_drift():
    parents, supports = _viewer_bonus_frames(
        support_ex_date=date(2026, 1, 31),
    )

    valid, _ = _validate_scale_source_rows(parents, supports)

    assert valid is False


@pytest.mark.parametrize(
    "group",
    [
        "000660|2026-01-30|BONUS_ISSUE|0.111111111111",
        "005930|2026-01-31|BONUS_ISSUE|0.111111111111",
        "005930|2026-01-30|BONUS_ISSUE|0.11",
        "bonus-common",
    ],
)
def test_scale_source_audit_rejects_viewer_bonus_group_drift(group):
    parents, supports = _viewer_bonus_frames(
        support_semantic_group_keys=json.dumps(
            [group], separators=(",", ":"),
        ),
    )

    valid, _ = _validate_scale_source_rows(parents, supports)

    assert valid is False


@pytest.mark.parametrize(
    ("action_type", "security_class", "report_name"),
    [
        ("ex_dividend", "COMMON", KIND_REFERENCE_REPORT_NAME_99311),
        ("ex_dividend", "PREFERRED", KIND_REFERENCE_REPORT_NAME_99311),
        ("ex_dividend", "COMMON", KIND_REFERENCE_REPORT_NAME_70767),
        ("rights_detachment", "COMMON", KIND_REFERENCE_REPORT_NAME_99311),
        ("combined_detachment", "COMMON", KIND_REFERENCE_REPORT_NAME_99311),
    ],
)
def test_scale_source_audit_accepts_persisted_kind_corroboration(
    action_type, security_class, report_name,
):
    parents, supports = _kind_corroboration_frames(
        base_action_type=action_type,
        support_entitlement_security_class=security_class,
        support_report_name=report_name,
    )

    valid, group_count = _validate_scale_source_rows(parents, supports)

    assert valid is True
    assert group_count == 1


@pytest.mark.parametrize(
    "changes",
    [
        {"support_entitlement_security_class": "COMMON_AND_PREFERRED"},
        {"support_distributed_security_class": "COMMON"},
        {"support_reference_price": 69_100.0},
        {"support_reason": "현금배당"},
        {"support_report_name": "배당락 기준가격 공지"},
        {"support_action_type": "stock_dividend"},
        {"support_ex_date": date(2026, 3, 17)},
        {"support_ratio_numerator": 0.1, "support_ratio_denominator": 1.0},
        {"target_cash_receipt_no": "20260316800588"},
        {"target_adjustment_date": date(2026, 3, 17)},
    ],
)
def test_scale_source_audit_rejects_kind_class_reference_or_target_tamper(
    changes,
):
    parents, supports = _kind_corroboration_frames(**changes)

    valid, _ = _validate_scale_source_rows(parents, supports)

    assert valid is False


@pytest.mark.parametrize(
    "counter",
    ["scale_cash_action_mismatch_count", "scale_support_action_mismatch_count"],
)
def test_audit_rejects_same_run_cash_or_support_body_drift(counter):
    state = _safe_state()
    state[counter] = 1

    report = audit(conn=_Connection(state))

    assert report["safe_for_research"] is False
    assert report["checks"]["cash_scale_source_exact_parity"] is False


def _replace_resolution_row(state, **changes):
    columns = (*RESOLUTION_DIGEST_COLUMNS, "resolved_ex_date")
    row = dict(zip(columns, state["resolution_digest_rows"][0], strict=True))
    row.update(changes)
    state["resolution_digest_rows"][0] = tuple(row[column] for column in columns)
    return row


def _replace_runtime_metadata(state, resolution_row, **changes):
    contract = list(state["contract"])
    contract[5] = copy.deepcopy(contract[5])
    runtime = contract[5]["cash_adjustment_scale_evidence"]
    digest_frame = pd.DataFrame([resolution_row])
    runtime["row_digest"] = resolution_evidence_digest(
        digest_frame[list(RESOLUTION_DIGEST_COLUMNS)]
    )
    runtime.update(changes)
    state["contract"] = tuple(contract)


def _stable_scale_state_without_external_evidence():
    state = copy.deepcopy(_safe_state())
    state["scale_parent_rows"] = []
    state["scale_support_rows"] = []
    empty_parents = pd.DataFrame(columns=SOURCE_EVIDENCE_COLUMNS)
    empty_supports = pd.DataFrame(columns=SUPPORT_ACTION_COLUMNS)
    empty_source = {
        "contract": SOURCE_EVIDENCE_CONTRACT,
        "manifest_sha256": hashlib.sha256(b"").hexdigest(),
        "manifest_parent_row_count": 0,
        "manifest_parent_row_digest": source_manifest_digest(empty_parents),
        "manifest_support_action_count": 0,
        "manifest_support_action_digest": support_manifest_digest(
            empty_supports
        ),
        "manifest_support_semantic_group_count": 0,
        "persisted_parent_row_count": 0,
        "persisted_parent_row_digest": source_evidence_digest(empty_parents),
        "persisted_support_action_count": 0,
        "persisted_support_action_digest": support_action_digest(empty_supports),
        "persisted_support_semantic_group_count": 0,
        "changed_scale_coverage_count": 0,
        "unresolved_count": 0,
    }
    state["published_action_digest_rows"] = state[
        "published_action_digest_rows"
    ][:-1]
    action_frame = pd.DataFrame(
        state["published_action_digest_rows"],
        columns=PUBLISHED_ACTION_DIGEST_COLUMNS,
    )
    published = {
        "published_action_count": 3,
        "published_action_row_digest": published_action_digest(action_frame),
        "published_action_scope_contract": (
            "issuer_cash_ex_plus_manifest_scale_support_v1"
        ),
        "included_cash_action_parity_count": 2,
        "included_cash_action_parity_digest": (
            state["contract"][5]["action_snapshot"]["published_actions"]
            ["included_cash_action_parity_digest"]
        ),
    }
    stable_row = _replace_resolution_row(
        state,
        previous_adj_close=100.0,
        previous_price_scale=1.0,
        selected_cash_scale=1.0,
        adjusted_cash_amount=100.0,
        cash_adjustment_scale_basis="STABLE_PRICE_SCALE",
        scale_change_detected=False,
        scale_evidence_action_snapshot_run_id=None,
        scale_evidence_key=None,
        scale_price_factor_observed=1.0,
        scale_price_factor_reference=1.0,
    )
    _replace_runtime_metadata(
        state, stable_row,
        stable_scale_event_count=1,
        changed_scale_event_count=0,
    )
    contract = list(state["contract"])
    contract[5] = copy.deepcopy(contract[5])
    action_metadata = contract[5]["action_snapshot"]
    action_metadata["action_count"] = 3
    action_metadata["published_actions"] = published
    action_metadata["cash_adjustment_scale_evidence"] = empty_source
    action_metadata["pit_scope"].update({
        "input_action_count": 5,
        "included_action_count": 3,
        "included_by_corp_cls": {"Y": 2, "E": 1},
    })
    state["contract"] = tuple(contract)
    snapshot = list(state["snapshot"])
    snapshot[5] = 3
    snapshot[6] = copy.deepcopy(snapshot[6])
    snapshot[6]["published_actions"] = published
    snapshot[6]["cash_adjustment_scale_evidence"] = empty_source
    snapshot[6]["pit_scope"].update({
        "input_action_count": 5,
        "included_action_count": 3,
        "included_by_corp_cls": {"Y": 2, "E": 1},
    })
    state["snapshot"] = tuple(snapshot)
    state["persisted_action_counts"] = (3, 2)
    return state


def test_audit_runtime_digest_includes_raw_and_adjusted_cash():
    state = _safe_state()
    row = _replace_resolution_row(
        state, raw_cash_amount=110.0, adjusted_cash_amount=99.0,
    )
    # Keep the formula valid and publish a matching count contract, but retain
    # the original digest.  The independent recomputation must still reject it.
    assert row["adjusted_cash_amount"] == (
        row["raw_cash_amount"] * row["selected_cash_scale"]
    )

    report = audit(conn=_Connection(state))

    assert report["safe_for_research"] is False
    assert report["checks"]["cash_adjustment_scale_verified"] is False


def test_audit_rejects_reference_factor_outside_exact_price_interval():
    state = _safe_state()
    row = _replace_resolution_row(
        state, scale_price_factor_reference=0.8,
    )
    _replace_runtime_metadata(state, row)

    report = audit(conn=_Connection(state))

    assert report["safe_for_research"] is False
    assert report["checks"]["cash_adjustment_scale_verified"] is False


def test_audit_rejects_stable_scale_row_that_consumes_evidence():
    state = _safe_state()
    row = _replace_resolution_row(
        state,
        previous_adj_close=100.0,
        previous_price_scale=1.0,
        selected_cash_scale=1.0,
        adjusted_cash_amount=100.0,
        cash_adjustment_scale_basis="STABLE_PRICE_SCALE",
        scale_change_detected=False,
        scale_price_factor_observed=1.0,
        scale_price_factor_reference=1.0,
        # Deliberately keep scale_evidence_* populated.
    )
    _replace_runtime_metadata(
        state, row, stable_scale_event_count=1, changed_scale_event_count=0,
    )

    report = audit(conn=_Connection(state))

    assert report["safe_for_research"] is False
    assert report["checks"]["cash_adjustment_scale_verified"] is False


def test_audit_accepts_stable_scale_only_when_source_evidence_is_empty():
    report = audit(conn=_Connection(
        _stable_scale_state_without_external_evidence()
    ))

    assert report["safe_for_research"] is True
    assert report["checks"]["cash_scale_source_exact_parity"] is True
    assert report["checks"]["cash_adjustment_scale_verified"] is True


def test_audit_rejects_changed_scale_without_exact_parent_and_unused_parent():
    state = _safe_state()
    row = _replace_resolution_row(state, scale_evidence_key="missing-evidence")
    _replace_runtime_metadata(state, row)

    report = audit(conn=_Connection(state))

    assert report["safe_for_research"] is False
    assert report["checks"]["cash_adjustment_scale_verified"] is False


def test_audit_binds_explicit_first_listing_exclusion_count():
    state = _safe_state()
    contract = list(state["contract"])
    contract[5] = copy.deepcopy(contract[5])
    contract[5]["cash_adjustment_scale_evidence"][
        "first_listing_exclusion_count"
    ] = 0
    state["contract"] = tuple(contract)

    report = audit(conn=_Connection(state))

    assert report["safe_for_research"] is False
    assert report["checks"]["cash_adjustment_scale_verified"] is False
