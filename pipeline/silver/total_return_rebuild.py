"""Bounded, fail-closed KRX gross-total-return rebuild preview.

The standalone command is read-only.  Direct ``--apply`` is disabled because
it cannot close the complete DART publish, rebuild, and independent-audit
transaction.  Production writes are available only through the closed
``pipeline.daily_full`` or ``pipeline.dart_silver_backfill_ecs`` orchestrator.

Examples
--------
Preview without persistent writes::

    uv run python -m pipeline.silver.total_return_rebuild

Preview local complete Bronze actions against certified RDS prices without
publishing actions, DQ state, or the return contract::

    uv run python -m pipeline.silver.total_return_rebuild \
        --actions-base /complete/dart/snapshot

Do not compose standalone ``dart_extra_load --apply`` and
``total_return_rebuild --apply`` commands.  The closed orchestrator supplies
one verified action snapshot and performs the required DART re-upsert, return
rebuild, contract promotion, and audit under its certification lock.
``--actions-base`` remains preview-only and never performs that upsert.
"""
from __future__ import annotations

import argparse
import json
import re
from dataclasses import asdict, dataclass
from datetime import date
from decimal import Decimal, ROUND_HALF_UP
from itertools import islice
from typing import Iterable, Iterator, Sequence
from uuid import UUID

import numpy as np
import pandas as pd

from pipeline.common import db
from pipeline.silver import corporate_actions
from pipeline.silver.dart_action_snapshot import (
    DEFAULT_COVERAGE_START,
    verify_snapshot_manifest,
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
from pipeline.silver.cash_adjustment_scale_evidence import (
    RESOLUTION_DIGEST_COLUMNS,
    RESOLUTION_EVIDENCE_CONTRACT,
    SOURCE_EVIDENCE_COLUMNS,
    SOURCE_EVIDENCE_CONTRACT,
    SUPPORT_ACTION_COLUMNS,
    bind_source_evidence,
    resolution_evidence_digest,
    source_evidence_metadata,
    source_evidence_digest,
    support_action_digest,
    support_manifest_digest,
    _support_group_count,
    verify_source_evidence_manifest,
)
from pipeline.silver.return_contract import (
    CONTRACT_RELEASE,
    KRX_TICKER_REGEX,
    acquire_return_rebuild_lock,
    release_return_rebuild_lock,
)
from pipeline.silver.return_identity import (
    ASSET_IDENTITY_CONTRACT,
    CERTIFIED_MARKETS,
    krx_common_stock_identity_digest,
    map_actions_to_pit_assets,
)
from pipeline.silver.total_returns import (
    apply_dividends_to_prices,
    classify_cash_dividend_revisions,
    resolve_dividend_ex_dates,
)
from pipeline.silver_quality import repository
from pipeline.silver_quality.models import (
    CheckResult,
    CheckStatus,
    Severity,
)


METHODOLOGY_VERSION = "krx_gross_dividend_reinvested_v3"
RESOLUTION_VERSION = "krx_dividend_resolution_v2"
DIVIDEND_TREATMENT = "gross_cash_dividend_reinvested_on_ex_date"
CONTRACT_COVERAGE_START = DEFAULT_COVERAGE_START

_PRICE_STAGE = "_stg_krx_total_return_rebuild"
_AUDIT_STAGE = "_stg_dividend_event_resolution"

_BLOCKING_APPLICATION_STATUSES = frozenset({
    "unresolved_ex_date",
    "no_price_series",
    "invalid_cash_amount",
})
_EXPLICIT_EXCLUSIONS = {
    "before_market_coverage": "BEFORE_MARKET_COVERAGE",
    "pending_future_trade": "PENDING_FUTURE_TRADE",
    "before_listing_or_episode_start": "BEFORE_LISTING_OR_EPISODE_START",
    "listing_episode_gap": "LISTING_EPISODE_GAP",
}

_AUDIT_COLUMNS = [
    "quality_run_id",
    "asset_id",
    "source",
    "action_key",
    "resolution_version",
    "is_canonical",
    "excluded_reason",
    "resolved_ex_date",
    "ex_date_basis",
    "applied_trade_date",
    "raw_cash_amount",
    "adjusted_cash_amount",
    "source_announcement_date",
    "revision_group_key",
    "source_evidence_status",
    "cash_amount_status",
    "correction_of_action_key",
    "revision_root_action_key",
    "revision_kind",
    "viewer_evidence_sha256",
    "economic_evidence_sha256",
    "reviewed_correction_id",
    "payment_date_quality_status",
    "previous_trade_date",
    "previous_close",
    "previous_adj_close",
    "applied_close",
    "applied_adj_close",
    "previous_price_scale",
    "applied_price_scale",
    "selected_cash_scale",
    "cash_adjustment_scale_basis",
    "scale_change_detected",
    "scale_evidence_action_snapshot_run_id",
    "scale_evidence_key",
    "scale_price_factor_observed",
    "scale_price_factor_reference",
    "scale_price_factor_parity",
]


@dataclass
class RebuildSummary:
    """Compact audit summary returned by dry-run and apply modes."""

    apply: bool
    asset_count: int = 0
    price_row_count: int = 0
    cash_action_count: int = 0
    canonical_event_count: int = 0
    applied_event_count: int = 0
    excluded_event_count: int = 0
    coverage_start: str | None = None
    coverage_end: str | None = None
    run_id: str | None = None
    action_snapshot_run_id: str | None = None
    action_snapshot_manifest_sha256: str | None = None
    action_snapshot_digest: str | None = None
    action_snapshot_body_count: int = 0
    action_snapshot_action_count: int = 0
    action_snapshot_coverage_start: str | None = None
    action_snapshot_coverage_end: str | None = None
    action_snapshot_input_action_count: int = 0
    action_snapshot_excluded_action_count: int = 0
    action_snapshot_included_corp_cls_counts: dict[str, int] | None = None
    action_snapshot_excluded_corp_cls_counts: dict[str, int] | None = None
    action_snapshot_excluded_reason_counts: dict[str, int] | None = None
    action_snapshot_source_receipts: dict | None = None
    action_snapshot_published_actions: dict | None = None
    action_snapshot_disclosure_observation_audit: dict | None = None
    action_source: str = "rds_certified_snapshot"
    local_actions_base: str | None = None
    local_actions_fingerprint: str | None = None
    unmapped_action_count: int = 0
    out_of_scope_action_count: int = 0
    asset_identity_contract: str = ASSET_IDENTITY_CONTRACT
    asset_identity_digest: str | None = None
    asset_identity_row_count: int = 0
    asset_identity_asset_count: int = 0
    source_price_coverage_start: str | None = None
    source_price_coverage_end: str | None = None
    stable_scale_event_count: int = 0
    changed_scale_event_count: int = 0
    resolution_parity_count: int = 0
    action_snapshot_cash_scale_evidence: dict | None = None

    def absorb(self, batch: "BatchRebuild") -> None:
        self.asset_count += int(batch.prices["asset_id"].nunique())
        self.price_row_count += len(batch.prices)
        self.cash_action_count += len(batch.audit)
        self.canonical_event_count += batch.canonical_event_count
        self.applied_event_count += batch.applied_event_count
        self.excluded_event_count += batch.excluded_event_count
        self.stable_scale_event_count += batch.stable_scale_event_count
        self.changed_scale_event_count += batch.changed_scale_event_count
        self.resolution_parity_count += batch.resolution_parity_count
        if not batch.prices.empty:
            start = batch.prices["trade_date"].min().date().isoformat()
            end = batch.prices["trade_date"].max().date().isoformat()
            self.coverage_start = min(
                value for value in (self.coverage_start, start) if value
            )
            self.coverage_end = max(
                value for value in (self.coverage_end, end) if value
            )


@dataclass
class BatchRebuild:
    prices: pd.DataFrame
    audit: pd.DataFrame
    canonical_event_count: int
    applied_event_count: int
    excluded_event_count: int
    stable_scale_event_count: int = 0
    changed_scale_event_count: int = 0
    resolution_parity_count: int = 0


@dataclass(frozen=True)
class LocalActionSnapshot:
    actions: pd.DataFrame
    base: str
    fingerprint: str
    unmapped_count: int
    out_of_scope_count: int = 0
    manifest_sha256: str | None = None
    body_count: int = 0
    coverage_start: str | None = None
    coverage_end: str | None = None
    input_action_count: int = 0
    excluded_action_count: int = 0
    included_corp_cls_counts: dict[str, int] | None = None
    excluded_corp_cls_counts: dict[str, int] | None = None
    excluded_reason_counts: dict[str, int] | None = None
    scale_source_evidence: pd.DataFrame | None = None
    scale_support_actions: pd.DataFrame | None = None
    cash_scale_evidence: dict | None = None


@dataclass(frozen=True)
class CertifiedActionSnapshot:
    run_id: UUID
    manifest_sha256: str
    body_digest: str
    body_count: int
    coverage_start: date
    coverage_end: date
    action_count: int
    input_action_count: int
    excluded_action_count: int
    included_corp_cls_counts: dict[str, int]
    excluded_corp_cls_counts: dict[str, int]
    excluded_reason_counts: dict[str, int]
    source_receipts: dict
    published_actions: dict
    disclosure_observation_audit: dict
    scale_source_evidence: pd.DataFrame
    scale_support_actions: pd.DataFrame
    cash_scale_evidence: dict


def _chunks(values: Iterable[int], size: int) -> Iterator[list[int]]:
    if size <= 0:
        raise ValueError("batch_size must be positive")
    iterator = iter(values)
    while chunk := list(islice(iterator, size)):
        yield chunk


def _assert_contract_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT to_regclass('public.dividend_event_resolution'),
                   to_regclass('public.price_return_contract'),
                   to_regclass('public.dart_action_snapshot_contract'),
                   to_regclass('public.dividend_source_receipt'),
                   to_regclass(
                       'public.cash_adjustment_scale_source_evidence'
                   ),
                   to_regclass(
                       'public.cash_adjustment_scale_support_action'
                   )
            """
        )
        row = cur.fetchone()
    if not row or any(value is None for value in row):
        raise RuntimeError(
            "KRX total-return schema가 없습니다. "
            "migrations 009/010을 먼저 적용하세요."
        )
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT count(*)
            FROM information_schema.columns
            WHERE table_schema='public'
              AND table_name='price_daily'
              AND column_name IN (
                  'total_return_quality_run_id','total_return_loaded_at'
              )
            """
        )
        column_count = int(cur.fetchone()[0])
    if column_count != 2:
        raise RuntimeError("price_daily total-return lineage columns are missing")
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT count(*)
            FROM information_schema.columns
            WHERE table_schema='public'
              AND table_name='dividend_event_resolution'
              AND column_name = ANY(%s)
            """,
            (list(RESOLUTION_DIGEST_COLUMNS[4:]),),
        )
        resolution_column_count = int(cur.fetchone()[0])
    if resolution_column_count != len(RESOLUTION_DIGEST_COLUMNS[4:]):
        raise RuntimeError("dividend resolution-v2 scale columns are missing")
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT count(*)
            FROM information_schema.columns
            WHERE table_schema='public'
              AND table_name='corporate_action'
              AND column_name='corp_cls'
            """
        )
        corp_cls_column_count = int(cur.fetchone()[0])
    if corp_cls_column_count != 1:
        raise RuntimeError("corporate_action corp_cls evidence column is missing")
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT pg_get_constraintdef(oid)
            FROM pg_constraint
            WHERE conrelid='public.dividend_source_receipt'::regclass
              AND conname='dividend_source_receipt_ticker_check'
              AND contype='c'
            """
        )
        ticker_constraint = cur.fetchone()
    if not ticker_constraint or KRX_TICKER_REGEX not in str(
        ticker_constraint[0]
    ):
        raise RuntimeError(
            "dividend source receipt requires the six-character "
            "uppercase alphanumeric KRX ticker contract"
        )


def _source_receipt_contract_frame(conn, run_id: UUID) -> pd.DataFrame:
    columns = list(SOURCE_RECEIPT_DIGEST_COLUMNS)
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT {','.join(columns)} FROM dividend_source_receipt "
            "WHERE quality_run_id=%s ORDER BY receipt_no",
            (run_id,),
        )
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=columns)


def _published_action_contract_frame(conn, run_id: UUID) -> pd.DataFrame:
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
    conn, run_id: UUID,
) -> tuple[pd.DataFrame, pd.DataFrame]:
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
    parents = pd.DataFrame(parent_rows, columns=SOURCE_EVIDENCE_COLUMNS)
    supports = pd.DataFrame(support_rows, columns=SUPPORT_ACTION_COLUMNS)
    if not parents.empty:
        if parents["evidence_key"].duplicated().any():
            raise RuntimeError("persisted cash-scale parents are duplicated")
        if supports.empty:
            raise RuntimeError("persisted cash-scale support actions are empty")
        for parent in parents.to_dict("records"):
            child = supports[supports["evidence_key"].eq(parent["evidence_key"])]
            if (
                len(child) != int(parent["support_action_count"])
                or support_manifest_digest(child)
                != parent["support_action_digest"]
                or _support_group_count(child)
                != int(parent["support_semantic_group_count"])
            ):
                raise RuntimeError(
                    "persisted cash-scale parent/child digest parity failed"
                )
    elif not supports.empty:
        raise RuntimeError("orphan persisted cash-scale support actions")
    return parents, supports


def _action_cash_parity_frame(actions: pd.DataFrame) -> pd.DataFrame:
    return actions[actions["action_type"].eq("cash_dividend")].rename(
        columns={
            "action_key": "receipt_no",
            "correction_of_action_key": "previous_receipt_no",
            "revision_root_action_key": "revision_root_receipt_no",
        }
    )[list(INCLUDED_CASH_PARITY_COLUMNS)].copy()


def _certified_asset_ids(conn) -> list[int]:
    """Return common shares with certified prices inside the 2015+ contract."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT p.asset_id
            FROM price_daily p
            JOIN asset a ON a.asset_id=p.asset_id
            JOIN dq_run q ON q.run_id=p.quality_run_id
            WHERE p.source='KRX'
              AND a.asset_type='stock'
              AND a.instrument_type='common_stock'
              AND a.exchange='KRX'
              AND p.market IN ('KOSPI','KOSDAQ')
              AND q.status='CERTIFIED'
              AND p.trade_date >= %s
            ORDER BY p.asset_id
            """,
            (CONTRACT_COVERAGE_START,),
        )
        return [int(row[0]) for row in cur.fetchall()]


def _certified_action_snapshot_run(
    conn,
    *,
    required_end: date,
) -> CertifiedActionSnapshot:
    """Resolve the latest complete, certified DART snapshot re-upsert."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT q.run_id, s.manifest_sha256, s.body_digest, s.body_count,
                   s.coverage_start, s.coverage_end, s.action_count, s.metadata
            FROM dq_run q
            JOIN dart_action_snapshot_contract s
              ON s.quality_run_id=q.run_id
            WHERE q.mode='dart_dividend_action_backfill'
              AND q.status='CERTIFIED'
              AND s.coverage_start=%s
              AND s.coverage_end >= %s
            ORDER BY q.finished_at DESC NULLS LAST,
                     q.started_at DESC, q.run_id DESC
            LIMIT 1
            """,
            (CONTRACT_COVERAGE_START, required_end),
        )
        snapshot = cur.fetchone()
    if not snapshot:
        raise RuntimeError(
            "certified dart_dividend_action_backfill이 없습니다. complete "
            "snapshot을 pipeline.silver.dart_extra_load로 재업서트하세요."
        )
    snapshot_metadata = snapshot[7] or {}
    if isinstance(snapshot_metadata, str):
        snapshot_metadata = json.loads(snapshot_metadata)
    markets = snapshot_metadata.get("markets")
    if markets != list(CERTIFIED_MARKETS):
        raise RuntimeError(
            "certified DART action snapshot market scope mismatch: "
            f"expected={list(CERTIFIED_MARKETS)} actual={markets}"
        )
    disclosure_observation_audit = snapshot_metadata.get(
        "disclosure_observation_audit"
    ) or {}
    if (
        not isinstance(disclosure_observation_audit, dict)
        or disclosure_observation_audit.get("contract")
        != "latest_manifest_interval_mutable_list_fields_v3"
        or re.fullmatch(
            r"[0-9a-f]{64}",
            str(disclosure_observation_audit.get(
                "mutable_conflict_digest", "",
            )),
        ) is None
        or int(disclosure_observation_audit.get(
            "observation_count", -1,
        )) < int(disclosure_observation_audit.get(
            "unique_receipt_count", 0,
        ))
    ):
        raise RuntimeError(
            "certified DART snapshot disclosure observation audit invalid"
        )
    pit_scope = snapshot_metadata.get("pit_scope")
    required_pit_fields = {
        "contract", "input_action_count", "included_action_count",
        "excluded_action_count", "included_by_corp_cls",
        "excluded_by_corp_cls", "excluded_by_reason",
    }
    if not isinstance(pit_scope, dict) or not required_pit_fields.issubset(
        pit_scope
    ):
        raise RuntimeError(
            "certified DART action snapshot lacks PIT scope evidence"
        )
    if pit_scope["contract"] != (
        "event_date_identity_common_stock_certified_kospi_kosdaq_price_episode"
    ):
        raise RuntimeError("certified DART action snapshot PIT contract mismatch")
    input_action_count = int(pit_scope["input_action_count"])
    included_action_count = int(pit_scope["included_action_count"])
    excluded_action_count = int(pit_scope["excluded_action_count"])
    included_classes = pit_scope.get("included_by_corp_cls") or {}
    excluded_classes = pit_scope.get("excluded_by_corp_cls") or {}
    excluded_reasons = pit_scope.get("excluded_by_reason") or {}
    if not all(isinstance(value, dict) for value in (
        included_classes, excluded_classes, excluded_reasons,
    )):
        raise RuntimeError("certified DART action snapshot PIT counts invalid")
    if (
        min(input_action_count, included_action_count, excluded_action_count) < 0
        or included_action_count != int(snapshot[6])
        or input_action_count != included_action_count + excluded_action_count
        or sum(int(value) for value in included_classes.values())
        != included_action_count
        or sum(int(value) for value in excluded_classes.values())
        != excluded_action_count
        or sum(int(value) for value in excluded_reasons.values())
        != excluded_action_count
    ):
        raise RuntimeError(
            "certified DART action snapshot PIT partition parity failed"
        )
    source_receipts = snapshot_metadata.get("source_receipts") or {}
    source_total = int(source_receipts.get("source_cash_receipt_count", -1))
    source_included = int(source_receipts.get(
        "included_cash_receipt_count", -1,
    ))
    source_excluded = int(source_receipts.get(
        "excluded_cash_receipt_count", -1,
    ))
    source_reasons = source_receipts.get(
        "cash_receipt_exclusion_reasons",
    ) or {}
    published_actions = snapshot_metadata.get("published_actions") or {}
    cash_scale_evidence = snapshot_metadata.get(
        "cash_adjustment_scale_evidence"
    ) or {}
    digest_pattern = re.compile(r"^[0-9a-f]{64}$")
    if (
        not isinstance(source_reasons, dict)
        or not isinstance(published_actions, dict)
        or not isinstance(cash_scale_evidence, dict)
        or source_total < 1
        or source_total != source_included + source_excluded
        or source_included > included_action_count
        or sum(int(value) for value in source_reasons.values())
        != source_excluded
        or int(source_receipts.get("unresolved_cash_receipt_count", -1)) != 0
        or digest_pattern.fullmatch(str(source_receipts.get(
            "source_receipt_row_digest", "",
        ))) is None
        or digest_pattern.fullmatch(str(source_receipts.get(
            "terminal_economic_receipt_digest", "",
        ))) is None
        or int(source_receipts.get(
            "terminal_economic_receipt_count", -1,
        )) != int(source_receipts.get("economic_decision_count", -2))
        or int(published_actions.get("published_action_count", -1))
        != included_action_count
        or published_actions.get("published_action_scope_contract")
        != "issuer_cash_ex_plus_manifest_scale_support_v1"
        or digest_pattern.fullmatch(str(published_actions.get(
            "published_action_row_digest", "",
        ))) is None
        or int(published_actions.get(
            "included_cash_action_parity_count", -1,
        )) != source_included
        or digest_pattern.fullmatch(str(published_actions.get(
            "included_cash_action_parity_digest", "",
        ))) is None
        or cash_scale_evidence.get("contract") != SOURCE_EVIDENCE_CONTRACT
        or int(cash_scale_evidence.get("unresolved_count", -1)) != 0
        or int(cash_scale_evidence.get(
            "changed_scale_coverage_count", -1,
        )) != int(cash_scale_evidence.get(
            "persisted_parent_row_count", -2,
        ))
        or int(cash_scale_evidence.get(
            "manifest_parent_row_count", -1,
        )) != int(cash_scale_evidence.get(
            "persisted_parent_row_count", -2,
        ))
        or int(cash_scale_evidence.get(
            "manifest_support_action_count", -1,
        )) != int(cash_scale_evidence.get(
            "persisted_support_action_count", -2,
        ))
        or int(cash_scale_evidence.get(
            "manifest_support_semantic_group_count", -1,
        )) != int(cash_scale_evidence.get(
            "persisted_support_semantic_group_count", -2,
        ))
        or any(
            digest_pattern.fullmatch(str(cash_scale_evidence.get(key, "")))
            is None
            for key in (
                "manifest_sha256", "manifest_parent_row_digest",
                "manifest_support_action_digest",
                "persisted_parent_row_digest",
                "persisted_support_action_digest",
            )
        )
    ):
        raise RuntimeError(
            "certified DART source receipt partition parity failed"
        )
    scale_parents, scale_supports = _scale_source_contract_frames(
        conn, snapshot[0],
    )
    if (
        len(scale_parents) != int(cash_scale_evidence[
            "persisted_parent_row_count"
        ])
        or source_evidence_digest(scale_parents)
        != cash_scale_evidence["persisted_parent_row_digest"]
        or len(scale_supports) != int(cash_scale_evidence[
            "persisted_support_action_count"
        ])
        or support_action_digest(scale_supports)
        != cash_scale_evidence["persisted_support_action_digest"]
        or _support_group_count(scale_supports)
        != int(cash_scale_evidence[
            "persisted_support_semantic_group_count"
        ])
    ):
        raise RuntimeError("certified cash-scale DB/metadata digest parity failed")
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT count(*)
            FROM cash_adjustment_scale_support_action se
            JOIN cash_adjustment_scale_source_evidence pe
              ON pe.action_snapshot_run_id=se.action_snapshot_run_id
             AND pe.evidence_key=se.evidence_key
            LEFT JOIN corporate_action ca
              ON ca.asset_id=pe.asset_id
             AND ca.source=se.support_action_source
             AND ca.action_key=se.support_action_key
             AND ca.action_type=se.support_action_type
             AND ca.quality_run_id=se.support_action_quality_run_id
            WHERE se.action_snapshot_run_id=%s
              AND (
                  ca.asset_id IS NULL
                  OR ca.source_body_sha256 IS DISTINCT FROM
                     se.support_action_body_sha256
                  OR ca.announcement_date IS DISTINCT FROM
                     se.support_announcement_date
                  OR ca.ex_date IS DISTINCT FROM se.support_ex_date
                  OR ca.record_date IS DISTINCT FROM se.support_record_date
                  OR ca.ratio_numerator IS DISTINCT FROM
                     se.support_ratio_numerator
                  OR ca.ratio_denominator IS DISTINCT FROM
                     se.support_ratio_denominator
                  OR ca.expected_price_factor IS DISTINCT FROM
                     se.support_expected_price_factor
                  OR ca.report_name IS DISTINCT FROM se.support_report_name
                  OR ca.action_scope IS DISTINCT FROM se.support_action_scope
              )
            """,
            (snapshot[0],),
        )
        invalid_support_parity = int(cur.fetchone()[0])
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
                  OR dr.economic_evidence_sha256 IS DISTINCT FROM
                     pe.cash_economic_sha256
                  OR dr.source_evidence_status IS DISTINCT FROM
                     pe.cash_source_evidence_status
              )
            """,
            (snapshot[0],),
        )
        invalid_cash_parity = int(cur.fetchone()[0])
    if invalid_support_parity or invalid_cash_parity:
        raise RuntimeError(
            "certified cash-scale action/body snapshot parity failed: "
            f"support={invalid_support_parity} cash={invalid_cash_parity}"
        )
    action_snapshot = CertifiedActionSnapshot(
        run_id=snapshot[0],
        manifest_sha256=str(snapshot[1]),
        body_digest=str(snapshot[2]),
        body_count=int(snapshot[3]),
        coverage_start=snapshot[4],
        coverage_end=snapshot[5],
        action_count=int(snapshot[6]),
        input_action_count=input_action_count,
        excluded_action_count=excluded_action_count,
        included_corp_cls_counts={
            str(key): int(value)
            for key, value in included_classes.items()
        },
        excluded_corp_cls_counts={
            str(key): int(value)
            for key, value in excluded_classes.items()
        },
        excluded_reason_counts={
            str(key): int(value)
            for key, value in excluded_reasons.items()
        },
        source_receipts=source_receipts,
        published_actions=published_actions,
        disclosure_observation_audit=disclosure_observation_audit,
        scale_source_evidence=scale_parents,
        scale_support_actions=scale_supports,
        cash_scale_evidence=cash_scale_evidence,
    )
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT count(*) FILTER (WHERE ca.action_scope IS NULL),
                   count(*) FILTER (
                       WHERE ca.action_scope='ISSUER'
                         AND ca.action_type='cash_dividend'
                   )
            FROM corporate_action ca
            WHERE ca.quality_run_id=%s
              AND (
                  (ca.source='DART_DISCLOSURE'
                   AND ca.action_type IN ('cash_dividend','ex_dividend'))
                  OR EXISTS (
                      SELECT 1
                      FROM cash_adjustment_scale_support_action se
                      WHERE se.action_snapshot_run_id=ca.quality_run_id
                        AND se.support_action_source=ca.source
                        AND se.support_action_key=ca.action_key
                        AND se.support_action_type=ca.action_type
                  )
              )
            """,
            (action_snapshot.run_id,),
        )
        null_scope_count, issuer_cash_count = cur.fetchone()
    if int(null_scope_count) > 0:
        raise RuntimeError(
            "latest certified DART snapshot에 NULL action_scope가 "
            f"{null_scope_count}건 있습니다"
        )
    if int(issuer_cash_count) == 0:
        raise RuntimeError(
            "latest certified DART snapshot에 ISSUER cash-dividend가 없습니다"
        )
    with conn.cursor() as cur:
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
                   AND ca.action_type IN ('cash_dividend','ex_dividend'))
                  OR EXISTS (
                      SELECT 1
                      FROM cash_adjustment_scale_support_action se
                      WHERE se.action_snapshot_run_id=ca.quality_run_id
                        AND se.support_action_source=ca.source
                        AND se.support_action_key=ca.action_key
                        AND se.support_action_type=ca.action_type
                  )
              )
              AND a.asset_type='stock'
              AND a.instrument_type='common_stock'
              AND a.exchange='KRX'
            """,
            (action_snapshot.run_id,),
        )
        persisted_action_count, persisted_cash_action_count = [
            int(value) for value in cur.fetchone()
        ]
    if persisted_action_count != action_snapshot.action_count:
        raise RuntimeError(
            "certified DART snapshot/action row parity failed: "
            f"contract={action_snapshot.action_count} "
            f"persisted={persisted_action_count}"
        )
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT count(*),count(DISTINCT d.receipt_no),
                   count(*) FILTER (WHERE d.mapping_status='INCLUDED'),
                   count(*) FILTER (WHERE d.mapping_status='EXCLUDED'),
                   count(*) FILTER (
                       WHERE d.cash_amount_status='ATTACHMENT_ONLY'
                   ),
                   count(*) FILTER (
                       WHERE d.cash_amount_status='NO_COMMON_CASH_DIVIDEND'
                   ),
                   count(*) FILTER (
                       WHERE d.cash_amount_status='NO_ECONOMIC_EVENT'
                   ),
                   count(*) FILTER (
                       WHERE d.cash_amount_status=
                           'POSITIVE_PENDING_RECORD_DATE'
                   ),
                   count(*) FILTER (
                       WHERE NOT coalesce((
                           (d.source_evidence_status=
                                'VERIFIED_OPENDART_DOCUMENT'
                            AND coalesce(d.viewer_evidence_sha256,'')=''
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
                            AND d.cash_amount_status='ATTACHMENT_ONLY'
                            AND d.revision_kind='ATTACHMENT_ONLY'
                            AND d.previous_receipt_no IS NOT NULL)
                           OR
                           (d.source_evidence_status=
                                'VERIFIED_REVIEWED_SOURCE_ERRATUM'
                            AND coalesce(d.viewer_evidence_sha256,'')=''
                            AND d.economic_evidence_sha256 ~
                                '^[0-9a-f]{64}$'
                            AND btrim(coalesce(
                                d.reviewed_correction_id,''
                            ))<>'')
                       ),false)
                          OR d.cash_amount_status NOT IN (
                              'POSITIVE','POSITIVE_PENDING_RECORD_DATE',
                              'NO_COMMON_CASH_DIVIDEND','NO_ECONOMIC_EVENT',
                              'ATTACHMENT_ONLY'
                          )
                          OR coalesce(d.receipt_no,'') !~ '^[0-9]{14}$'
                          OR coalesce(d.revision_root_receipt_no,'') !~
                              '^[0-9]{14}$'
                          OR coalesce(d.terminal_receipt_no,'') !~
                              '^[0-9]{14}$'
                          OR d.terminal_announcement_date IS NULL
                          OR d.is_terminal_economic_revision IS DISTINCT FROM
                              (d.receipt_no=d.terminal_receipt_no)
                          OR (
                              d.previous_receipt_no IS NOT NULL
                              AND d.previous_receipt_no !~ '^[0-9]{14}$'
                          )
                          OR (
                              d.source_evidence_status=
                                  'VERIFIED_ATTACHMENT_CORRECTION'
                              AND NOT EXISTS (
                                  SELECT 1
                                  FROM dividend_source_receipt prior
                                  WHERE prior.quality_run_id=d.quality_run_id
                                    AND prior.receipt_no=
                                        d.previous_receipt_no
                                    AND prior.ticker=d.ticker
                                    AND prior.revision_root_receipt_no=
                                        d.revision_root_receipt_no
                              )
                          )
                          OR coalesce(d.ticker,'') !~ '^[0-9A-Z]{6}$'
                          OR d.pit_event_date IS NULL
                          OR coalesce(d.mapping_status,'') NOT IN (
                              'INCLUDED','EXCLUDED'
                          )
                          OR (
                              d.mapping_status='INCLUDED'
                              AND (d.asset_id IS NULL
                                   OR d.excluded_reason IS NOT NULL)
                          )
                          OR (
                              d.mapping_status='EXCLUDED'
                              AND d.excluded_reason IS NULL
                          )
                          OR NOT coalesce((
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
                                   'NO_ECONOMIC_EVENT','ATTACHMENT_ONLY'
                               ) AND d.cash_amount IS NULL)
                          ),false)
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
            (action_snapshot.run_id,),
        )
        (
            observed_source_total,
            observed_source_distinct,
            observed_source_included,
            observed_source_excluded,
            observed_source_attachment,
            observed_source_no_common,
            observed_source_cancelled,
            observed_source_pending,
            observed_source_invalid,
            observed_terminal_families,
            observed_terminal_pending,
        ) = [int(value) for value in cur.fetchone()]
        cur.execute(
            """
            SELECT excluded_reason,count(*)
            FROM dividend_source_receipt
            WHERE quality_run_id=%s AND mapping_status='EXCLUDED'
            GROUP BY excluded_reason ORDER BY excluded_reason
            """,
            (action_snapshot.run_id,),
        )
        observed_source_reasons = {
            str(reason): int(count) for reason, count in cur.fetchall()
        }
        cur.execute(
            """
            SELECT mapping_status,
                   coalesce(nullif(btrim(corp_cls),''),'UNKNOWN'),count(*)
            FROM dividend_source_receipt
            WHERE quality_run_id=%s
            GROUP BY mapping_status,
                     coalesce(nullif(btrim(corp_cls),''),'UNKNOWN')
            ORDER BY mapping_status,2
            """,
            (action_snapshot.run_id,),
        )
        observed_included_classes: dict[str, int] = {}
        observed_excluded_classes: dict[str, int] = {}
        for mapping_status, corp_cls, count in cur.fetchall():
            target = (
                observed_included_classes
                if mapping_status == "INCLUDED"
                else observed_excluded_classes
            )
            target[str(corp_cls)] = int(count)
    source_receipt_rows = _source_receipt_contract_frame(
        conn, action_snapshot.run_id,
    )
    persisted_action_rows = _published_action_contract_frame(
        conn, action_snapshot.run_id,
    )
    included_receipt_rows = source_receipt_rows[
        source_receipt_rows["mapping_status"].eq("INCLUDED")
    ]
    action_cash_parity_rows = _action_cash_parity_frame(
        persisted_action_rows
    )
    observed_source_digest = source_receipt_digest(source_receipt_rows)
    observed_terminal_digest = terminal_source_receipt_digest(
        source_receipt_rows
    )
    observed_action_digest = published_action_digest(
        persisted_action_rows
    )
    observed_receipt_parity_digest = included_cash_parity_digest(
        included_receipt_rows
    )
    observed_action_parity_digest = included_cash_parity_digest(
        action_cash_parity_rows
    )
    expected_included_classes = source_receipts.get(
        "included_cash_receipts_by_corp_cls",
    ) or {}
    expected_excluded_classes = source_receipts.get(
        "excluded_cash_receipts_by_corp_cls",
    ) or {}
    if (
        observed_source_total != source_total
        or observed_source_distinct != source_total
        or observed_source_included != source_included
        or observed_source_included != persisted_cash_action_count
        or observed_source_excluded != source_excluded
        or observed_source_attachment
        != int(source_receipts.get("attachment_correction_count", -1))
        or observed_source_no_common
        != int(source_receipts.get("no_common_cash_dividend_count", -1))
        or observed_source_cancelled
        != int(source_receipts.get("withdrawn_or_cancelled_count", -1))
        or observed_source_pending
        != int(source_receipts.get("pending_record_date_count", -1))
        or observed_source_invalid != 0
        or observed_terminal_pending != 0
        or observed_terminal_families
        != int(source_receipts.get("economic_decision_count", -1))
        or observed_terminal_families
        != int(source_receipts.get("terminal_economic_receipt_count", -1))
        or observed_source_digest
        != source_receipts.get("source_receipt_row_digest")
        or observed_terminal_digest
        != source_receipts.get("terminal_economic_receipt_digest")
        or len(persisted_action_rows)
        != int(published_actions.get("published_action_count", -1))
        or observed_action_digest
        != published_actions.get("published_action_row_digest")
        or len(action_cash_parity_rows)
        != int(published_actions.get(
            "included_cash_action_parity_count", -1,
        ))
        or observed_receipt_parity_digest != observed_action_parity_digest
        or observed_action_parity_digest
        != published_actions.get("included_cash_action_parity_digest")
        or observed_source_reasons != source_reasons
        or observed_included_classes != expected_included_classes
        or observed_excluded_classes != expected_excluded_classes
    ):
        raise RuntimeError(
            "certified DART source receipt DB/metadata parity failed"
        )
    return action_snapshot


def _prepare_local_action_snapshot(
    conn,
    base: str,
    *,
    required_end: date,
    asset_identity_digest: str | None = None,
) -> LocalActionSnapshot:
    """Prepare a complete local Bronze action snapshot for read-only preview."""
    verified = verify_snapshot_manifest(
        base,
        required_start=CONTRACT_COVERAGE_START,
        required_end=required_end,
    )
    scale_evidence = verify_source_evidence_manifest(
        verified.base,
        required_start=verified.coverage_start,
        required_end=verified.coverage_end,
    )
    if getattr(
        verified,
        "cash_adjustment_scale_source_evidence",
        scale_evidence.metadata,
    ) != scale_evidence.metadata:
        raise RuntimeError("local action/cash-scale manifest metadata mismatch")
    candidates, _ = corporate_actions.prepare(
        verified.base,
        coverage_start=verified.coverage_start,
        coverage_end=verified.coverage_end,
        verified_snapshot_sha256=verified.manifest_sha256,
    )
    from pipeline.silver.dart_extra_load import (
        _manifest_support_action_candidates,
        _total_return_actions,
    )
    manifest_support = _manifest_support_action_candidates(scale_evidence)
    if not manifest_support.empty:
        candidates = pd.concat(
            [candidates, manifest_support], ignore_index=True,
        )
    required = {
        "identifier", "source", "rcept_no", "event_type", "action_scope",
    }
    missing = required - set(candidates.columns)
    if missing:
        raise RuntimeError(
            "local corporate-action candidates missing columns: "
            f"{sorted(missing)}"
        )
    scoped = _total_return_actions(candidates, scale_evidence)
    if scoped.empty or not scoped["event_type"].eq("cash_dividend").any():
        raise RuntimeError(
            "local complete Bronze has no ISSUER DART cash-dividend"
        )

    mapped, mapping_stats, source_action_frame = map_actions_to_pit_assets(
        conn,
        scoped,
        coverage_start=CONTRACT_COVERAGE_START,
        include_audit=True,
        verified_snapshot_sha256=verified.manifest_sha256,
        asset_identity_digest=asset_identity_digest,
    )
    normalized = corporate_actions.normalize_for_publish(mapped)
    if normalized.empty or not normalized["action_type"].eq("cash_dividend").any():
        raise RuntimeError(
            "local ISSUER cash-dividend does not map to common-stock scope"
        )
    normalized["asset_id"] = normalized["asset_id"].astype("int64")
    # A local preview is held to the same immutable cash/support-action/body
    # parity as the publisher.  Merely copying ``asset_id`` onto manifest
    # parents would let a tampered child action reach the return calculation.
    if scale_evidence.frame.empty:
        bound_scale = bind_source_evidence(
            scale_evidence,
            receipt_frame=pd.DataFrame(),
            published_actions=pd.DataFrame(),
            action_snapshot_run_id=None,
        )
    else:
        from pipeline.silver.dart_extra_load import _source_receipt_frame

        receipt_frame = _source_receipt_frame(
            source_action_frame, quality_run_id=None,
        )
        published_for_binding = normalized.copy()
        published_for_binding["quality_run_id"] = None
        bound_scale = bind_source_evidence(
            scale_evidence,
            receipt_frame=receipt_frame,
            published_actions=published_for_binding,
            action_snapshot_run_id=None,
        )
    local_scale = bound_scale.frame
    local_scale_support = bound_scale.support_frame
    local_scale_metadata = source_evidence_metadata(
        local_scale,
        local_scale_support,
        verified=scale_evidence,
    )
    # total_returns groups on `identifier`; asset_id is stable across ticker
    # history and matches the RDS price query's grouping key.
    normalized["identifier"] = normalized["asset_id"].astype(str)
    normalized = normalized.rename(columns={
        "action_type": "event_type",
        "ex_date": "effective_date",
    })
    action_columns = [
        "asset_id",
        "identifier",
        "source",
        "action_key",
        "event_type",
        "announcement_date",
        "effective_date",
        "record_date",
        "cash_amount",
        "filing_id",
        "cash_amount_status",
        "source_evidence_status",
        "correction_of_action_key",
        "revision_root_action_key",
        "revision_kind",
        "viewer_evidence_sha256",
        "economic_evidence_sha256",
        "reviewed_correction_id",
        "payment_date_quality_status",
    ]
    actions = normalized.reindex(columns=action_columns).sort_values(
        ["asset_id", "announcement_date", "action_key"],
        kind="mergesort",
    ).reset_index(drop=True)
    repeated = verify_snapshot_manifest(
        verified.base,
        required_start=CONTRACT_COVERAGE_START,
        required_end=required_end,
    )
    if repeated != verified:
        raise RuntimeError("local DART action snapshot changed during parse")
    return LocalActionSnapshot(
        actions=actions,
        base=verified.base,
        fingerprint=verified.body_digest,
        unmapped_count=0,
        out_of_scope_count=mapping_stats.out_of_scope_instrument_count,
        manifest_sha256=verified.manifest_sha256,
        body_count=verified.body_count,
        coverage_start=verified.coverage_start.isoformat(),
        coverage_end=verified.coverage_end.isoformat(),
        input_action_count=getattr(mapping_stats, "input_count", len(scoped)),
        excluded_action_count=sum(
            getattr(mapping_stats, "excluded_reason_counts", {}).values()
        ),
        included_corp_cls_counts=getattr(
            mapping_stats, "included_corp_cls_counts", {},
        ),
        excluded_corp_cls_counts=getattr(
            mapping_stats, "excluded_corp_cls_counts", {},
        ),
        excluded_reason_counts=getattr(
            mapping_stats, "excluded_reason_counts", {},
        ),
        scale_source_evidence=local_scale,
        scale_support_actions=local_scale_support,
        cash_scale_evidence=local_scale_metadata,
    )


def _global_krx_sessions(conn) -> pd.Series:
    """Load one global KRX session calendar, never an asset-local calendar."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT p.trade_date
            FROM price_daily p
            JOIN asset a ON a.asset_id=p.asset_id
            JOIN dq_run q ON q.run_id=p.quality_run_id
            WHERE p.source='KRX'
              AND a.asset_type='stock'
              AND a.instrument_type='common_stock'
              AND a.exchange='KRX'
              AND p.market IN ('KOSPI','KOSDAQ')
              AND q.status='CERTIFIED'
              AND p.trade_date >= %s
            ORDER BY p.trade_date
            """,
            (CONTRACT_COVERAGE_START,),
        )
        rows = cur.fetchall()
    sessions = pd.Series([row[0] for row in rows], dtype="datetime64[ns]")
    if sessions.empty:
        raise RuntimeError("certified KRX stock session이 없습니다")
    return sessions


def _source_price_coverage(conn) -> tuple[date, date]:
    """Record raw certified price history; do not certify it as total return."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT min(p.trade_date), max(p.trade_date)
            FROM price_daily p
            JOIN asset a ON a.asset_id=p.asset_id
            JOIN dq_run q ON q.run_id=p.quality_run_id
            WHERE p.source='KRX'
              AND a.asset_type='stock'
              AND a.instrument_type='common_stock'
              AND a.exchange='KRX'
              AND p.market IN ('KOSPI','KOSDAQ')
              AND q.status='CERTIFIED'
            """
        )
        coverage_start, coverage_end = cur.fetchone()
    if coverage_start is None or coverage_end is None:
        raise RuntimeError("certified KRX common-stock source price is empty")
    return coverage_start, coverage_end


def _certified_prices(conn, asset_ids: Sequence[int]) -> pd.DataFrame:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT p.asset_id, p.asset_id::text AS identifier,
                   p.trade_date, p.close, p.adj_close
            FROM price_daily p
            JOIN asset a ON a.asset_id=p.asset_id
            JOIN dq_run q ON q.run_id=p.quality_run_id
            WHERE p.asset_id = ANY(%s)
              AND p.source='KRX'
              AND a.asset_type='stock'
              AND a.instrument_type='common_stock'
              AND a.exchange='KRX'
              AND p.market IN ('KOSPI','KOSDAQ')
              AND q.status='CERTIFIED'
              AND p.trade_date >= %s
            ORDER BY p.asset_id, p.trade_date
            """,
            (list(asset_ids), CONTRACT_COVERAGE_START),
        )
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=[
        "asset_id", "identifier", "trade_date", "close", "adj_close",
    ])


def _issuer_dart_actions(
    conn,
    asset_ids: Sequence[int],
    action_snapshot_run_id: UUID,
) -> pd.DataFrame:
    """Read no inherited/related-company or uncertified action evidence."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT ca.asset_id, ca.asset_id::text AS identifier,
                   ca.source, ca.action_key,
                   ca.action_type AS event_type,
                   ca.announcement_date, ca.ex_date AS effective_date,
                   ca.record_date, ca.cash_amount, ca.filing_id
                   ,ca.cash_amount_status,ca.source_evidence_status,
                   ca.correction_of_action_key,ca.revision_root_action_key,
                   ca.revision_kind,ca.viewer_evidence_sha256,
                   ca.economic_evidence_sha256,ca.reviewed_correction_id,
                   ca.payment_date_quality_status
            FROM corporate_action ca
            JOIN asset a ON a.asset_id=ca.asset_id
            JOIN dq_run q ON q.run_id=ca.quality_run_id
            WHERE ca.asset_id = ANY(%s)
              AND ca.source='DART_DISCLOSURE'
              AND ca.action_scope='ISSUER'
              AND ca.action_type IN ('cash_dividend', 'ex_dividend')
              AND a.asset_type='stock'
              AND a.instrument_type='common_stock'
              AND a.exchange='KRX'
              AND q.status='CERTIFIED'
              AND ca.quality_run_id=%s
            ORDER BY ca.asset_id, ca.announcement_date, ca.action_key
            """,
            (list(asset_ids), action_snapshot_run_id),
        )
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=[
        "asset_id",
        "identifier",
        "source",
        "action_key",
        "event_type",
        "announcement_date",
        "effective_date",
        "record_date",
        "cash_amount",
        "filing_id",
        "cash_amount_status",
        "source_evidence_status",
        "correction_of_action_key",
        "revision_root_action_key",
        "revision_kind",
        "viewer_evidence_sha256",
        "economic_evidence_sha256",
        "reviewed_correction_id",
        "payment_date_quality_status",
    ])


def _audit_frame(
    classified: pd.DataFrame,
    resolved_events: pd.DataFrame,
    *,
    run_id: UUID | None,
) -> pd.DataFrame:
    """Map every cash source row to an applied event or explicit exclusion."""
    resolved_by_key: dict[tuple[str, str, str], pd.Series] = {}
    for _, event in resolved_events.iterrows():
        key = (
            str(event["identifier"]),
            str(event["source"]),
            str(event["dividend_key"]),
        )
        resolved_by_key[key] = event

    records: list[dict] = []
    for _, action in classified.iterrows():
        key = (
            str(action["identifier"]),
            str(action["source"]),
            str(action["dividend_key"]),
        )
        source_canonical = bool(action["is_canonical"])
        event = resolved_by_key.get(key) if source_canonical else None
        excluded_reason = action["excluded_reason"]
        audit_canonical = source_canonical

        if source_canonical:
            if event is None:
                raise RuntimeError(
                    f"canonical dividend has no resolution: {key}"
                )
            application_status = str(event["application_status"])
            if application_status in _BLOCKING_APPLICATION_STATUSES:
                raise RuntimeError(
                    "unresolved canonical dividend "
                    f"{key}: {application_status}"
                )
            if application_status == "applied":
                excluded_reason = None
            elif application_status in _EXPLICIT_EXCLUSIONS:
                audit_canonical = False
                excluded_reason = _EXPLICIT_EXCLUSIONS[application_status]
            else:
                raise RuntimeError(
                    f"unknown dividend application status {application_status!r}"
                )

        records.append({
            "quality_run_id": run_id,
            "asset_id": int(action["asset_id"]),
            "source": str(action["source"]),
            "action_key": str(action["dividend_key"]),
            "resolution_version": RESOLUTION_VERSION,
            "is_canonical": audit_canonical,
            "excluded_reason": excluded_reason,
            "resolved_ex_date": (
                event["resolved_ex_date"] if event is not None else None
            ),
            "ex_date_basis": (
                event["ex_date_basis"] if event is not None else None
            ),
            "applied_trade_date": (
                event["applied_trade_date"] if event is not None else None
            ),
            "raw_cash_amount": action["cash_amount"],
            "adjusted_cash_amount": (
                event["adjusted_cash_amount"] if event is not None else None
            ),
            "source_announcement_date": action.get("announcement_date"),
            "revision_group_key": action.get("revision_group_key"),
            "source_evidence_status": action.get("source_evidence_status"),
            "cash_amount_status": action.get("cash_amount_status"),
            "correction_of_action_key": action.get(
                "correction_of_action_key"
            ),
            "revision_root_action_key": action.get(
                "revision_root_action_key"
            ),
            "revision_kind": action.get("revision_kind"),
            "viewer_evidence_sha256": action.get(
                "viewer_evidence_sha256"
            ),
            "economic_evidence_sha256": action.get(
                "economic_evidence_sha256"
            ),
            "reviewed_correction_id": action.get("reviewed_correction_id"),
            "payment_date_quality_status": action.get(
                "payment_date_quality_status"
            ),
            "previous_trade_date": (
                event.get("previous_trade_date") if event is not None else None
            ),
            "previous_close": (
                event.get("previous_close") if event is not None else None
            ),
            "previous_adj_close": (
                event.get("previous_adj_close") if event is not None else None
            ),
            "applied_close": (
                event.get("applied_close") if event is not None else None
            ),
            "applied_adj_close": (
                event.get("applied_adj_close") if event is not None else None
            ),
            "previous_price_scale": (
                event.get("previous_price_scale") if event is not None else None
            ),
            "applied_price_scale": (
                event.get("applied_price_scale") if event is not None else None
            ),
            "selected_cash_scale": (
                event.get("selected_cash_scale") if event is not None else None
            ),
            "cash_adjustment_scale_basis": (
                event.get("cash_adjustment_scale_basis")
                if event is not None else None
            ),
            "scale_change_detected": (
                event.get("scale_change_detected") if event is not None else None
            ),
            "scale_evidence_action_snapshot_run_id": (
                event.get("scale_evidence_action_snapshot_run_id")
                if event is not None else None
            ),
            "scale_evidence_key": (
                event.get("scale_evidence_key") if event is not None else None
            ),
            "scale_price_factor_observed": (
                event.get("scale_price_factor_observed")
                if event is not None else None
            ),
            "scale_price_factor_reference": (
                event.get("scale_price_factor_reference")
                if event is not None else None
            ),
            "scale_price_factor_parity": (
                event.get("scale_price_factor_parity")
                if event is not None else None
            ),
        })
    return pd.DataFrame(records, columns=_AUDIT_COLUMNS)


def _assert_dividend_yields(
    rebuilt: pd.DataFrame,
    events: pd.DataFrame,
    *,
    max_dividend_yield: float,
) -> None:
    """Reject suspicious parsed amounts instead of silently compounding them."""
    if not 0 < max_dividend_yield <= 1:
        raise ValueError("max_dividend_yield must be in (0, 1]")
    applied = events[events["application_status"].eq("applied")].copy()
    if applied.empty:
        return
    ordered = rebuilt.sort_values(["identifier", "trade_date"]).copy()
    ordered["previous_adj_close"] = ordered.groupby(
        "identifier", sort=False,
    )["adj_close"].shift(1)
    reference = ordered[[
        "identifier", "trade_date", "previous_adj_close",
    ]].rename(columns={
        "trade_date": "applied_trade_date",
        "previous_adj_close": "_rebuilt_previous_adj_close",
    })
    applied = applied.merge(
        reference,
        on=["identifier", "applied_trade_date"],
        how="left",
        validate="many_to_one",
    )
    rebuilt_previous = pd.to_numeric(
        applied["_rebuilt_previous_adj_close"], errors="coerce",
    )
    if "previous_adj_close" in applied:
        runtime_previous = pd.to_numeric(
            applied["previous_adj_close"], errors="coerce",
        )
        previous_parity = (
            runtime_previous.notna()
            & rebuilt_previous.notna()
            & np.isclose(runtime_previous, rebuilt_previous, rtol=0, atol=0)
        )
        if not previous_parity.all():
            raise RuntimeError(
                "runtime dividend previous-price lineage disagrees with "
                "rebuilt prices"
            )
    else:
        # The standalone audit helper accepts legacy test/input frames.  The
        # production v2 path always supplies runtime lineage and exercises the
        # exact branch above.
        applied["previous_adj_close"] = rebuilt_previous
    applied["cash_yield"] = (
        pd.to_numeric(applied["adjusted_cash_amount"], errors="coerce")
        / pd.to_numeric(applied["previous_adj_close"], errors="coerce")
    )
    invalid = applied[
        applied["cash_yield"].isna()
        | ~np.isfinite(applied["cash_yield"])
        | applied["cash_yield"].le(0)
        | applied["cash_yield"].gt(max_dividend_yield)
    ]
    if not invalid.empty:
        sample = invalid[[
            "identifier", "dividend_key", "cash_yield",
        ]].head(5).to_dict("records")
        raise RuntimeError(
            "dividend cash yield is outside the fail-closed bound "
            f"(0, {max_dividend_yield}]: {sample}"
        )
    aggregate = applied.groupby(
        ["identifier", "applied_trade_date"], as_index=False, dropna=False,
    ).agg(
        adjusted_cash_amount=("adjusted_cash_amount", "sum"),
        previous_adj_close=("previous_adj_close", "first"),
        source_event_count=("dividend_key", "size"),
    )
    aggregate["aggregate_cash_yield"] = (
        pd.to_numeric(aggregate["adjusted_cash_amount"], errors="coerce")
        / pd.to_numeric(aggregate["previous_adj_close"], errors="coerce")
    )
    invalid_aggregate = aggregate[
        aggregate["aggregate_cash_yield"].isna()
        | ~np.isfinite(aggregate["aggregate_cash_yield"])
        | aggregate["aggregate_cash_yield"].le(0)
        | aggregate["aggregate_cash_yield"].gt(max_dividend_yield)
    ]
    if not invalid_aggregate.empty:
        sample = invalid_aggregate[[
            "identifier", "applied_trade_date", "source_event_count",
            "aggregate_cash_yield",
        ]].head(5).to_dict("records")
        raise RuntimeError(
            "aggregate same-day dividend yield is outside the fail-closed "
            f"bound (0, {max_dividend_yield}]: {sample}"
        )


def _build_batch(
    prices: pd.DataFrame,
    actions: pd.DataFrame,
    sessions: pd.Series,
    *,
    run_id: UUID | None,
    max_dividend_yield: float,
    scale_source_evidence: pd.DataFrame | None = None,
) -> BatchRebuild:
    if prices.empty:
        raise RuntimeError("asset batch has no certified KRX stock prices")
    expected_keys = prices[["asset_id", "trade_date"]].copy()
    expected_keys["trade_date"] = pd.to_datetime(
        expected_keys["trade_date"], errors="coerce",
    ).dt.normalize()
    if expected_keys.duplicated().any():
        raise RuntimeError("duplicate certified KRX asset/date price rows")

    classified = classify_cash_dividend_revisions(actions)
    canonical = classified[classified["is_canonical"]].copy().reset_index(
        drop=True,
    )
    market_coverage_start = pd.to_datetime(sessions, errors="coerce").min()
    market_coverage_end = pd.to_datetime(sessions, errors="coerce").max()
    resolved = resolve_dividend_ex_dates(canonical, actions, sessions)
    if not resolved.empty:
        record_dates = pd.to_datetime(resolved["record_date"], errors="coerce")
        resolved["_runner_resolution_status"] = None
        before_coverage = record_dates.lt(market_coverage_start)
        future_inference = (
            resolved["ex_date_basis"].eq("KRX_T2_INFERRED")
            & record_dates.gt(market_coverage_end)
        )
        resolved.loc[
            before_coverage, "_runner_resolution_status",
        ] = "before_market_coverage"
        resolved.loc[
            future_inference, "_runner_resolution_status",
        ] = "pending_future_trade"
        unresolved_by_boundary = before_coverage | future_inference
        resolved.loc[unresolved_by_boundary, "resolved_ex_date"] = pd.NaT
        resolved.loc[unresolved_by_boundary, "ex_date_basis"] = None
    rebuilt, events = apply_dividends_to_prices(
        prices, resolved, scale_source_evidence=scale_source_evidence,
    )
    if not events.empty and "_runner_resolution_status" in events:
        boundary_status = events["_runner_resolution_status"].notna()
        events.loc[boundary_status, "application_status"] = events.loc[
            boundary_status, "_runner_resolution_status"
        ]
    if len(rebuilt) != len(prices):
        raise RuntimeError("total-return rebuild changed price row count")
    actual_keys = rebuilt[["asset_id", "trade_date"]]
    if set(map(tuple, actual_keys.to_numpy())) != set(
        map(tuple, expected_keys.to_numpy())
    ):
        raise RuntimeError("total-return rebuild changed price keys")

    total_return = pd.to_numeric(
        rebuilt["total_return_close"], errors="coerce",
    )
    if (
        total_return.isna().any()
        or (~np.isfinite(total_return)).any()
        or total_return.le(0).any()
    ):
        raise RuntimeError("rebuilt total_return_close must be finite and positive")
    _assert_dividend_yields(
        rebuilt,
        events,
        max_dividend_yield=max_dividend_yield,
    )
    audit = _audit_frame(classified, events, run_id=run_id)
    if len(audit) != len(classified):
        raise RuntimeError("not every DART cash action received an audit decision")

    output = rebuilt[[
        "asset_id", "trade_date", "total_return_close",
    ]].copy()
    output["total_return_quality_run_id"] = run_id
    applied_audit = audit[
        audit["is_canonical"] & audit["excluded_reason"].isna()
    ]
    if not applied_audit.empty:
        # Match the NUMERIC(28,12)/(28,8) database audit exactly.  pandas
        # ``round`` uses bankers' rounding and disagrees with the publisher's
        # ROUND_HALF_UP contract on exact half-quantum cash values.
        resolution_parity = pd.Series(
            [
                Decimal(str(row["adjusted_cash_amount"])).quantize(
                    Decimal("0.00000001"), rounding=ROUND_HALF_UP,
                )
                == (
                    Decimal(str(row["raw_cash_amount"]))
                    * Decimal(str(row["selected_cash_scale"]))
                ).quantize(Decimal("0.00000001"), rounding=ROUND_HALF_UP)
                and bool(row["scale_price_factor_parity"])
                for row in applied_audit.to_dict("records")
            ],
            index=applied_audit.index,
            dtype="bool",
        )
        if not resolution_parity.all():
            raise RuntimeError("resolution-v2 adjusted-cash/scale parity failed")
    else:
        resolution_parity = pd.Series(dtype="bool")
    return BatchRebuild(
        prices=output,
        audit=audit,
        canonical_event_count=int(classified["is_canonical"].sum()),
        applied_event_count=int(
            events["application_status"].eq("applied").sum()
        ),
        excluded_event_count=int((~audit["is_canonical"]).sum()),
        stable_scale_event_count=int(
            applied_audit["scale_change_detected"].eq(False).sum()
        ),
        changed_scale_event_count=int(
            applied_audit["scale_change_detected"].eq(True).sum()
        ),
        resolution_parity_count=int(resolution_parity.sum()),
    )


def _create_temp_stages(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"""
            CREATE TEMP TABLE {_PRICE_STAGE} (
                asset_id BIGINT NOT NULL,
                trade_date DATE NOT NULL,
                total_return_close NUMERIC(28,8) NOT NULL,
                total_return_quality_run_id UUID NOT NULL,
                PRIMARY KEY(asset_id, trade_date)
            ) ON COMMIT DROP
            """
        )
        cur.execute(
            f"""
            CREATE TEMP TABLE {_AUDIT_STAGE}
            (LIKE dividend_event_resolution INCLUDING DEFAULTS)
            ON COMMIT DROP
            """
        )


def _publish_batch(conn, batch: BatchRebuild) -> tuple[int, int]:
    """COPY one bounded batch and atomically update price plus event audit."""
    price_rows = list(
        batch.prices[[
            "asset_id", "trade_date", "total_return_close",
            "total_return_quality_run_id",
        ]].astype(object).where(
            pd.notna(batch.prices[[
                "asset_id", "trade_date", "total_return_close",
                "total_return_quality_run_id",
            ]]), None,
        ).itertuples(index=False, name=None)
    )
    audit_frame = batch.audit.reindex(columns=_AUDIT_COLUMNS)
    audit_rows = list(
        audit_frame.astype(object).where(
            pd.notna(audit_frame), None,
        ).itertuples(index=False, name=None)
    )
    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE {_PRICE_STAGE}")
        with cur.copy(
            f"COPY {_PRICE_STAGE} "
            "(asset_id,trade_date,total_return_close,"
            "total_return_quality_run_id) FROM STDIN"
        ) as copy:
            for row in price_rows:
                copy.write_row(row)
        cur.execute(
            f"""
            UPDATE price_daily p
            SET total_return_close=s.total_return_close,
                total_return_quality_run_id=s.total_return_quality_run_id,
                total_return_loaded_at=now()
            FROM {_PRICE_STAGE} s
            WHERE p.asset_id=s.asset_id
              AND p.trade_date=s.trade_date
              AND p.source='KRX'
              AND p.market IN ('KOSPI','KOSDAQ')
              AND EXISTS (
                  SELECT 1 FROM dq_run q
                  WHERE q.run_id=p.quality_run_id
                    AND q.status='CERTIFIED'
              )
            """
        )
        updated = int(cur.rowcount)
        if updated != len(price_rows):
            raise RuntimeError(
                "certified price changed during rebuild: "
                f"expected={len(price_rows)} updated={updated}"
            )

        cur.execute(f"TRUNCATE {_AUDIT_STAGE}")
        if audit_rows:
            columns = ",".join(_AUDIT_COLUMNS)
            with cur.copy(
                f"COPY {_AUDIT_STAGE} ({columns}) FROM STDIN"
            ) as copy:
                for row in audit_rows:
                    copy.write_row(row)
            cur.execute(
                f"""
                INSERT INTO dividend_event_resolution ({columns})
                SELECT {columns} FROM {_AUDIT_STAGE}
                """
            )
            audited = int(cur.rowcount)
            if audited != len(audit_rows):
                raise RuntimeError(
                    "dividend audit upsert row parity failed: "
                    f"expected={len(audit_rows)} actual={audited}"
                )
        else:
            audited = 0
    return updated, audited


def _pass_results(summary: RebuildSummary) -> list[CheckResult]:
    return [
        CheckResult(
            rule_code="KRX_TOTAL_RETURN_INPUT_SCOPE",
            dataset="price_daily",
            severity=Severity.CRITICAL,
            status=CheckStatus.PASS,
            expected="certified KRX stocks and certified ISSUER DART only",
            actual=(
                f"assets={summary.asset_count}, "
                f"cash_actions={summary.cash_action_count}"
            ),
        ),
        CheckResult(
            rule_code="KRX_TOTAL_RETURN_ROW_PARITY",
            dataset="price_daily",
            severity=Severity.CRITICAL,
            status=CheckStatus.PASS,
            expected="every input price key updated exactly once",
            actual=f"rows={summary.price_row_count}",
        ),
        CheckResult(
            rule_code="KRX_DIVIDEND_EVENT_RESOLUTION",
            dataset="dividend_event_resolution",
            severity=Severity.CRITICAL,
            status=CheckStatus.PASS,
            expected="every cash action applied or explicitly excluded",
            actual=(
                f"canonical={summary.canonical_event_count}, "
                f"applied={summary.applied_event_count}, "
                f"excluded={summary.excluded_event_count}"
            ),
        ),
    ]


def _mark_contract_building(conn, run_id: UUID) -> None:
    """Invalidate any older certification before an apply run starts.

    This small state change is committed before the long rebuild transaction.
    A crash or failed batch therefore leaves research fail-closed at BUILDING,
    while all price/audit changes from the failed run are rolled back.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO price_return_contract (
                source,asset_type,field_name,methodology_version,
                dividend_treatment,status,quality_run_id,metadata,
                certified_at,updated_at
            ) VALUES (
                'KRX','stock','total_return_close',%s,%s,'BUILDING',%s,
                '{"reason":"certified rebuild in progress"}'::jsonb,
                NULL,now()
            )
            ON CONFLICT (source,asset_type,field_name) DO UPDATE SET
                methodology_version=EXCLUDED.methodology_version,
                dividend_treatment=EXCLUDED.dividend_treatment,
                status='BUILDING',
                coverage_start=NULL,
                coverage_end=NULL,
                quality_run_id=EXCLUDED.quality_run_id,
                metadata=EXCLUDED.metadata,
                certified_at=NULL,
                updated_at=now()
            """,
            (METHODOLOGY_VERSION, DIVIDEND_TREATMENT, run_id),
        )
    conn.commit()


def _certify_contract(
    conn,
    summary: RebuildSummary,
    run_id: UUID,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT {','.join(RESOLUTION_DIGEST_COLUMNS)} "
            "FROM dividend_event_resolution "
            "WHERE quality_run_id=%s AND is_canonical "
            "AND excluded_reason IS NULL "
            "ORDER BY asset_id,source,action_key",
            (run_id,),
        )
        resolution_rows = cur.fetchall()
        cur.execute(
            """
            SELECT count(*) FILTER (
                       WHERE excluded_reason='BEFORE_LISTING_OR_EPISODE_START'
                   ),
                   count(*) FILTER (
                       WHERE NOT is_canonical AND excluded_reason IS NOT NULL
                   )
            FROM dividend_event_resolution
            WHERE quality_run_id=%s
            """,
            (run_id,),
        )
        first_listing_exclusion_count, explicit_exclusion_count = [
            int(value) for value in cur.fetchone()
        ]
    resolution_frame = pd.DataFrame(
        resolution_rows, columns=RESOLUTION_DIGEST_COLUMNS,
    )
    if len(resolution_frame) != summary.applied_event_count:
        raise RuntimeError("resolution-v2 applied row-count parity failed")
    adjusted_cash_parity_count = 0
    for row in resolution_frame.to_dict("records"):
        expected = (
            Decimal(str(row["raw_cash_amount"]))
            * Decimal(str(row["selected_cash_scale"]))
        ).quantize(Decimal("0.00000001"), rounding=ROUND_HALF_UP)
        actual = Decimal(str(row["adjusted_cash_amount"])).quantize(
            Decimal("0.00000001"), rounding=ROUND_HALF_UP,
        )
        if actual != expected or not bool(row["scale_price_factor_parity"]):
            raise RuntimeError("resolution-v2 cash/scale parity failed")
        adjusted_cash_parity_count += 1
    runtime_evidence = {
        "contract": RESOLUTION_EVIDENCE_CONTRACT,
        "row_count": len(resolution_frame),
        "row_digest": resolution_evidence_digest(resolution_frame),
        "applied_event_count": summary.applied_event_count,
        "stable_scale_event_count": summary.stable_scale_event_count,
        "changed_scale_event_count": summary.changed_scale_event_count,
        "unresolved_count": 0,
        "resolution_parity_count": summary.resolution_parity_count,
        "adjusted_cash_parity_count": adjusted_cash_parity_count,
        "first_listing_exclusion_count": first_listing_exclusion_count,
        "explicit_exclusion_count": explicit_exclusion_count,
        "adj_close_decimal_places": 4,
        "cash_in_adj_close": False,
    }
    source_scale_metadata = summary.action_snapshot_cash_scale_evidence or {}
    if (
        summary.stable_scale_event_count + summary.changed_scale_event_count
        != summary.applied_event_count
        or summary.resolution_parity_count != summary.applied_event_count
        or adjusted_cash_parity_count != summary.applied_event_count
        or int(source_scale_metadata.get(
            "persisted_parent_row_count", -1,
        )) != summary.changed_scale_event_count
        or int(source_scale_metadata.get(
            "changed_scale_coverage_count", -1,
        )) != summary.changed_scale_event_count
        or explicit_exclusion_count != summary.excluded_event_count
        or first_listing_exclusion_count > explicit_exclusion_count
    ):
        raise RuntimeError("resolution-v2 event partition parity failed")
    metadata = {
        "contract_release": CONTRACT_RELEASE,
        "certified_scope": {
            "source": "KRX",
            "asset_type": "stock",
            "instrument_type": "common_stock",
            "markets": list(CERTIFIED_MARKETS),
            "coverage_start": CONTRACT_COVERAGE_START.isoformat(),
        },
        "asset_count": summary.asset_count,
        "price_row_count": summary.price_row_count,
        "cash_action_count": summary.cash_action_count,
        "canonical_event_count": summary.canonical_event_count,
        "applied_event_count": summary.applied_event_count,
        "excluded_event_count": summary.excluded_event_count,
        "resolution_version": RESOLUTION_VERSION,
        "action_snapshot_run_id": summary.action_snapshot_run_id,
        "action_snapshot": {
            "manifest_sha256": summary.action_snapshot_manifest_sha256,
            "body_digest": summary.action_snapshot_digest,
            "body_count": summary.action_snapshot_body_count,
            "action_count": summary.action_snapshot_action_count,
            "coverage_start": summary.action_snapshot_coverage_start,
            "coverage_end": summary.action_snapshot_coverage_end,
            "pit_scope": {
                "contract": (
                    "event_date_identity_common_stock_"
                    "certified_kospi_kosdaq_price_episode"
                ),
                "input_action_count": (
                    summary.action_snapshot_input_action_count
                ),
                "included_action_count": (
                    summary.action_snapshot_action_count
                ),
                "excluded_action_count": (
                    summary.action_snapshot_excluded_action_count
                ),
                "included_by_corp_cls": (
                    summary.action_snapshot_included_corp_cls_counts or {}
                ),
                "excluded_by_corp_cls": (
                    summary.action_snapshot_excluded_corp_cls_counts or {}
                ),
                "excluded_by_reason": (
                    summary.action_snapshot_excluded_reason_counts or {}
                ),
            },
            "source_receipts": summary.action_snapshot_source_receipts or {},
            "published_actions": (
                summary.action_snapshot_published_actions or {}
            ),
            "disclosure_observation_audit": (
                summary.action_snapshot_disclosure_observation_audit or {}
            ),
            "cash_adjustment_scale_evidence": (
                summary.action_snapshot_cash_scale_evidence or {}
            ),
        },
        "asset_identity": {
            "contract": summary.asset_identity_contract,
            "digest": summary.asset_identity_digest,
            "row_count": summary.asset_identity_row_count,
            "asset_count": summary.asset_identity_asset_count,
        },
        "source_price_history_metadata_only": {
            "coverage_start": summary.source_price_coverage_start,
            "coverage_end": summary.source_price_coverage_end,
            "certified_as_total_return": False,
            "markets": list(CERTIFIED_MARKETS),
        },
        "per_row_run_parity": {
            "quality_field": "total_return_quality_run_id",
            "expected": summary.price_row_count,
            "actual": summary.price_row_count,
            "passed": True,
        },
        "input_scope": {
            "prices": "CERTIFIED KRX common_stock KOSPI/KOSDAQ",
            "actions": (
                "CERTIFIED issuer DART cash/ex actions plus exact referenced "
                "scale-support corporate_action rows, bound by event-date "
                "PIT identity and source-body digest"
            ),
            "cash_scale_source_evidence": (
                "append-only content-addressed cash/action and separate "
                "previous/adjustment KRX source objects; changed scale exact "
                "1:1 parent, stable scale no parent"
            ),
        },
        "research_role": {
            "role": "ex_post_realized_forward_return_label",
            "feature_pit_safe": False,
            "action_vintage": "latest_corrected_action_snapshot",
            "feature_guidance": (
                "use adj_close price returns for return-based features until "
                "a bitemporal action-vintage contract exists"
            ),
        },
        "cash_adjustment_scale_evidence": runtime_evidence,
    }
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT count(*), count(DISTINCT p.asset_id),
                   min(p.trade_date), max(p.trade_date),
                   count(*) FILTER (
                       WHERE p.total_return_quality_run_id=%s
                   )
            FROM price_daily p
            JOIN asset a ON a.asset_id=p.asset_id
            JOIN dq_run q ON q.run_id=p.quality_run_id
            WHERE p.source='KRX'
              AND a.asset_type='stock'
              AND a.instrument_type='common_stock'
              AND a.exchange='KRX'
              AND p.market IN ('KOSPI','KOSDAQ')
              AND q.status='CERTIFIED'
              AND p.trade_date >= %s
            """,
            (run_id, CONTRACT_COVERAGE_START),
        )
        (
            row_count,
            asset_count,
            coverage_start,
            coverage_end,
            run_parity_count,
        ) = cur.fetchone()
        if int(row_count) != summary.price_row_count:
            raise RuntimeError(
                "final price-row parity failed: "
                f"expected={summary.price_row_count} actual={row_count}"
            )
        if int(asset_count) != summary.asset_count:
            raise RuntimeError(
                "final asset parity failed: "
                f"expected={summary.asset_count} actual={asset_count}"
            )
        if (
            coverage_start.isoformat() != summary.coverage_start
            or coverage_end.isoformat() != summary.coverage_end
        ):
            raise RuntimeError("final total-return coverage bounds changed")
        if int(run_parity_count) != summary.price_row_count:
            raise RuntimeError(
                "total_return_quality_run_id parity failed: "
                f"expected={summary.price_row_count} actual={run_parity_count}"
            )
        cur.execute(
            """
            SELECT count(*)
            FROM dividend_event_resolution
            WHERE quality_run_id=%s
            """,
            (run_id,),
        )
        audit_count = int(cur.fetchone()[0])
        if audit_count != summary.cash_action_count:
            raise RuntimeError(
                "append-only dividend audit parity failed: "
                f"expected={summary.cash_action_count} actual={audit_count}"
            )

        # This is intentionally the last data operation.  No batch can make
        # the contract visible as CERTIFIED before this point.
        cur.execute(
            """
            INSERT INTO price_return_contract (
                source,asset_type,field_name,methodology_version,
                dividend_treatment,status,coverage_start,coverage_end,
                quality_run_id,metadata,certified_at,updated_at
            ) VALUES (
                'KRX','stock','total_return_close',%s,%s,'CERTIFIED',
                %s,%s,%s,%s::jsonb,clock_timestamp(),clock_timestamp()
            )
            ON CONFLICT (source,asset_type,field_name) DO UPDATE SET
                methodology_version=EXCLUDED.methodology_version,
                dividend_treatment=EXCLUDED.dividend_treatment,
                status='CERTIFIED',
                coverage_start=EXCLUDED.coverage_start,
                coverage_end=EXCLUDED.coverage_end,
                quality_run_id=EXCLUDED.quality_run_id,
                metadata=EXCLUDED.metadata,
                certified_at=clock_timestamp(),
                updated_at=clock_timestamp()
            """,
            (
                METHODOLOGY_VERSION,
                DIVIDEND_TREATMENT,
                coverage_start,
                coverage_end,
                run_id,
                json.dumps(metadata, ensure_ascii=False),
            ),
        )


def _rebuild(
    conn,
    *,
    apply: bool,
    batch_size: int,
    max_dividend_yield: float,
    run_id: UUID | None,
    actions_base: str | None = None,
) -> RebuildSummary:
    if apply and actions_base is not None:
        raise ValueError("local actions cannot be used by apply rebuilds")
    asset_ids = _certified_asset_ids(conn)
    if not asset_ids:
        raise RuntimeError("certified KRX common-stock prices가 없습니다")
    sessions = _global_krx_sessions(conn)
    required_action_coverage_end = pd.to_datetime(sessions).max().date()
    source_price_start, source_price_end = _source_price_coverage(conn)
    identity = krx_common_stock_identity_digest(
        conn,
        coverage_start=CONTRACT_COVERAGE_START,
        coverage_end=required_action_coverage_end,
    )
    local_snapshot = (
        _prepare_local_action_snapshot(
            conn,
            actions_base,
            required_end=required_action_coverage_end,
            asset_identity_digest=identity.digest,
        )
        if actions_base is not None
        else None
    )
    certified_snapshot = (
        None
        if local_snapshot is not None
        else _certified_action_snapshot_run(
            conn,
            required_end=required_action_coverage_end,
        )
    )
    summary = RebuildSummary(
        apply=apply,
        run_id=str(run_id) if run_id is not None else None,
        action_snapshot_run_id=(
            str(certified_snapshot.run_id)
            if certified_snapshot is not None
            else None
        ),
        action_snapshot_manifest_sha256=(
            local_snapshot.manifest_sha256
            if local_snapshot is not None
            else certified_snapshot.manifest_sha256
        ),
        action_snapshot_digest=(
            local_snapshot.fingerprint
            if local_snapshot is not None
            else certified_snapshot.body_digest
        ),
        action_snapshot_body_count=(
            local_snapshot.body_count
            if local_snapshot is not None
            else certified_snapshot.body_count
        ),
        action_snapshot_action_count=(
            len(local_snapshot.actions)
            if local_snapshot is not None
            else certified_snapshot.action_count
        ),
        action_snapshot_coverage_start=(
            local_snapshot.coverage_start
            if local_snapshot is not None
            else certified_snapshot.coverage_start.isoformat()
        ),
        action_snapshot_coverage_end=(
            local_snapshot.coverage_end
            if local_snapshot is not None
            else certified_snapshot.coverage_end.isoformat()
        ),
        action_snapshot_input_action_count=(
            local_snapshot.input_action_count
            if local_snapshot is not None
            else certified_snapshot.input_action_count
        ),
        action_snapshot_excluded_action_count=(
            local_snapshot.excluded_action_count
            if local_snapshot is not None
            else certified_snapshot.excluded_action_count
        ),
        action_snapshot_included_corp_cls_counts=(
            local_snapshot.included_corp_cls_counts
            if local_snapshot is not None
            else certified_snapshot.included_corp_cls_counts
        ),
        action_snapshot_excluded_corp_cls_counts=(
            local_snapshot.excluded_corp_cls_counts
            if local_snapshot is not None
            else certified_snapshot.excluded_corp_cls_counts
        ),
        action_snapshot_excluded_reason_counts=(
            local_snapshot.excluded_reason_counts
            if local_snapshot is not None
            else certified_snapshot.excluded_reason_counts
        ),
        action_snapshot_source_receipts=(
            {} if local_snapshot is not None
            else certified_snapshot.source_receipts
        ),
        action_snapshot_published_actions=(
            {} if local_snapshot is not None
            else certified_snapshot.published_actions
        ),
        action_snapshot_disclosure_observation_audit=(
            {} if local_snapshot is not None
            else certified_snapshot.disclosure_observation_audit
        ),
        action_snapshot_cash_scale_evidence=(
            local_snapshot.cash_scale_evidence
            if local_snapshot is not None
            else certified_snapshot.cash_scale_evidence
        ),
        action_source=(
            "local_complete_bronze"
            if local_snapshot is not None
            else "rds_certified_snapshot"
        ),
        local_actions_base=(
            local_snapshot.base if local_snapshot is not None else None
        ),
        local_actions_fingerprint=(
            local_snapshot.fingerprint
            if local_snapshot is not None
            else None
        ),
        unmapped_action_count=(
            local_snapshot.unmapped_count
            if local_snapshot is not None
            else 0
        ),
        out_of_scope_action_count=(
            local_snapshot.out_of_scope_count
            if local_snapshot is not None
            else 0
        ),
        asset_identity_digest=identity.digest,
        asset_identity_row_count=identity.row_count,
        asset_identity_asset_count=identity.asset_count,
        source_price_coverage_start=source_price_start.isoformat(),
        source_price_coverage_end=source_price_end.isoformat(),
    )
    if apply:
        _create_temp_stages(conn)

    for batch_number, asset_batch in enumerate(
        _chunks(asset_ids, batch_size), start=1,
    ):
        prices = _certified_prices(conn, asset_batch)
        if local_snapshot is not None:
            actions = local_snapshot.actions[
                local_snapshot.actions["asset_id"].isin(asset_batch)
            ].copy()
        else:
            actions = _issuer_dart_actions(
                conn,
                asset_batch,
                certified_snapshot.run_id,
            )
        scale_source = (
            local_snapshot.scale_source_evidence
            if local_snapshot is not None
            else certified_snapshot.scale_source_evidence
        )
        if scale_source is None:
            scale_source = pd.DataFrame(columns=SOURCE_EVIDENCE_COLUMNS)
        batch_scale_source = scale_source[
            scale_source["asset_id"].isin(asset_batch)
        ].copy()
        batch = _build_batch(
            prices,
            actions,
            sessions,
            run_id=run_id,
            max_dividend_yield=max_dividend_yield,
            scale_source_evidence=batch_scale_source,
        )
        if apply:
            _publish_batch(conn, batch)
        summary.absorb(batch)
        print(
            "[total-return] "
            f"batch={batch_number} assets={len(asset_batch)} "
            f"prices={len(batch.prices)} actions={len(batch.audit)}",
            flush=True,
        )

    if summary.cash_action_count == 0:
        raise RuntimeError(
            "selected action source의 mapped ISSUER cash-dividend가 0건입니다; "
            "계약을 가격수익률로 잘못 인증하지 않습니다"
        )
    identity_after = krx_common_stock_identity_digest(
        conn,
        coverage_start=CONTRACT_COVERAGE_START,
        coverage_end=required_action_coverage_end,
    )
    if identity_after != identity:
        raise RuntimeError("KRX asset identity changed during total-return rebuild")
    return summary


def run(
    *,
    apply: bool = False,
    batch_size: int = 100,
    max_dividend_yield: float = 1.0,
    actions_base: str | None = None,
    conn=None,
) -> RebuildSummary:
    """Run a read-only preview or an explicit transaction-wide rebuild."""
    if apply and actions_base is not None:
        raise ValueError(
            "--actions-base is read-only preview evidence and cannot be "
            "combined with --apply"
        )
    owns_connection = conn is None
    connection = conn or db.connect()
    context = None
    rebuild_lock_acquired = False
    try:
        if not apply:
            # Put every dry-run query, including schema preflight, under a
            # database-enforced read-only transaction.  Local action preview
            # therefore cannot mutate RDS even if a future helper regresses.
            with connection.transaction():
                with connection.cursor() as cur:
                    cur.execute(
                        "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, "
                        "READ ONLY"
                    )
                repository.assert_schema(connection)
                _assert_contract_schema(connection)
                summary = _rebuild(
                    connection,
                    apply=False,
                    batch_size=batch_size,
                    max_dividend_yield=max_dividend_yield,
                    run_id=None,
                    actions_base=actions_base,
                )
            print(json.dumps(asdict(summary), ensure_ascii=False, indent=2))
            return summary

        repository.assert_schema(connection)
        _assert_contract_schema(connection)
        connection.commit()
        acquire_return_rebuild_lock(connection)
        rebuild_lock_acquired = True
        context = repository.start_run(
            connection,
            mode="krx_total_return_rebuild",
            status="RUNNING",
            partition_key=METHODOLOGY_VERSION,
        )
        try:
            _mark_contract_building(connection, context.run_id)
            with connection.transaction():
                with connection.cursor() as cur:
                    cur.execute(
                        "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"
                    )
                summary = _rebuild(
                    connection,
                    apply=True,
                    batch_size=batch_size,
                    max_dividend_yield=max_dividend_yield,
                    run_id=context.run_id,
                )
                _certify_contract(connection, summary, context.run_id)
                repository.finish_run(
                    connection,
                    context,
                    "CERTIFIED",
                    _pass_results(summary),
                    commit=False,
                )
                # Certification is not externally visible until an
                # independent reader re-derives every contract invariant from
                # the same uncommitted snapshot.  Any failed check raises and
                # rolls back price rows, receipt audit, run status and
                # contract promotion together.
                from pipeline.silver import total_return_audit

                independent_report = total_return_audit.audit(
                    conn=connection,
                    use_existing_transaction=True,
                )
                if not independent_report["safe_for_research"]:
                    failed_checks = sorted(
                        key for key, passed in independent_report[
                            "checks"
                        ].items() if not passed
                    )
                    raise RuntimeError(
                        "independent total-return certification audit failed: "
                        f"{failed_checks}"
                    )
        except Exception as exc:
            connection.rollback()
            failure = CheckResult(
                rule_code="KRX_TOTAL_RETURN_REBUILD",
                dataset="price_daily",
                severity=Severity.CRITICAL,
                status=CheckStatus.FAIL,
                expected="atomic certified KRX gross-total-return rebuild",
                actual=str(exc),
                failed_count=1,
            )
            repository.finish_run(
                connection,
                context,
                "FAILED",
                [failure],
                error_message=str(exc),
            )
            raise
        print(json.dumps(asdict(summary), ensure_ascii=False, indent=2))
        return summary
    finally:
        if rebuild_lock_acquired:
            try:
                release_return_rebuild_lock(connection)
                connection.commit()
            except Exception:
                connection.rollback()
        if owns_connection:
            connection.close()


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    write_mode = parser.add_mutually_exclusive_group()
    write_mode.add_argument(
        "--apply",
        action="store_true",
        help=(
            "standalone apply는 비활성화됨; 쓰기는 daily_full 또는 "
            "dart_silver_backfill_ecs closed orchestrator 전용"
        ),
    )
    write_mode.add_argument(
        "--actions-base",
        help=(
            "로컬 complete Bronze root의 DART actions로 read-only preview; "
            "--apply와 함께 사용할 수 없음"
        ),
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="메모리에 올릴 asset 수 (기본 100)",
    )
    parser.add_argument(
        "--max-dividend-yield",
        type=float,
        default=1.0,
        help="이 값을 초과한 개별 현금배당/직전 조정종가 비율은 차단",
    )
    return parser.parse_args(argv)


def main() -> None:
    args = parse_args()
    if not args.apply:
        run(
            apply=False,
            batch_size=args.batch_size,
            max_dividend_yield=args.max_dividend_yield,
            actions_base=args.actions_base,
        )
        return
    raise RuntimeError(
        "direct total-return --apply is disabled: it cannot prove parity with "
        "the latest raw DART action generation; use pipeline.daily_full or "
        "pipeline.dart_silver_backfill_ecs --phase dart-extras"
    )


if __name__ == "__main__":
    main()
