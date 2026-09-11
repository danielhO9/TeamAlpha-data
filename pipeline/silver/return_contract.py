"""Lifecycle helpers for the derived KRX total-return contract.

Raw KRX prices and issuer cash-dividend actions are inputs to
``total_return_close``.  Publishing either input invalidates any prior
certification in the same transaction. The closed incremental rebuild promotes
the contract after recomputing changed assets, extending unchanged assets, and
independently auditing the complete inherited lineage.

Migration 009 is intentionally optional while older development databases and
unit-test fixtures are still in use.  Missing contract tables therefore make
invalidation a safe no-op; absence of a contract can never be mistaken for a
certified one by research readers.
"""
from __future__ import annotations

import re
from uuid import UUID


CONTRACT_SOURCE = "KRX"
CONTRACT_ASSET_TYPE = "stock"
CONTRACT_FIELD = "total_return_close"
CONTRACT_RELEASE = "krx_total_return_v3_cash_scale_evidence_2026_08"
# KRX began assigning six-character uppercase alphanumeric short codes after
# the numeric namespace became constrained.  They are first-class ticker
# identifiers (for example 0008Z0), not malformed numeric codes.
KRX_TICKER_REGEX = r"^[0-9A-Z]{6}$"
_KRX_TICKER_PATTERN = re.compile(KRX_TICKER_REGEX)
# Stable signed bigint shared by every KRX price/action writer and the rebuild.
# Session-locking the rebuild prevents a source writer from invalidating or
# changing inputs between BUILDING and CERTIFIED.
RETURN_WRITER_LOCK_KEY = 5_248_954_287_015_001


def normalize_krx_ticker(value: object) -> str:
    """Canonicalize a KRX short code without changing alphanumeric codes."""
    rendered = str(value or "").strip().upper()
    if rendered.isdigit() and len(rendered) <= 6:
        return rendered.zfill(6)
    return rendered


def is_valid_krx_ticker(value: object) -> bool:
    return _KRX_TICKER_PATTERN.fullmatch(normalize_krx_ticker(value)) is not None


def acquire_return_writer_transaction_lock(conn) -> None:
    """Serialize a source writer with the total-return rebuild."""
    with conn.cursor() as cur:
        cur.execute("SELECT pg_advisory_xact_lock(%s)", (RETURN_WRITER_LOCK_KEY,))


def acquire_return_rebuild_lock(conn) -> None:
    """Hold the common writer lock for the rebuild connection lifetime."""
    with conn.cursor() as cur:
        cur.execute("SELECT pg_advisory_lock(%s)", (RETURN_WRITER_LOCK_KEY,))


def release_return_rebuild_lock(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("SELECT pg_advisory_unlock(%s)", (RETURN_WRITER_LOCK_KEY,))


def invalidate_krx_total_return(
    conn,
    *,
    reason: str,
    quality_run_id: UUID | None,
) -> bool:
    """Atomically demote an existing KRX total-return contract to BUILDING.

    Returns ``True`` when the contract row existed and was updated.  The
    caller owns the transaction; a failed price/action publish rolls this
    invalidation back with the source write.
    """
    rendered_reason = str(reason).strip()
    if not rendered_reason:
        raise ValueError("total-return invalidation reason must be non-empty")

    with conn.cursor() as cur:
        cur.execute(
            "SELECT to_regclass('public.price_return_contract')"
        )
        row = cur.fetchone()
        if not row or row[0] is None:
            return False
        cur.execute(
            """
            UPDATE price_return_contract
            SET status='BUILDING',
                quality_run_id=%s,
                metadata=coalesce(metadata, '{}'::jsonb) ||
                    jsonb_strip_nulls(jsonb_build_object(
                        'invalidated_reason', %s::text,
                        'invalidated_by_run_id', %s::uuid::text,
                        'invalidated_at', now()
                    )),
                certified_at=NULL,
                updated_at=now()
            WHERE source=%s
              AND asset_type=%s
              AND field_name=%s
            """,
            (
                quality_run_id,
                rendered_reason,
                quality_run_id,
                CONTRACT_SOURCE,
                CONTRACT_ASSET_TYPE,
                CONTRACT_FIELD,
            ),
        )
        return cur.rowcount > 0
