"""Publish and load an immutable listing-period contract after RDS audit."""
from __future__ import annotations

import re
from datetime import date, datetime

from psycopg.types.json import Jsonb

from pipeline.bronze.kis_history import digest
from pipeline.silver_quality import repository
from pipeline.silver_quality.models import CheckResult, CheckStatus, Severity


def validate(contract, asset_ids, start, end):
    body = {k: v for k, v in contract.items() if k != 'snapshot_id'}
    if contract.get('schema') != 'asset-listing-snapshot-v1' or digest(body) != contract.get('snapshot_id'):
        raise ValueError('invalid listing snapshot fingerprint')
    if not contract.get('evidence_uri') or any(not re.fullmatch(r'[0-9a-f]{64}', contract.get(k, ''))
            for k in ('audit_sha256', 'evidence_sha256')):
        raise ValueError('listing snapshot requires evidence and audit')
    observed = datetime.fromisoformat(contract['observed_at'])
    if observed.tzinfo is None:
        raise ValueError('listing observation must be timezone aware')
    if not date.fromisoformat(contract['coverage_start']) <= start <= end <= date.fromisoformat(contract['verified_through']):
        raise ValueError('listing snapshot does not cover requested dates')
    ids = contract['asset_ids']
    if len(ids) != len(set(ids)) or set(ids) != set(asset_ids):
        raise ValueError('listing snapshot universe mismatch')
    grouped = {}
    for row in contract['periods']:
        aid = row['asset_id']
        if aid not in ids or not re.fullmatch(r'[0-9A-Z]{6}', row['ticker']):
            raise ValueError('invalid listing asset or ticker')
        lo = date.fromisoformat(row['start'])
        hi = date.fromisoformat(row['end']) if row['end'] else date.max
        if hi < lo:
            raise ValueError('invalid listing period')
        grouped.setdefault(aid, []).append((lo, hi, row['ticker']))
    if set(grouped) != set(ids):
        raise ValueError('missing listing periods')
    for periods in grouped.values():
        ordered = sorted(periods)
        if any(a[1] >= b[0] for a, b in zip(ordered, ordered[1:])):
            raise ValueError('overlapping listing periods')
    return contract['periods']


def load(cur, snapshot_id, asset_ids, start, end):
    cur.execute('''SELECT s.payload,q.status FROM asset_listing_snapshot s
        JOIN dq_run q ON q.run_id=s.quality_run_id WHERE s.snapshot_id=%s''', (snapshot_id,))
    row = cur.fetchone()
    if not row or row[1] != 'CERTIFIED':
        raise ValueError('listing snapshot missing or uncertified')
    return validate(row[0], asset_ids, start, end)


def publish(conn, contract, audit):
    """Metadata certification is distinct from complete price/flow coverage."""
    start = date.fromisoformat(contract['coverage_start'])
    end = date.fromisoformat(contract['verified_through'])
    periods = validate(contract, contract['asset_ids'], start, end)
    if digest(audit) != contract['audit_sha256']:
        raise ValueError('listing audit fingerprint mismatch')
    if audit.get('asset_ids') != contract['asset_ids'] or audit.get('periods_sha256') != digest(periods):
        raise ValueError('listing audit covers different assets or periods')
    if audit.get('coverage_start') != str(start) or audit.get('verified_through') != str(end):
        raise ValueError('listing audit covers different dates')
    for key in ('identity_errors', 'prices_outside_periods', 'duplicate_prices', 'uncertified_prices', 'expected_price_gaps'):
        if audit.get(key) != 0:
            raise ValueError(f'listing audit failed: {key}')
    with conn.cursor() as cur:
        cur.execute('SELECT snapshot_id FROM asset_listing_snapshot WHERE snapshot_id=%s', (contract['snapshot_id'],))
        if cur.fetchone():
            load(cur, contract['snapshot_id'], contract['asset_ids'], start, end)
            conn.rollback()
            return contract['snapshot_id']
    conn.rollback()
    ctx = repository.start_run(conn, mode='asset_listing_metadata', input_fingerprint=contract['snapshot_id'])
    try:
        with conn.transaction():
            with conn.cursor() as cur:
                # Reject stale or ambiguous mappings at publication, even if
                # the earlier read-only audit succeeded.
                for aid in contract['asset_ids']:
                    rows = [r for r in periods if r['asset_id'] == aid]
                    for r in rows:
                        lo = max(start, date.fromisoformat(r['start']))
                        hi = min(end, date.fromisoformat(r['end'])) if r['end'] else end
                        if lo > hi:
                            continue
                        cur.execute('''SELECT asset_id,valid_from,valid_to FROM asset_identifier
                            WHERE source='KRX' AND identifier_type='ticker' AND identifier=%s
                            AND valid_from<=%s AND (valid_to IS NULL OR valid_to>=%s)''', (r['ticker'], hi, lo))
                        matches = cur.fetchall()
                        if len(matches) != 1 or matches[0][0] != aid or matches[0][1] > lo or (matches[0][2] is not None and matches[0][2] < hi):
                            raise ValueError('listing identifier changed after audit')
                cur.execute('''INSERT INTO asset_listing_snapshot
                    (snapshot_id,payload,source_uri,observed_at,quality_run_id)
                    VALUES (%s,%s,%s,%s,%s)''', (contract['snapshot_id'], Jsonb(contract),
                    contract['evidence_uri'], contract['observed_at'], ctx.run_id))
            result = CheckResult('LISTING_SOURCE_IDENTITY_PERIODS', 'asset_listing_snapshot', Severity.ERROR,
                CheckStatus.PASS, 'source-backed periods and RDS identity/price audit', str(len(periods)))
            repository.finish_run(conn, ctx, 'CERTIFIED', [result], commit=False)
    except Exception as exc:
        conn.rollback()
        result = CheckResult('LISTING_ATOMIC_PUBLISH', 'asset_listing_snapshot', Severity.ERROR,
            CheckStatus.FAIL, 'atomic certified snapshot', str(exc), 1)
        repository.finish_run(conn, ctx, 'FAILED', [result], error_message=str(exc))
        raise
    return contract['snapshot_id']
