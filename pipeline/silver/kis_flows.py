"""Source-specific checks and atomic, versioned KIS Silver publication."""
from __future__ import annotations

import json
from datetime import datetime, time, timedelta, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo

from psycopg.types.json import Jsonb

from pipeline.bronze.kis_history import digest
from pipeline.silver_quality import repository
from pipeline.silver_quality.models import CheckResult, CheckStatus, Severity

CATEGORIES = ('prsn', 'orgn', 'frgn')
METRICS = ('seln_vol', 'shnu_vol', 'ntby_qty', 'seln_tr_pbmn', 'shnu_tr_pbmn', 'ntby_tr_pbmn')


def integer(row, key, *, nonnegative=False):
    if row.get(key) in (None, ''):
        raise ValueError(f'missing {key}')
    value = Decimal(str(row[key]))
    if not value.is_finite() or value != value.to_integral_value():
        raise ValueError(f'non-integer {key}')
    if nonnegative and value < 0:
        raise ValueError(f'negative {key}')
    return int(value)


def investor(row):
    values = {}
    for cat in CATEGORIES:
        for metric in METRICS:
            key = f'{cat}_{metric}'
            value = integer(row, key, nonnegative=not metric.startswith('ntby'))
            values[key] = value * (1_000_000 if metric.endswith('pbmn') else 1)
        if values[f'{cat}_shnu_vol'] - values[f'{cat}_seln_vol'] != values[f'{cat}_ntby_qty']:
            raise ValueError(f'{cat}: buy-sell quantity mismatch')
        # KIS rounds each amount independently to million KRW.
        residual = values[f'{cat}_shnu_tr_pbmn'] - values[f'{cat}_seln_tr_pbmn'] - values[f'{cat}_ntby_tr_pbmn']
        if abs(residual) > 1_000_000:
            raise ValueError(f'{cat}: buy-sell amount mismatch')
    return values


def short_sale(row, volume_row, *, same_market):
    qty = integer(row, 'ssts_cntg_qty', nonnegative=True)
    amount = integer(row, 'ssts_tr_pbmn', nonnegative=True)
    volume = integer(volume_row, 'acml_vol', nonnegative=True)
    if same_market and qty > volume:
        raise ValueError('short quantity exceeds original daily volume')
    ratio = Decimal(qty) * 100 / volume if same_market and volume else None
    return {'short_volume': qty, 'short_amount_krw': amount,
            'original_daily_volume': volume,
            'short_ratio_pct': str(ratio) if ratio is not None else None,
            'ratio_status': 'VALID' if ratio is not None else ('ZERO_DENOMINATOR' if same_market else 'MARKET_SCOPE_UNVERIFIED'),
            'provider_volume': row.get('acml_vol'),
            'provider_ratio_pct': row.get('ssts_vol_rlim')}


def combined(krx, nxt, provider):
    result = {key: krx[key] + nxt[key] for key in krx}
    for key, value in result.items():
        tolerance = 1_000_000 if key.endswith('pbmn') else 0
        if abs(value - provider[key]) > tolerance:
            raise ValueError(f'KRX+NXT vs UN mismatch: {key}')
    return result


def observation(asset_id, ticker, day, venue, kind, values, receipts, policy):
    if not receipts:
        raise ValueError('missing raw evidence')
    observed = max(datetime.fromisoformat(r['fetched_at']) for r in receipts)
    if any(datetime.fromisoformat(r['fetched_at']).tzinfo is None for r in receipts):
        raise ValueError('naive observation timestamp')
    lag = int(policy['availability_lag_calendar_days'])
    hour = int(policy['availability_hour_kst'])
    minute = int(policy.get('availability_minute_kst', 30))
    if not 0 <= minute < 60:
        raise ValueError('invalid availability minute')
    if lag < 1 or not 0 <= hour < 24 or not policy['version']:
        raise ValueError('explicit conservative availability policy required')
    available = datetime.combine(day + timedelta(days=lag), time(hour, minute), ZoneInfo('Asia/Seoul'))
    revision = digest({'values': values, 'policy': policy, 'kind': kind, 'venue': venue})
    return {'asset_id': int(asset_id), 'ticker': ticker, 'trade_date': day,
            'venue': venue, 'kind': kind, 'revision': revision, 'values': values,
            'first_observed_at': observed, 'research_available_at': available,
            'availability_basis': 'POLICY_ASSUMPTION', 'policy_version': policy['version'],
            'source_uris': sorted({r['raw_uri'] for r in receipts}),
            'historical_revision_risk': True}


def publish(conn, rows, *, fingerprint):
    """Caller has validated a complete partition; certify in the same transaction."""
    ctx = repository.start_run(conn, mode='kis_market_flows', input_fingerprint=fingerprint)
    result = CheckResult('KIS_PARTITION', 'kis_market_observation', Severity.ERROR,
                         CheckStatus.PASS, 'complete source/identity/unit checks', str(len(rows)))
    try:
        with conn.transaction():
            with conn.cursor() as cur:
                for r in rows:
                    # Validate historical identifier against the actual RDS asset mapping.
                    cur.execute('''SELECT DISTINCT asset_id FROM asset_identifier
                        WHERE source='KRX' AND identifier_type='ticker' AND identifier=%s
                          AND valid_from<=%s AND (valid_to IS NULL OR valid_to>=%s)''',
                        (r['ticker'], r['trade_date'], r['trade_date']))
                    if {x[0] for x in cur.fetchall()} != {r['asset_id']}:
                        raise ValueError('ambiguous or changed historical ticker mapping')
                    lock_key = int(digest([r['asset_id'],str(r['trade_date']),r['venue'],r['kind']])[:15],16)
                    cur.execute('SELECT pg_advisory_xact_lock(%s)',(lock_key,))
                    cur.execute('''SELECT revision FROM kis_market_observation
                        WHERE asset_id=%s AND trade_date=%s AND venue=%s AND kind=%s
                        ORDER BY first_observed_at DESC,revision DESC LIMIT 1''',
                        (r['asset_id'],r['trade_date'],r['venue'],r['kind']))
                    previous=cur.fetchone()
                    if previous and previous[0]==r['revision']:
                        continue  # preserve the first observation of an unchanged version
                    cur.execute('''INSERT INTO kis_market_observation
                        (asset_id,ticker,trade_date,venue,kind,revision,payload,
                         first_observed_at,research_available_at,availability_basis,
                         policy_version,source_uris,historical_revision_risk,quality_run_id)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (asset_id,trade_date,venue,kind,revision,first_observed_at) DO NOTHING''',
                        (r['asset_id'],r['ticker'],r['trade_date'],r['venue'],r['kind'],r['revision'],
                         Jsonb(r['values']),r['first_observed_at'],r['research_available_at'],
                         r['availability_basis'],r['policy_version'],r['source_uris'],True,ctx.run_id))
            repository.finish_run(conn, ctx, 'CERTIFIED', [result], commit=False)
    except Exception as exc:
        conn.rollback()
        failed = CheckResult('KIS_ATOMIC_PUBLISH','kis_market_observation',Severity.ERROR,
                             CheckStatus.FAIL,'atomic checked publish',str(exc),1)
        repository.finish_run(conn,ctx,'FAILED',[failed],error_message=str(exc))
        raise
    return str(ctx.run_id)
