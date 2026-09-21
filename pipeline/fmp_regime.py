"""FMP regime inputs: immutable Bronze receipts and versioned Silver observations.

Backfills retain actual receipt times; historical dates do not imply PIT availability.
No score or trading signal is computed here.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
from datetime import date, datetime, timedelta

from psycopg.types.json import Jsonb

from pipeline.bronze.fmp import FMPClient, collect_raw, verify_raw_object
from pipeline.common import db
from pipeline.common.paths import base_uri
from pipeline.common.sink import read_bytes
from pipeline.silver_quality import migrate, repository
from pipeline.silver_quality.models import CheckResult, CheckStatus, Severity

SYMBOLS = {'^VIX': 'index', 'DX-Y.NYB': 'index', '^SOX': 'index',
           '^GSPC': 'index', 'HYG': 'etf', 'IEF': 'etf', 'LQD': 'etf', 'TLT': 'etf'}
MATURITIES = ('month1', 'month2', 'month3', 'month6', 'year1', 'year2',
              'year3', 'year5', 'year7', 'year10', 'year20', 'year30')
REQUIRED_MATURITIES = ('month3', 'year2', 'year10')


def windows(start, end):
    """Bound to 28 days: Treasury endpoint silently truncates long requests."""
    if start > end:
        raise ValueError('start must not exceed end')
    while start <= end:
        last = min(end, start + timedelta(days=27))
        yield start, last
        start = last + timedelta(days=1)


def number(value):
    if isinstance(value, bool) or value is None:
        raise ValueError('missing or boolean numeric value')
    value = float(value)
    if not math.isfinite(value):
        raise ValueError('nonfinite value')
    return value


def parse(body, series, start, end, *, excluded=None):
    data = json.loads(body)
    if not isinstance(data, list):
        raise ValueError('FMP response must be an array')
    seen, rows = set(), []
    for raw in data:
        day = date.fromisoformat(raw['date'])
        if not start <= day <= end or day in seen:
            raise ValueError('duplicate or out-of-range date')
        seen.add(day)
        if series == 'US_TREASURY':
            for key in REQUIRED_MATURITIES:
                number(raw.get(key))
            for key in MATURITIES:
                if raw.get(key) is not None:
                    rows.append((day, key, number(raw[key]), 'percent', raw))
        else:
            if raw.get('symbol') != series:
                raise ValueError('symbol mismatch')
            values = {key: number(raw.get(key)) for key in ('open', 'high', 'low', 'close')}
            # DXY sometimes emits zero weekend placeholders. Keep the raw row
            # and audit its exclusion; do not forward-fill prices.
            if series == 'DX-Y.NYB' and day.weekday() >= 5 and values['close'] == 0:
                if excluded is not None:
                    excluded.append({'date': str(day), 'reason': 'DXY_ZERO_WEEKEND'})
                continue
            if min(values.values()) <= 0 or values['high'] < max(values['open'], values['close']) or values['low'] > min(values['open'], values['close']):
                raise ValueError('invalid OHLC')
            if raw.get('volume') is not None and number(raw['volume']) < 0:
                raise ValueError('negative volume')
            rows.append((day, 'close', values['close'], 'USD' if SYMBOLS[series] == 'etf' else 'index_points', raw))
    return rows


def publish(conn, rows, *, series, uri, received_at, fingerprint, excluded=()):
    context = repository.start_run(conn, mode='fmp_regime', input_fingerprint=fingerprint)
    check = CheckResult('FMP_REGIME_VALID', 'fmp_regime_observation', Severity.ERROR,
                        CheckStatus.PASS, 'shape, date, identity, finite values and OHLC validated', str(len(rows)))
    checks = [check]
    if excluded:
        checks.append(CheckResult('FMP_REGIME_WEEKEND_EXCLUDED', 'fmp_regime_observation',
            Severity.MODIFIED, CheckStatus.PASS, 'exclude zero DXY weekend placeholders',
            str(len(excluded)), samples=list(excluded)))
    try:
        with conn.transaction():
            with conn.cursor() as cur:
                for day, metric, value, unit, payload in rows:
                    revision = hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
                    cur.execute('''INSERT INTO fmp_regime_observation
                        (series,observation_date,metric,value,unit,payload,revision,
                         observed_at,source_uri,quality_run_id)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT DO NOTHING''',
                        (series, day, metric, value, unit, Jsonb(payload), revision,
                         received_at, uri, context.run_id))
            repository.finish_run(conn, context, 'CERTIFIED', checks, commit=False)
    except Exception as exc:
        conn.rollback()
        repository.finish_run(conn, context, 'FAILED', [], error_message=type(exc).__name__)
        raise


def collect_snapshot(client, *, root, endpoint, params, prefix):
    """Resume intact receipts; never repair existing snapshot objects in place.

    A payload without its manifest (or the reverse) has no complete receipt.
    Preserve either partial object and require a new refresh ID for recollection.
    """
    base = root.rstrip('/') + '/' + prefix.strip('/')
    uris = [base + '/response.json', base + '/manifest.json']
    body, manifest = (read_bytes(uri) for uri in uris)
    if body is not None or manifest is not None:
        try:
            receipt = json.loads(manifest) if manifest is not None else None
            valid = (body is not None and isinstance(receipt, dict)
                     and verify_raw_object(*uris)
                     and receipt.get('provider') == 'FMP'
                     and receipt.get('endpoint') == endpoint
                     and receipt.get('request_params') == params
                     and receipt.get('status_code') == 200)
        except (TypeError, ValueError):
            valid = False
        if not valid:
            raise ValueError('existing Bronze snapshot is corrupt or incomplete; '
                             'preserve it and use a new --refresh-id')
        return uris
    return collect_raw(client, root=root, endpoint=endpoint, params=params,
                       prefix=prefix, extension='json')


def run(start, end, *, dest='local', apply=False, refresh_id=None, client=None):
    client = client or FMPClient(min_interval=0.15)
    root = base_uri(dest)
    refresh_id = refresh_id or 'backfill-v1'
    if not refresh_id.replace('-', '').replace('_', '').isalnum():
        raise ValueError('invalid refresh id')
    conn = db.connect() if apply else None
    totals, latest, exclusions = {}, {}, {}
    try:
        if conn:
            migrate.assert_current(conn)
        for first, last in windows(start, end):
            for series in (*SYMBOLS, 'US_TREASURY'):
                endpoint = 'treasury-rates' if series == 'US_TREASURY' else 'historical-price-eod/full'
                params = {'from': first.isoformat(), 'to': last.isoformat()}
                if series != 'US_TREASURY':
                    params['symbol'] = series
                prefix = f'regime/fmp/series={series}/from={first}/to={last}/snapshot={refresh_id}'
                uris = collect_snapshot(client, root=root, endpoint=endpoint,
                                        params=params, prefix=prefix)
                if not verify_raw_object(*uris):
                    raise ValueError('Bronze checksum mismatch')
                body = read_bytes(uris[0])
                receipt = json.loads(read_bytes(uris[1]))
                received = datetime.fromisoformat(receipt['received_at'])
                if received.tzinfo is None:
                    raise ValueError('naive receipt timestamp')
                excluded = []
                rows = parse(body, series, first, last, excluded=excluded)
                exclusions[series] = exclusions.get(series, 0) + len(excluded)
                if not rows and (last-first).days >= 7:
                    raise ValueError(f'empty weekly window: {series} {first} {last}')
                totals[series] = totals.get(series, 0) + len(rows)
                if rows:
                    latest[series] = max(latest.get(series, date.min), max(r[0] for r in rows))
                if conn and rows:
                    publish(conn, rows, series=series, uri=uris[0], received_at=received,
                            fingerprint=receipt['sha256'], excluded=excluded)
            print(f'[fmp-regime] checked {first}..{last}', flush=True)
        return {'counts': totals, 'excluded': exclusions,
                'latest': {s: d.isoformat() for s, d in latest.items()}, 'published': apply}
    finally:
        if conn:
            conn.close()


def run_daily(day, *, dest='s3'):
    target = datetime.strptime(day, '%Y%m%d').date()
    result = run(target-timedelta(days=10), target, dest=dest, apply=True,
                 refresh_id='daily-'+day)
    stale = [s for s in (*SYMBOLS, 'US_TREASURY')
             if s not in result['latest'] or
             (target-date.fromisoformat(result['latest'][s])).days > 6]
    if stale:
        raise RuntimeError(f'FMP regime stale series: {stale}')
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--start', type=date.fromisoformat, default=date(2015, 1, 1))
    parser.add_argument('--end', type=date.fromisoformat, required=True)
    parser.add_argument('--dest', choices=('local', 's3'), default='local')
    parser.add_argument('--apply', action='store_true', help='Publish validated observations to Silver')
    parser.add_argument('--refresh-id', help='New immutable snapshot ID to re-fetch existing dates')
    args = parser.parse_args()
    print(json.dumps(run(args.start, args.end, dest=args.dest, apply=args.apply,
                         refresh_id=args.refresh_id), sort_keys=True))


if __name__ == '__main__':
    main()
