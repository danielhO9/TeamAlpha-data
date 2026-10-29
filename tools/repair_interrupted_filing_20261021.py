"""Repair exactly the interrupted Koas filing from verified Bronze objects.

No collection, price publication, action publication or return rebuild occurs.
Apply requires the shared certification lock and completed core/Gold recovery.
"""
from __future__ import annotations

import argparse
from datetime import date
import hashlib
import json
from pathlib import Path

import pandas as pd

from pipeline import dart_silver_backfill_ecs as coordinator
from pipeline.silver import dividends, financials
from pipeline.silver_quality import repository
from pipeline.silver_quality.models import CandidateBundle
from pipeline.silver_quality.runner import evaluate, assert_publishable, print_summary
from pipeline.gold import run as gold_run

FILING = '20261021000109'
PREFIX = 'dividends/dart/alot-matter/year=2026/report=11012/corp=071950/rcept=' + FILING
OBJECTS = {
    'financials/dart/year=2026/corp=071950/11012.json': '1ac5d44b4d725c54da1d8fc1fdca003401b9b1e064dd0c4e3dbc6df78ffbac55',
    PREFIX + '/response.json': '90b52d33843588ebb4edb27683410567012087b80456972629b688f37b98d5d6',
    PREFIX + '/manifest.json': 'ba652e1c6856294948b45b8871efc9e4fd95f21ac510ca64cfa8af46097adfaf',
}


def prepare(root: Path):
    for key, expected in OBJECTS.items():
        if hashlib.sha256((root / key).read_bytes()).hexdigest() != expected:
            raise ValueError('Bronze hash changed: ' + key)
    financial, stats = financials.prepare(str(root), files=[str(root / next(iter(OBJECTS)))])
    dividend, d_stats = dividends.prepare(str(root), files=[str(root / PREFIX / 'response.json')])
    frame = pd.concat([financial, dividend], ignore_index=True)
    if frame.empty or set(frame['filing_id']) != {FILING} or set(frame['identifier']) != {'071950'}:
        raise ValueError('unexpected or empty repair scope')
    if frame['available_date'].min() != date(2026, 10, 22):
        raise ValueError('unexpected repaired availability date')
    stats = dict(stats)
    stats['dividend'] = d_stats
    for key in ('input_rows', 'transformed_rows', 'excluded_rows', 'rejected_rows'):
        stats[key] = int(stats.get(key, 0)) + int(d_stats.get(key, 0))
    return CandidateBundle(fundamentals=frame, stats={'fundamental': stats})


def apply_bundle(conn, bundle):
    # Recovery prerequisites prevent this repair from becoming a second replay.
    with conn.cursor() as cur:
        cur.execute("SELECT asset_id FROM asset_identifier WHERE source='KRX' AND identifier='071950' AND valid_to IS NULL")
        owners = cur.fetchall()
        if owners != [(4569,)]:
            raise RuntimeError('Koas identifier ownership changed')
        cur.execute("SELECT max(target_date) FROM dq_run WHERE mode='daily' AND status='CERTIFIED'")
        if cur.fetchone()[0] < date(2026, 10, 27):
            raise RuntimeError('core recovery not complete')
        cur.execute("""SELECT count(*), count(latest), min(latest), max(latest) FROM (
            SELECT (SELECT v.as_of_date FROM gold.factor_value v
                    WHERE v.factor_id=f.factor_id ORDER BY v.as_of_date DESC LIMIT 1) latest
            FROM gold.factor f WHERE f.status='APPROVED') s""")
        if cur.fetchone() != (49, 49, date(2026, 10, 27), date(2026, 10, 27)):
            raise RuntimeError('expected October 27 Gold recovery is not complete')
    bundle.stats['_existing_krx_identifiers'] = {'071950'}
    results = evaluate(bundle)
    print_summary(results)
    assert_publishable(results)
    fingerprint = hashlib.sha256(json.dumps(OBJECTS, sort_keys=True).encode()).hexdigest()
    context = repository.start_run(conn, mode='interrupted_filing_repair',
                                   target_date=date(2026, 10, 21), input_fingerprint=fingerprint)
    try:
        with conn.transaction():
            count = financials.publish(conn, bundle.fundamentals, {'071950': 4569}, context.run_id)
            if count != len(bundle.fundamentals):
                raise RuntimeError('repair publication count mismatch')
            repository.save_metrics(conn, context.run_id, bundle)
            repository.finish_run(conn, context, 'CERTIFIED', results, commit=False)
        print(f'[filing-repair] CERTIFIED run={context.run_id} rows={count}', flush=True)
    except Exception as exc:
        repository.finish_run(conn, context, 'FAILED', results, error_message=str(exc))
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--base', required=True, help='isolated directory with the three pinned Bronze files')
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    bundle = prepare(Path(args.base).resolve())
    bundle.stats['_existing_krx_identifiers'] = {'071950'}
    checks = evaluate(bundle)
    print_summary(checks)
    assert_publishable(checks)
    print(json.dumps({'rows': len(bundle.fundamentals),
                      'earliest_available': str(bundle.fundamentals['available_date'].min()),
                      'filing': FILING, 'apply': args.apply}), flush=True)
    if args.apply:
        conn = coordinator.acquire_daily_certification_lock()
        try:
            if not coordinator.total_return_contract_ready(conn=conn):
                raise RuntimeError('total-return recovery must finish before repair')
            apply_bundle(conn, bundle)
            # A changed financial observation also changes cross-sectional
            # ranks. Recompute only the four affected sessions, preserving
            # all Gold dates before the filing became available.
            first = bundle.fundamentals['available_date'].min()
            gold_run.run_approved_range(conn, start_date=first,
                                        end_date=date(2026, 10, 27), apply=True)
            print('[filing-repair] COMPLETE Silver and affected Gold dates', flush=True)
        finally:
            coordinator.release_daily_certification_lock(conn)


if __name__ == '__main__':
    main()
