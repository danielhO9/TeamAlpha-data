"""Audited one-off recovery: reuse Sep 26 immutable context and checkpoints.

Run inside the deployed ECS image with its normal environment and shared lock.
No core daily collection, financial backfill, or total-return rebuild occurs.
"""
import json
import os
from datetime import date
from pathlib import Path

import exchange_calendars as xcals

from pipeline import dart_silver_backfill_ecs as locks
from pipeline import daily_full, kis_flows as k, kis_provider_daily as provider
from pipeline.bronze.kis_history import digest
from pipeline.common.sink import write_text
from pipeline.gold import run as gold

RESULT_KEY = ('daily_references/daily_results/'
              '827b57996c1c4fcbf79fc19beea5e9991bde893b6c5dde490b715460574e2a91.json')


def main():
    root = os.environ['KIS_BRONZE_ROOT'].rstrip('/')
    conn = locks.acquire_daily_certification_lock()
    try:
        previous = k.read_json(f'{root}/{RESULT_KEY}')
        assert len(previous['failures']) == 208
        assert {(v['start'], v['end']) for v in previous['failures']} == {
            ('2026-09-17', '2026-09-23')}
        # Object hash includes the signed document; universe_hash hashes the
        # unsigned contents. They are deliberately different identifiers.
        manifest_uri = (f'{root}/daily_references/manifests/'
                        '991a5b07bd39bc327ad8d7b1ca73a7cd74eff0eae85391ae2608fea7167d1283.json')
        assert k.read_json(manifest_uri)['sha256'] == previous['universe_hash']
        pending_state = k.read_json(previous['reference_state_uri'])
        assert digest({a:b for a,b in pending_state.items() if a!='sha256'}) == pending_state['sha256']
        assert date.fromisoformat(pending_state['through']) == date(2026,9,23)
        policy = k.checked_policy(k.read_json(os.environ['KIS_POLICY_URI']))
        current = k.read_json(os.environ['KIS_DAILY_REFERENCE_URI'])
        assert date.fromisoformat(current['through']) <= date(2026,9,23), 'newer recovery already ran'
        assert locks.total_return_contract_ready(conn=conn)
        assert locks.certified_krx_price_coverage_end(conn=conn) >= date(2026,9,23)
        print('[recovery] starting exact failed KIS window; certified checkpoints retained', flush=True)
        result = k.run(conn=conn, manifest_uri=manifest_uri,
                       policy_uri=os.environ['KIS_POLICY_URI'], root=root,
                       start=date(2026,9,17), end=date(2026,9,23),
                       publish=True, refresh=False)
        provider._freeze(root, 'recovery_results', result)
        if result['failures']:
            raise RuntimeError(f"KIS still incomplete: {len(result['failures'])} partitions")
        sessions = [x.date() for x in xcals.get_calendar('XKRX').sessions_in_range(
            '2026-09-17', '2026-09-23')]
        excluded = {v['date'] for v in policy.get('calendar_exclusions', [])}
        sessions = [d for d in sessions if str(d) not in excluded]
        coverage = provider.verify_collection(conn, manifest_uri, policy, sessions)
        write_text(json.dumps(pending_state, sort_keys=True), os.environ['KIS_DAILY_REFERENCE_URI'])
        print('[recovery] KIS COVERAGE PASS '+json.dumps(coverage), flush=True)

        target = date(2026,9,23)
        with conn.cursor() as cur:
            cur.execute("""SELECT count(*), count(*) FILTER (WHERE EXISTS (
                SELECT 1 FROM gold.factor_value v
                WHERE v.factor_id=f.factor_id AND v.as_of_date=%s))
                FROM gold.factor f WHERE f.status='APPROVED'""", (target,))
            total, ready = cur.fetchone()
        if not total:
            raise RuntimeError('no approved Gold factors')
        if ready == 0:
            gold.run_approved_daily(conn, as_of_date=target, apply=True)
        elif ready != total:
            raise RuntimeError('partially populated Gold date; inspect before replacing')
        else:
            print('[recovery] Gold already populated; skipped', flush=True)
        # Sep 24-25 are KRX holidays. They need no Gold date, but their US
        # sessions still need the scheduled FMP increment.
        for krx_day in ('20260923', '20260924', '20260925'):
            daily_full._run_fmp_incremental(os.environ['S3_BRONZE_BUCKET'], Path('/app/data'),
                                           krx_day, certification_lock=conn)
        print('[recovery] COMPLETE through scheduled target 20260925', flush=True)
    finally:
        locks.release_daily_certification_lock(conn)


if __name__ == '__main__':
    main()
