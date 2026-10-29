"""Finish only the interrupted filing and outstanding FMP tail."""
from datetime import date
import json
import os
from pathlib import Path
import tempfile

import boto3

import repair_interrupted_filing_20261021 as repair
from pipeline import daily_full, dart_silver_backfill_ecs as coordinator
from pipeline.gap_replay import _weekdays
from pipeline.gold import run as gold
from pipeline.silver_quality import freshness


def main():
    bucket = os.environ['S3_BRONZE_BUCKET']
    s3 = boto3.client('s3')
    checkpoint = 'ops/recovery/20261029/koas-gold-repair-complete.json'
    conn = coordinator.acquire_daily_certification_lock()
    try:
        if not coordinator.total_return_contract_ready(conn=conn):
            raise RuntimeError('core total-return recovery is not complete')
        try:
            done = json.loads(s3.get_object(Bucket=bucket, Key=checkpoint)['Body'].read())
        except s3.exceptions.NoSuchKey:
            done = None
        if done is not None:
            if done != {'filing': repair.FILING, 'objects': repair.OBJECTS,
                        'gold_from': '2026-10-22', 'gold_to': '2026-10-27'}:
                raise RuntimeError('unexpected recovery checkpoint')
            print('[tail-recovery] Koas Silver/Gold already completed', flush=True)
        else:
            with tempfile.TemporaryDirectory(prefix='koas-repair-') as temp:
                root = Path(temp)
                for key in repair.OBJECTS:
                    path = root / key
                    path.parent.mkdir(parents=True, exist_ok=True)
                    s3.download_file(bucket, key, str(path))
                bundle = repair.prepare(root)
                repair.apply_bundle(conn, bundle)
                gold.run_approved_range(conn, start_date=date(2026, 10, 22),
                                        end_date=date(2026, 10, 27), apply=True)
                s3.put_object(Bucket=bucket, Key=checkpoint, Body=json.dumps({
                    'filing': repair.FILING, 'objects': repair.OBJECTS,
                    'gold_from': '2026-10-22', 'gold_to': '2026-10-27',
                }, sort_keys=True).encode(), IfNoneMatch='*')
                print('[tail-recovery] Koas Silver and affected Gold COMPLETE', flush=True)
        for day in _weekdays('20261013', '20261027'):
            print(f'[tail-recovery] FMP start calendar_day={day}', flush=True)
            daily_full._run_fmp_incremental(bucket, Path('/app/data'), day,
                                           certification_lock=conn)
        print(f'[tail-recovery] COMPLETE {freshness.assert_fresh()}', flush=True)
    finally:
        coordinator.release_daily_certification_lock(conn)


if __name__ == '__main__':
    main()
