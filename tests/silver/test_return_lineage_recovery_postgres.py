"""Keep non-null values with uncertified lineage out of incremental reuse."""
from datetime import date
from pathlib import Path
import shutil
import subprocess
import tempfile
from uuid import uuid4

import psycopg
import pytest

import pipeline.silver.total_return_rebuild as rebuild

from pipeline.silver.total_return_rebuild import (
    IncrementalBaseline, _changed_return_asset_ids,
)


@pytest.mark.skipif(any(shutil.which(x) is None for x in ('initdb', 'pg_ctl')),
                    reason='PostgreSQL binaries unavailable')
def test_incremental_rebuild_selects_uncertified_nonnull_returns(tmp_path, monkeypatch):
    socket = Path(tempfile.mkdtemp(prefix='tr-lineage-', dir='/tmp'))
    data = tmp_path / 'pg'
    def command(*args):
        subprocess.run(args, check=True, capture_output=True, text=True)
    started = False
    try:
        command('initdb', '-D', str(data), '-A', 'trust', '-U', 'postgres',
                '--no-locale', '--encoding=UTF8')
        command('pg_ctl', '-D', str(data), '-l', str(tmp_path / 'pg.log'),
                '-o', f"-F -k {socket} -h '' -p 55439", '-w', 'start')
        started = True
        with psycopg.connect(host=str(socket), port=55439, user='postgres') as c:
            text_columns = '''source action_key action_type action_scope cash_amount
                filing_id cash_amount_status source_evidence_status
                correction_of_action_key revision_root_action_key revision_kind
                viewer_evidence_sha256 economic_evidence_sha256 reviewed_correction_id
                payment_date_quality_status'''.split()
            c.execute('CREATE TEMP TABLE corporate_action (asset_id int, quality_run_id uuid, '
                      'announcement_date date, ex_date date, record_date date, '
                      + ','.join(x + ' text' for x in text_columns) + ')')
            c.execute('CREATE TEMP TABLE cash_adjustment_scale_source_evidence '
                      '(asset_id int,evidence_key text,manifest_row_sha256 text,action_snapshot_run_id uuid)')
            c.execute('CREATE TEMP TABLE asset '
                      '(asset_id int,asset_type text,instrument_type text,exchange text)')
            c.execute('CREATE TEMP TABLE dq_run (run_id uuid,status text,mode text)')
            c.execute('CREATE TEMP TABLE price_daily '
                      '(asset_id int,source text,market text,trade_date date,quality_run_id uuid,'
                      'total_return_close numeric,total_return_quality_run_id uuid,adj_close numeric DEFAULT 100)')
            raw, good, failed, wrong, bad_raw = [uuid4() for _ in range(5)]
            for run, status, mode in [(raw,'CERTIFIED','daily'),
                                      (good,'CERTIFIED','krx_total_return_rebuild'),
                                      (failed,'FAILED','krx_total_return_rebuild'),
                                      (wrong,'CERTIFIED','daily'),(bad_raw,'FAILED','daily')]:
                c.execute('INSERT INTO dq_run VALUES(%s,%s,%s)', (run,status,mode))
            for aid in range(1,8):
                c.execute("INSERT INTO asset VALUES(%s,'stock','common_stock','KRX')", (aid,))
            rows = [(1,raw,100,good), (2,raw,100,failed), (3,raw,100,wrong),
                    (4,raw,100,None), (5,raw,100,good), (6,raw,None,None),
                    (7,bad_raw,100,failed)]
            for aid, source_run, value, return_run in rows:
                c.execute("INSERT INTO price_daily VALUES(%s,'KRX','KOSPI','2026-09-10',%s,%s,%s,100)",
                          (aid,source_run,value,return_run))
            # A new trailing row with an already certified prefix stays on the cheap append path.
            c.execute("INSERT INTO price_daily VALUES(5,'KRX','KOSPI','2026-09-11',%s,999,NULL,110)", (raw,))
            c.execute("INSERT INTO price_daily VALUES(5,'KRX','KOSPI','2026-09-12',%s,NULL,NULL,121)", (raw,))
            c.execute("INSERT INTO price_daily VALUES(5,'KRX','KOSPI','2026-09-13',%s,999,%s,133.1)", (raw,failed))
            baseline = IncrementalBaseline(coverage_end=date(2026,9,10),
                                           action_snapshot_run_id=uuid4(), run_id=good)
            affected = _changed_return_asset_ids(c, baseline, uuid4())
            assert affected == [2,3,4,6]
            captured = []
            def capture(_conn, batch):
                captured.extend(batch.prices.to_dict('records'))
                return len(batch.prices), 0
            monkeypatch.setattr(rebuild, '_publish_batch', capture)
            run_id = uuid4()
            assert rebuild._append_unchanged_prices(
                c, run_id=run_id, affected_asset_ids=affected) == 3
            assert [r['asset_id'] for r in captured] == [5,5,5]
            assert [r['total_return_close'] for r in captured] == pytest.approx([110,121,133.1])
            assert all(r['total_return_quality_run_id'] == run_id for r in captured)
    finally:
        if started:
            command('pg_ctl','-D',str(data),'-m','immediate','-w','stop')
        shutil.rmtree(socket)
