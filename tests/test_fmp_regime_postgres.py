"""Exercise actual DQ transaction, deduplication, revision and as-of semantics."""
import json
import shutil
import subprocess
import tempfile
from datetime import date, datetime, timezone
from pathlib import Path

import psycopg
import pytest

from pipeline import fmp_regime as r


@pytest.mark.skipif(not shutil.which('initdb') or not shutil.which('pg_ctl'), reason='PostgreSQL binaries unavailable')
def test_publish_and_asof_on_postgres(tmp_path):
    cluster=tmp_path/'pg'
    socket=Path(tempfile.mkdtemp(prefix='regime-pg-',dir='/tmp'))
    def command(args):
        subprocess.run(args,check=True,capture_output=True,text=True)
    command(['initdb','-D',str(cluster),'-A','trust','-U','postgres','--no-locale','--encoding=UTF8'])
    command(['pg_ctl','-D',str(cluster),'-l',str(tmp_path/'pg.log'),'-o',f"-F -k {socket} -h '' -p 55439",'-w','start'])
    try:
        with psycopg.connect(host=str(socket),port=55439,user='postgres',dbname='postgres') as conn:
            migrations=Path(r.__file__).parent/'silver_quality'/'migrations'
            # DQ tables from the real migration, before its unrelated asset ALTERs.
            conn.execute((migrations/'001_quality.sql').read_text().split('ALTER TABLE asset ADD')[0])
            conn.execute((migrations/'017_fmp_regime.sql').read_text())
            conn.commit()
            raw=dict(symbol='HYG',date='2015-01-02',open=80,high=82,low=79,close=81,volume=0)
            rows=r.parse(json.dumps([raw]),'HYG',date(2015,1,1),date(2015,1,31))
            kwargs=dict(series='HYG',uri='s3://test/raw.json',received_at=datetime(2026,9,18,tzinfo=timezone.utc),fingerprint='a'*64)
            r.publish(conn,rows,**kwargs)
            r.publish(conn,rows,**kwargs)
            assert conn.execute('SELECT count(*) FROM fmp_regime_latest').fetchone()[0]==1
            assert conn.execute("SELECT count(*) FROM fmp_regime_asof('2025-01-01Z')").fetchone()[0]==0
            conn.rollback()
            later=r.parse(json.dumps([{**raw,'close':82}]),'HYG',date(2015,1,1),date(2015,1,31))
            r.publish(conn,later,**{**kwargs,'received_at':datetime(2026,9,19,tzinfo=timezone.utc)})
            assert conn.execute('SELECT value FROM fmp_regime_latest').fetchone()[0]==82
            assert conn.execute("SELECT value FROM fmp_regime_asof('2026-09-18T12:00:00Z')").fetchone()[0]==81
            conn.rollback()
            # Invalid rows cause the full batch publication to roll back.
            with pytest.raises(psycopg.errors.CheckViolation):
                r.publish(conn,[(date(2015,1,3),'close',-1,'USD',raw)],**kwargs)
            assert conn.execute('SELECT count(*) FROM fmp_regime_observation').fetchone()[0]==2
            assert conn.execute("SELECT count(*) FROM dq_run WHERE status='FAILED'").fetchone()[0]==1
    finally:
        command(['pg_ctl','-D',str(cluster),'-m','immediate','-w','stop'])
        socket.rmdir()
