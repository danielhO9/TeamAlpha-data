"""Build corrected immutable candidates, then atomically switch approvals.

Old factor values are retained. Interrupted candidate builds are safe to rerun;
the live catalog is not changed until all changed factors have full coverage.
"""
import hashlib
import json
import time
from datetime import date
from pathlib import Path
from psycopg.types.json import Jsonb
from pipeline.common import db
from pipeline.gold import bulk_daily
from pipeline.gold.run import ROOT, load_manifest, implementation_hash, build_stage_sql

KEYS = (*bulk_daily.LEGACY_FACTOR_KEYS, 'market_leverage',
        'paid_in_capital_ratio', 'operating_return_on_capital_employed')
STANDALONE_START = {'market_leverage':date(2015,6,25),
    'paid_in_capital_ratio':date(2015,6,25),
    'operating_return_on_capital_employed':date(2017,2,27)}


def run(start=date(2018,3,2), end=date(2026,9,30)):
    manifest=load_manifest()
    with db.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_lock(2026100201)")
            cur.execute("SELECT factor_id,factor_key,version,description,config FROM gold.factor WHERE status='APPROVED' AND factor_key=ANY(%s)",(list(KEYS),))
            old={r[1]:r for r in cur.fetchall()}
            if set(old)!=set(KEYS):raise ValueError('Incomplete live factor catalog')
            candidates={}
            for key, row in old.items():
                path=bulk_daily.SQL_PATH if key in bulk_daily.LEGACY_FACTOR_KEYS else ROOT/manifest[key]['sql']
                digest=implementation_hash(path)
                cur.execute("SELECT factor_id,version FROM gold.factor WHERE factor_key=%s AND implementation_hash=%s AND status='CANDIDATE' ORDER BY version DESC LIMIT 1",(key,digest))
                found=cur.fetchone()
                if found:
                    candidates[key]=(found[0],key,int(row[4]['predicted_sign']));continue
                cur.execute("SELECT coalesce(max(version),0)+1 FROM gold.factor WHERE factor_key=%s",(key,))
                version=cur.fetchone()[0]
                config=dict(row[4]);config.update(frequency='daily',supersedes_version=row[2],
                    financial_units='krw_previous_closed_fx_v1',
                    research_definition_hash=hashlib.sha256((str(config.get('research_definition_hash'))+digest).encode()).hexdigest()[:16])
                if key in manifest:config['research_definition_hash']=manifest[key]['research_definition_hash']
                cur.execute("""INSERT INTO gold.factor(factor_key,version,description,implementation_uri,
                    implementation_hash,config,evaluation,status) VALUES (%s,%s,%s,%s,%s,%s,%s,'CANDIDATE') RETURNING factor_id""",
                    (key,version,row[3],f'repo://TeamAlpha-data/{path.relative_to(ROOT)}',digest,Jsonb(config),Jsonb({'reason':'financial_units_repair','source_factor_id':row[0]})))
                candidates[key]=(cur.fetchone()[0],key,int(config['predicted_sign']))
        conn.commit()
        bulk_daily.run_shared_backfill(conn,start_date=start,end_date=end,apply=True,
            factor_rows=[candidates[k] for k in bulk_daily.LEGACY_FACTOR_KEYS])
        # Separate connection avoids temp-name collisions with bulk staging.
        for key in ('market_leverage','paid_in_capital_ratio','operating_return_on_capital_employed'):
            for lo,hi in bulk_daily._year_chunks(min(start, STANDALONE_START[key]),end):
                print('standalone',key,lo,hi,flush=True)
                with db.connect() as part:
                    with part.cursor() as cur:
                        cur.execute("SET LOCAL work_mem='256MB'")
                        cur.execute("SET LOCAL statement_timeout='1800s'")
                        cur.execute(build_stage_sql((ROOT/manifest[key]['sql']).read_text()),{'start_date':lo,'end_date':hi})
                        cur.execute("SELECT count(*),count(distinct(asset_id,as_of_date)),count(*) FILTER(WHERE value::text IN ('NaN','Infinity','-Infinity') OR rank<=0) FROM _gold_factor_values")
                        n,unique,invalid=cur.fetchone()
                        if n!=unique or invalid or n==0:raise ValueError('Invalid factor partition')
                        cur.execute("DELETE FROM gold.factor_value WHERE factor_id=%s AND as_of_date BETWEEN %s AND %s",(candidates[key][0],lo,hi))
                        cur.execute("INSERT INTO gold.factor_value(factor_id,asset_id,as_of_date,value,rank) SELECT %s,asset_id,as_of_date,value,rank FROM _gold_factor_values",(candidates[key][0],))
                        print('rows',n,flush=True)
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute("SELECT factor_key,factor_id FROM gold.factor WHERE status='APPROVED' AND factor_key=ANY(%s) FOR UPDATE",(list(KEYS),))
                if dict(cur.fetchall())!={k:r[0] for k,r in old.items()}:raise ValueError('Concurrent approval change')
                for key,(fid,_,_) in candidates.items():
                    cur.execute("""SELECT min(as_of_date),max(as_of_date),count(distinct as_of_date),count(*)
                        FROM gold.factor_value WHERE factor_id=%s""",(fid,))
                    lo,hi,days,n=cur.fetchone()
                    expected_start = min(start, STANDALONE_START.get(key, start))
                    if lo!=expected_start or hi!=end or days<2000:raise ValueError(f'Incomplete candidate {key}: {lo}/{hi}/{days}')
                    cur.execute("UPDATE gold.factor SET status='RETIRED' WHERE factor_id=%s",(old[key][0],))
                    cur.execute("UPDATE gold.factor SET status='APPROVED',evaluation=evaluation||%s WHERE factor_id=%s",
                        (Jsonb({'passed':True,'approved_by':'explicit_user_request_20261001','coverage_start':str(lo),'coverage_end':str(hi),'days':days,'rows':n}),fid))
        print('APPROVED CORRECTED GOLD',len(candidates),flush=True)


def extend_early_history():
    """Recover the pre-2018 standalone history in an already staged build."""
    manifest = load_manifest()
    for key, start in STANDALONE_START.items():
        path = ROOT / manifest[key]['sql']
        with db.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT factor_id FROM gold.factor WHERE factor_key=%s AND implementation_hash=%s AND status='CANDIDATE' ORDER BY version DESC LIMIT 1",(key,implementation_hash(path)))
                row = cur.fetchone()
                if not row: raise ValueError(f'Missing staged candidate {key}')
                fid = row[0]
        for lo, hi in bulk_daily._year_chunks(start,date(2018,3,1)):
            with db.connect() as conn:
                with conn.cursor() as cur:
                    cur.execute("SET LOCAL work_mem='256MB'")
                    cur.execute(build_stage_sql(path.read_text()),{'start_date':lo,'end_date':hi})
                    cur.execute("SELECT count(*),count(distinct(asset_id,as_of_date)),count(*) FILTER(WHERE value::text IN ('NaN','Infinity','-Infinity') OR rank<=0) FROM _gold_factor_values")
                    n, unique, invalid = cur.fetchone()
                    if n != unique or invalid or not n: raise ValueError('Invalid early history')
                    cur.execute("DELETE FROM gold.factor_value WHERE factor_id=%s AND as_of_date BETWEEN %s AND %s",(fid,lo,hi))
                    cur.execute("INSERT INTO gold.factor_value(factor_id,asset_id,as_of_date,value,rank) SELECT %s,asset_id,as_of_date,value,rank FROM _gold_factor_values",(fid,))
            print('early-history',key,lo,hi,n,flush=True)


def promote_completed():
    """Resume only final validation/promotion, never recompute finished candidates."""
    manifest = load_manifest()
    with db.connect() as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT pg_advisory_xact_lock(2026100201)')
            candidates = []
            for key in KEYS:
                path = bulk_daily.SQL_PATH if key in bulk_daily.LEGACY_FACTOR_KEYS else ROOT/manifest[key]['sql']
                cur.execute("SELECT factor_id,evaluation->>'source_factor_id' FROM gold.factor WHERE factor_key=%s AND implementation_hash=%s AND status='CANDIDATE' ORDER BY version DESC LIMIT 1 FOR UPDATE",(key,implementation_hash(path)))
                row = cur.fetchone()
                if not row: raise ValueError(f'Missing candidate {key}')
                fid, old = row[0], int(row[1])
                cur.execute("SELECT factor_id FROM gold.factor WHERE factor_key=%s AND status='APPROVED' FOR UPDATE",(key,))
                if cur.fetchone() != (old,): raise ValueError(f'Approval changed for {key}')
                cur.execute("SELECT min(as_of_date),max(as_of_date),count(distinct as_of_date),count(*),count(*) FILTER(WHERE value::text IN ('NaN','Infinity','-Infinity') OR rank<=0) FROM gold.factor_value WHERE factor_id=%s",(fid,))
                lo, hi, days, n, invalid = cur.fetchone()
                if lo != STANDALONE_START.get(key,date(2018,3,2)) or hi != date(2026,9,30) or days < 2000 or invalid:
                    raise ValueError(f'Incomplete {key}: {lo}, {hi}, {days}, {invalid}')
                candidates.append((fid,old,dict(passed=True,approved_by='explicit_user_request_20261001',coverage_start=str(lo),coverage_end=str(hi),days=days,rows=n)))
                print('validated',key,n,flush=True)
            for fid, old, evidence in candidates:
                cur.execute("UPDATE gold.factor SET status='RETIRED' WHERE factor_id=%s",(old,))
                cur.execute("UPDATE gold.factor SET status='APPROVED',evaluation=evaluation||%s WHERE factor_id=%s",(Jsonb(evidence),fid))
    print('APPROVED CORRECTED GOLD',len(candidates),flush=True)


if __name__=='__main__':
    from dotenv import load_dotenv
    load_dotenv()
    run()
