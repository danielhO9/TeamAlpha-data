"""Closed, backed-up repair of one reviewed price-unit error.

Not a general SQL repair interface. The existing full action-snapshot and
total-return audit gates remain mandatory. Gold is a separate downstream job.
"""
import hashlib
import json
import math
import os
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory

import boto3
from psycopg.rows import dict_row

from pipeline import dart_silver_backfill_ecs as workflow
from pipeline.silver import total_return_rebuild
from pipeline.silver.prices import _read_marcap
from pipeline.silver.return_contract import (
    acquire_return_rebuild_lock, release_return_rebuild_lock,
    invalidate_krx_total_return,
)
from pipeline.silver.reviewed_price_references import CHEIL_REVIEW_ID, CHEIL_EVIDENCE_URL
from pipeline.silver_quality import repository
from pipeline.silver_quality.models import CheckResult,CheckStatus,Severity

ASSET=4188
EVENT=date(2026,2,9)


def validate_prices(rows):
    indexed={r['trade_date']:r for r in rows}
    before=indexed[date(2026,2,6)];event=indexed[EVENT]
    if (float(before['close'])!=2080 or float(event['close'])!=625000
            or int(before['shares'])!=29129064 or int(event['shares'])!=19419):
        raise ValueError('Reviewed source anchors changed')
    if len(indexed)!=len(rows):
        raise ValueError('Duplicate historical price')
    if not all(math.isfinite(float(r[k])) and float(r[k])>0
               for r in rows for k in ('close','adj_close')):
        raise ValueError('Invalid historical price')
    scale=(float(before['adj_close'])/2080)/(float(event['adj_close'])/625000)
    if math.isclose(scale,1500,rel_tol=1e-8):return 'already_corrected'
    if not math.isclose(scale,1,rel_tol=1e-8):
        raise ValueError('Unexpected adjustment scale; refusing compounding repair')
    return 'needs_repair'


def verify_bronze(bucket, prefix):
    """Bind the correction to the preserved actual quote, not a synthetic row."""
    s3=boto3.client('s3')
    key='stock/marcap/date=2026-02-09/all.parquet'
    blob=s3.get_object(Bucket=bucket,Key=key)['Body'].read()
    digest=hashlib.sha256(blob).hexdigest()
    with TemporaryDirectory() as temporary:
        path=Path(temporary)/key
        path.parent.mkdir(parents=True)
        path.write_bytes(blob)
        frame=_read_marcap(temporary)
        rows=frame.loc[frame.ticker.eq('052670')]
        if len(rows)!=1 or float(rows.iloc[0].prev_diff)!=-2495000:
            raise RuntimeError('Reviewed Bronze quote verification failed')
    backup=f'{prefix}/evidence/bronze-{digest}.parquet'
    s3.put_object(Bucket=bucket,Key=backup,Body=blob)
    if s3.get_object(Bucket=bucket,Key=backup)['Body'].read()!=blob:
        raise RuntimeError('Bronze evidence readback failed')
    return dict(key=key,sha256=digest,preserved_key=backup)


def repair_prices(conn,bucket,prefix):
    workflow.assert_daily_certification_lock(conn)
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("SELECT * FROM price_daily WHERE asset_id=%s AND source='KRX' ORDER BY trade_date",(ASSET,))
        rows=cur.fetchall()
        cur.execute("SELECT 1 FROM asset_identifier WHERE asset_id=%s AND source='KRX' AND identifier='052670' LIMIT 1",(ASSET,))
        if not cur.fetchone():raise ValueError('Asset identity mismatch')
    state=validate_prices(rows)
    if state=='already_corrected':
        print('PRICE_ALREADY_CORRECTED; certification still required',flush=True)
        return
    # Immutable remote backup must exist and round-trip before any source write.
    blob=json.dumps(rows,default=str,sort_keys=True).encode()
    digest=hashlib.sha256(blob).hexdigest()
    key=f'{prefix}/backup/{ASSET}-{digest}.json'
    s3=boto3.client('s3')
    s3.put_object(Bucket=bucket,Key=key,Body=blob,ContentType='application/json')
    if hashlib.sha256(s3.get_object(Bucket=bucket,Key=key)['Body'].read()).hexdigest()!=digest:
        raise RuntimeError('Backup readback failed')
    context=repository.start_run(conn,mode='reviewed_price_reference_repair',
        target_date=EVENT,partition_key=CHEIL_REVIEW_ID,input_fingerprint=digest)
    try:
        with conn.transaction():
            with conn.cursor() as cur:
                cur.execute("UPDATE price_daily SET adj_close=adj_close*1500,quality_run_id=%s WHERE asset_id=%s AND source='KRX' AND trade_date<%s",(context.run_id,ASSET,EVENT))
                changed=cur.rowcount
                if changed!=sum(r['trade_date']<EVENT for r in rows):
                    raise RuntimeError('Unexpected affected-row count')
                cur.execute("UPDATE price_daily SET total_return_close=NULL,total_return_quality_run_id=NULL,total_return_loaded_at=NULL WHERE asset_id=%s AND source='KRX'",(ASSET,))
            with conn.cursor(row_factory=dict_row) as cur:
                cur.execute("SELECT * FROM price_daily WHERE asset_id=%s AND source='KRX' ORDER BY trade_date",(ASSET,))
                after=cur.fetchall()
            if validate_prices(after)!='already_corrected':raise RuntimeError('Repair readback failed')
            allowed={'adj_close','quality_run_id','total_return_close','total_return_quality_run_id','total_return_loaded_at'}
            if len(after)!=len(rows):raise RuntimeError('History shape changed')
            for old,new in zip(rows,after):
                if any(old[k]!=new[k] for k in old if k not in allowed):
                    raise RuntimeError('Unrelated source column changed')
                expected=old['adj_close']*(1500 if old['trade_date']<EVENT else 1)
                if new['adj_close']!=expected:raise RuntimeError('Exact adjusted price mismatch')
            result=CheckResult(rule_code='REVIEWED_CONSOLIDATION_EXACT_READBACK',dataset='price_daily',
                severity=Severity.CRITICAL,status=CheckStatus.PASS,
                expected='Reviewed 1500:1 consolidation; all non-derived source columns unchanged',
                actual=json.dumps(dict(changed_rows=changed,backup_key=key,backup_sha256=digest,evidence=CHEIL_EVIDENCE_URL)))
            invalidate_krx_total_return(conn,reason=CHEIL_REVIEW_ID,quality_run_id=context.run_id)
            repository.finish_run(conn,context,'CERTIFIED',[result],commit=False)
        print(f'PRICE_REPAIRED rows={changed} backup={key}',flush=True)
    except BaseException as exc:
        conn.rollback()
        repository.finish_run(conn,context,'FAILED',[],error_message=str(exc))
        raise


def main():
    bucket=os.environ['S3_BRONZE_BUCKET'];prefix=os.environ['REPAIR_OUTPUT_PREFIX']
    lock=workflow.acquire_daily_certification_lock()
    writer=False
    try:
        acquire_return_rebuild_lock(lock);writer=True
        evidence=verify_bronze(bucket,prefix)
        _,end=total_return_rebuild._source_price_coverage(lock)
        # Existing closed gates run BEFORE source mutation; no bypass for missing evidence.
        workflow.prepare_total_return_snapshot(end,certification_lock=lock)
        workflow.preview_total_return_actions(end,conn=lock)
        repair_prices(lock,bucket,prefix)
        report=workflow.close_total_return_contract(end,certification_lock=lock)
        with lock.cursor() as cur:
            cur.execute("SELECT trade_date,total_return_close FROM price_daily WHERE asset_id=%s AND source='KRX' AND trade_date IN ('2026-02-06','2026-02-09') ORDER BY trade_date",(ASSET,))
            prices=cur.fetchall()
        if len(prices)!=2 or not math.isclose(float(prices[1][1]/prices[0][1]-1),625000/3120000-1,abs_tol=1e-7):
            invalidate_krx_total_return(lock,reason='reviewed_consolidation_oracle_failed',quality_run_id=None)
            lock.commit()
            raise RuntimeError('Certified return disagrees with independent consolidation oracle')
        report.update(status='COMPLETE',reviewed_correction=CHEIL_REVIEW_ID,coverage_end=str(end),bronze_evidence=evidence)
        boto3.client('s3').put_object(Bucket=bucket,Key=f'{prefix}/completion.json',
            Body=json.dumps(report,default=str).encode(),ContentType='application/json')
    except BaseException as exc:
        boto3.client('s3').put_object(Bucket=bucket,Key=f'{prefix}/failure.json',
            Body=json.dumps(dict(status='FAILED',error=str(exc))).encode(),ContentType='application/json')
        raise
    finally:
        if writer:release_return_rebuild_lock(lock)
        workflow.release_daily_certification_lock(lock)


if __name__=='__main__':main()
