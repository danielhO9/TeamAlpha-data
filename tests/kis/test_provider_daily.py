from datetime import date
from unittest.mock import Mock, MagicMock, patch
import json
import pytest
from pipeline import kis_provider_daily as daily
from pipeline import kis_flows as k
from pipeline.bronze.kis_history import digest
from tests.kis.test_history import flow

D = date(2026, 9, 17)

def policy():
    return dict(version='trusted-v1', availability_lag_calendar_days=1, availability_hour_kst=8,
                short_market='KRX', short_market_basis='PROVIDER_TRUST', provider_trust_evidence='s3://approval',
                short_market_evidence='s3://past-check', short_market_verified_from='2015-01-01',
                short_market_verified_through='2026-09-11')


def test_trust_is_explicit_and_does_not_relabel_external_verification():
    p = policy()
    k.checked_policy(p)
    del p['provider_trust_evidence']
    with pytest.raises(ValueError, match='acceptance'): k.checked_policy(p)
    class Client:
        def history(self, kind, *args):
            data = flow() if kind == 'investor' else ({'ssts_cntg_qty':'2','ssts_tr_pbmn':'40','acml_vol':'100','ssts_vol_rlim':'2'} if kind=='short' else {'acml_vol':'100'})
            return {D:data}, [{'raw_uri':'raw', 'fetched_at':'2026-09-18T01:00:00+00:00'}]
    row = k.collect_partition(Client(), 1, '005930', [D], 'J', policy())[-1]
    assert row['values']['short_ratio_pct'] == '2'
    assert row['values']['market_scope_basis'] == 'PROVIDER_TRUST'
    assert row['values']['external_scope_verified_through'] == '2026-09-11'
    assert row['values']['market_scope_evidence'] == 's3://approval'


def body(rows, field='brdinfoTimeList'):
    return {field: rows, 'totalCnt': len(rows)}


def permitted(ticker='005930'):
    return dict(aggDd='20260917', isuSrdCd=ticker, cptrTrdPmsnCd='7', tdhlYn='N', trdIpsbRsn='-', accTdQty=100)


def test_nxt_absence_is_not_silently_treated_as_zero():
    with pytest.raises(ValueError, match='active NXT'):
        daily.nxt_day(D, body([permitted()]), body([], 'trdisuChgList'), {'000660':True}, {1:'000660'}, {}, 'raw')
    active, rows = daily.nxt_day(D, body([permitted()]), body([], 'trdisuChgList'), {}, {1:'000660'}, {}, 'raw')
    assert rows[0]['status']=='INELIGIBLE'
    assert rows[0]['basis']=='LAUNCH_BASELINE_AND_DAILY_EVENT_EXCLUSION'


def test_nxt_wrong_date_and_truncation_rejected():
    for b in [dict(totalCnt=2,brdinfoTimeList=[permitted()]), body([dict(permitted(),aggDd='20260916')])]:
        with pytest.raises(ValueError, match='incomplete or wrong-date'):
            daily.nxt_day(D,b,body([],'trdisuChgList'),{},{1:'005930'},{},'raw')


def test_nxt_entry_and_restriction():
    events=body([dict(aggDd='20260917',isuSrdCd='A005930',addExlCd='편입')],'trdisuChgList')
    active, rows=daily.nxt_day(D,body([permitted()]),events,{},{1:'005930'},{},'raw')
    assert active['005930'] and rows[0]['status']=='ELIGIBLE'
    _, rows=daily.nxt_day(D,body([dict(permitted(),tdhlYn='Y',accTdQty=0)]),events,{},{1:'005930'},{},'raw')
    assert rows[0]['status']=='INELIGIBLE'


def test_delisting_excludes_effective_day_but_not_previous_day():
    p=[dict(asset_id=1,ticker='006380',start='2000-01-01',end=None)]
    assert daily.close_periods(p,[('006380',D)],D,D)[0]['end']=='2026-09-16'
    assert p[0]['end'] is None
    with pytest.raises(ValueError,match='outside requested'): daily.close_periods(p,[('006380',D)],date(2026,9,18),date(2026,9,18))


def test_audit_rejects_missing_dates_and_identity_mismatch():
    conn=MagicMock(); cur=conn.cursor.return_value.__enter__.return_value
    p=[dict(asset_id=1,ticker='005930',start='2000-01-01',end=None)]
    for rows in [[],[(1,D,'CERTIFIED','000660',1)],[(1,D,'FAILED','005930',1)],[(1,D,'CERTIFIED','005930',1)]*2]:
        cur.fetchall.return_value=rows
        with pytest.raises(ValueError,match='coverage failed'): daily.audit_periods(conn,p,[1],[D])
    cur.fetchall.return_value=[(1,D,'CERTIFIED','005930',1)]
    assert daily.audit_periods(conn,p,[1],[D])['expected_price_gaps']==0


def test_failed_ingestion_does_not_advance_reference_watermark(monkeypatch):
    for name,value in {'KIS_BRONZE_ROOT':'root','KIS_UNIVERSE_URI':'manifest','KIS_POLICY_URI':'policy','KIS_DAILY_REFERENCE_URI':'checkpoint'}.items(): monkeypatch.setenv(name,value)
    manifest=dict(as_of='2026-08-10',asset_ids=[1]);manifest['sha256']=digest(manifest)
    with patch.object(k,'read_json',side_effect=[manifest,policy(),{'through':'2026-09-01'}]), patch.object(daily,'prepare',return_value=('current','state',{'through':str(D)})) as prep, patch.object(k,'run',return_value={'failures':[{'error':'missing'}]}) as run, patch.object(daily,'_freeze'),patch.object(k,'Client'),patch.object(daily,'write_text') as write:
        with pytest.raises(RuntimeError,match='incomplete'): daily.daily('20260917',conn=Mock())
        write.assert_not_called()
        assert run.call_args.kwargs['start']==date(2026,9,2)
        assert prep.call_args.args[1]['as_of']=='2026-08-10'


def test_equal_row_count_with_wrong_day_is_not_accepted():
    conn=MagicMock(); cur=conn.cursor.return_value.__enter__.return_value
    rows=[(1,D,'J','investor',None),(1,D,'J','short','VALID'),(1,date(2026,9,16),'UN','investor',None)]
    cur.fetchall.return_value=rows
    with patch.object(k,'read_json',return_value={'asset_ids':[1]}),patch.object(k,'expected_partitions',return_value={(1,'005930',0):[D]}),patch.object(k,'nxt_dates',return_value=([],[D])):
        with pytest.raises(ValueError,match='final coverage'): daily.verify_collection(conn,'manifest',policy(),[D])
        cur.fetchall.return_value=[(*r[:1],D,*r[2:]) for r in rows]
        assert daily.verify_collection(conn,'manifest',policy(),[D])['missing']==0


def test_missing_price_requires_provider_delisting_and_date_agreement():
    conn=MagicMock();conn.cursor.return_value.__enter__.return_value.fetchall.return_value=[]
    client=Mock();p=[dict(asset_id=1,ticker='006380',start='2000-01-01',end=None)]
    client.stock_info.return_value=({'lstg_abol_dt':''},{'raw_uri':'source'})
    with pytest.raises(ValueError,match='does not confirm'):daily.refresh_listing(conn,p,[1],[D],date(2026,9,16),D,client,[])
    client.stock_info.return_value=({'lstg_abol_dt':'20260918'},{'raw_uri':'source'})
    with pytest.raises(ValueError,match='before provider delisting'):daily.refresh_listing(conn,p,[1],[D],date(2026,9,16),D,client,[])
    client.stock_info.return_value=({'lstg_abol_dt':'20260917'},{'raw_uri':'source'})
    evidence=[]
    assert daily.refresh_listing(conn,p,[1],[D],date(2026,9,16),D,client,evidence)[0]['end']=='2026-09-16'
    assert evidence==['source']


def test_stock_info_accepts_only_exact_or_documented_product_identity():
    from pipeline.bronze.kis_history import Client
    client=Client('/unused',token='test')
    for value in ['006380','00000A006380']:
        with patch.object(client,'_capture',return_value=({'output':{'pdno':value}},{})):
            assert client.stock_info('006380')[0]['pdno']==value
    with patch.object(client,'_capture',return_value=({'output':{'pdno':'00000A005930'}},{})):
        with pytest.raises(ValueError,match='identity'):client.stock_info('006380')
