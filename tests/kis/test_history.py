from datetime import date,datetime,timezone
from decimal import Decimal
import json

import pytest

from pipeline.bronze.kis_history import Client
from pipeline.silver.kis_flows import investor,short_sale,combined,observation
from pipeline.kis_flows import export_universe,daily,checked_policy


def flow(q=2, money=1):
    return {f'{c}_{m}':str(v) for c in ('prsn','orgn','frgn') for m,v in
            [('seln_vol',q),('shnu_vol',q+1),('ntby_qty',1),
             ('seln_tr_pbmn',money),('shnu_tr_pbmn',money+1),('ntby_tr_pbmn',1)]}


def test_pagination_uses_oldest_minus_one_and_keeps_halted_days():
    c=Client('/unused',token='test');calls=[]
    def page(kind,ticker,venue,start,end):
        calls.append(end)
        if len(calls)==1:return [(date(2015,1,6),{'v':0}),(date(2015,1,5),{'v':1})],{'page':1}
        return [(date(2015,1,2),{'v':2})],{'page':2}
    c.page=page
    rows,receipts=c.history('investor','005930','J',date(2015,1,2),date(2015,1,6))
    assert calls==[date(2015,1,6),date(2015,1,4)]
    assert rows[date(2015,1,6)]=={'v':0} and len(receipts)==2


def test_empty_response_is_not_zero_filled():
    c=Client('/unused',token='test');c.page=lambda *a:([],{})
    assert c.history('short','005930','J',date(2015,1,2),date(2015,1,5))[0]=={}


def test_wrong_anchor_fails():
    c=Client('/unused',token='test')
    c.page=lambda *a:([(date(2020,1,1),{})],{})
    with pytest.raises(ValueError,match='advance'):
        c.history('short','005930','J',date(2015,1,2),date(2015,1,5))


def test_investor_units_and_rounding():
    f=flow();f['orgn_ntby_tr_pbmn']='2'
    assert investor(f)['orgn_ntby_tr_pbmn']==2_000_000
    f['orgn_ntby_tr_pbmn']='3'
    with pytest.raises(ValueError,match='amount'):investor(f)


@pytest.mark.parametrize('bad',['','NaN','Infinity','1.5','-1'])
def test_bad_buy_quantity_blocked(bad):
    f=flow();f['prsn_shnu_vol']=bad
    with pytest.raises(ValueError):investor(f)


def test_net_quantity_cannot_be_inconsistent():
    f=flow();f['prsn_ntby_qty']='8'
    with pytest.raises(ValueError,match='quantity'):investor(f)


def test_raw_volume_fixes_known_kr_motors_case():
    r=short_sale({'ssts_cntg_qty':'18621','ssts_tr_pbmn':'19459075',
                  'acml_vol':'11383','ssts_vol_rlim':'163.59'},
                 {'acml_vol':'1003392'},same_market=True)
    assert abs(Decimal(r['short_ratio_pct'])-Decimal('1.85580511'))<Decimal('.00000001')
    assert r['provider_ratio_pct']=='163.59'


@pytest.mark.parametrize('same,volume,status',[(False,'100','MARKET_SCOPE_UNVERIFIED'),(True,'0','ZERO_DENOMINATOR')])
def test_no_unsafe_ratio(same,volume,status):
    r=short_sale({'ssts_cntg_qty':'0','ssts_tr_pbmn':'0'},{'acml_vol':volume},same_market=same)
    assert r['short_ratio_pct'] is None and r['ratio_status']==status


def test_short_exceeds_volume_blocked():
    with pytest.raises(ValueError):
        short_sale({'ssts_cntg_qty':'101','ssts_tr_pbmn':'1'},{'acml_vol':'100'},same_market=True)


def test_combined_does_not_average_ratios_and_requires_parity():
    a=investor(flow());b=investor(flow());total={k:v*2 for k,v in a.items()}
    assert combined(a,b,total)==total
    total['prsn_shnu_vol']+=1
    with pytest.raises(ValueError,match='UN'):combined(a,b,total)


def test_observed_time_does_not_backdate_to_trade_day():
    policy={'version':'assumed-v1','availability_lag_calendar_days':1,'availability_hour_kst':8}
    r=observation(1,'005930',date(2015,1,2),'J','investor',investor(flow()),
                  [{'fetched_at':'2026-09-11T00:00:00+00:00','raw_uri':'s3://test/raw'}],policy)
    assert r['first_observed_at'].year==2026
    assert r['research_available_at'].year==2015
    assert r['historical_revision_risk'] and r['availability_basis']=='POLICY_ASSUMPTION'


def test_historical_membership_includes_exited_stock(tmp_path):
    source=tmp_path/'monthly.csv';dest=tmp_path/'universe.json'
    source.write_text('asset_id,Code,trade_date,in_universe\n1,000001,2015-01-30,True\n1,000001,2026-08-31,False\n2,000002,2026-08-31,True\n')
    assert export_universe(source,str(dest))['asset_ids']==[1,2]


def test_daily_is_disabled_without_flag(monkeypatch):
    monkeypatch.delenv('KIS_FLOWS_ENABLED',raising=False)
    assert daily('20260910',conn=object()) is None


def test_market_scope_requires_evidence():
    with pytest.raises(ValueError,match='evidence'):
        checked_policy({'version':'v1','availability_lag_calendar_days':1,'availability_hour_kst':8,'short_market':'KRX'})


def test_availability_policy_uses_half_past_eight():
    p={'version':'v1','availability_lag_calendar_days':1,'availability_hour_kst':8}
    r=observation(1,'005930',date(2015,1,2),'J','investor',{},
                  [{'fetched_at':'2026-09-11T00:00:00+00:00','raw_uri':'raw'}],p)
    assert (r['research_available_at'].hour,r['research_available_at'].minute)==(8,30)


def test_missing_partition_dates_rejects_instead_of_zero_fill():
    from pipeline.kis_flows import collect_partition
    class Empty:
        def history(self,*args): return {},[]
    with pytest.raises(ValueError,match='no zero fill'):
        collect_partition(Empty(),1,'005930',[date(2025,3,4)],'NX',{})


def test_short_scope_policy_requires_ordered_verification_interval():
    base={'version':'scope-v1','availability_lag_calendar_days':1,
          'availability_hour_kst':8,'short_market':'KRX',
          'short_market_evidence':'s3://evidence/market-scope.json',
          'short_market_verified_from':'2015-01-01',
          'short_market_verified_through':'2026-09-10'}
    assert checked_policy(base)==base
    with pytest.raises(ValueError,match='reversed'):
        checked_policy({**base,'short_market_verified_from':'2026-09-11'})
    with pytest.raises(KeyError,match='verified_from'):
        checked_policy({k:v for k,v in base.items() if k!='short_market_verified_from'})


def test_short_market_bounds_and_provenance_use_krx_volume():
    from pipeline.kis_flows import collect_partition
    days=[date(2026,9,d) for d in (8,9,10,11)]
    receipt={'fetched_at':'2026-09-12T00:00:00+00:00','raw_uri':'s3://test/raw'}
    class Source:
        def history(self,kind,ticker,venue,start,end):
            assert venue=='J'  # Never substitute the larger integrated denominator.
            data=flow() if kind=='investor' else ({'ssts_cntg_qty':'692808',
                'ssts_tr_pbmn':'184051995500','acml_vol':'22517075'}
                if kind=='short' else {'acml_vol':'22517075'})
            return {d:data for d in days},[receipt]
    policy={'version':'scope-v1','availability_lag_calendar_days':1,
            'availability_hour_kst':8,'short_market':'KRX',
            'short_market_verified_from':'2026-09-09',
            'short_market_verified_through':'2026-09-10',
            'short_market_evidence':'s3://evidence/market-scope.json'}
    rows=collect_partition(Source(),2325,'005930',days,'J',policy)
    short=[r['values'] for r in rows if r['kind']=='short']
    assert [r['ratio_status'] for r in short]==[
        'MARKET_SCOPE_UNVERIFIED','VALID','VALID','MARKET_SCOPE_UNVERIFIED']
    assert short[1]['short_market']=='KRX'
    assert short[1]['volume_market']=='KRX'
    assert short[1]['volume_adjustment']=='UNADJUSTED'
    assert short[1]['market_scope_evidence']==policy['short_market_evidence']
    assert Decimal(short[1]['short_ratio_pct']).quantize(Decimal('.000001'))==Decimal('3.076812')
    assert short[0]['short_market']=='UNKNOWN' and short[0]['market_scope_evidence'] is None
