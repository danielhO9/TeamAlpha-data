import pytest
from pipeline.nxt_eligibility import reconcile


def sources():
    rows=[{'ticker':str(i).zfill(6),'date':'20250304','state':'SOURCE_PERMITTED','volume':1} for i in range(10)]
    events=[{'isuSrdCd':'A'+r['ticker'],'aggDd':'20250304','addExlCd':'편입'} for r in rows]
    return {'20250304':rows},{'20250304':events}


def test_missing_whole_day_rejected():
    s,e=sources()
    with pytest.raises(ValueError,match='coverage'):
        reconcile(days=['20250304','20250305'],snapshots=s,changes=e,asset_tickers={1:'000001'},evidence='raw')


def test_active_missing_snapshot_not_zero_filled():
    s,e=sources();s['20250305']=[{**r,'date':'20250305'} for r in s['20250304'] if r['ticker']!='000001'];e['20250305']=[]
    with pytest.raises(ValueError,match='active event'):
        reconcile(days=list(s),snapshots=s,changes=e,asset_tickers={1:'000001'},evidence='raw')


def test_explicit_delisting_closes_missing_exit():
    s,e=sources();s['20250305']=[{**r,'date':'20250305'} for r in s['20250304'] if r['ticker']!='000001'];e['20250305']=[]
    out,counts=reconcile(days=list(s),snapshots=s,changes=e,asset_tickers={1:'000001'},evidence='raw',exclusions=[{'ticker':'000001','start':'20250305','end':'20250305','evidence':'issuer announcement'}])
    assert [r['status'] for r in out]==['ELIGIBLE','INELIGIBLE']
    assert counts['official_exclusion_over_missing_exit']==1


def test_restriction_requires_zero_volume():
    s,e=sources();s['20250304'][1].update(state='RESTRICTED',volume=2)
    with pytest.raises(ValueError,match='unresolved'):
        reconcile(days=list(s),snapshots=s,changes=e,asset_tickers={1:'000001'},evidence='raw')


def test_nonmember_and_partial_session_are_distinct():
    s,e=sources();s['20250304'][1]['state']='SOURCE_PARTIAL_PERMITTED'
    out,_=reconcile(days=list(s),snapshots=s,changes=e,asset_tickers={1:'000001',2:'000099'},evidence='raw')
    assert out[0]['status']=='ELIGIBLE' and out[0]['session_state']=='SOURCE_PARTIAL_PERMITTED'
    assert out[1]['status']=='INELIGIBLE'
