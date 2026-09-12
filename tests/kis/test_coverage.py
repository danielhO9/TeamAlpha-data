from datetime import date
from unittest.mock import patch
import pytest
from pipeline.kis_flows import expected_partitions, nxt_dates, run
from pipeline.bronze.kis_history import digest

D=date(2026,9,10)

class Cursor:
    def __init__(self,assets,rows):self.answers=iter([assets,rows])
    def __enter__(self):return self
    def __exit__(self,*args):pass
    def execute(self,*args):pass
    def fetchall(self):return next(self.answers)

class Conn:
    def __init__(self,assets,rows):self.c=Cursor(assets,rows);self.rolled=False
    def cursor(self):return self.c
    def rollback(self):self.rolled=True

@pytest.mark.parametrize('row',[(1,None,D,D,'CERTIFIED'),(1,'005930',D,None,None),(1,'005930',D,D,'FAILED')])
def test_expected_dates_reject_missing_evidence(row):
    c=Conn([(1,date(2010,1,1),None)],[row])
    with pytest.raises(ValueError,match='coverage failed'):
        expected_partitions(c,{'asset_ids':[1]},D,D)
    assert c.rolled

def test_duplicate_mapping_rejected():
    row=(1,'005930',D,D,'CERTIFIED')
    with pytest.raises(ValueError,match='coverage failed'):
        expected_partitions(Conn([(1,date(2010,1,1),None)],[row,row]),{'asset_ids':[1]},D,D)

@pytest.mark.parametrize('assets',[[],[(1,None,None)]])
def test_missing_asset_or_listing_date_rejected(assets):
    with pytest.raises(ValueError):
        expected_partitions(Conn(assets,[]),{'asset_ids':[1]},D,D)

def test_outside_listing_interval_can_be_empty():
    assert expected_partitions(Conn([(1,date(2010,1,1),date(2011,1,1))],[]),{'asset_ids':[1]},D,D)=={}

def interval(status):
    return {'asset_id':1,'start':'2025-03-04','end':'2026-09-10','status':status,'evidence':'s3://evidence/nxt.json'}

def test_noneligible_is_distinct_from_unknown():
    assert nxt_dates({'nxt_intervals':[interval('INELIGIBLE')]},1,[D])==([],[D])
    assert nxt_dates({'nxt_intervals':[interval('ELIGIBLE')]},1,[D])==([D],[])
    with pytest.raises(ValueError,match='unknown'):
        nxt_dates({},1,[D])
    with pytest.raises(ValueError,match='ambiguous'):
        nxt_dates({'nxt_intervals':[interval('ELIGIBLE'),interval('INELIGIBLE')]},1,[D])

def test_nxt_start_is_not_assumed_eligible():
    assert nxt_dates({},1,[date(2015,1,2)])==([],[])

def test_nxt_unknown_fails_before_any_collection(tmp_path):
    manifest={'scope':'union_of_all_historical_in_universe_rows','asset_ids':[1],'as_of':str(D)}
    manifest['sha256']=digest(manifest)
    policy={'version':'v','availability_lag_calendar_days':1,'availability_hour_kst':8,'short_market':'UNKNOWN'}
    with patch('pipeline.kis_flows.read_json',side_effect=[manifest,policy]),patch('pipeline.kis_flows.expected_partitions',return_value={(1,'005930',0):[D]}),patch('pipeline.kis_flows.collect_partition') as collect:
        with pytest.raises(ValueError,match='eligibility unknown'):
            run(conn=object(),manifest_uri='m',policy_uri='p',root=str(tmp_path),start=D,end=D,client=object())
        collect.assert_not_called()

def test_noneligible_does_not_request_nx_or_un_and_derives_integrated(tmp_path):
    from tests.kis.test_history import flow
    manifest={'scope':'union_of_all_historical_in_universe_rows','asset_ids':[1],'as_of':str(D),'nxt_intervals':[interval('INELIGIBLE')]}
    manifest['sha256']=digest(manifest)
    policy={'version':'v','availability_lag_calendar_days':1,'availability_hour_kst':8,'short_market':'UNKNOWN','venues':['NX','UN']}
    class Client:
        def history(self,kind,ticker,venue,start,end):
            assert venue=='J' and kind=='investor'
            return {D:flow()},[{'raw_uri':'raw','fetched_at':'2026-09-11T00:00:00+00:00'}]
    with patch('pipeline.kis_flows.read_json',side_effect=[manifest,policy]),patch('pipeline.kis_flows.expected_partitions',return_value={(1,'005930',0):[D]}),patch('pipeline.kis_flows.migrate.assert_current'),patch('pipeline.kis_flows.silver.publish',return_value='run') as publish:
        r=run(conn=object(),manifest_uri='m',policy_uri='p',root=str(tmp_path),start=D,end=D,client=Client(),publish=True)
        assert not r['failures'] and r['published']==1
        obs=publish.call_args.args[1][0]
        assert obs['venue']=='UN' and obs['values']['_derivation']=='KRX_ONLY_VERIFIED_NON_NXT'


def test_daily_includes_target_even_when_prices_are_missing(monkeypatch):
    from pipeline.kis_flows import daily
    for k,v in {'KIS_FLOWS_ENABLED':'1','KIS_UNIVERSE_URI':'m','KIS_POLICY_URI':'p','KIS_BRONZE_ROOT':'r'}.items():
        monkeypatch.setenv(k,v)
    with patch('pipeline.kis_flows.read_json',side_effect=[{'version':'v','availability_lag_calendar_days':1,'availability_hour_kst':8,'short_market':'UNKNOWN'},{'as_of':str(D)}]),patch('pipeline.kis_flows.run',return_value={'failures':[]}) as collect:
        daily('20260910',conn=object())
        assert collect.call_args.kwargs['end']==D
        assert collect.call_args.kwargs['start']==date(2026,9,4)


def test_calendar_exclusion_requires_evidence():
    from pipeline.kis_flows import checked_policy
    p={'version':'v','availability_lag_calendar_days':1,'availability_hour_kst':8,'short_market':'UNKNOWN','calendar_exclusions':[{'date':'2026-07-17'}]}
    with pytest.raises(ValueError,match='calendar exclusion'):
        checked_policy(p)


def test_daily_excludes_verified_special_holiday(monkeypatch):
    from pipeline.kis_flows import daily
    for k,v in {'KIS_FLOWS_ENABLED':'1','KIS_UNIVERSE_URI':'m','KIS_POLICY_URI':'p','KIS_BRONZE_ROOT':'r'}.items():
        monkeypatch.setenv(k,v)
    policy={'version':'v','availability_lag_calendar_days':1,'availability_hour_kst':8,'short_market':'UNKNOWN','calendar_exclusions':[{'date':'2026-07-17','evidence':'official holiday notice'}]}
    with patch('pipeline.kis_flows.read_json',side_effect=[policy,{'as_of':'2026-07-16'}]),patch('pipeline.kis_flows.run',return_value={'failures':[]}) as collect:
        daily('20260717',conn=object())
        assert collect.call_args.kwargs['end']==date(2026,7,16)
