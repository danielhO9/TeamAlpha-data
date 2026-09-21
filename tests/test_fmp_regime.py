import json
from datetime import date, datetime, timezone

import pytest

from pipeline import fmp_regime as r
from pipeline.bronze.fmp import RawResponse


def price(day='2026-09-14', symbol='HYG'):
    return dict(symbol=symbol, date=day, open=80, high=82, low=79, close=81, volume=0)


def test_windows_cover_range_without_overlap():
    parts = list(r.windows(date(2015, 1, 1), date(2015, 4, 1)))
    assert parts[0][0] == date(2015, 1, 1)
    assert parts[-1][1] == date(2015, 4, 1)
    assert sum((b-a).days+1 for a,b in parts) == 91
    assert all((b-a).days < 28 for a,b in parts)


@pytest.mark.parametrize('change', [dict(close=None), dict(close=float('nan')),
    dict(close=True), dict(high=70), dict(volume=-1), dict(symbol='SPY'),
    dict(date='2026-09-16')])
def test_price_rejects_bad_values(change):
    with pytest.raises(ValueError):
        r.parse(json.dumps([{**price(), **change}]), 'HYG', date(2026,9,1), date(2026,9,15))


def test_duplicates_and_error_payload_rejected():
    for body in [json.dumps([price(),price()]), '{"Error Message":"not authorized"}']:
        with pytest.raises(ValueError):
            r.parse(body,'HYG',date(2026,9,1),date(2026,9,15))


def test_treasury_preserves_zero_negative_and_null_maturity():
    body=json.dumps([dict(date='2015-01-02',month2=None,month3=0,year2=-0.1,year10=2)])
    rows=r.parse(body,'US_TREASURY',date(2015,1,1),date(2015,1,31))
    assert {x[1]:x[2] for x in rows} == dict(month3=0,year2=-0.1,year10=2)
    assert {x[3] for x in rows} == {'percent'}


def test_dxy_zero_weekend_is_audited_but_weekday_fails():
    row={**price('2026-09-06','DX-Y.NYB'),'low':0,'close':0}
    excluded=[]
    assert r.parse(json.dumps([row]),'DX-Y.NYB',date(2026,9,1),date(2026,9,15),excluded=excluded)==[]
    assert excluded==[{'date':'2026-09-06','reason':'DXY_ZERO_WEEKEND'}]
    with pytest.raises(ValueError):
        r.parse(json.dumps([{**row,'date':'2026-09-07'}]),'DX-Y.NYB',date(2026,9,1),date(2026,9,15))


def test_bronze_resume_and_fresh_snapshot(tmp_path,monkeypatch):
    monkeypatch.setattr(r,'base_uri',lambda dest:str(tmp_path))
    class Client:
        calls=0
        def get(self,endpoint,params):
            self.calls+=1
            body=([dict(date='2026-09-14',month3=4,year2=4,year10=4)]
                  if endpoint=='treasury-rates' else [price(symbol=params['symbol'])])
            return RawResponse(endpoint,params,200,'application/json',datetime.now(timezone.utc),json.dumps(body).encode())
    c=Client()
    kwargs=dict(start=date(2026,9,10),end=date(2026,9,15),client=c)
    first=r.run(**kwargs)
    original = {p: p.read_bytes() for p in tmp_path.rglob('*.json')}
    r.run(**kwargs)
    assert c.calls==9 and len(first['latest'])==9
    assert all(p.read_bytes() == body for p, body in original.items())
    r.run(**kwargs,refresh_id='refresh-2')
    assert c.calls==18
    assert all(p.read_bytes() == body for p, body in original.items())


@pytest.mark.parametrize('damage', [
    'payload', 'same_length_payload', 'manifest', 'manifest_array',
    'payload_only', 'manifest_only', 'incomplete', 'request_mismatch',
])
def test_existing_snapshot_damage_never_calls_api_or_overwrites(tmp_path, monkeypatch, damage):
    monkeypatch.setattr(r, 'base_uri', lambda dest: str(tmp_path))
    class Client:
        calls = 0
        def get(self, endpoint, params):
            self.calls += 1
            body = ([dict(date='2026-09-14', month3=4, year2=4, year10=4)]
                    if endpoint == 'treasury-rates' else [price(symbol=params['symbol'])])
            return RawResponse(endpoint, params, 200, 'application/json',
                               datetime.now(timezone.utc), json.dumps(body).encode())
    client = Client()
    kwargs = dict(start=date(2026, 9, 10), end=date(2026, 9, 15), client=client)
    r.run(**kwargs)
    prefix = (tmp_path / 'regime/fmp/series=^VIX/from=2026-09-10'
              / 'to=2026-09-15/snapshot=backfill-v1')
    payload, manifest = prefix / 'response.json', prefix / 'manifest.json'
    if damage == 'payload':
        payload.write_bytes(b'corrupt')
    elif damage == 'same_length_payload':
        payload.write_bytes(payload.read_bytes().replace(b'81', b'82'))
    elif damage == 'manifest':
        manifest.write_bytes(b'not json')
    elif damage == 'manifest_array':
        manifest.write_bytes(b'[]')
    elif damage == 'payload_only':
        manifest.unlink()
    elif damage == 'manifest_only':
        payload.unlink()
    else:
        metadata = json.loads(manifest.read_bytes())
        metadata['complete' if damage == 'incomplete' else 'request_params'] = (
            False if damage == 'incomplete' else {})
        manifest.write_bytes(json.dumps(metadata).encode())
    damaged = {p: p.read_bytes() for p in tmp_path.rglob('*.json')}
    with pytest.raises(ValueError, match='corrupt or incomplete'):
        r.run(**kwargs)
    assert client.calls == 9
    assert {p: p.read_bytes() for p in tmp_path.rglob('*.json')} == damaged
    # Recovery uses a distinct snapshot; the damaged evidence stays unchanged.
    r.run(**kwargs, refresh_id='recovery-2')
    assert client.calls == 18
    assert all(p.read_bytes() == body for p, body in damaged.items())


def test_s3_same_size_corruption_fails_before_common_resume(monkeypatch):
    import hashlib
    from pipeline.bronze import fmp
    root, prefix = 's3://test-bronze', 'regime/test/snapshot=test'
    payload_uri, manifest_uri = root + '/' + prefix + '/response.json', root + '/' + prefix + '/manifest.json'
    original = b'original'
    objects = {
        payload_uri: b'corrupt!',
        manifest_uri: json.dumps(dict(
            complete=True, object_uri=payload_uri, content_length=len(original),
            sha256=hashlib.sha256(original).hexdigest(), provider='FMP',
            endpoint='treasury-rates', request_params={}, status_code=200,
        )).encode(),
    }
    before = dict(objects)
    monkeypatch.setattr(r, 'read_bytes', objects.get)
    monkeypatch.setattr(fmp, 'read_bytes', objects.get)
    monkeypatch.setattr(r, 'collect_raw', lambda *a, **k: pytest.fail('must not recollect'))
    with pytest.raises(ValueError, match='corrupt or incomplete'):
        r.collect_snapshot(object(), root=root, endpoint='treasury-rates', params={}, prefix=prefix)
    assert objects == before


def test_daily_fails_on_missing_or_stale_series(monkeypatch):
    monkeypatch.setattr(r,'run',lambda *a,**kw:{'latest':{'^VIX':'2026-09-01'}})
    with pytest.raises(RuntimeError,match='stale'):
        r.run_daily('20260915')


def test_certified_equity_batch_does_not_skip_regime(monkeypatch):
    from pipeline import daily_full as daily
    calls=[]
    monkeypatch.setattr(daily.dart_silver_backfill_ecs,'assert_daily_certification_lock',lambda c:None)
    monkeypatch.setattr(daily.repository,'certified_target_exists',lambda *a:True)
    monkeypatch.setattr(daily.fmp_macro,'run_daily',lambda d:None)
    monkeypatch.setattr(daily.fmp_external,'run_daily',lambda d:None)
    monkeypatch.setattr(daily.fmp_regime,'run_daily',lambda d:calls.append(d))
    daily._run_fmp_incremental('bucket',None,'20260915',certification_lock=object())
    assert calls==['20260914']
