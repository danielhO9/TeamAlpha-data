"""Rate budgeting, bounded concurrency, and unchanged resume/publication semantics."""
from concurrent.futures import ThreadPoolExecutor
from datetime import date
import json
import threading
from unittest.mock import patch

import pytest
import requests

from pipeline.bronze import kis_history as history, kis_market_flows as api
from pipeline import kis_flows as k


class Clock:
    def __init__(self): self.now = 0.0
    def monotonic(self): return self.now
    def sleep(self, seconds): self.now += seconds


def test_all_workers_share_spacing_and_rate_limit_cooldown(monkeypatch):
    clock = Clock()
    monkeypatch.setattr(history.time, 'monotonic', clock.monotonic)
    monkeypatch.setattr(history.time, 'sleep', clock.sleep)
    gate = history.RequestGate(.056)
    with ThreadPoolExecutor(max_workers=12) as pool:
        list(pool.map(lambda _: gate.wait(), range(180)))
    assert clock.now == pytest.approx(179 * .056)
    gate.retry('EGW00201', 2)
    gate.wait()
    assert clock.now == pytest.approx(179 * .056 + 2)
    assert gate.metrics()['requests'] == 181
    assert gate.metrics()['retries'] == {'EGW00201': 1}


@pytest.mark.parametrize('interval', [0, -1, .01, float('nan'), float('inf')])
def test_invalid_or_over_quota_rate_rejected(interval):
    with pytest.raises(ValueError): history.RequestGate(interval)


@pytest.mark.parametrize('error', ['EGW00201', 'EGW00316', 'NETWORK'])
def test_every_retry_acquires_permit_and_is_bounded(monkeypatch, error):
    monkeypatch.setenv('KIS_APP_KEY', 'test-key')
    monkeypatch.setenv('KIS_APP_SECRET', 'test-secret')
    monkeypatch.setattr(api.time, 'sleep', lambda _: None)
    permits, retries = [], []
    class Session:
        def get(self, *a, **kw):
            assert len(permits) == len(retries) + 1
            if error == 'NETWORK': raise requests.Timeout('timeout')
            class Response:
                status_code = 500
                content = b'{}'
                def json(self): return {'rt_cd': '1', 'msg_cd': error, 'msg1': 'retry'}
            return Response()
    with pytest.raises((RuntimeError, requests.Timeout)):
        api._request(session=Session(), access_token='test', path='/test', tr_id='test',
                     params={}, before_request=lambda: permits.append(1),
                     on_retry=lambda code, delay: retries.append((code, delay)))
    assert len(permits) == 4
    assert retries == [(error, 1), (error, 2), (error, 4)]


def test_concurrent_pages_share_one_token_and_preserve_raw(tmp_path, monkeypatch):
    tokens = []
    monkeypatch.setattr(api, 'issue_token', lambda session: tokens.append(1) or 'token')
    def request(**kwargs):
        assert kwargs['access_token'] == 'token'
        return b'{"output2":[]}', {'output2': []}, {}
    monkeypatch.setattr(api, '_request', request)
    client = history.Client(str(tmp_path))
    with ThreadPoolExecutor(max_workers=8) as pool:
        rows = list(pool.map(lambda i: client.page('investor', f'{i:06d}', 'J',
                        date(2015,1,2), date(2015,1,5)), range(16)))
    assert len(tokens) == 1
    assert all(not data for data, receipt in rows)
    for _, receipt in rows:
        assert history.read_bytes(receipt['raw_uri']) == b'{"output2":[]}'


def test_prefetch_overlaps_bounds_memory_and_surfaces_failure_in_order():
    barrier = threading.Barrier(3)
    started = []
    def collect(i):
        started.append(i)
        if i < 3: barrier.wait(timeout=3)
        if i == 1: raise ValueError('source incomplete')
        return i
    iterator = k._prefetch(range(8), collect, 3)
    item, future = next(iterator)
    assert future.result() == 0 and item == 0
    assert set(started) == {0,1,2}  # Never queue an entire year of row payloads.
    item, future = next(iterator)
    assert item == 1
    with pytest.raises(ValueError, match='source incomplete'): future.result()
    assert [(item, future.result()) for item, future in iterator] == [(i,i) for i in range(2,8)]


def test_fast_resume_uses_same_checkpoints_and_main_thread_sql(tmp_path, monkeypatch):
    day = date(2015,1,2)
    manifest = {'scope':'union_of_all_historical_in_universe_rows',
                'asset_ids':[1,2,3], 'as_of':str(day)}
    manifest['sha256'] = history.digest(manifest)
    policy = {'version':'test', 'short_market':'UNKNOWN', 'venues':['J']}
    parts = {(i,f'{i:06d}',0):[day] for i in range(1,4)}
    published, collected = [], []
    main_thread = threading.get_ident()
    def collect(client, aid, ticker, days, venue, policy):
        assert threading.get_ident() != main_thread
        collected.append(aid)
        if aid == 2: raise ValueError('missing date')
        return [{'asset_id': aid}]
    def publish(conn, rows, fingerprint):
        assert threading.get_ident() == main_thread
        published.append((rows, fingerprint))
        return 'certified-run'
    class Conn:
        def rollback(self): assert threading.get_ident() == main_thread
    monkeypatch.setattr(k, 'read_json', lambda uri: manifest if uri == 'm' else policy)
    monkeypatch.setattr(k, 'checked_policy', lambda p: p)
    monkeypatch.setattr(k, 'expected_partitions', lambda *a: parts)
    monkeypatch.setattr(k.migrate, 'assert_current', lambda c: None)
    monkeypatch.setattr(k, 'collect_partition', collect)
    monkeypatch.setattr(k.silver, 'publish', publish)
    monkeypatch.setattr(k.repository, 'start_run', lambda *a, **kw: object())
    monkeypatch.setattr(k.repository, 'finish_run', lambda *a, **kw: None)
    args = dict(conn=Conn(), manifest_uri='m', policy_uri='p', root=str(tmp_path),
                start=day,end=day,publish=True,client=object())
    first = k.run(**args, workers=1)
    assert first['published'] == 2 and len(first['failures']) == 1
    keys = [fp for _,fp in published]
    assert keys == [history.digest({'aid':i,'ticker':f'{i:06d}','dates':[str(day)],
            'venue':'J','mode':'DIRECT','policy':policy,'universe':manifest['sha256']}) for i in (1,3)]
    collected.clear()
    second = k.run(**args, workers=4)
    assert second['skipped_partitions'] == 2 and second['published'] == 0
    assert collected == [2]  # Failed partition alone is retried at the new speed.
    assert len(published) == 2
    assert len(list(tmp_path.glob('market_flows/kis_history/checkpoints/*.json'))) == 2


def test_checkpoint_inventory_paginates_and_ignores_empty_or_nested(monkeypatch):
    import boto3
    key = 'a' * 64
    class Paginator:
        def paginate(self, **kwargs):
            assert kwargs == {'Bucket':'bucket','Prefix':'prefix/market_flows/kis_history/checkpoints/'}
            prefix = kwargs['Prefix']
            return [{'Contents':[{'Key':prefix+key+'.json','Size':20}]},
                    {'Contents':[{'Key':prefix+'b'*64+'.json','Size':0},
                                 {'Key':prefix+'nested/'+key+'.json','Size':20}]}]
    class S3:
        def get_paginator(self, name):
            assert name == 'list_objects_v2'
            return Paginator()
    monkeypatch.setattr(boto3, 'client', lambda name: S3())
    assert k._checkpoint_keys('s3://bucket/prefix') == {key}


def test_token_throttle_retries_once_after_minute_and_shares_result(tmp_path, monkeypatch):
    clock = Clock()
    monkeypatch.setattr(history.time, 'monotonic', clock.monotonic)
    monkeypatch.setattr(history.time, 'sleep', clock.sleep)
    calls = []
    def token(session):
        calls.append(clock.now)
        if len(calls) == 1: raise RuntimeError('KIS token code=EGW00133')
        return 'token'
    monkeypatch.setattr(api, 'issue_token', token)
    monkeypatch.setattr(api, '_request', lambda **kw: (b'{}', {'output2':[]}, {}))
    client = history.Client(str(tmp_path))
    with ThreadPoolExecutor(max_workers=12) as pool:
        list(pool.map(lambda i: client.page('investor', f'{i:06d}', 'J',
                    date(2015,1,2), date(2015,1,5)), range(24)))
    assert calls == [0,61]
