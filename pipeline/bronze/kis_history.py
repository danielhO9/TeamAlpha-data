"""Bounded, resumable KIS history capture. No order calls or Silver writes."""
from __future__ import annotations

import hashlib
import json
import math
import os
import threading
import time
from datetime import date, datetime, timedelta, timezone

import requests

from pipeline.bronze import kis_market_flows as api
from pipeline.common.sink import read_bytes, write_text

ENDPOINTS = {
    'investor': (api.INVESTOR_PATH, api.INVESTOR_TR_ID, 'output2', 'stck_bsop_date'),
    'short': (api.SHORT_SALE_PATH, api.SHORT_SALE_TR_ID, 'output2', 'stck_bsop_date'),
    'volume': ('/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice',
               'FHKST03010100', 'output2', 'stck_bsop_date'),
}


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


class RequestGate:
    """One evenly spaced request budget shared by all threads, including retries.

    This is a process-local gate: run only one collector for a KIS account.
    Explicitly opt in to a faster interval after checking the account's quota.
    """
    def __init__(self, interval):
        if not math.isfinite(interval) or interval < 1 / 18:
            raise ValueError('KIS request interval must be finite and >= 1/18 seconds')
        self.interval = interval
        self.lock = threading.Lock()
        self.last_request = None
        self.blocked_until = 0.0
        self.requests = 0
        self.retries = {}

    def wait(self):
        with self.lock:
            now = time.monotonic()
            due = max(now if self.last_request is None else self.last_request + self.interval,
                      self.blocked_until)
            # Recheck the clock: never release a permit early after an interrupted sleep.
            while now < due:
                time.sleep(due - now)
                now = time.monotonic()
            self.last_request = now
            self.requests += 1

    def retry(self, code, delay):
        with self.lock:
            self.retries[code] = self.retries.get(code, 0) + 1
            if code == 'EGW00201':
                self.blocked_until = max(self.blocked_until, time.monotonic() + delay)

    def metrics(self):
        with self.lock:
            return {'requests': self.requests, 'retries': dict(self.retries),
                    'interval_seconds': self.interval}


class Client:
    def __init__(self, root, *, session=None, token=None, interval=None):
        self.root = root.rstrip('/')
        self._session = session
        self._local = threading.local()
        self._token_lock = threading.Lock()
        self.token = token
        self._next_token_attempt = 0.0
        self.token_issued_at = time.monotonic() if token else None
        self.interval = float(os.environ.get('KIS_HISTORY_INTERVAL_SECONDS', '1.05')
                              if interval is None else interval)
        self.gate = RequestGate(self.interval)
        # Construct the SDK client before worker threads start, then reuse its pool.
        self._s3 = None
        if self.root.startswith('s3://'):
            import boto3
            from botocore.config import Config
            self._s3 = boto3.Session().client('s3', config=Config(max_pool_connections=32))

    @property
    def session(self):
        if self._session is not None:
            return self._session  # Injected sessions must support the caller's concurrency.
        if not hasattr(self._local, 'session'):
            self._local.session = requests.Session()
        return self._local.session

    def _read(self, uri):
        if self._s3 is None:
            return read_bytes(uri)
        from botocore.exceptions import ClientError
        bucket, _, key = uri[5:].partition('/')
        try:
            return self._s3.get_object(Bucket=bucket, Key=key)['Body'].read()
        except ClientError as exc:
            if exc.response.get('Error', {}).get('Code') in {'NoSuchKey', '404'}:
                return None
            raise

    def _write(self, text, uri):
        if self._s3 is None:
            return write_text(text, uri)
        bucket, _, key = uri[5:].partition('/')
        self._s3.put_object(Bucket=bucket, Key=key, Body=text.encode('utf-8'))
        return uri

    def page(self, kind, ticker, venue, start, end):
        if kind not in ENDPOINTS or venue not in ('J', 'NX', 'UN'):
            raise ValueError('unsupported KIS request')
        if kind == 'short' and venue != 'J':
            raise ValueError('NXT/UN short-sale endpoint is not certified')
        api._ticker(ticker)
        path, tr_id, output, date_key = ENDPOINTS[kind]
        params = {'FID_COND_MRKT_DIV_CODE': venue, 'FID_INPUT_ISCD': ticker}
        if kind == 'investor':
            params.update(FID_INPUT_DATE_1=end.strftime('%Y%m%d'),
                          FID_ORG_ADJ_PRC='', FID_ETC_CLS_CODE='')
        else:
            params.update(FID_INPUT_DATE_1=start.strftime('%Y%m%d'),
                          FID_INPUT_DATE_2=end.strftime('%Y%m%d'))
        if kind == 'volume':
            params.update(FID_PERIOD_DIV_CODE='D', FID_ORG_ADJ_PRC='1')
        with self._token_lock:
            if self.token is None or (self.token_issued_at is not None and time.monotonic() - self.token_issued_at > 23 * 3600):
                for attempt in range(2):
                    # The token endpoint has a separate one-per-minute budget.
                    now = time.monotonic()
                    while now < self._next_token_attempt:
                        time.sleep(self._next_token_attempt - now)
                        now = time.monotonic()
                    self._next_token_attempt = now + 61
                    try:
                        self.token = api.issue_token(self.session)
                        break
                    except RuntimeError as exc:
                        if attempt or 'EGW00133' not in str(exc):
                            raise
                self.token_issued_at = time.monotonic()
            token = self.token
        raw, body, headers = api._request(session=self.session,
            access_token=token, path=path, tr_id=tr_id, params=params,
            before_request=self.gate.wait, on_retry=self.gate.retry)
        fetched = datetime.now(timezone.utc).isoformat()
        request_id = digest({'path': path, 'params': params})
        content_hash = hashlib.sha256(raw).hexdigest()
        uri = f'{self.root}/market_flows/kis_history/{request_id}/{content_hash}.json'
        prior = self._read(uri)
        if prior is not None and prior != raw:
            raise RuntimeError('immutable Bronze hash conflict')
        if prior is None:
            self._write(raw.decode('utf-8'), uri)
        receipt = {'schema': 'kis-history-v1', 'kind': kind, 'ticker': ticker,
                   'venue': venue, 'params': params, 'endpoint': path,
                   'raw_uri': uri, 'sha256': content_hash, 'fetched_at': fetched,
                   'headers': headers, 'provider_available_at': None}
        self._write(json.dumps(receipt, sort_keys=True),
                   f'{self.root}/market_flows/kis_history/observations/{request_id}/{fetched}.json')
        # Keep successful but malformed/empty responses in Bronze too.
        rows = body.get(output)
        if not isinstance(rows, list):
            raise ValueError(f'{kind}: missing row array {output}; raw={uri}')
        normalized = []
        for row in rows:
            if not row or not row.get(date_key):
                continue
            day = datetime.strptime(row[date_key], '%Y%m%d').date()
            if day > end:
                raise ValueError('response contains dates after requested anchor')
            normalized.append((day, row))
        return normalized, receipt

    def history(self, kind, ticker, venue, start, end):
        cursor = end
        found = {}
        receipts = []
        for _ in range(10000):
            if cursor < start:
                break
            rows, receipt = self.page(kind, ticker, venue, start, cursor)
            receipts.append(receipt)
            if not rows:
                break  # caller must classify missing dates; never fabricate zeros
            for day, row in rows:
                if start <= day <= end:
                    if day in found and found[day] != row:
                        raise ValueError(f'conflicting overlapping page: {ticker} {day}')
                    found[day] = row
            oldest = min(day for day, _ in rows)
            if oldest > cursor:
                raise ValueError('pagination did not advance')
            cursor = oldest - timedelta(days=1)
        else:
            raise ValueError('pagination safety bound exceeded')
        return found, receipts
