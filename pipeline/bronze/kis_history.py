"""Bounded, resumable KIS history capture. No order calls or Silver writes."""
from __future__ import annotations

import hashlib
import json
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


class Client:
    def __init__(self, root, *, session=None, token=None, interval=1.05):
        self.root = root.rstrip('/')
        self.session = session or requests.Session()
        self.token = token
        self.token_issued_at = time.monotonic() if token else None
        self.interval = max(1.0, interval)
        self.last_request = 0.0

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
        if self.token is None or (self.token_issued_at is not None and time.monotonic() - self.token_issued_at > 23 * 3600):
            self.token = api.issue_token(self.session)
            self.token_issued_at = time.monotonic()
        for attempt in range(4):
            time.sleep(max(0, self.interval - (time.monotonic() - self.last_request)))
            self.last_request = time.monotonic()
            try:
                raw, body, headers = api._request(session=self.session,
                    access_token=self.token, path=path, tr_id=tr_id, params=params)
                break
            except (requests.ConnectionError, requests.Timeout):
                if attempt == 3:
                    raise
                time.sleep(2 ** attempt)
        fetched = datetime.now(timezone.utc).isoformat()
        request_id = digest({'path': path, 'params': params})
        content_hash = hashlib.sha256(raw).hexdigest()
        uri = f'{self.root}/market_flows/kis_history/{request_id}/{content_hash}.json'
        prior = read_bytes(uri)
        if prior is not None and prior != raw:
            raise RuntimeError('immutable Bronze hash conflict')
        if prior is None:
            write_text(raw.decode('utf-8'), uri)
        receipt = {'schema': 'kis-history-v1', 'kind': kind, 'ticker': ticker,
                   'venue': venue, 'params': params, 'endpoint': path,
                   'raw_uri': uri, 'sha256': content_hash, 'fetched_at': fetched,
                   'headers': headers, 'provider_available_at': None}
        write_text(json.dumps(receipt, sort_keys=True),
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
