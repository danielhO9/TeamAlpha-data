"""Daily KIS collection under explicit provider trust, without KRX web login.

The research export is pinned by content hash; its source date is never advanced.
Provider listing metadata and public NXT reference deltas are captured before expected-date validation.
Only reference metadata is checkpointed here. KIS row checks remain in kis_flows.
"""
from __future__ import annotations

import copy
import json
import os
from datetime import date, datetime, timedelta, timezone

import requests

from pipeline.bronze.kis_history import digest
from pipeline.common.sink import write_text
from pipeline.silver import asset_lifecycle


def close_periods(periods, events, first, last):
    result = copy.deepcopy(periods)
    for ticker, day in events:
        if not first <= day <= last: raise ValueError('provider delisting date outside requested delta')
        matches = [r for r in result if r['ticker'] == ticker
                   and date.fromisoformat(r['start']) < day
                   and (r['end'] is None or date.fromisoformat(r['end']) >= day)]
        if not matches: continue  # Not a member of the pinned historical watchlist.
        if len(matches) != 1: raise ValueError('ambiguous delisting identity')
        matches[0]['end'] = (day - timedelta(days=1)).isoformat()
    return result


def nxt_day(day, body, changes, active, tickers, exclusions, evidence):
    """Continue the certified launch/event baseline; absence alone is not exclusion."""
    key = day.strftime('%Y%m%d')
    for value, field in ((body, 'brdinfoTimeList'), (changes, 'trdisuChgList')):
        rows = value[field]
        if int(value['totalCnt']) != len(rows) or any(r['aggDd'] != key for r in rows):
            raise ValueError('incomplete or wrong-date NXT response')
    rows = body['brdinfoTimeList']
    daily = {r['isuSrdCd'].removeprefix('A'): r for r in rows}
    if not daily or len(daily) != len(rows): raise ValueError('empty/duplicate NXT snapshot')
    active = dict(active)
    seen = set()
    for event in changes['trdisuChgList']:
        ticker = event['isuSrdCd'].removeprefix('A')
        if ticker in seen or event['addExlCd'] not in ('편입', '편출'):
            raise ValueError('ambiguous NXT change event')
        seen.add(ticker)
        active[ticker] = event['addExlCd'] == '편입'
    intervals = []
    for aid, ticker in tickers.items():
        row = daily.get(ticker)
        if row:
            if ticker in exclusions: raise ValueError('NXT observation conflicts with permanent exclusion')
            reason, halt, code = row.get('trdIpsbRsn'), row.get('tdhlYn'), row.get('cptrTrdPmsnCd')
            permitted = halt in (None, 'N') and reason in (None, '', '-')
            if permitted and (code == '7' or (code == '6' and row.get('cptrTrdPmsnCdNm') == '메인+애프터 마켓')):
                status, basis = 'ELIGIBLE', 'OFFICIAL_DAILY_PERMISSION'
            elif (halt == 'Y' or code == '0' or reason not in (None, '', '-')) and row.get('accTdQty') == 0:
                status, basis = 'INELIGIBLE', 'OFFICIAL_RESTRICTION_ZERO_VOLUME'
            else: raise ValueError(f'unresolved NXT status: {ticker} {key}')
        elif ticker in exclusions:
            if not exclusions[ticker]: raise ValueError('exclusion evidence required')
            status, basis = 'INELIGIBLE', 'OFFICIAL_DELISTING_OR_EXCLUSION'
        elif not active.get(ticker, False):
            status, basis = 'INELIGIBLE', 'LAUNCH_BASELINE_AND_DAILY_EVENT_EXCLUSION'
        else: raise ValueError(f'active NXT event but missing daily row: {ticker} {key}')
        intervals.append(dict(asset_id=int(aid), start=str(day), end=str(day), status=status,
                              basis=basis, evidence=evidence, observed_days=1))
    return active, intervals


def _freeze(root, label, value):
    uri = f'{root}/daily_references/{label}/{digest(value)}.json'
    write_text(json.dumps(value, sort_keys=True), uri)
    return uri


def _fetch(session, root, url, params, *, html=False):
    response = session.post(url, data=params, timeout=45)
    response.raise_for_status()
    # Evidence includes exact request and response bytes, without credentials/cookies.
    import base64
    evidence = dict(url=url, params=params, observed_at=datetime.now(timezone.utc).isoformat(),
                    body_base64=base64.b64encode(response.content).decode())
    uri = _freeze(root, 'sources', evidence)
    return (response.content if html else response.json()), uri


def audit_periods(conn, periods, ids, sessions):
    """Independent calendar/listing expectations versus certified prices and PIT identifiers."""
    expected = {(r['asset_id'], d): r['ticker'] for r in periods for d in sessions
                if date.fromisoformat(r['start']) <= d and (r['end'] is None or d <= date.fromisoformat(r['end']))}
    with conn.cursor() as cur:
        cur.execute('''SELECT p.asset_id,p.trade_date,q.status,i.identifier,i.asset_id
            FROM price_daily p LEFT JOIN dq_run q ON q.run_id=p.quality_run_id
            LEFT JOIN asset_identifier i ON i.asset_id=p.asset_id AND i.source='KRX'
              AND i.identifier_type='ticker' AND i.valid_from<=p.trade_date
              AND (i.valid_to IS NULL OR i.valid_to>=p.trade_date)
            WHERE p.source='KRX' AND p.market IN ('KOSPI','KOSDAQ')
              AND p.asset_id=ANY(%s) AND p.trade_date BETWEEN %s AND %s''', (ids, min(sessions), max(sessions)))
        rows = cur.fetchall()
    conn.rollback()
    seen = set()
    errors = dict(identity_errors=0, prices_outside_periods=0, duplicate_prices=0, uncertified_prices=0)
    for aid, day, status, ticker, mapped in rows:
        key = (aid, day)
        if key in seen: errors['duplicate_prices'] += 1
        seen.add(key)
        if key not in expected: errors['prices_outside_periods'] += 1
        if status != 'CERTIFIED': errors['uncertified_prices'] += 1
        if mapped != aid or ticker != expected.get(key): errors['identity_errors'] += 1
    errors['expected_price_gaps'] = len(set(expected) - seen)
    if any(errors.values()): raise ValueError(f'daily listing/price coverage failed: {errors}')
    return dict(asset_ids=ids, periods_sha256=digest(periods), coverage_start=str(min(sessions)),
                verified_through=str(max(sessions)), price_rows=len(rows), **errors)


def refresh_listing(conn, periods, ids, sessions, through, target, client, evidence):
    # Missing expected prices are candidates for metadata lookup, never proof of delisting.
    expected = {(r['asset_id'], d) for r in periods for d in sessions
                if date.fromisoformat(r['start']) <= d and (r['end'] is None or d <= date.fromisoformat(r['end']))}
    with conn.cursor() as cur:
        cur.execute("""SELECT asset_id,trade_date FROM price_daily
            WHERE source='KRX' AND market IN ('KOSPI','KOSDAQ') AND asset_id=ANY(%s)
              AND trade_date BETWEEN %s AND %s""", (ids, min(sessions), max(sessions)))
        actual = set(cur.fetchall())
    conn.rollback()
    missing = expected - actual
    events = []
    for aid in sorted({a for a, d in missing}):
        candidates = [r for r in periods if r['asset_id']==aid and r['end'] is None]
        if len(candidates)!=1 or client is None:
            raise ValueError(f'expected price missing without one live listing: {aid}')
        ticker = candidates[0]['ticker']
        row, receipt = client.stock_info(ticker)
        value = row.get('lstg_abol_dt', '')
        # Use the terminal product date, not a market-transfer exit date.
        if not value or value=='00000000':
            raise ValueError(f'expected price missing; KIS does not confirm terminal delisting: {ticker}')
        day = datetime.strptime(value, '%Y%m%d').date()
        if any(d < day for a, d in missing if a==aid):
            raise ValueError(f'price missing before provider delisting: {ticker}')
        events.append((ticker, day))
        evidence.append(receipt['raw_uri'])
    return close_periods(periods, events, through + timedelta(days=1), target) if events else periods


def prepare(conn, manifest, policy, sessions, root, checkpoint_uri, *, session=None, client=None):
    """Refresh public references. The shared daily writer lock must be held by caller."""
    from pipeline.kis_flows import read_json
    state = read_json(checkpoint_uri)
    unsigned = {k: v for k, v in state.items() if k != 'sha256'}
    if state.get('sha256') != digest(unsigned): raise ValueError('reference checkpoint hash mismatch')
    if state['research_export_sha256'] != manifest['sha256']:
        raise ValueError('watchlist changed; reconcile a new reference seed before collecting')
    if state.get('schema') != 'kis-daily-reference-v1': raise ValueError('invalid reference checkpoint schema')
    through = date.fromisoformat(state['through'])
    target = max(sessions)
    if through > target: raise ValueError('reference checkpoint newer than requested daily target')
    if target - through > timedelta(days=40): raise ValueError('reference gap exceeds bounded daily recovery; run reference catch-up')
    session = session or requests.Session()
    state = copy.deepcopy(state)
    evidence = []
    if target > through:
        import exchange_calendars as xcals
        cal = xcals.get_calendar('XKRX', start=str(through), end=str(target + timedelta(days=7)))
        closed = {r['date'] for r in policy.get('calendar_exclusions', [])}
        for value in cal.sessions_in_range(str(through + timedelta(days=1)), str(target)):
            day = value.date()
            if str(day) in closed: continue
            common = dict(scMktId='', searchKeyword='', pageIndex=1, pageUnit=1000)
            body, uri = _fetch(session, root, 'https://www.nextrade.co.kr/brdinfoTime/brdinfoTimeList.do',
                               dict(common, scAggDd=day.strftime('%Y%m%d')))
            changes, changes_uri = _fetch(session, root, 'https://www.nextrade.co.kr/trdisuChg/trdisuChgList.do',
                                          dict(common, scBeginDe=day.strftime('%Y%m%d'), scEndDe=day.strftime('%Y%m%d')))
            evidence.extend([uri, changes_uri])
            state['nxt_event_active'], intervals = nxt_day(day, body, changes, state['nxt_event_active'],
                state['asset_tickers'], state['permanent_exclusions'], uri)
            state['nxt_intervals'].extend(intervals)
    periods = refresh_listing(conn, state['periods'], manifest['asset_ids'], sessions, through, target, client, evidence)
    state['periods'] = periods
    # Validate period shape/overlap before forming dictionary expectations.
    shape = dict(schema='asset-listing-snapshot-v1', asset_ids=manifest['asset_ids'],
                 coverage_start=str(min(sessions)), verified_through=str(target),
                 observed_at=datetime.now(timezone.utc).isoformat(), periods=periods,
                 evidence_uri=checkpoint_uri, audit_sha256='0'*64, evidence_sha256='0'*64)
    shape['snapshot_id'] = digest(shape)
    asset_lifecycle.validate(shape, manifest['asset_ids'], min(sessions), target)
    audit = audit_periods(conn, periods, manifest['asset_ids'], sessions)
    proof = dict(parent_checkpoint=state['sha256'], parent_uri=checkpoint_uri, sources=evidence,
                 audit=audit, verification_basis='PUBLIC_REFERENCE_AND_CERTIFIED_PRICE_COVERAGE')
    evidence_uri = _freeze(root, 'listing_audit', proof)
    contract = {k: v for k, v in shape.items() if k != 'snapshot_id'}
    import hashlib
    contract.update(evidence_uri=evidence_uri,
                    evidence_sha256=hashlib.sha256(json.dumps(proof, sort_keys=True).encode()).hexdigest(),
                    audit_sha256=digest(audit))
    contract['snapshot_id'] = digest(contract)
    snapshot_id = asset_lifecycle.publish(conn, contract, audit)
    current = copy.deepcopy(manifest)
    current.update(listing_snapshot_id=snapshot_id, nxt_intervals=state['nxt_intervals'],
                   nxt_verified_through=str(target), daily_watchlist_basis='PINNED_RESEARCH_EXPORT')
    current.pop('sha256')
    current['sha256'] = digest(current)
    manifest_uri = _freeze(root, 'manifests', current)
    state.update(through=str(target), last_evidence_uri=evidence_uri)
    state.pop('sha256')
    state['sha256'] = digest(state)
    # Save immutable state first, then update the small mutable pointer body. No secrets.
    state_uri = _freeze(root, 'states', state)
    return manifest_uri, state_uri, state


def verify_collection(conn, manifest_uri, policy, sessions):
    from pipeline import kis_flows as k
    manifest = k.read_json(manifest_uri)
    partitions = k.expected_partitions(conn, manifest, min(sessions), max(sessions), policy.get('calendar_exclusions', []))
    expected = set()
    for (aid, ticker, window), days in partitions.items():
        for day in days:
            expected.update((aid, day, venue, kind) for venue, kind in [('J','investor'), ('J','short'), ('UN','investor')])
        eligible, _ = k.nxt_dates(manifest, aid, days)
        expected.update((aid, day, 'NX', 'investor') for day in eligible)
    with conn.cursor() as cur:
        cur.execute("""SELECT asset_id,trade_date,venue,kind,payload->>'ratio_status'
            FROM kis_market_latest WHERE asset_id=ANY(%s) AND trade_date BETWEEN %s AND %s
            AND policy_version=%s""", (manifest['asset_ids'], min(sessions), max(sessions), policy['version']))
        rows = cur.fetchall()
    conn.rollback()
    actual = {tuple(r[:4]) for r in rows}
    bad = sum(r[3]=='short' and r[4] not in ('VALID','ZERO_DENOMINATOR') for r in rows)
    result = dict(expected=len(expected), actual=len(actual), missing=len(expected-actual),
                  unexpected=len(actual-expected), duplicate=len(rows)-len(actual), invalid_ratio=bad)
    if any(result[x] for x in ('missing','unexpected','duplicate','invalid_ratio')):
        raise ValueError(f'KIS daily final coverage failed: {result}')
    return result


def daily(day, *, conn):
    from pipeline import kis_flows as k
    target = datetime.strptime(day, '%Y%m%d').date()
    from zoneinfo import ZoneInfo
    if target >= datetime.now(ZoneInfo('Asia/Seoul')).date(): raise ValueError('completed days only')
    root = os.environ['KIS_BRONZE_ROOT'].rstrip('/')
    manifest = k.read_json(os.environ['KIS_UNIVERSE_URI'])
    if digest({a: b for a, b in manifest.items() if a != 'sha256'}) != manifest['sha256']:
        raise ValueError('research export hash mismatch')
    policy = k.checked_policy(k.read_json(os.environ['KIS_POLICY_URI']))
    if policy.get('short_market_basis') != 'PROVIDER_TRUST': raise ValueError('provider trust policy required')
    import exchange_calendars as xcals
    state = k.read_json(os.environ['KIS_DAILY_REFERENCE_URI'])
    through = date.fromisoformat(state['through'])
    lower = min(target-timedelta(days=30), through)
    calendar = xcals.get_calendar('XKRX', start=str(lower), end=str(target+timedelta(days=7)))
    closed = {r['date'] for r in policy.get('calendar_exclusions', [])}
    available = [v.date() for v in calendar.sessions_in_range(str(lower), str(target)) if str(v.date()) not in closed]
    if len(available) < 5: raise ValueError('five market sessions required')
    sessions = [d for d in available if d >= min(available[-5], through + timedelta(days=1))]
    client = k.Client(root)
    manifest_uri, state_uri, pending_state = prepare(conn, manifest, policy, sessions, root, os.environ['KIS_DAILY_REFERENCE_URI'], client=client)
    result = k.run(conn=conn, manifest_uri=manifest_uri, policy_uri=os.environ['KIS_POLICY_URI'],
                   root=root, start=min(sessions), end=max(sessions), publish=True, refresh=True, client=client)
    result.update(reference_state_uri=state_uri, watchlist_source_as_of=manifest['as_of'],
                  market_scope_basis='PROVIDER_TRUST', krx_external_verification_required=False)
    _freeze(root, 'daily_results', result)
    if result['failures']: raise RuntimeError(f"KIS daily incomplete: {len(result['failures'])} partitions")
    result['coverage'] = verify_collection(conn, manifest_uri, policy, sessions)
    result['status'] = 'PASS'
    _freeze(root, 'daily_results', result)
    write_text(json.dumps(pending_state, sort_keys=True), os.environ['KIS_DAILY_REFERENCE_URI'])
    return result
