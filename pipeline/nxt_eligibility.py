"""Reconcile official daily NXT snapshots and daily change events before KIS use.

Source rows must have passed date, pagination and duplicate validation. Missing
snapshots are errors. Never infer exclusion from a missing daily row alone.
"""
from collections import Counter, defaultdict
from datetime import date


def reconcile(*, days, snapshots, changes, asset_tickers, evidence, exclusions=()):
    if not days or days != sorted(set(days)) or not evidence:
        raise ValueError('ordered, unique coverage days and evidence required')
    if set(snapshots) != set(days) or set(changes) != set(days):
        raise ValueError('complete daily snapshot and event coverage required')
    if days[0] != '20250304':
        raise ValueError('reconstruction must start at NXT launch')
    initial={r['ticker'] for r in snapshots[days[0]]}
    initial_events={r['isuSrdCd'].removeprefix('A') for r in changes[days[0]] if r['addExlCd']=='편입'}
    if len(initial)!=10 or initial!=initial_events:
        raise ValueError('launch baseline must agree on all ten symbols')
    active={}; intervals=[]; previous={}; resolutions=Counter(); unknown=[]
    for day in days:
        daily={r['ticker']:r for r in snapshots[day]}
        if len(daily)!=len(snapshots[day]) or not daily:
            raise ValueError(f'duplicate/empty daily snapshot: {day}')
        seen=set()
        for event in changes[day]:
            ticker=event['isuSrdCd'].removeprefix('A')
            if event['aggDd']!=day or ticker in seen or event['addExlCd'] not in ('편입','편출'):
                raise ValueError('ambiguous change event')
            seen.add(ticker);active[ticker]=event['addExlCd']=='편입'
        for aid,ticker in asset_tickers.items():
            row=daily.get(ticker)
            excluded=[v for v in exclusions if v['ticker']==ticker and v['start']<=day<=v['end']]
            if row:
                if excluded:
                    raise ValueError('exclusion conflicts with a daily source observation')
                if row['date']!=day:
                    raise ValueError('wrong snapshot date')
                state=row['state'];volume=row.get('volume')
                if state in ('SOURCE_PERMITTED','SOURCE_PARTIAL_PERMITTED'):
                    status='ELIGIBLE';basis='OFFICIAL_DAILY_PERMISSION'
                    if not active.get(ticker,False):
                        resolutions['daily_permission_over_missing_reentry']+=1
                elif state=='RESTRICTED' and volume==0:
                    status='INELIGIBLE';basis='OFFICIAL_RESTRICTION_ZERO_VOLUME'
                    if not active.get(ticker,False):
                        resolutions['restricted_row_matches_exclusion']+=1
                else:
                    unknown.append({'asset_id':aid,'ticker':ticker,'date':day,'reason':'unresolved daily status'})
                    continue
            elif excluded:
                if len(excluded)!=1 or not excluded[0].get('evidence'):
                    raise ValueError('exclusion needs one authoritative interval')
                status='INELIGIBLE';basis='OFFICIAL_DELISTING_OR_EXCLUSION'
                resolutions['official_exclusion_over_missing_exit']+=1
            elif not active.get(ticker,False):
                status='INELIGIBLE';basis='LAUNCH_BASELINE_AND_DAILY_EVENT_EXCLUSION'
            else:
                unknown.append({'asset_id':aid,'ticker':ticker,'date':day,'reason':'active event but no snapshot'})
                continue
            # Preserve partial-session vs full permission and restriction reason.
            session_state=row['state'] if row else 'ABSENT_CONFIRMED'
            signature=(status,basis,session_state)
            old=previous.get(aid)
            iso=date.fromisoformat(day).isoformat()
            if old and old[0]==signature:
                old[1]['end']=iso;old[1]['observed_days']+=1
            else:
                item={'asset_id':aid,'start':iso,'end':iso,'status':status,
                      'basis':basis,'session_state':session_state,'observed_days':1,'evidence':evidence}
                intervals.append(item);previous[aid]=(signature,item)
    if unknown:
        raise ValueError(f'unresolved NXT coverage: {len(unknown)}; samples={unknown[:20]}')
    return intervals,dict(resolutions)
