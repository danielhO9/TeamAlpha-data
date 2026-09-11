"""KIS history/backfill and opt-in daily collection. No implicit migrations."""
from __future__ import annotations

import argparse
import json
import os
from datetime import date, datetime, timedelta

from zoneinfo import ZoneInfo

import pandas as pd

from pipeline.bronze.kis_history import Client, digest
from pipeline.common import db
from pipeline.common.sink import read_bytes, write_text
from pipeline.silver import kis_flows as silver
from pipeline.silver_quality import migrate, repository
from pipeline.silver_quality.models import CheckResult,CheckStatus,Severity

NXT_START = date(2025, 3, 4)


def export_universe(monthly_csv, destination, nxt_intervals=None):
    """Export ALL historical memberships, never just the most recent month."""
    frame = pd.read_csv(monthly_csv, dtype={'Code': str, 'asset_id': str})
    required = {'asset_id','Code','trade_date','in_universe'}
    if not required <= set(frame.columns):
        raise ValueError(f'monthly export requires {required}')
    flags = frame.in_universe.astype(str).str.lower()
    if not flags.isin(['true','false','1','0']).all():
        raise ValueError('invalid membership flags')
    dates = pd.to_datetime(frame.trade_date, errors='raise')
    selected = frame.loc[flags.isin(['true','1']) & (dates>=pd.Timestamp('2015-01-01'))]
    if selected.empty:
        raise ValueError('empty historical research universe')
    members = selected[['asset_id','Code','trade_date']].drop_duplicates().sort_values(['asset_id','trade_date'])
    value = {'schema':'kis-universe-v1','as_of':dates.max().date().isoformat(),
             'scope':'union_of_all_historical_in_universe_rows',
             'asset_ids':sorted({int(x) for x in members.asset_id}),
             'memberships':members.to_dict('records')}
    if nxt_intervals is not None:
        value['nxt_intervals'] = read_json(nxt_intervals)
        if not isinstance(value['nxt_intervals'],list):
            raise ValueError('NXT interval file must be a JSON array')
    value['sha256'] = digest(value)
    write_text(json.dumps(value,sort_keys=True),destination)
    return value


def read_json(uri):
    raw = read_bytes(uri)
    if raw is None:
        raise ValueError(f'missing configuration: {uri}')
    return json.loads(raw)


def expected_partitions(conn, manifest, start, end):
    """Build expectations independently of price/identifier availability."""
    import exchange_calendars as xcals
    ids = manifest['asset_ids']
    if not ids or len(ids) != len(set(ids)):
        raise ValueError('empty or duplicate universe')
    calendar = xcals.get_calendar('XKRX', start=str(start-timedelta(days=7)), end=str(end+timedelta(days=7)))
    sessions = [v.date() for v in calendar.sessions_in_range(str(start), str(end))]
    try:
        with conn.cursor() as cur:
            cur.execute('SELECT asset_id,listed_from,listed_to FROM asset WHERE asset_id=ANY(%s)', (ids,))
            assets = cur.fetchall()
            if {r[0] for r in assets} != set(ids):
                raise ValueError('universe contains unknown RDS assets')
            if any(r[1] is None for r in assets):
                raise ValueError('listing start missing; cannot certify expected coverage')
            if any(r[2] is not None and r[2] < r[1] for r in assets):
                raise ValueError('invalid listing interval')
            cur.execute('''SELECT a.asset_id,i.identifier,d.day,p.trade_date,q.status
                FROM asset a CROSS JOIN unnest(%s::date[]) AS d(day)
                LEFT JOIN price_daily p ON p.asset_id=a.asset_id AND p.source='KRX'
                  AND p.trade_date=d.day AND p.market IN ('KOSPI','KOSDAQ')
                LEFT JOIN dq_run q ON q.run_id=p.quality_run_id
                LEFT JOIN asset_identifier i ON i.asset_id=a.asset_id
                  AND i.source='KRX' AND i.identifier_type='ticker'
                  AND i.valid_from<=d.day AND (i.valid_to IS NULL OR i.valid_to>=d.day)
                WHERE a.asset_id=ANY(%s) AND d.day>=a.listed_from
                  AND (a.listed_to IS NULL OR d.day<=a.listed_to)
                ORDER BY a.asset_id,d.day''', (sessions, ids))
            rows = cur.fetchall()
    finally:
        conn.rollback()
    seen = set(); partitions = {}; errors = []
    for aid,ticker,day,price_day,status in rows:
        if (aid,day) in seen:
            errors.append((aid,str(day),'ambiguous price/identifier'))
        seen.add((aid,day))
        if ticker is None or price_day is None or status != 'CERTIFIED':
            errors.append((aid,str(day),'missing identifier, price or certification'))
            continue
        key = (aid,ticker,(day-start).days//90)
        partitions.setdefault(key,[]).append(day)
    if errors:
        raise ValueError(f'expected coverage failed: {len(errors)} gaps; sample={errors[:20]}')
    return partitions  # Empty is valid only outside every known listing interval.


def market_plan(dates, venues):
    """Explicit, evidence-backed NXT intervals; absence never means ineligible."""
    plan=[]
    if 'J' in venues:
        plan.append(('J',dates,'DIRECT'))
    return plan


def nxt_dates(manifest, aid, dates):
    eligible=[]; ineligible=[]
    for day in dates:
        if day < NXT_START:
            continue
        matches=[r for r in manifest.get('nxt_intervals', [])
                 if int(r['asset_id'])==aid
                 and date.fromisoformat(r['start'])<=day<=date.fromisoformat(r['end'])]
        if len(matches)!=1 or not matches[0].get('evidence'):
            raise ValueError(f'NXT eligibility unknown/ambiguous: asset={aid}, date={day}')
        status=matches[0]['status']
        if status not in ('ELIGIBLE','INELIGIBLE'):
            raise ValueError('invalid NXT eligibility status')
        (eligible if status=='ELIGIBLE' else ineligible).append(day)
    return eligible,ineligible


def checked_policy(policy):
    if not policy.get('version') or int(policy['availability_lag_calendar_days'])<1:
        raise ValueError('invalid availability policy')
    if not 0<=int(policy['availability_hour_kst'])<24:
        raise ValueError('invalid availability hour')
    if not 0<=int(policy.get('availability_minute_kst',30))<60:
        raise ValueError('invalid availability minute')
    if policy.get('short_market') not in ('UNKNOWN','KRX'):
        raise ValueError('invalid short market scope')
    if policy['short_market']=='KRX' and not policy.get('short_market_evidence'):
        raise ValueError('short market scope requires evidence reference')
    if policy['short_market']=='KRX':
        date.fromisoformat(policy['short_market_verified_through'])
    venues=policy.get('venues',['J','NX','UN'])
    if not venues or len(set(venues))!=len(venues) or not set(venues)<= {'J','NX','UN'}:
        raise ValueError('invalid requested venues')
    return policy


def collect_partition(client, aid, ticker, dates, venue, policy):
    start,end=min(dates),max(dates)
    data, receipts = client.history('investor',ticker,venue,start,end)
    missing = sorted(set(dates)-set(data))
    if missing:
        raise ValueError(f'{venue} investor missing {len(missing)} days; no zero fill')
    output = [silver.observation(aid,ticker,d,venue,'investor',silver.investor(data[d]),receipts,policy) for d in dates]
    if venue=='J':
        short,sr=client.history('short',ticker,'J',start,end)
        volume,vr=client.history('volume',ticker,'J',start,end)
        if set(dates)-set(short) or set(dates)-set(volume):
            raise ValueError('missing short-sale or original-volume dates')
        through=date.fromisoformat(policy.get('short_market_verified_through','0001-01-01'))
        for day in dates:
            values=silver.short_sale(short[day],volume[day],same_market=policy['short_market']=='KRX' and day<=through)
            output.append(silver.observation(aid,ticker,day,'J','short',values,sr+vr,policy))
    return output


def run(*, conn, manifest_uri, policy_uri, root, start, end, publish=False, refresh=False, client=None):
    if start<date(2015,1,1) or start>end or end>=datetime.now(ZoneInfo('Asia/Seoul')).date():
        raise ValueError('require completed dates from 2015 onward')
    manifest=read_json(manifest_uri)
    claimed=manifest.get('sha256'); unsigned={k:v for k,v in manifest.items() if k!='sha256'}
    if manifest.get('scope')!='union_of_all_historical_in_universe_rows' or digest(unsigned)!=claimed:
        raise ValueError('universe provenance/hash mismatch')
    policy=checked_policy(read_json(policy_uri))
    if publish:
        migrate.assert_current(conn)
    partitions=expected_partitions(conn,manifest,start,end)
    client=client or Client(root)
    summary={'partitions':len(partitions),'published':0,'bronze_only':not publish,'failures':[],
             'universe_as_of':manifest['as_of'],'universe_hash':claimed,
             'assets_without_expected_dates':sorted(set(manifest['asset_ids'])-{k[0] for k in partitions})}
    plans=[]
    venues=policy.get('venues',['J','NX','UN'])
    # Preflight ALL market applicability before any source calls or writes.
    for (aid,ticker,_),dates in partitions.items():
        entries=market_plan(dates,venues)
        if 'NX' in venues or 'UN' in venues:
            eligible,ineligible=nxt_dates(manifest,aid,dates)
            if eligible:
                entries += [(v,eligible,'DIRECT') for v in ('NX','UN') if v in venues]
            if ineligible and 'UN' in venues:
                entries.append(('UN',ineligible,'KRX_ONLY'))
        plans.extend((aid,ticker,venue,vd,mode) for venue,vd,mode in entries)
    for aid,ticker,venue,vd,mode in plans:
        key=digest({'aid':aid,'ticker':ticker,'dates':[d.isoformat() for d in vd],
                    'venue':venue,'mode':mode,'policy':policy,'universe':claimed})
        checkpoint=f'{root}/market_flows/kis_history/checkpoints/{key}.json'
        if not refresh and publish and read_bytes(checkpoint):continue
        try:
            if mode=='KRX_ONLY':
                data,receipts=client.history('investor',ticker,'J',min(vd),max(vd))
                if set(vd)-set(data):
                    raise ValueError('missing KRX component for non-NXT integrated series')
                output=[silver.observation(aid,ticker,d,'UN','investor',
                        {**silver.investor(data[d]),'_derivation':'KRX_ONLY_VERIFIED_NON_NXT',
                         '_market_evidence':next(r['evidence'] for r in manifest['nxt_intervals']
                             if int(r['asset_id'])==aid and date.fromisoformat(r['start'])<=d<=date.fromisoformat(r['end']))},
                        receipts,policy) for d in vd]
            else:
                output=collect_partition(client,aid,ticker,vd,venue,policy)
            # UN is independently delivered, not mislabeled as a derived sum.
            if venue=='UN' and mode=='DIRECT':
                krx,kr=client.history('investor',ticker,'J',min(vd),max(vd))
                nxt,nr=client.history('investor',ticker,'NX',min(vd),max(vd))
                if set(vd)-set(krx) or set(vd)-set(nxt):
                    raise ValueError('cannot derive UN without both market components')
                for obs in output:
                    day=obs['trade_date']
                    values=silver.combined(silver.investor(krx[day]),silver.investor(nxt[day]),obs['values'])
                    # Bind all three source requests to the derived revision.
                    extra=[{'fetched_at':obs['first_observed_at'].isoformat(),'raw_uri':u} for u in obs['source_uris']]
                    obs.update(silver.observation(aid,ticker,day,'UN','investor',values,extra+kr+nr,policy))
            if publish:
                run_id=silver.publish(conn,output,fingerprint=key)
                write_text(json.dumps({'run_id':run_id,'rows':len(output)}),checkpoint)
                summary['published']+=len(output)
        except Exception as exc:
            failure={'ticker':ticker,'venue':venue,'start':min(vd).isoformat(),'end':max(vd).isoformat(),
                     'error_type':type(exc).__name__,'error':str(exc)}
            summary['failures'].append(failure)
            if publish:
                conn.rollback()
                ctx=repository.start_run(conn,mode='kis_market_flows',input_fingerprint=key)
                check=CheckResult('KIS_SOURCE_COVERAGE','kis_market_observation',Severity.ERROR,
                                  CheckStatus.FAIL,'complete matched source window',str(exc),1)
                repository.finish_run(conn,ctx,'FAILED',[check],error_message=str(exc))
            write_text(json.dumps(failure),f'{root}/market_flows/kis_history/failures/{key}.json')
    write_text(json.dumps(summary,sort_keys=True),f'{root}/market_flows/kis_history/runs/{datetime.now().isoformat()}.json')
    return summary


def daily(day, *, conn):
    """Called by the existing ECS schedule only after explicit deployment opt-in."""
    if os.environ.get('KIS_FLOWS_ENABLED','0')!='1':return None
    target=datetime.strptime(day,'%Y%m%d').date()
    import exchange_calendars as xcals
    calendar=xcals.get_calendar('XKRX',start=str(target-timedelta(days=30)),end=str(target+timedelta(days=7)))
    dates=[v.date() for v in calendar.sessions_in_range(str(target-timedelta(days=30)),str(target))][-5:]
    if len(dates)<5:raise RuntimeError('five market sessions required')
    manifest=read_json(os.environ['KIS_UNIVERSE_URI'])
    if (target-date.fromisoformat(manifest['as_of'])).days>40:
        raise RuntimeError('refresh historical factor universe export; older than 40 days')
    result=run(conn=conn,manifest_uri=os.environ['KIS_UNIVERSE_URI'],policy_uri=os.environ['KIS_POLICY_URI'],
               root=os.environ['KIS_BRONZE_ROOT'],start=min(dates),end=max(dates),publish=True,refresh=True)
    if result['failures']:raise RuntimeError(f"KIS daily incomplete: {len(result['failures'])} partitions; see Bronze failures")
    return result


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    sub=parser.add_subparsers(dest='command',required=True)
    export=sub.add_parser('export-universe');export.add_argument('--monthly-csv',required=True);export.add_argument('--dest',required=True);export.add_argument('--nxt-intervals')
    back=sub.add_parser('backfill')
    for name in ('manifest','policy','root','start','end'):back.add_argument('--'+name,required=True)
    back.add_argument('--publish',action='store_true');back.add_argument('--refresh',action='store_true')
    args=parser.parse_args()
    if args.command=='export-universe':
        print(json.dumps(export_universe(args.monthly_csv,args.dest,args.nxt_intervals)));return
    with db.connect() as conn:
        result=run(conn=conn,manifest_uri=args.manifest,policy_uri=args.policy,root=args.root,
                   start=date.fromisoformat(args.start),end=date.fromisoformat(args.end),publish=args.publish,refresh=args.refresh)
    print(json.dumps(result))
    if result['failures']:raise SystemExit(1)

if __name__=='__main__':main()
