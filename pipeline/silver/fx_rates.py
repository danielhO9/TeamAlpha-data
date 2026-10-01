"""Audited KRW-per-one-foreign-unit quotes for financial factor inputs.

Run with --start YYYY-MM-DD --end YYYY-MM-DD [--apply]. Raw API evidence is
captured even in dry-run; only --apply publishes certified Silver observations.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from datetime import date, timedelta
from decimal import Decimal
from uuid import uuid4

from pipeline.bronze.fmp import FMPClient, collect_raw
from pipeline.common import db
from pipeline.common.paths import base_uri
from pipeline.common.sink import read_bytes

CURRENCIES = ("USD", "CNY", "JPY", "HKD", "GBP")


def parse_quotes(payload: bytes, currency: str, start: date, end: date):
    rows = json.loads(payload)
    if not isinstance(rows, list) or not rows:
        raise ValueError(f"missing FX response: {currency}/{start}/{end}")
    result = {}
    for row in rows:
        if row.get("symbol") != currency + "KRW":
            raise ValueError("FX pair mismatch (must be KRW per ONE foreign unit)")
        day = date.fromisoformat(row["date"])
        if not start <= day <= end:
            raise ValueError("FX response outside requested dates")
        # Weekend labels are not a completed weekday FX fixing. In particular
        # FMP's Sunday reopening bars can contain inconsistent OHLC fields.
        if day.weekday() >= 5:
            continue
        values = [Decimal(str(row[k])) for k in ("open", "high", "low", "close")]
        if any(not v.is_finite() or v <= 0 for v in values):
            raise ValueError("FX non-positive/non-finite observation")
        op, high, low, close = values
        if high < max(op, close) or low > min(op, close) or high < low:
            raise ValueError("FX OHLC inconsistency")
        if day in result and result[day] != close:
            raise ValueError("FX duplicate conflict")
        result[day] = close
    return sorted(result.items())


def run(start: date, end: date, *, apply=False, dest="local"):
    if start > end or end >= date.today():
        raise ValueError("FX range must contain completed calendar days only")
    client = FMPClient(timeout=(10, 45))
    quotes = []
    for currency in CURRENCIES:
        for year in range(start.year, end.year + 1):
            lo, hi = max(start, date(year, 1, 1)), min(end, date(year, 12, 31))
            paths = collect_raw(client, root=base_uri(dest),
                endpoint="historical-price-eod/full",
                params={"symbol": currency+"KRW", "from": str(lo), "to": str(hi)},
                prefix=f"fx/financial-units/pair={currency}KRW/from={lo}/to={hi}",
                extension="json")
            payload = read_bytes(paths[0])
            digest = hashlib.sha256(payload).hexdigest()
            parsed = parse_quotes(payload, currency, lo, hi)
            quotes.extend((currency, d, v, "FMP", digest) for d, v in parsed)
            print(f"FX {currency} {year}: {len(parsed)}", flush=True)
        days = sorted(d for c, d, *_ in quotes if c == currency)
        if (days[0] > start + timedelta(days=7)
                or days[-1] < end - timedelta(days=7)
                or any((b-a).days > 7 for a,b in zip(days, days[1:]))):
            raise ValueError(f"FX coverage gap > 7 days: {currency}")
    if apply:
        with db.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT pg_advisory_xact_lock(2026100101)")
                run_id = uuid4()
                cur.execute("""INSERT INTO dq_run(run_id,mode,ruleset_version,status,
                    total_rule_count,finished_at,input_fingerprint)
                    VALUES (%s,'financial_fx_repair','financial_units_v1','CERTIFIED',1,now(),%s)""",
                    (run_id, hashlib.sha256(str(quotes).encode()).hexdigest()))
                cur.execute("CREATE TEMP TABLE _fx_stage (LIKE fx_rate_daily) ON COMMIT DROP")
                with cur.copy("COPY _fx_stage (currency,rate_date,krw_per_unit,source,evidence_sha256,quality_run_id) FROM STDIN") as copy:
                    for row in quotes:
                        copy.write_row((*row,run_id))
                cur.execute("""SELECT s.currency,s.rate_date FROM _fx_stage s
                    JOIN fx_rate_daily old USING(currency,rate_date)
                    WHERE old.krw_per_unit<>s.krw_per_unit LIMIT 1""")
                conflict=cur.fetchone()
                if conflict:
                    raise ValueError(f"existing FX revision requires review: {conflict}")
                cur.execute("INSERT INTO fx_rate_daily SELECT * FROM _fx_stage ON CONFLICT DO NOTHING")
                cur.execute("""INSERT INTO dq_result(run_id,dataset_name,rule_code,severity,status,
                    expected_value,actual_value) VALUES (%s,'fx_rate_daily','FX_PAIR_OHLC_COVERAGE',
                    'INFO','PASS','positive finite quotes, pair identity, no >7d gaps',%s)""",
                    (run_id, f"rows={len(quotes)}; {start}..{end}"))
    return len(quotes)


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--start", type=date.fromisoformat, required=True)
    p.add_argument("--end", type=date.fromisoformat, required=True)
    p.add_argument("--apply", action="store_true")
    p.add_argument("--dest", choices=("local", "s3"), default="local")
    a = p.parse_args()
    print("FX rows", run(a.start,a.end,apply=a.apply,dest=a.dest))
