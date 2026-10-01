"""Dry-run by default. Exact receipt + API anchor + same-receipt XML evidence.

Preserves raw API statement lines and Bronze. Updates only standardized monetary
fundamentals, retaining a full original-row journal and a separate certified run.
"""
from __future__ import annotations

import argparse
import hashlib
import html
import io
import json
import re
import zipfile
from decimal import Decimal
from uuid import uuid4

from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from pipeline.bronze.corporate_actions import _fetch_document
from pipeline.common import db
from pipeline.common.paths import base_uri
from pipeline.common.sink import read_bytes, write_bytes
from pipeline.silver.reviewed_financial_units import REVIEWED, DIVISORS, DOCUMENT_UNITS

ALIASES = {
    "total_assets": ("자산총계", "자산합계"),
    "current_assets": ("유동자산",), "noncurrent_assets": ("비유동자산",),
    "total_liabilities": ("부채총계", "부채합계"),
    "current_liabilities": ("유동부채",), "noncurrent_liabilities": ("비유동부채",),
    "total_equity": ("자본총계", "자본합계"), "capital_stock": ("자본금",),
    "retained_earnings": ("이익잉여금", "결손금"),
    "revenue": ("매출", "영업수익", "수익"),
    "operating_income": ("영업이익", "영업손실", "영업손익"),
    "pretax_income": ("법인세",),
    "net_income": ("순이익", "순손실"),
    "comprehensive_income": ("포괄손익", "포괄이익", "포괄손실"),
}


def document_rows(payload):
    rows = []
    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        for name in archive.namelist():
            content = archive.read(name)
            try:
                text = content.decode("utf-8")
            except UnicodeDecodeError:
                text = content.decode("cp949")  # older DART XML mislabels its encoding
            for tr in re.findall(r"<TR\b[^>]*>.*?</TR>", text, re.S | re.I):
                cells = re.findall(r"<(?:TD|TE|TH)\b[^>]*>(.*?)</(?:TD|TE|TH)>", tr, re.S | re.I)
                cells = [html.unescape(re.sub(r"<[^>]+>", "", c)).strip() for c in cells]
                rows.append(cells)
    return rows


def evidence_match(rows, metric, value):
    for cells in rows:
        label = re.sub(r"\s+", "", " ".join(cells[:1]))
        if not any(alias in label for alias in ALIASES.get(metric, ())):
            continue
        for cell in cells[1:]:
            token = re.sub(r"\s+", "", cell).replace(",", "").replace("−", "-")
            if token.startswith("(") and token.endswith(")"):
                token = "-" + token[1:-1]
            try:
                if Decimal(token) == value:
                    return cells
            except Exception:
                continue
    return None


def run(*, apply=False):
    plans, unresolved = [], []
    with db.connect() as conn:
        with conn.cursor(row_factory=dict_row) as cur:
            cur.execute("SET LOCAL statement_timeout='120s'")
            for receipt, (ticker, anchors) in REVIEWED.items():
                divisor = DIVISORS.get(receipt, Decimal(1_000_000))
                document_unit = DOCUMENT_UNITS.get(receipt, Decimal(1))
                uri = f"{base_uri('local')}/financials/unit_evidence/rcept={receipt}/document.zip"
                payload = read_bytes(uri)
                if payload is None:
                    payload = _fetch_document(receipt)
                    write_bytes(payload, uri)
                digest = hashlib.sha256(payload).hexdigest()
                evidence = document_rows(payload)
                cur.execute("""SELECT f.* FROM fundamental f WHERE f.source='DART'
                    AND f.revision_key=%s AND f.currency='KRW' AND f.unit_type='currency'
                    AND f.data_basis='STANDARDIZED' AND EXISTS (
                        SELECT 1 FROM asset_identifier i WHERE i.asset_id=f.asset_id
                        AND i.source='KRX' AND i.identifier_type='ticker' AND i.identifier=%s)
                    ORDER BY f.fs_type,f.metric""", (receipt,ticker))
                records = cur.fetchall()
                for scope, original in anchors.items():
                    scope_rows = [r for r in records if r['fs_type']==scope]
                    anchor = [r for r in scope_rows if r['metric']=='total_assets']
                    if len(anchor)!=1:
                        raise ValueError(f"missing/ambiguous scope: {receipt}/{scope}")
                    if anchor[0]['value'] == Decimal(original)/divisor:
                        continue
                    if anchor[0]['value'] != Decimal(original):
                        raise ValueError(f"changed anchor: {receipt}/{scope}")
                    for row in scope_rows:
                        corrected = row['value']/divisor
                        matched = evidence_match(evidence, row['metric'], corrected/document_unit)
                        if matched is None:
                            unresolved.append((receipt,scope,row['metric'],str(corrected)))
                            continue
                        proof = {"receipt": receipt, "document_sha256": digest,
                                 "document_uri": uri, "matched_cells": matched,
                                 "divisor": str(divisor), "document_unit_krw": str(document_unit)}
                        plans.append((row,corrected,proof))
                print(receipt, "verified",sum(p[0]['revision_key']==receipt for p in plans),flush=True)
            print("UNRESOLVED",json.dumps(unresolved,ensure_ascii=False),flush=True)
            if unresolved:
                raise ValueError("unresolved monetary fields: no repairs applied")
            if apply and plans:
                cur.execute("SELECT pg_advisory_xact_lock(2026100102)")
                run_id = uuid4()
                cur.execute("""INSERT INTO dq_run(run_id,mode,ruleset_version,status,total_rule_count,
                    finished_at) VALUES (%s,'reviewed_financial_units','financial_units_v1','CERTIFIED',1,now())""",(run_id,))
                for row,corrected,proof in plans:
                    key = {k:row[k] for k in ('asset_id','source','statement_type','data_basis',
                        'period_end','fiscal_period','fs_type','revision_key','metric')}
                    repair_id = hashlib.sha256(json.dumps(key,default=str,sort_keys=True).encode()).hexdigest()
                    cur.execute("""INSERT INTO financial_unit_repair
                        (repair_id,run_id,original_row,corrected_value,evidence) VALUES (%s,%s,%s,%s,%s)""",
                        (repair_id,run_id,Jsonb(json.loads(json.dumps(row,default=str))),corrected,Jsonb(proof)))
                    where = ' AND '.join(f'{k}=%s' for k in key)
                    cur.execute(f"UPDATE fundamental SET value=%s,quality_run_id=%s,loaded_at=now() WHERE {where} AND value=%s",
                        (corrected,run_id,*key.values(),row['value']))
                    if cur.rowcount!=1:
                        raise ValueError("concurrent financial change; transaction rolled back")
                cur.execute("""INSERT INTO dq_result(run_id,dataset_name,rule_code,severity,status,
                    expected_value,actual_value) VALUES (%s,'fundamental','REVIEWED_XML_UNIT_REPAIR',
                    'MODIFIED','PASS','same-receipt document confirms corrected amount',%s)""",
                    (run_id,f"corrected_rows={len(plans)}; original rows in financial_unit_repair"))
            if not apply:
                conn.rollback()
    return len(plans)


if __name__ == '__main__':
    from dotenv import load_dotenv
    load_dotenv()
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--apply',action='store_true')
    args=p.parse_args()
    print('repair rows',run(apply=args.apply))
