"""Synthetic original-source contracts only: no network, database or returns."""
from __future__ import annotations

import copy
import io
import json
import zipfile
from dataclasses import replace
from pathlib import Path

import pytest

from pipeline.silver import dart_historical_revenue as m
from pipeline.silver_quality.rules.dart_historical_revenue import check_document_revenue

CORP = "00126380"
STOCK = "005930"
FIRST = "20150515000001"
SECOND = "20150601000001"
QUALITY = "00000000-0000-0000-0000-000000000001"


def synthetic_parser(raw, quarter, corp):
    """A source-hash-bound fake parser; source tests do not bless actual DART."""
    result = json.loads(raw)
    result.update(xml_sha256=m.sha256(raw), parser_source_sha256=m.sha256(Path(__file__).read_bytes()),
                  xml_issuer_codes=[corp])
    return result


class Fixture:
    def __init__(self):
        self.artifacts = {}
        self.rows = []
        self.response = {"status": "000", "page_no": 1, "total_page": 1, "total_count": 0, "list": []}
        self.request = {"endpoint": "https://opendart.fss.or.kr/api/list.json", "params": {
            "corp_code": CORP, "bgn_de": "20150101", "end_de": "20230731", "page_no": 1,
            "page_count": 100, "last_reprt_at": "N", "pblntf_ty": "A"}}
        self.corpus = {"schema_version": "dart-original-corpus-v1", "all_planned_issuer_lists_complete": True,
                       "issuers": [{"corp_code": CORP, "covered_from": "2015-01-01", "covered_through": "2023-07-31", "pages": []}],
                       "documents": []}
        self.lineage = {"schema_version": "dart-original-lineage-v1", "classified_receipts": [], "scopes": [
            {"scope_id": "s1", "corp_code": CORP, "stock_code": STOCK, "quarter": "Q1", "period_start": "2015-01-01",
             "period_end": "2015-03-31", "events": []}]}
        self.fy = {"rows": [{"asset_id": 2325, "filing_id": "20160330000001", "status": "CERTIFIED",
                             "quality_run_id": QUALITY, "period_end": "2015-12-31"}]}
        self.fy_issuer = {"status": "000", "list": [{"rcept_no": "20160330000001", "corp_code": CORP}]}
        payload = {"schema": "asset-listing-snapshot-v1", "coverage_start": "2000-01-01", "verified_through": "2023-07-31",
                   "periods": [{"asset_id": 2325, "ticker": STOCK, "start": "2000-01-01", "end": None}]}
        payload["snapshot_id"] = m.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode())
        self.listing_proof = {"schema_version": "certified-listing-snapshot-extract-v1", "status": "CERTIFIED",
                              "quality_run_id": QUALITY, "snapshot_id": payload["snapshot_id"], "payload": payload}
        self.listing = {"identity_methodology": "FROZEN_KRX_LISTING_EPISODES_V1",
                        "identity_evidence_sha256": self.add(self.listing_proof),
                        "rows": [{"asset_id": 2325, "stock_code": STOCK, "valid_from": "2000-01-01",
                                  "valid_until_exclusive": "2023-08-01", "status": "CERTIFIED", "quality_run_id": QUALITY}]}
        self.mapping = {"stock_code": STOCK, "corp_code": CORP, "asset_id": 2325, "valid_from": "2000-01-01",
                        "valid_until_exclusive": "2023-08-01", "basis": "CERTIFIED_FY_AND_LISTING_EPISODE",
                        "fy_receipt_no": "20160330000001"}
        self.identity = {"schema_version": "dart-historical-identity-v1", "mappings": [self.mapping]}
        self.stage_flags = {"source_collection_finished": True, "all_planned_issuer_lists_complete": True}
        self.add_event(FIRST, "2015-05-15", "ORIGINAL", 100)

    def add(self, value):
        raw = value if isinstance(value, bytes) else m.canonical_bytes(value)
        digest = m.sha256(raw)
        self.artifacts[digest] = raw
        return digest

    def add_event(self, receipt, filed, kind, value=None, *, scope_status=None):
        title = ("[기재정정]" if kind == "CORRECTION" else "[철회]" if kind == "WITHDRAWAL" else "") + "분기보고서 (2015.03)"
        self.response["list"].append({"rcept_no": receipt, "rcept_dt": filed.replace("-", ""), "corp_code": CORP, "report_nm": title})
        self.response["total_count"] += 1
        self.lineage["classified_receipts"].append({"receipt_no": receipt, "scope_ids": ["s1"], "reason": "official periodic filing"})
        events = self.lineage["scopes"][0]["events"]
        event = {"receipt_no": receipt, "kind": kind}
        if kind in {"CORRECTION", "WITHDRAWAL"}:
            parent = events[-1]["receipt_no"]
            text = "official parent " + parent
            event.update(parent_receipt_no=parent, proof={"artifact_sha256": self.add(text.encode()), "text": text})
        elif kind == "UNRESOLVED":
            event["reason"] = "revision family not proved"
        events.append(event)
        if value is None:
            return
        parsed = []
        for fs in ("CFS", "OFS"):
            for role in ("CURRENT_QUARTER", "CURRENT_YTD"):
                parsed.append({"fs_type": fs, "amount_role": role, "metric": "revenue",
                               "period_start": "2015-01-01", "period_end": "2015-03-31",
                               "amount_krw_decimal": str(value), "currency": "KRW",
                               "amount_raw": str(value), "unit_raw": "원", "unit_scale_to_krw": 1,
                               "header_table_anchor": {"table_sha256": "a" * 64},
                               "value_table_anchor": {"table_sha256": "b" * 64}})
        xml = m.canonical_bytes({"rows": parsed, "scope_status": scope_status or {"CFS": "PARSED", "OFS": "PARSED"}})
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as archive:
            archive.writestr("/" + receipt + ".xml", xml)
        doc = {"receipt_no": receipt, "corp_code": CORP, "zip_sha256": self.add(buf.getvalue()),
               "xml_sha256": m.sha256(xml), "xml_member_name": "/" + receipt + ".xml"}
        self.corpus["documents"].append(doc)
        for row in parsed:
            self.rows.append({**row, **doc, "quarter": "Q1", "stock_code": STOCK, "official_filed_date": filed,
                              "pit_status": "PIT_UNVERIFIED", "input_connected": False,
                              "source_publication_candidate": kind in {"ORIGINAL", "CORRECTION"}})

    def args(self):
        self.corpus["issuers"][0]["pages"] = [{"request_sha256": self.add(self.request), "response_sha256": self.add(self.response)}]
        self.mapping.update(fy_reference_sha256=self.add(self.fy), fy_issuer_sha256=self.add(self.fy_issuer),
                            listing_snapshot_sha256=self.add(self.listing))
        source = ({"source_contract_sha256": self.corpus["collector_contract_sha256"], "source_run_sha256": self.corpus["source_run_sha256"]}
                  if "collector_contract_sha256" in self.corpus else {})
        stage = m.canonical_bytes({"schema_version": "dart-historical-revenue-staging-v1", "rows": self.rows, **self.stage_flags, **source})
        manifest = {"schema_version": "dart-revenue-checked-manifest-v1", "dataset_version": "synthetic-v1",
                    "stage_sha256": m.sha256(stage), "parser_source_sha256": m.sha256(Path(__file__).read_bytes()),
                    "corpus_sha256": self.add(self.corpus), "lineage_sha256": self.add(self.lineage),
                    "identity_sha256": self.add(self.identity),
                    "approved_row_sha256": [m.object_sha256(r) for r in self.rows if r["amount_role"] == "CURRENT_QUARTER"],
                    "review": {"decision": "ACCEPT", "reviewer": "synthetic reviewer", "reviewed_at": "2026-10-02T12:00:00+09:00"}}
        return stage, manifest, self.artifacts

    def prepare(self):
        return m.prepare(*self.args(), parse_document=synthetic_parser)


def test_exact_current_quarter_replay_and_bounded_pit_not_pilot_certification():
    p = Fixture().prepare()
    assert len(p.rows) == 2
    assert {r["fs_type"] for r in p.rows} == {"CFS", "OFS"}
    assert p.rows[0]["stock_code"] == "005930"
    assert p.rows[0]["value"] == "100.000000"
    assert p.rows[0]["available_date"] == "2015-05-16"
    assert p.rows[0]["available_at"] == "2015-05-16T00:00:00+09:00"
    assert p.rows[0]["valid_until_exclusive"] == "2023-08-01"
    assert p.rows[0]["evidence"]["cross_vintage_ytd_reconciliation_claim"] is False
    assert p.evidence["full_history_certified"] is False
    assert p.evidence["covered_through_by_corp"] == {CORP: "2023-07-31"}
    assert p.evidence["verified_scope_identity_mappings"][0]["asset_id"] == 2325
    assert not any(r.blocks_publish for r in check_document_revenue(p))


@pytest.mark.parametrize("field", ["source_collection_finished", "all_planned_issuer_lists_complete"])
def test_partial_collection_cannot_be_reviewed_into_certification(field):
    f = Fixture()
    f.stage_flags[field] = False
    with pytest.raises(m.EvidenceError, match="incomplete"):
        f.prepare()


@pytest.mark.parametrize("mutation,match", [
    (lambda f: f.request["params"].update(last_reprt_at="Y"), "every original"),
    (lambda f: f.request["params"].update(crtfc_key="not-a-real-key"), "secret"),
    (lambda f: f.response.update(total_count=2), "count/receipt"),
    (lambda f: f.response.update(total_page=2), "total_page"),
    (lambda f: f.response.update(page_no=2), "page number"),
    (lambda f: f.corpus.update(all_planned_issuer_lists_complete=False), "incomplete"),
    (lambda f: f.mapping.update(valid_from="0001-01-01"), "date.min"),
    (lambda f: f.fy["rows"][0].update(status="PIT_UNVERIFIED"), "not CERTIFIED"),
    (lambda f: f.fy["rows"][0].update(period_end="2016-12-31"), "target fiscal year"),
    (lambda f: f.fy_issuer["list"][0].update(corp_code="99999999"), "issuer mismatch"),
    (lambda f: f.listing["rows"].append(copy.deepcopy(f.listing["rows"][0])), "ambiguous"),
    (lambda f: f.listing["rows"][0].update(asset_id=9999), "ticker-reused"),
    (lambda f: f.rows[0].update(amount_krw_decimal="101"), "parser replay"),
    (lambda f: f.rows[0].update(official_filed_date="2015-05-14"), "official list"),
    (lambda f: f.lineage["classified_receipts"][0].update(scope_ids=[]), "excluded from lineage"),
    (lambda f: f.rows[0].update(stock_code="005931"), "stock differs"),
])
def test_invalid_claims_fail_closed(mutation, match):
    f = Fixture()
    mutation(f)
    with pytest.raises(m.EvidenceError, match=match):
        f.prepare()


def test_missing_raw_or_parser_hash_changed_rejected():
    f = Fixture()
    stage, manifest, artifacts = f.args()
    manifest["parser_source_sha256"] = "0" * 64
    with pytest.raises(m.EvidenceError, match="parser implementation"):
        m.prepare(stage, manifest, artifacts, parse_document=synthetic_parser)
    stage, manifest, artifacts = f.args()
    artifacts[f.corpus["documents"][0]["zip_sha256"]] = b"corrupt"
    with pytest.raises(m.EvidenceError, match="artifact hash mismatch"):
        m.prepare(stage, manifest, artifacts, parse_document=synthetic_parser)


def test_ytd_must_not_be_approved_as_quarter():
    f = Fixture()
    stage, manifest, artifacts = f.args()
    manifest["approved_row_sha256"].append(m.object_sha256(f.rows[1]))
    with pytest.raises(m.EvidenceError, match="YTD is evidence"):
        m.prepare(stage, manifest, artifacts, parse_document=synthetic_parser)


def test_corrected_version_closes_old_window_at_conservative_next_day():
    f = Fixture()
    f.add_event(SECOND, "2015-06-01", "CORRECTION", 105)
    p = f.prepare()
    original = [r for r in p.rows if r["revision_key"] == FIRST]
    corrected = [r for r in p.rows if r["revision_key"] == SECOND]
    assert {r["valid_until_exclusive"] for r in original} == {"2015-06-02"}
    assert {r["available_date"] for r in corrected} == {"2015-06-02"}
    assert {r["value"] for r in corrected} == {"105.000000"}


@pytest.mark.parametrize("kind", ["WITHDRAWAL", "UNRESOLVED", "CORRECTION"])
def test_unavailable_revision_is_an_explicit_block_not_skipped(kind):
    f = Fixture()
    f.add_event(SECOND, "2015-06-01", kind)
    p = f.prepare()
    assert len(p.rows) == 2
    assert p.rows[0]["valid_until_exclusive"] == "2015-06-02"
    assert p.blocking_windows[0]["valid_from"] == "2015-06-02"
    assert p.blocking_windows[0]["valid_until_exclusive"] == "2023-08-01"
    assert check_document_revenue(p)[-1].status.value == "FAIL"
    assert check_document_revenue(p)[-1].blocks_publish is False


def test_parent_link_cannot_be_claimed_without_original_anchor():
    f = Fixture()
    f.add_event(SECOND, "2015-06-01", "CORRECTION", 105)
    f.lineage["scopes"][0]["events"][1]["proof"]["text"] = "missing " + FIRST
    with pytest.raises(m.EvidenceError, match="anchor absent"):
        f.prepare()


def test_identity_expiry_closes_value_and_is_explicitly_blocked():
    f = Fixture()
    f.mapping["valid_until_exclusive"] = "2016-01-01"
    p = f.prepare()
    assert p.rows[0]["valid_until_exclusive"] == "2016-01-01"
    assert p.blocking_windows == ({"scope_id": "s1", "valid_from": "2016-01-01", "valid_until_exclusive": "2023-08-01", "reason": "IDENTITY_EPISODE_END"},)


def test_cfs_parse_failure_is_not_ofs_fallback_permission():
    f = Fixture()
    f.add_event(SECOND, "2015-06-01", "CORRECTION", 105, scope_status={"CFS": "UNRESOLVED", "OFS": "PARSED"})
    with pytest.raises(m.EvidenceError, match="unresolved CFS/OFS"):
        f.prepare()


def test_collector_endpoint_alias_binds_actual_source_bytes_and_sort():
    f = Fixture()
    f.request["endpoint"] = "list.json"
    f.request["params"].update(sort="date", sort_mth="asc")
    source = b'ENDPOINTS = {"list.json": "https://opendart.fss.or.kr/api/list.json"}\n'
    contract = {"collector_sha256": f.add(source), "manifest_sha256": "a" * 64,
                "manifest": {"issuers": [{"corp_code": CORP}], "list_start": "20150101", "list_end": "20230731"}}
    f.corpus["collector_contract_sha256"] = f.add(contract)
    f.corpus["source_run_sha256"] = f.add({"completed_at": "2026-10-02T12:00:00+09:00", "manifest_sha256": "a" * 64,
                                         "complete_issuer_list_count": 1, "issuers": [{"corp_code": CORP}]})
    assert len(f.prepare().rows) == 2
    f.corpus["collector_contract_sha256"] = f.add({**contract, "collector_sha256": f.add(source.replace(b"opendart.fss.or.kr", b"example.invalid"))})
    with pytest.raises(m.EvidenceError, match="nonofficial"):
        f.prepare()


def test_dq_independent_bad_window_or_naive_time_blocks():
    p = Fixture().prepare()
    row = {**p.rows[0], "available_at": "2015-05-16T00:00:00", "valid_until_exclusive": "2015-05-15"}
    checks = check_document_revenue(replace(p, rows=(row,)))
    assert any(c.blocks_publish and c.rule_code == "DOCUMENT_REVENUE_PIT_WINDOW" for c in checks)


class FakeCursor:
    def __init__(self, conn):
        self.conn = conn

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, sql, params):
        self.conn.calls.append((sql, params))

    def fetchone(self):
        return self.conn.existing

    def fetchall(self):
        return self.conn.rows


class FakeConnection:
    autocommit = False

    def __init__(self):
        self.calls = []
        self.existing = None
        self.rows = []

    def cursor(self):
        return FakeCursor(self)

    def commit(self):
        raise AssertionError("publisher must never commit caller transaction")


def publish_args(f):
    stage, manifest, artifacts = f.args()
    return dict(stage_bytes=stage, checked_manifest=manifest, artifacts=artifacts,
                parse_document=synthetic_parser, authorize_write=True)


def test_publication_requires_explicit_authority_and_no_autocommit():
    conn = FakeConnection()
    args = publish_args(Fixture())
    args["authorize_write"] = False
    with pytest.raises(m.EvidenceError, match="authorization"):
        m.publish(conn, **args)
    args["authorize_write"] = True
    conn.autocommit = True
    with pytest.raises(m.EvidenceError, match="caller-owned"):
        m.publish(conn, **args)
    assert conn.calls == []


def test_insert_only_source_tables_and_certification_in_caller_transaction(monkeypatch):
    conn = FakeConnection()
    finishes = []
    monkeypatch.setattr(m.repository, "finish_run", lambda *args, **kw: finishes.append((args, kw)))
    result = m.publish(conn, **publish_args(Fixture()))
    assert result["inserted"] == 2 and result["committed"] is False
    assert finishes[0][0][2] == "CERTIFIED" and finishes[0][1] == {"commit": False}
    statements = "\n".join(sql for sql, _ in conn.calls)
    assert "UPDATE" not in statements and "DELETE" not in statements
    assert "INSERT INTO fundamental\n" not in statements
    assert "INSERT INTO fundamental_document_revenue" in statements
    assert "INSERT INTO fundamental_document_dataset" in statements


def test_exact_existing_version_idempotent_mismatch_never_overwrites():
    f, conn = Fixture(), FakeConnection()
    p = f.prepare()
    conn.existing = (p.manifest_sha256, p.bundle_sha256, QUALITY, "CERTIFIED")
    conn.rows = [(r["observation_key"], r["payload_sha256"]) for r in p.rows]
    receipt = m.publish(conn, **publish_args(f))
    assert receipt["inserted"] == 0 and receipt["reused"] is True
    assert not any("INSERT" in sql for sql, _ in conn.calls)
    conn.rows[0] = (conn.rows[0][0], "0" * 64)
    with pytest.raises(m.EvidenceError, match="never overwrite"):
        m.publish(conn, **publish_args(f))


def test_queries_and_migration_preserve_existing_core_tables_and_window_contract():
    assert "r.available_date<=%s AND %s<r.valid_until_exclusive" in m.ASOF_SQL
    assert "q.status='CERTIFIED'" in m.ASOF_SQL
    assert "r.dataset_version=%s" in m.BOUNDED_LEDGER_SQL
    assert m.query_source_sha256() != m.query_source_sha256(bounded_ledger=True)
    sql = Path("pipeline/silver_quality/migrations/020_dart_historical_revenue.sql").read_text()
    assert "BEFORE UPDATE OR DELETE OR TRUNCATE" in sql
    assert "FLOW_CURRENT_QUARTER" in sql
    assert "ALTER TABLE fundamental " not in sql
    assert "available_date::timestamp AT TIME ZONE 'Asia/Seoul'" in sql
