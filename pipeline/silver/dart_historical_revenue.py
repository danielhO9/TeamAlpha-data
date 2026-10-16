"""Append-only, bounded-PIT original-document revenue (not a core DART upsert).

This module performs no network access, opens no database connection and never
commits. ``prepare`` is the source-only API. A reviewed manifest is necessary,
but not sufficient: it binds raw list pages, ZIP/XML bytes, replayed parser
output, revision lineage and independently certified historical identifiers.
The ordinary STAGED_PIT_UNVERIFIED pilot file alone is never publishable.

Manifest v1 (all hashes lowercase SHA256, JSON hashes use ``canonical_bytes``):
  schema_version: dart-revenue-checked-manifest-v1
  dataset_version, stage_sha256, parser_source_sha256
  corpus_sha256, lineage_sha256, identity_sha256
  approved_row_sha256: [canonical hash of each approved CURRENT_QUARTER row]
  review: {decision: ACCEPT, reviewer: ..., reviewed_at: ISO aware timestamp}
Artifacts are supplied as {sha256: original_bytes}; there is no path extraction.

Corpus v1: ``issuers`` [{corp_code, covered_from, covered_through, pages:
 [{request_sha256,response_sha256}]}], ``documents`` [{receipt_no,corp_code,
 zip_sha256,xml_member_name,xml_sha256}]. Request JSON has endpoint and params
 (corp_code,bgn_de,end_de,page_no,page_count,last_reprt_at=N,pblntf_ty=A).
 All pagination and every declared coverage interval must be complete.

Lineage v1: ``classified_receipts`` [{receipt_no,scope_ids,reason}], ``scopes``
 [{scope_id,corp_code,stock_code,quarter,period_start,period_end,events:
 [{receipt_no,kind: ORIGINAL|CORRECTION|WITHDRAWAL|UNRESOLVED,
 parent_receipt_no?,proof?:{artifact_sha256,text},reason?}]}]. Every listed
 receipt is explicitly classified. Unsupported/withdrawn revisions close the
 previous value's window; they are never silently skipped or forward-filled.

Identity v1: ``mappings`` [{stock_code,corp_code,asset_id,valid_from,
 valid_until_exclusive,basis:CERTIFIED_FY_AND_LISTING_EPISODE,
 fy_reference_sha256,fy_receipt_no,fy_issuer_sha256,listing_snapshot_sha256}].
 FY reference JSON: rows(asset_id,filing_id,quality_run_id,status=CERTIFIED).
 FY issuer is an original OpenDART list.json response containing that receipt
 and corp_code. Listing snapshot JSON: rows(asset_id,stock_code,valid_from,
 valid_until_exclusive,quality_run_id,status=CERTIFIED), plus
 identity_methodology and identity_evidence_sha256 (hash of its frozen proof).
 Current corpCode.xml/date.min mappings alone cannot pass this contract.

CERTIFIED describes only included source rows within [available,valid_until).
Readers MUST use an as-of window or explicit expiry barriers before replay;
blind union into a forward-filling fundamental panel is forbidden.
"""
from __future__ import annotations

import ast
import hashlib
import html
import inspect
import io
import json
import re
import zipfile
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Callable, Mapping
from uuid import UUID, uuid4
from zoneinfo import ZoneInfo

from pipeline.silver_quality import QUALITY_RULESET_VERSION, repository
from pipeline.silver_quality.models import BatchContext, QualityGateError
from pipeline.silver_quality.rules.dart_historical_revenue import check_document_revenue

CONTRACT_VERSION = "dart-original-revenue-bounded-pit-v1"
MODE = "dart_historical_document_revenue"
MAX_XML_BYTES = 128 * 1024 * 1024
KST = ZoneInfo("Asia/Seoul")
HASH_RE = re.compile(r"[a-f0-9]{64}")
RECEIPT_RE = re.compile(r"[0-9]{14}")
PARSED_KEYS = (
    "fs_type", "metric", "account_label_raw", "amount_role", "period_start", "period_end",
    "period_start_evidence", "declared_report_period_start", "declared_report_period_end",
    "period_header_raw", "duration_column_raw", "amount_raw", "unit_raw",
    "unit_scale_to_krw", "amount_krw_decimal", "currency", "section",
    "statement_heading", "header_table_anchor", "value_table_anchor",
    "source_row_zero_based", "source_cell_zero_based", "expanded_column_zero_based",
    "value_physical_line", "column_header_path", "expanded_header_rows",
)


class EvidenceError(ValueError):
    """A source claim could not be reproduced from its bound evidence."""


def canonical_bytes(value) -> bytes:
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"),
                      allow_nan=False).encode("utf-8")


def sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def object_sha256(value) -> str:
    return sha256(canonical_bytes(value))


def _require(condition, message):
    if not condition:
        raise EvidenceError(message)


def _date(value) -> date:
    _require(isinstance(value, str), "ISO date must be a string")
    parsed = date.fromisoformat(value)
    _require(parsed.isoformat() == value, "noncanonical ISO date")
    return parsed


def _receipt(value):
    _require(isinstance(value, str) and RECEIPT_RE.fullmatch(value), "invalid receipt_no")
    return value


def _artifact(artifacts, digest):
    _require(isinstance(digest, str) and HASH_RE.fullmatch(digest), "invalid artifact SHA256")
    data = artifacts.get(digest)
    _require(isinstance(data, bytes) and sha256(data) == digest, f"artifact hash mismatch: {digest}")
    return data


def _json(artifacts, digest):
    cache = getattr(artifacts, "json_cache", None)
    if cache is not None:
        if digest not in cache:
            cache[digest] = json.loads(_artifact(artifacts, digest))
        return cache[digest]
    return json.loads(_artifact(artifacts, digest))


class _EvidenceStore(Mapping):
    """Cache small parsed evidence, not multi-GiB raw ZIP/XML corpus bytes."""
    def __init__(self, source):
        self.source, self.json_cache = source, {}

    def __getitem__(self, key):
        return self.source[key]

    def __iter__(self):
        return iter(self.source)

    def __len__(self):
        return len(self.source)


def _schema(value, expected):
    _require(isinstance(value, dict) and value.get("schema_version") == expected,
             f"expected schema {expected}")


def _certified(row):
    _require(row.get("status") == "CERTIFIED", "identity/FY evidence is not CERTIFIED")
    UUID(str(row["quality_run_id"]))


def _validate_corpus(corpus, artifacts):
    _schema(corpus, "dart-original-corpus-v1")
    _require(corpus.get("all_planned_issuer_lists_complete") is True, "planned issuer list collection incomplete")
    receipts, horizons, documents = {}, {}, {}
    alias_verified = False
    if corpus.get("collector_contract_sha256"):
        contract = _json(artifacts, corpus["collector_contract_sha256"])
        plan = contract["manifest"]
        planned = {i["corp_code"] for i in plan["issuers"]}
        _require(len(planned) == len(plan["issuers"]) and planned == {i["corp_code"] for i in corpus["issuers"]},
                 "corpus issuer scope differs from frozen collection plan")
        run = _json(artifacts, corpus["source_run_sha256"])
        _require(run.get("completed_at") and datetime.fromisoformat(run["completed_at"]).tzinfo is not None
                 and run.get("manifest_sha256") == contract["manifest_sha256"]
                 and run.get("complete_issuer_list_count") == len(planned)
                 and {i["corp_code"] for i in run["issuers"]} == planned,
                 "completed official collection/list scope is not proved by its run receipt")
        for issuer in corpus["issuers"]:
            _require(_date(issuer["covered_from"]).strftime("%Y%m%d") == plan["list_start"]
                     and _date(issuer["covered_through"]).strftime("%Y%m%d") == plan["list_end"],
                     "corpus verification horizon differs from frozen collection plan")
        source = _artifact(artifacts, contract["collector_sha256"])
        assignments = [node.value for node in ast.parse(source).body if isinstance(node, ast.Assign)
                       and any(isinstance(t, ast.Name) and t.id == "ENDPOINTS" for t in node.targets)]
        _require(len(assignments) == 1 and ast.literal_eval(assignments[0]).get("list.json")
                 == "https://opendart.fss.or.kr/api/list.json", "unbound/nonofficial collector endpoint alias")
        alias_verified = True
    for issuer in corpus["issuers"]:
        corp = issuer["corp_code"]
        _require(re.fullmatch(r"[0-9]{8}", corp), "invalid corp_code")
        _require(corp not in horizons, "duplicate corpus issuer")
        begin, end = _date(issuer["covered_from"]), _date(issuer["covered_through"])
        _require(begin <= end, "empty corpus coverage")
        groups = defaultdict(list)
        for page in issuer["pages"]:
            req, res = _json(artifacts, page["request_sha256"]), _json(artifacts, page["response_sha256"])
            if req.get("endpoint") == "list.json":
                _require(alias_verified, "unbound/nonofficial collector endpoint alias")
            else:
                _require(req.get("endpoint") == "https://opendart.fss.or.kr/api/list.json", "not official list endpoint")
            p = req["params"]
            required = {"corp_code", "bgn_de", "end_de", "page_no", "page_count", "last_reprt_at", "pblntf_ty"}
            _require(required <= set(p) <= required | {"sort", "sort_mth"},
                     "list request has missing filters/secret or unsupported filters")
            _require(p.get("sort", "date") == "date" and p.get("sort_mth", "asc") == "asc", "unsupported list ordering")
            _require(p["corp_code"] == corp and p["last_reprt_at"] == "N" and p["pblntf_ty"] == "A",
                     "list must include every original and corrected periodic filing")
            a, b = datetime.strptime(p["bgn_de"], "%Y%m%d").date(), datetime.strptime(p["end_de"], "%Y%m%d").date()
            _require(begin <= a <= b <= end, "list request outside declared corpus interval")
            number, count = int(p["page_no"]), int(p["page_count"])
            _require(number >= 1 and 1 <= count <= 100, "bad list pagination")
            _require(res.get("status") in {"000", "013"}, "unsuccessful official list response")
            if res["status"] == "013":
                _require(number == 1 and not res.get("list"), "invalid empty list page")
                total, pages, listed = 0, 1, []
            else:
                total, pages, listed = int(res["total_count"]), int(res["total_page"]), res["list"]
                _require(int(res["page_no"]) == number, "list page number drift")
                _require(pages == max(1, (total + count - 1) // count), "list total_page inconsistency")
            groups[(a, b, count)].append((number, pages, total, listed))
        intervals = []
        for (a, b, count), pages in groups.items():
            n, total = pages[0][1:3]
            _require(sorted(p[0] for p in pages) == list(range(1, n + 1)), "incomplete/duplicate list pages")
            _require(all(p[1:3] == (n, total) for p in pages), "list total drift between pages")
            listed = [entry for page in pages for entry in page[3]]
            _require(len(listed) == total and len({r["rcept_no"] for r in listed}) == total,
                     "list count/receipt uniqueness mismatch")
            for row in listed:
                receipt = _receipt(row["rcept_no"])
                filed = datetime.strptime(row["rcept_dt"], "%Y%m%d").date()
                _require(row["corp_code"] == corp and a <= filed <= b, "listed issuer/date mismatch")
                record = {"receipt_no": receipt, "corp_code": corp, "filed": filed.isoformat(),
                          "report_nm": row["report_nm"]}
                _require(receipt not in receipts or receipts[receipt] == record, "conflicting official list entries")
                receipts[receipt] = record
            intervals.append((a, b))
        covered = begin
        for a, b in sorted(intervals):
            _require(a <= covered, "gap in official list search coverage")
            covered = max(covered, b + timedelta(days=1))
        _require(covered == end + timedelta(days=1), "official list search does not cover horizon")
        horizons[corp] = (begin, end + timedelta(days=1))
    for doc in corpus["documents"]:
        receipt = _receipt(doc["receipt_no"])
        _require(receipt not in documents and receipt in receipts, "duplicate/unlisted document")
        _require(doc["corp_code"] == receipts[receipt]["corp_code"], "document issuer drift")
        _require(all(HASH_RE.fullmatch(doc[k]) for k in ("zip_sha256", "xml_sha256")), "invalid document content hashes")
        documents[receipt] = doc
    return receipts, horizons, documents


def _document_bytes(doc, artifacts):
    # Lazy mappings may refer to GiB-sized corpora: load one approved receipt,
    # never keep every XML/ZIP in memory and never extract member paths.
    raw_zip = _artifact(artifacts, doc["zip_sha256"])
    with zipfile.ZipFile(io.BytesIO(raw_zip)) as archive:
        members = [i for i in archive.infolist() if i.filename == doc["xml_member_name"]]
        _require(len(members) == 1 and members[0].file_size <= MAX_XML_BYTES, "ambiguous/oversized ZIP XML member")
        _require(not members[0].is_dir(), "XML member is a directory")
        xml = archive.read(members[0])
    _require(sha256(xml) == doc["xml_sha256"], "ZIP/XML SHA mismatch")
    return xml


def _parsed_scopes(replay, xml):
    statuses = replay.get("scope_status")
    if statuses == {"CFS": "PARSED", "OFS": "PARSED"}:
        return {"CFS", "OFS"}
    _require(statuses == {"CFS": "EXPLICITLY_ABSENT", "OFS": "PARSED"}
             and replay.get("schema_version") == "dart-historical-revenue-parse-v2"
             and replay.get("cfs_absence_proven") is True,
             "approved document has unresolved CFS/OFS scope")
    details = replay.get("scope_details", {}).get("CFS", {})
    proofs = details.get("absence_evidence", [])
    _require(details.get("absence_proven") is True and len(proofs) == 1,
             "explicit CFS absence needs its original section evidence")
    proof = proofs[0]
    encoding = replay.get("decoding", {}).get("strict_decoding")
    _require(encoding in {"utf-8", "cp949"}, "absence evidence strict encoding missing")
    decoded = xml.decode(encoding, errors="strict")
    _require(decoded.encode(encoding) == xml, "absence XML encoding roundtrip mismatch")
    start, end = proof.get("body_start_character_offset"), proof.get("body_end_character_offset_exclusive")
    _require(type(start) is int and type(end) is int and 0 <= start < end <= len(decoded), "absence source offsets invalid")
    body = decoded[start:end]
    exact_text = " ".join(html.unescape(re.sub(r"<[^>]+>", " ", body).replace("&cr;", "\n")).split())
    _require(proof.get("claim") == "REPORT_EXPLICITLY_DECLARES_CFS_NOT_APPLICABLE"
             and proof.get("evidence_rule") == "UNIQUE_MAIN_CFS_SECTION_COMPLETE_DECLARATION_WITHOUT_TABLES_V1"
             and proof.get("scope_table_count") == 0
             and re.sub(r"\s+", "", proof.get("section_title", "")) == "2.연결재무제표"
             and proof.get("xml_sha256") == sha256(xml)
             and proof.get("source_slice_sha256") == sha256(body.encode(encoding))
             and proof.get("exact_text") == exact_text and bool(exact_text)
             and proof.get("body_physical_start_line") == decoded.count("\n", 0, start) + 1
             and proof.get("body_physical_end_line") == decoded.count("\n", 0, end) + 1,
             "explicit CFS absence source slice does not reproduce")
    return {"OFS"}


def _validate_lineage(lineage, receipts, horizons, artifacts):
    _schema(lineage, "dart-original-lineage-v1")
    scopes = {s["scope_id"]: s for s in lineage["scopes"]}
    _require(len(scopes) == len(lineage["scopes"]) and scopes, "duplicate/empty lineage scopes")
    by_corp, classified_for_scope = defaultdict(list), defaultdict(set)
    for sid, scope in scopes.items():
        by_corp[scope["corp_code"]].append((sid, _date(scope["period_end"]).strftime("%Y.%m")))
    classified = {r["receipt_no"]: r for r in lineage["classified_receipts"]}
    _require(len(classified) == len(lineage["classified_receipts"]) and set(classified) == set(receipts),
             "every official listed receipt must be classified exactly once")
    for receipt, classification in classified.items():
        _require(classification.get("reason") and len(set(classification["scope_ids"])) == len(classification["scope_ids"]),
                 "receipt classification needs reason/unique scopes")
        _require(all(s in scopes for s in classification["scope_ids"]), "unknown classified scope")
        for sid in classification["scope_ids"]:
            classified_for_scope[sid].add(receipt)
        name = receipts[receipt]["report_nm"].replace(" ", "")
        for sid, period in by_corp[receipts[receipt]["corp_code"]]:
            if period in name and ("분기보고서" in name or "반기보고서" in name):
                _require(sid in classification["scope_ids"], "relevant periodic filing excluded from lineage")
    for sid, scope in scopes.items():
        corp = scope["corp_code"]
        _require(corp in horizons and re.fullmatch(r"[0-9]{6}", scope["stock_code"]), "invalid lineage identity")
        _require(scope["quarter"] in {"Q1", "Q2", "Q3"}, "only interim quarters supported")
        start, end = _date(scope["period_start"]), _date(scope["period_end"])
        _require(start <= end and horizons[corp][0] <= end < horizons[corp][1], "period outside searched history")
        events = scope["events"]
        ids = [e["receipt_no"] for e in events]
        expected = classified_for_scope[sid]
        _require(ids and len(set(ids)) == len(ids) and set(ids) == expected, "lineage omits/duplicates classified events")
        _require(all(receipts[r]["corp_code"] == corp for r in ids), "lineage issuer mismatch")
        _require(ids == sorted(ids, key=lambda r: (receipts[r]["filed"], r)), "lineage not in official receipt order")
        previous = None
        for event in events:
            receipt, kind = event["receipt_no"], event["kind"]
            _require(kind in {"ORIGINAL", "CORRECTION", "WITHDRAWAL", "UNRESOLVED"}, "unknown revision kind")
            name = receipts[receipt]["report_nm"]
            _require(_date(receipts[receipt]["filed"]) >= end, "filing before period end")
            if kind == "ORIGINAL":
                _require(previous is None and "정정" not in name and "철회" not in name, "original vintage not proved")
            elif kind == "UNRESOLVED":
                _require(event.get("reason"), "unresolved lineage needs a blocking reason")
            else:
                _require(previous is not None and event.get("parent_receipt_no") == previous,
                         "nonlinear/missing revision parent must remain UNRESOLVED")
                proof = event["proof"]
                raw = _artifact(artifacts, proof["artifact_sha256"])
                text = proof["text"]
                _require(previous in text and any(text.encode(enc) in raw for enc in ("utf-8", "cp949")
                                                   if _encodable(text, enc)), "revision parent anchor absent")
                _require((kind == "CORRECTION" and "정정" in name) or (kind == "WITHDRAWAL" and "철회" in name),
                         "revision type differs from official title")
            previous = receipt
    return scopes


def _encodable(text, encoding):
    try:
        text.encode(encoding)
        return True
    except UnicodeEncodeError:
        return False


def _identity(identity, scope, available, artifacts):
    choices = [m for m in identity["mappings"] if m["stock_code"] == scope["stock_code"]
               and m["corp_code"] == scope["corp_code"]
               and _date(m["valid_from"]) <= available < _date(m["valid_until_exclusive"])]
    _require(len(choices) == 1, "no unique historical issuer/listed-asset mapping")
    m = choices[0]
    _require(m["basis"] == "CERTIFIED_FY_AND_LISTING_EPISODE", "unsupported historical identity proof")
    _require(_date(m["valid_from"]) != date.min, "current mapping backdated to date.min is not historical proof")
    asset = int(m["asset_id"])
    _require(asset > 0, "invalid historical asset_id")
    fy = _json(artifacts, m["fy_reference_sha256"])
    if fy.get("source_extract_sha256"):
        original = _json(artifacts, fy["source_extract_sha256"])
        _require(original["rows"] == fy["rows"] and original["query_sha256"] == fy["query_sha256"]
                 == sha256(fy["source_query"].encode()), "scoped FY reference differs from its read-only original")
    fy_rows = [r for r in fy["rows"] if int(r["asset_id"]) == asset and r["filing_id"] == m["fy_receipt_no"]]
    _require(fy_rows, "FY certified receipt does not map to requested asset")
    for r in fy_rows:
        _certified(r)
        _require(_date(r.get("period_end", fy.get("fiscal_period_end"))).year == _date(scope["period_end"]).year,
                 "FY identity reference is not the target fiscal year")
    issuer = _json(artifacts, m["fy_issuer_sha256"])
    _require(issuer.get("status") == "000" and any(r["rcept_no"] == m["fy_receipt_no"]
             and r["corp_code"] == scope["corp_code"] for r in issuer["list"]), "FY receipt issuer mismatch")
    listing = _json(artifacts, m["listing_snapshot_sha256"])
    _require(listing.get("identity_methodology") and listing["identity_methodology"] != "CURRENT_CORPCODE",
             "historical listing identity methodology missing")
    proof = _json(artifacts, listing["identity_evidence_sha256"])
    _schema(proof, "certified-listing-snapshot-extract-v1")
    _certified(proof)
    payload = proof["payload"]
    body = {k: v for k, v in payload.items() if k != "snapshot_id"}
    original_digest = sha256(json.dumps(body, sort_keys=True, separators=(",", ":")).encode())
    _require(payload.get("schema") == "asset-listing-snapshot-v1"
             and original_digest == payload["snapshot_id"] == proof["snapshot_id"],
             "frozen listing proof payload hash mismatch")
    matches = [r for r in listing["rows"] if r["stock_code"] == scope["stock_code"]
               and _date(r["valid_from"]) <= available < _date(r["valid_until_exclusive"])]
    _require(len(matches) == 1 and int(matches[0]["asset_id"]) == asset,
             "ambiguous/ticker-reused historical listing episode")
    _certified(matches[0])
    _require(matches[0]["quality_run_id"] == proof["quality_run_id"], "listing projection DQ differs from frozen proof")
    original_periods = [p for p in payload["periods"] if p["ticker"] == scope["stock_code"]
                        and int(p["asset_id"]) == asset and _date(p["start"]) <= available
                        and (p["end"] is None or available <= _date(p["end"]))]
    _require(len(original_periods) == 1, "projected historical identity absent/ambiguous in frozen source")
    episode = original_periods[0]
    source_end = (_date(episode["end"]) + timedelta(days=1) if episode["end"]
                  else _date(payload["verified_through"]) + timedelta(days=1))
    _require(_date(matches[0]["valid_from"]) >= max(_date(episode["start"]), _date(payload["coverage_start"]))
             and _date(matches[0]["valid_until_exclusive"]) <= min(source_end, _date(payload["verified_through"]) + timedelta(days=1)),
             "listing projection extends beyond independently frozen source")
    _require(_date(matches[0]["valid_from"]) != date.min
             and _date(m["valid_from"]) >= _date(matches[0]["valid_from"])
             and _date(m["valid_until_exclusive"]) <= _date(matches[0]["valid_until_exclusive"]),
             "mapping extends beyond certified historical episode")
    return m


@dataclass(frozen=True)
class PreparedRevenue:
    dataset_version: str
    manifest_sha256: str
    bundle_sha256: str
    rows: tuple[dict, ...]
    blocking_windows: tuple[dict, ...]
    evidence: dict


def prepare(stage_bytes: bytes, checked_manifest: dict, artifacts: Mapping[str, bytes], *,
            parse_document: Callable) -> PreparedRevenue:
    """Replay and validate all claims without DB writes; errors fail closed.

    Review explicitly selects current-quarter rows, not YTD or reconstructed
    amounts. Unapproved/unparseable later revisions create bounded gaps even
    when an earlier approved revision exists. Partial publication is explicit.
    """
    m = checked_manifest
    artifacts = _EvidenceStore(artifacts)
    _schema(m, "dart-revenue-checked-manifest-v1")
    _require(re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9_.-]{0,119}", m["dataset_version"]), "invalid dataset version")
    _require(sha256(stage_bytes) == m["stage_sha256"], "staged artifact SHA mismatch")
    review = m["review"]
    _require(review.get("decision") == "ACCEPT" and review.get("reviewer"), "independent checked manifest required")
    _require(datetime.fromisoformat(review["reviewed_at"]).tzinfo is not None, "review timestamp must be aware")
    parser_file = inspect.getsourcefile(parse_document)
    _require(parser_file is not None and sha256(Path(parser_file).read_bytes()) == m["parser_source_sha256"],
             "trusted parser implementation hash changed")
    stage = json.loads(stage_bytes)
    _schema(stage, "dart-historical-revenue-staging-v1")
    _require(stage.get("source_collection_finished") is True and stage.get("all_planned_issuer_lists_complete") is True,
             "incomplete/raw pilot staging cannot be certified")
    approved = set(m["approved_row_sha256"])
    _require(approved and len(approved) == len(m["approved_row_sha256"]), "empty/duplicate reviewed row hashes")
    staged = {object_sha256(r): r for r in stage["rows"]}
    _require(len(staged) == len(stage["rows"]) and approved <= set(staged), "approved row missing/duplicate staging rows")
    _require(all(staged[h]["amount_role"] == "CURRENT_QUARTER" for h in approved), "YTD is evidence, never a quarter observation")
    _require(all(staged[h].get("source_publication_candidate") is True for h in approved),
             "review cannot override unresolved/non-candidate staged rows")
    corpus, lineage, identity = (_json(artifacts, m[k]) for k in ("corpus_sha256", "lineage_sha256", "identity_sha256"))
    _schema(identity, "dart-historical-identity-v1")
    if corpus.get("collector_contract_sha256"):
        _require(stage.get("source_contract_sha256") == corpus["collector_contract_sha256"]
                 and stage.get("source_run_sha256") == corpus["source_run_sha256"],
                 "staging and corpus collection provenance differ")
    receipts, horizons, documents = _validate_corpus(corpus, artifacts)
    scopes = _validate_lineage(lineage, receipts, horizons, artifacts)
    by_receipt = defaultdict(list)
    for h in approved:
        by_receipt[staged[h]["receipt_no"]].append((h, staged[h]))
    rows, blocks, consumed, verified_mappings = [], [], set(), {}
    for sid, scope in scopes.items():
        events, horizon = scope["events"], horizons[scope["corp_code"]][1]
        for i, event in enumerate(events):
            receipt = event["receipt_no"]
            available = _date(receipts[receipt]["filed"]) + timedelta(days=1)
            until = (_date(receipts[events[i + 1]["receipt_no"]]["filed"]) + timedelta(days=1)
                     if i + 1 < len(events) else horizon)
            if until <= available:
                _require(not by_receipt[receipt], "same-day superseded revision cannot be approved")
                continue
            selected = [(h, r) for h, r in by_receipt[receipt]
                        if r["quarter"] == scope["quarter"] and r["corp_code"] == scope["corp_code"]]
            reason = None
            if event["kind"] in {"WITHDRAWAL", "UNRESOLVED"}:
                _require(not selected, "unresolved/withdrawn event cannot publish values")
                reason = event["kind"]
            elif receipt not in documents or not selected:
                reason = "DOCUMENT_OR_REVIEW_MISSING"
            else:
                doc = documents[receipt]
                xml = _document_bytes(doc, artifacts)
                replay = parse_document(xml, scope["quarter"], scope["corp_code"])
                _require(replay.get("parser_source_sha256") == m["parser_source_sha256"]
                         and replay.get("xml_sha256") == doc["xml_sha256"]
                         and replay.get("xml_issuer_codes") == [scope["corp_code"]], "parser replay provenance mismatch")
                parsed_scopes = _parsed_scopes(replay, xml)
                _require({r["fs_type"] for _, r in selected} == parsed_scopes and len(selected) == len(parsed_scopes),
                         "approved receipt must include every parsed current-quarter scope")
                identity_row = _identity(identity, scope, available, artifacts)
                verified_mappings[(sid, object_sha256(identity_row))] = {"scope_id": sid, **identity_row}
                event_until = until
                until = min(until, _date(identity_row["valid_until_exclusive"]))
                for h, row in selected:
                    _require(h not in consumed, "staged row assigned to multiple lineage scopes")
                    _require(row["stock_code"] == scope["stock_code"] and row["period_start"] == scope["period_start"]
                             and row["period_end"] == scope["period_end"], "staged period/stock differs from checked scope")
                    _require(row["official_filed_date"] == receipts[receipt]["filed"], "staged filing date differs from official list")
                    if "lineage_event_kind" in row:
                        _require(row["lineage_event_kind"] == event["kind"], "stage/review lineage classification differs")
                    for key in ("zip_sha256", "xml_member_name", "xml_sha256"):
                        _require(row[key] == doc[key], f"staged {key} differs from original corpus")
                    replay_rows = [r for r in replay["rows"] if r["fs_type"] == row["fs_type"]
                                   and r["amount_role"] == "CURRENT_QUARTER"]
                    _require(len(replay_rows) == 1 and all(row.get(k) == replay_rows[0].get(k) for k in PARSED_KEYS),
                             "staged source cell differs from independent parser replay")
                    ytd = [r for r in replay["rows"] if r["fs_type"] == row["fs_type"] and r["amount_role"] == "CURRENT_YTD"]
                    _require(len(ytd) == 1 and ytd[0]["period_end"] == row["period_end"], "current YTD evidence missing")
                    value = Decimal(row["amount_krw_decimal"])
                    _require(value.is_finite() and value >= 0 and row["currency"] == "KRW"
                             and row["metric"] == "revenue", "invalid current-quarter revenue/unit")
                    _require(value.as_tuple().exponent >= -6 and value < Decimal("1e32"),
                             "revenue cannot be stored in NUMERIC(38,6) without rounding")
                    if scope["quarter"] == "Q1":
                        _require(value == Decimal(ytd[0]["amount_krw_decimal"]), "Q1 current/YTD mismatch")
                    evidence = {"scope_id": sid, "stage_row_sha256": h, "receipt_no": receipt,
                                "zip_sha256": doc["zip_sha256"], "xml_sha256": doc["xml_sha256"],
                                "xml_member_name": doc["xml_member_name"], "parsed_source": replay_rows[0],
                                "current_ytd_evidence": ytd[0], "identity": identity_row,
                                "statement_scope_status": replay["scope_status"],
                                "statement_scope_details": replay.get("scope_details", {}),
                                "lineage_event": event, "cross_vintage_ytd_reconciliation_claim": False}
                    output = {"asset_id": int(identity_row["asset_id"]), "stock_code": row["stock_code"],
                              "corp_code": row["corp_code"], "source": "DART_DOCUMENT", "metric": "revenue",
                              "statement_type": "IS", "data_basis": "STANDARDIZED", "unit_type": "currency",
                              "quantity_semantics": "FLOW_CURRENT_QUARTER", "period_start": row["period_start"],
                              "period_end": row["period_end"], "fiscal_period": scope["quarter"],
                              "fs_type": row["fs_type"], "value": format(value, ".6f"), "currency": "KRW",
                              "filing_id": receipt, "revision_key": receipt, "filed": receipts[receipt]["filed"],
                              "available_date": available.isoformat(),
                              "available_at": datetime.combine(available, time.min, KST).isoformat(),
                              "valid_until_exclusive": until.isoformat(), "evidence": evidence}
                    key = [output[k] for k in ("asset_id", "period_end", "fiscal_period", "fs_type", "revision_key", "metric")]
                    output["observation_key"] = object_sha256(key)
                    output["payload_sha256"] = object_sha256(output)
                    rows.append(output)
                    consumed.add(h)
                if until < event_until:
                    blocks.append({"scope_id": sid, "valid_from": until.isoformat(),
                                   "valid_until_exclusive": event_until.isoformat(), "reason": "IDENTITY_EPISODE_END"})
            if reason:
                blocks.append({"scope_id": sid, "receipt_no": receipt, "valid_from": available.isoformat(),
                               "valid_until_exclusive": until.isoformat(), "reason": reason})
    _require(consumed == approved, "reviewed rows not covered by a valid lineage window")
    _require(rows, "no certified-window revenue observations")
    rows.sort(key=lambda r: r["observation_key"])
    evidence = {k: m[k] for k in ("stage_sha256", "corpus_sha256", "lineage_sha256", "identity_sha256", "parser_source_sha256", "review")}
    evidence.update(contract_version=CONTRACT_VERSION, scopes=list(scopes.values()),
                    publisher_source_sha256=sha256(Path(__file__).read_bytes()),
                    quality_rules_source_sha256=sha256(Path(inspect.getsourcefile(check_document_revenue)).read_bytes()),
                    verified_scope_identity_mappings=list(verified_mappings.values()),
                    covered_through_by_corp={corp: (end - timedelta(days=1)).isoformat()
                                            for corp, (_, end) in horizons.items()},
                    certification_scope="SOURCE_ROWS_WITH_BOUNDED_VALIDITY_ONLY", full_history_certified=False,
                    reader_contract="ASOF_WINDOW_REQUIRED_NO_UNBOUNDED_FORWARD_FILL")
    binding = {"rows": rows, "blocking_windows": blocks, "evidence": evidence}
    prepared = PreparedRevenue(m["dataset_version"], object_sha256(m), object_sha256(binding), tuple(rows), tuple(blocks), evidence)
    checks = check_document_revenue(prepared)
    if any(result.blocks_publish for result in checks):
        raise QualityGateError(checks)
    return prepared


def publish(conn, *, stage_bytes: bytes, checked_manifest: dict, artifacts: Mapping[str, bytes],
            parse_document: Callable, authorize_write: bool = False) -> dict:
    """Stage an atomic insert in the caller transaction, never commit or replace.

    Caller must use ``with conn.transaction():`` and commit only after reviewing
    its receipt. Autocommit is rejected. Retries reuse only an identical version
    with identical payloads and an already CERTIFIED DQ run. No core table write.
    """
    _require(authorize_write is True, "explicit publication authorization required")
    _require(getattr(conn, "autocommit", True) is False, "publication requires caller-owned transaction")
    p = prepare(stage_bytes, checked_manifest, artifacts, parse_document=parse_document)
    checks = check_document_revenue(p)
    with conn.cursor() as cur:
        cur.execute("SELECT pg_advisory_xact_lock(hashtextextended(%s,0))", ("dart-document-revenue:" + p.dataset_version,))
        cur.execute("""SELECT d.manifest_sha256,d.bundle_sha256,d.quality_run_id,q.status
                       FROM fundamental_document_dataset d JOIN dq_run q ON q.run_id=d.quality_run_id
                       WHERE d.dataset_version=%s""", (p.dataset_version,))
        existing = cur.fetchone()
        if existing:
            _require(existing[0] == p.manifest_sha256 and existing[1] == p.bundle_sha256
                     and existing[3] == "CERTIFIED", "existing immutable dataset/version conflict")
            cur.execute("SELECT observation_key,payload_sha256 FROM fundamental_document_revenue WHERE dataset_version=%s",
                        (p.dataset_version,))
            _require(dict(cur.fetchall()) == {r["observation_key"]: r["payload_sha256"] for r in p.rows},
                     "existing version rows differ; never overwrite")
            return {"dataset_version": p.dataset_version, "quality_run_id": str(existing[2]),
                    "rows": len(p.rows), "inserted": 0, "reused": True, "committed": False}
        run_id = uuid4()
        context = BatchContext(run_id, MODE, partition_key=p.dataset_version, input_fingerprint=p.bundle_sha256)
        cur.execute("""INSERT INTO dq_run(run_id,mode,partition_key,input_fingerprint,ruleset_version,status)
                       VALUES(%s,%s,%s,%s,%s,'RUNNING')""",
                    (run_id, MODE, p.dataset_version, p.bundle_sha256, QUALITY_RULESET_VERSION + "/" + CONTRACT_VERSION))
        cur.execute("""INSERT INTO fundamental_document_dataset
                       (dataset_version,manifest_sha256,bundle_sha256,quality_run_id,evidence,blocking_windows)
                       VALUES(%s,%s,%s,%s,%s::jsonb,%s::jsonb)""",
                    (p.dataset_version, p.manifest_sha256, p.bundle_sha256, run_id,
                     canonical_bytes(p.evidence).decode(), canonical_bytes(p.blocking_windows).decode()))
        for row in p.rows:
            cur.execute("""INSERT INTO fundamental_document_revenue
                           (dataset_version,observation_key,payload_sha256,asset_id,stock_code,corp_code,
                            period_start,period_end,fiscal_period,fs_type,value,filing_id,revision_key,filed,
                            available_date,available_at,valid_until_exclusive,evidence,quality_run_id)
                           VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s)""",
                        (p.dataset_version, row["observation_key"], row["payload_sha256"], row["asset_id"],
                         row["stock_code"], row["corp_code"], row["period_start"], row["period_end"],
                         row["fiscal_period"], row["fs_type"], Decimal(row["value"]), row["filing_id"],
                         row["revision_key"], row["filed"], row["available_date"], row["available_at"],
                         row["valid_until_exclusive"], canonical_bytes(row["evidence"]).decode(), run_id))
    repository.finish_run(conn, context, "CERTIFIED", checks, commit=False)
    return {"dataset_version": p.dataset_version, "quality_run_id": str(run_id), "rows": len(p.rows),
            "inserted": len(p.rows), "reused": False, "committed": False,
            "blocking_windows": len(p.blocking_windows), "transaction_owned_by_caller": True,
            "manifest_sha256": p.manifest_sha256, "bundle_sha256": p.bundle_sha256}


# Deliberately not the existing fundamental query. Exact version + as-of is
# mandatory, and both dataset/observation DQ bindings must still be CERTIFIED.
ASOF_SQL = """
SELECT r.* FROM public.fundamental_document_revenue r
JOIN public.fundamental_document_dataset d USING(dataset_version)
JOIN public.dq_run q ON q.run_id=r.quality_run_id AND q.status='CERTIFIED'
WHERE r.dataset_version=%s AND d.quality_run_id=r.quality_run_id
  AND r.available_date<=%s AND %s<r.valid_until_exclusive
ORDER BY r.asset_id,r.period_end,r.fs_type,r.revision_key
"""


# For an expiry-aware replay consumer ONLY. Its source contract must bind this
# exact query and every returned valid_until_exclusive. Not a legacy ledger.
BOUNDED_LEDGER_SQL = """
SELECT r.* FROM public.fundamental_document_revenue r
JOIN public.fundamental_document_dataset d USING(dataset_version)
JOIN public.dq_run q ON q.run_id=r.quality_run_id AND q.status='CERTIFIED'
WHERE r.dataset_version=%s AND d.quality_run_id=r.quality_run_id
ORDER BY r.asset_id,r.period_end,r.fs_type,r.available_date,r.revision_key
"""


DATASET_METADATA_SQL = """
SELECT d.*,q.status AS dq_status,q.input_fingerprint,q.ruleset_version
FROM public.fundamental_document_dataset d
JOIN public.dq_run q ON q.run_id=d.quality_run_id AND q.status='CERTIFIED'
WHERE d.dataset_version=%s
"""


def query_source_sha256(*, bounded_ledger=False):
    return sha256((BOUNDED_LEDGER_SQL if bounded_ledger else ASOF_SQL).encode())
