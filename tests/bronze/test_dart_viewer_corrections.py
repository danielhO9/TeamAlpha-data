import json
import sys
from datetime import date
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pytest

from pipeline.bronze import dart_viewer_corrections as viewer
from pipeline.bronze import financials
from pipeline.bronze.dart_viewer_corrections import (
    _parse_main_page,
    _parse_viewer_economic_body,
    _pending_terminal_dependencies,
    _validate_terminal_families,
    required_viewer_receipts,
    ViewerReceiptEvidence,
)


TEST_COVERAGE_START = date(2015, 1, 1)
TEST_COVERAGE_END = date(2099, 12, 31)


def _collect(base, **kwargs):
    kwargs.setdefault("coverage_start", TEST_COVERAGE_START)
    kwargs.setdefault("coverage_end", TEST_COVERAGE_END)
    return viewer.collect_viewer_corrections(base, **kwargs)


def _verify(base, **kwargs):
    kwargs.setdefault("required_start", TEST_COVERAGE_START)
    kwargs.setdefault("required_end", TEST_COVERAGE_END)
    return viewer.verify_viewer_corrections(base, **kwargs)


def _required(base, **kwargs):
    kwargs.setdefault("coverage_start", TEST_COVERAGE_START)
    kwargs.setdefault("coverage_end", TEST_COVERAGE_END)
    return required_viewer_receipts(base, **kwargs)


def _dart_fixture(name: str) -> Path:
    return Path(__file__).parents[1] / "fixtures" / "dart" / name


def _write_corp_codes(root: Path, pairs=()) -> None:
    entries = "".join(
        "<list><corp_code>" + corp + "</corp_code><stock_code>"
        + ticker + "</stock_code></list>"
        for corp, ticker in pairs
    )
    path = root / financials.CORPCODE_BRONZE_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"<result>{entries}</result>", encoding="utf-8")


def _write_disclosures(
    root, rows, *, start="20150101", end="20150131",
):
    rows = [
        {
            **row,
            "rcept_dt": row.get("rcept_dt")
            or str(row.get("rcept_no") or "")[:8],
        }
        for row in rows
    ]
    corp_code_path = root / financials.CORPCODE_BRONZE_PATH
    if not corp_code_path.is_file():
        _write_corp_codes(root)
    corp_to_stock = dict(financials.load_listed_corps_from_bronze(str(root)))
    interval = (
        root / "corporate_actions" / "dart" / "manifests"
        / f"from={start}" / f"to={end}"
    )
    interval.mkdir(parents=True, exist_ok=True)
    interval.joinpath("disclosures_v3.json").write_text(
        json.dumps(rows, ensure_ascii=False), encoding="utf-8",
    )
    structured_queries = viewer._structured_query_keys(rows, corp_to_stock)
    document_candidates = viewer._document_candidate_receipts(
        rows, corp_to_stock,
    )
    marker = {"status": "COMPLETE", "fromdate": start, "todate": end}
    interval.joinpath("structured_complete_v3.json").write_text(
        json.dumps({**marker, "query_count": len(structured_queries)}),
        encoding="utf-8",
    )
    interval.joinpath("documents_complete_v5.json").write_text(
        json.dumps({**marker, "candidate_count": len(document_candidates)}),
        encoding="utf-8",
    )


def test_main_page_uses_family_select_not_attachment_receipts():
    payload = b"""
    <script>
      viewDoc("20150102900228", "4435128", "0", "0", "0", "HTML", "");
    </script>
    <select id="family">
      <option value="rcpNo=20150102900228" selected>current</option>
      <option value="rcpNo=20141215900118">original</option>
    </select>
    <select id="att">
      <option value="rcpNo=19990101000001&amp;dcmNo=1">attachment</option>
    </select>
    """

    dcm, previous, root, family, economic_receipt, official_order = _parse_main_page(
        "20150102900228", payload,
    )

    assert dcm == "4435128"
    assert previous == "20141215900118"
    assert root == "20141215900118"
    assert family == ("20141215900118", "20150102900228")
    assert economic_receipt == "20150102900228"
    assert official_order == ("20150102900228", "20141215900118")


def test_attachment_current_may_be_absent_from_main_family():
    payload = b"""
    <script>
      viewDoc("20150205900125", "4500001", "0", "0", "0", "HTML", "");
    </script>
    <select id="family">
      <option value="rcpNo=20150204900001">original</option>
    </select>
    <select id="att">
      <option value="rcpNo=20150205900125&amp;dcmNo=4500001" selected>
        [attachment correction]
      </option>
    </select>
    """

    result = _parse_main_page(
        "20150205900125", payload, attachment_correction=True,
    )

    assert result == (
        "4500001",
        "20150204900001",
        "20150204900001",
        ("20150204900001", "20150205900125"),
        "20150204900001",
        ("20150204900001",),
    )


def test_live_cash_attachment_main_has_exact_family_att_and_viewer_target():
    attachment = viewer.parse_official_dart_main_page(
        "20210223800413",
        _dart_fixture(
            "20210223800413-cash-attachment-main.html"
        ).read_bytes(),
        expected_attachment_only=True,
    )
    economic = viewer.parse_official_dart_main_page(
        "20210223800278",
        _dart_fixture("20210223800278-cash-family-main.html").read_bytes(),
        expected_attachment_only=False,
    )

    assert attachment.current_selector == "ATTACHMENT"
    assert attachment.family_receipts == ("20210223800278",)
    assert attachment.dcm_no == "7821388"
    assert attachment.dtd == "HTML"
    assert attachment.attachment_keys == (
        "20210223800564:7822175",
        "20210223800413:7821388",
        "20210223800278:7820625",
    )
    assert economic.current_selector == "FAMILY"
    assert economic.dcm_no == "7820624"
    assert economic.family_receipts == attachment.family_receipts
    assert economic.attachment_keys == attachment.attachment_keys


def test_fetch_one_cash_attachment_uses_exact_selector_dtd_and_terminal(
    tmp_path, monkeypatch,
):
    source = "20210223800413"
    economic = "20210223800278"
    fixtures = {
        ("main.do", source): _dart_fixture(
            "20210223800413-cash-attachment-main.html"
        ).read_bytes(),
        ("main.do", economic): _dart_fixture(
            "20210223800278-cash-family-main.html"
        ).read_bytes(),
        ("viewer.do", source): _dart_fixture(
            "20210223800413-cash-attachment-viewer.html"
        ).read_bytes(),
        ("viewer.do", economic): _dart_fixture(
            "20210223800278-cash-economic-viewer.html"
        ).read_bytes(),
    }
    calls: list[str] = []

    def fetch(url, **_kwargs):
        calls.append(url)
        parsed = urlparse(url)
        query = parse_qs(parsed.query)
        receipt = query["rcpNo"][0]
        endpoint = parsed.path.rsplit("/", 1)[-1]
        if endpoint == "viewer.do":
            assert query["dtd"] == ["HTML"]
        return fixtures[(endpoint, receipt)]

    monkeypatch.setattr(viewer, "_get", fetch)
    # A legacy hardcoded-HTML cache must never satisfy the v2 DTD-bound path.
    legacy = viewer._receipt_main_path(tmp_path, source).with_name("viewer.html")
    legacy.parent.mkdir(parents=True)
    legacy.write_bytes(b"<html><body><table>stale</table></body></html>")
    legacy.with_name("economic_main.html").write_bytes(b"stale heuristic main")

    evidence = viewer._fetch_one(
        tmp_path,
        source,
        tries=1,
        timeout=1,
        rate_limiter=viewer._RateLimiter(1.0, 0.0),
        report_name="[첨부정정]현금ㆍ현물배당결정",
        report_names={
            source: "[첨부정정]현금ㆍ현물배당결정",
            economic: "현금ㆍ현물배당결정",
        },
    )

    assert evidence.correction_of_receipt_no == economic
    assert evidence.revision_root_receipt_no == economic
    assert evidence.current_selector == "ATTACHMENT"
    assert evidence.dtd == "HTML"
    assert evidence.economic_body_receipt_no == economic
    assert evidence.economic_body_dcm_no == "7820624"
    assert evidence.economic_body_dtd == "HTML"
    assert evidence.economic_classification == "ECONOMIC_DECISION"
    assert evidence.common_cash_amount == 50.0
    assert evidence.record_date == "2020-12-31"
    assert evidence.viewer_path == (
        viewer.OBJECT_ROOT_RELATIVE_PATH
        / f"sha256={evidence.viewer_sha256}.html"
    ).as_posix()
    assert evidence.economic_viewer_path == (
        viewer.OBJECT_ROOT_RELATIVE_PATH
        / f"sha256={evidence.economic_viewer_sha256}.html"
    ).as_posix()
    assert len(calls) == 4


def test_cash_attachment_and_economic_selector_mismatch_fails_closed(
    tmp_path, monkeypatch,
):
    source = "20210223800413"
    economic = "20210223800278"
    source_main = _dart_fixture(
        "20210223800413-cash-attachment-main.html"
    ).read_bytes()
    economic_main = _dart_fixture(
        "20210223800278-cash-family-main.html"
    ).read_text(encoding="utf-8").replace(
        "20210223800564&amp;dcmNo=7822175",
        "20210223800564&amp;dcmNo=9999999",
    ).encode()

    def fetch(url, **_kwargs):
        parsed = urlparse(url)
        query = parse_qs(parsed.query)
        receipt = query["rcpNo"][0]
        if parsed.path.endswith("/main.do"):
            return source_main if receipt == source else economic_main
        if receipt == source:
            return _dart_fixture(
                "20210223800413-cash-attachment-viewer.html"
            ).read_bytes()
        return _dart_fixture(
            "20210223800278-cash-economic-viewer.html"
        ).read_bytes()

    monkeypatch.setattr(viewer, "_get", fetch)
    with pytest.raises(RuntimeError, match="selectors disagree"):
        viewer._fetch_one(
            tmp_path,
            source,
            tries=1,
            timeout=1,
            rate_limiter=viewer._RateLimiter(1.0, 0.0),
            report_name="[첨부정정]현금ㆍ현물배당결정",
            report_names={
                source: "[첨부정정]현금ㆍ현물배당결정",
                economic: "현금ㆍ현물배당결정",
            },
        )


def test_cash_attachment_manifest_roundtrip_and_body_corruption(
    tmp_path, monkeypatch,
):
    source = "20210223800413"
    economic = "20210223800278"
    _write_disclosures(tmp_path, [
        {
            "rcept_no": source,
            "rcept_dt": "20210223",
            "stock_code": "005930",
            "report_nm": "[첨부정정]현금ㆍ현물배당결정",
        },
        {
            "rcept_no": economic,
            "rcept_dt": "20210223",
            "stock_code": "005930",
            "report_nm": "현금ㆍ현물배당결정",
        },
    ])
    fixtures = {
        ("main.do", source): _dart_fixture(
            "20210223800413-cash-attachment-main.html"
        ).read_bytes(),
        ("main.do", economic): _dart_fixture(
            "20210223800278-cash-family-main.html"
        ).read_bytes(),
        ("viewer.do", source): _dart_fixture(
            "20210223800413-cash-attachment-viewer.html"
        ).read_bytes(),
        ("viewer.do", economic): _dart_fixture(
            "20210223800278-cash-economic-viewer.html"
        ).read_bytes(),
    }

    def fetch(url, **_kwargs):
        parsed = urlparse(url)
        query = parse_qs(parsed.query)
        return fixtures[(parsed.path.rsplit("/", 1)[-1], query["rcpNo"][0])]

    monkeypatch.setattr(viewer, "_get", fetch)
    verified = _collect(
        str(tmp_path),
        apply=True,
        workers=1,
        request_interval_seconds=0.001,
    )

    assert verified.receipt_count == 1
    evidence = verified.receipts[0]
    assert evidence.receipt_no == source
    assert evidence.correction_of_receipt_no == economic
    assert evidence.economic_body_receipt_no == economic
    assert _verify(str(tmp_path)) == verified

    economic_viewer = tmp_path / evidence.economic_viewer_path
    original = economic_viewer.read_bytes()
    economic_viewer.write_bytes(original + b"corruption")
    with pytest.raises(RuntimeError, match="SHA/content length mismatch"):
        _verify(str(tmp_path))
    economic_viewer.write_bytes(original)
    assert _verify(str(tmp_path)) == verified

    manifest = tmp_path / viewer.MANIFEST_RELATIVE_PATH
    canonical = manifest.read_bytes()
    manifest.write_text(
        json.dumps(json.loads(canonical), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    with pytest.raises(RuntimeError, match="manifest is not canonical"):
        _verify(str(tmp_path))
    manifest.write_bytes(canonical)
    assert _verify(str(tmp_path)) == verified


def test_viewer_manifest_rolls_back_when_new_snapshot_verification_fails(
    tmp_path, monkeypatch,
):
    receipt = "20220802900375"
    _write_disclosures(tmp_path, [{
        "rcept_no": receipt,
        "stock_code": "005930",
        "report_nm": "현금ㆍ현물배당결정",
    }])
    main = f"""
    <script>viewDoc("{receipt}", "999", "0", "0", "0", "HTML", "");</script>
    <select id="family">
      <option value="rcpNo={receipt}" selected>current</option>
    </select>
    <select id="att"></select>
    """.encode()

    def fetch(url, **_kwargs):
        if urlparse(url).path.endswith("/main.do"):
            return main
        return _economic_viewer()

    monkeypatch.setattr(viewer, "_get", fetch)
    _collect(
        str(tmp_path), apply=True, workers=1,
        request_interval_seconds=0.001,
    )
    manifest = tmp_path / viewer.MANIFEST_RELATIVE_PATH
    previous = manifest.read_bytes()
    monkeypatch.setattr(
        viewer,
        "verify_viewer_corrections",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            RuntimeError("semantic verification failed")
        ),
    )

    with pytest.raises(RuntimeError, match="semantic verification failed"):
        _collect(
            str(tmp_path), apply=True, workers=1,
            request_interval_seconds=0.001,
        )
    assert manifest.read_bytes() == previous


def test_cash_attachment_does_not_revive_withdrawn_economic_terminal(
    tmp_path, monkeypatch,
):
    source = "20211224000003"
    withdrawal = "20211223000002"
    original = "20211222000001"
    source_main = f"""
      <script>viewDoc("{source}", "3", "0", "0", "0", "HTML", "");</script>
      <select id="family">
        <option value="rcpNo={withdrawal}">withdrawal</option>
        <option value="rcpNo={original}">original</option>
      </select>
      <select id="att">
        <option value="rcpNo={source}&amp;dcmNo=3" selected>attachment</option>
      </select>
    """.encode()
    terminal_main = f"""
      <script>viewDoc("{withdrawal}", "2", "0", "0", "0", "HTML", "");</script>
      <select id="family">
        <option value="rcpNo={withdrawal}" selected>withdrawal</option>
        <option value="rcpNo={original}">original</option>
      </select>
      <select id="att">
        <option value="rcpNo={source}&amp;dcmNo=3">attachment</option>
      </select>
    """.encode()

    def fetch(url, **_kwargs):
        parsed = urlparse(url)
        receipt = parse_qs(parsed.query)["rcpNo"][0]
        if parsed.path.endswith("/main.do"):
            return source_main if receipt == source else terminal_main
        if receipt == source:
            return b"<html><body><table><tr><td>attachment</td></tr></table></body></html>"
        # Even a stale-looking amount in the terminal body cannot revive a
        # withdrawal because classification is bound to its disclosure title.
        return _economic_viewer(amount="999", record_date="2021-12-31")

    monkeypatch.setattr(viewer, "_get", fetch)
    evidence = viewer._fetch_one(
        tmp_path,
        source,
        tries=1,
        timeout=1,
        rate_limiter=viewer._RateLimiter(1.0, 0.0),
        report_name="[첨부정정]현금ㆍ현물배당결정",
        report_names={
            source: "[첨부정정]현금ㆍ현물배당결정",
            withdrawal: "[철회]현금ㆍ현물배당결정",
            original: "현금ㆍ현물배당결정",
        },
    )

    assert evidence.economic_body_receipt_no == withdrawal
    assert evidence.economic_classification == "NO_ECONOMIC_EVENT"
    assert evidence.common_cash_amount is None
    assert evidence.record_date is None


def test_non_attachment_current_must_be_in_main_family():
    payload = b"""
    <script>
      viewDoc("20150205900125", "4500001", "0", "0", "0", "HTML", "");
    </script>
    <select id="family">
      <option value="rcpNo=20150204900001">original</option>
    </select>
    <select id="att"></select>
    """

    with pytest.raises(RuntimeError, match="current selection is missing"):
        _parse_main_page("20150205900125", payload)


def _economic_viewer(amount="300", record_date="2022-06-30"):
    return f"""
    <html><body><table>
      <tr><td>3. 1주당 배당금(원)</td><td>보통주식</td><td>{amount}</td></tr>
      <tr><td>6. 배당기준일</td><td>{record_date}</td></tr>
    </table></body></html>
    """.encode()


def test_viewer_economic_body_requires_common_dps_and_record_date():
    assert _parse_viewer_economic_body(
        _economic_viewer(), report_name="현금ㆍ현물배당결정",
    ) == ("ECONOMIC_DECISION", 300.0, "2022-06-30")


@pytest.mark.parametrize(
    "rendered",
    ["2025년 3월 10일", "2025-3-10"],
)
def test_viewer_economic_body_normalizes_non_padded_record_date(rendered):
    assert _parse_viewer_economic_body(
        _economic_viewer(record_date=rendered),
        report_name="현금ㆍ현물배당결정",
    ) == ("ECONOMIC_DECISION", 300.0, "2025-03-10")


def test_viewer_economic_body_rejects_invalid_calendar_date():
    with pytest.raises(RuntimeError, match="record date is invalid"):
        _parse_viewer_economic_body(
            _economic_viewer(record_date="2025-02-30"),
            report_name="현금ㆍ현물배당결정",
        )


def test_intermediate_positive_without_record_date_is_preserved_pending():
    assert _parse_viewer_economic_body(
        _economic_viewer(record_date="-"),
        report_name="[기재정정]현금ㆍ현물배당결정",
    ) == ("POSITIVE_PENDING_RECORD_DATE", 300.0, None)


def test_mutable_overlap_uses_latest_explicit_coverage_end(tmp_path):
    for start, end, marker in (
        ("20150101", "20150131", "유"),
        ("20150115", "20150201", "유정"),
    ):
        _write_disclosures(
            tmp_path,
            [{
                "rcept_no": "20150102900228",
                "stock_code": "005930",
                "report_nm": "현금ㆍ현물배당결정",
                "rm": marker,
            }],
            start=start,
            end=end,
        )

    assert viewer._cash_disclosures(tmp_path)["20150102900228"]["rm"] == "유정"


def test_mutable_overlap_same_coverage_end_uses_latest_start(tmp_path):
    for start, marker in (("20150101", "유"), ("20150115", "유정")):
        _write_disclosures(
            tmp_path,
            [{
                "rcept_no": "20150102900228",
                "stock_code": "005930",
                "report_nm": "현금ㆍ현물배당결정",
                "rm": marker,
            }],
            start=start,
            end="20150131",
        )
    assert viewer._cash_disclosures(tmp_path)["20150102900228"]["rm"] == "유정"


def test_cash_disclosure_loader_rejects_legacy_only_receipt(tmp_path):
    _write_disclosures(tmp_path, [])
    interval = (
        tmp_path / "corporate_actions/dart/manifests"
        / "from=20150101" / "to=20150131"
    )
    interval.joinpath("disclosures.json").write_text(json.dumps([{
        "rcept_no": "20150102900228",
        "stock_code": "005930",
        "report_nm": "[기재정정]현금ㆍ현물배당결정",
    }]), encoding="utf-8")

    with pytest.raises(RuntimeError, match="not authenticated by v3/v5"):
        viewer._cash_disclosures(tmp_path)


def test_cash_disclosure_loader_rejects_incomplete_v3_only_receipt(tmp_path):
    _write_disclosures(
        tmp_path, [], start="20150201", end="20150228",
    )
    incomplete = (
        tmp_path / "corporate_actions/dart/manifests"
        / "from=20150101" / "to=20150131"
    )
    incomplete.mkdir(parents=True)
    incomplete.joinpath("disclosures_v3.json").write_text(json.dumps([{
        "rcept_no": "20150102900228",
        "stock_code": "005930",
        "report_nm": "[기재정정]현금ㆍ현물배당결정",
    }]), encoding="utf-8")

    with pytest.raises(RuntimeError, match="incomplete v5 intervals"):
        viewer._cash_disclosures(tmp_path)


def test_cash_disclosure_loader_rejects_v5_candidate_count_drift(tmp_path):
    receipt = "20150102900228"
    _write_disclosures(tmp_path, [{
        "rcept_no": receipt,
        "stock_code": "005930",
        "report_nm": "[기재정정]현금ㆍ현물배당결정",
    }])
    marker = (
        tmp_path / "corporate_actions/dart/manifests"
        / "from=20150101/to=20150131/documents_complete_v5.json"
    )
    payload = json.loads(marker.read_text(encoding="utf-8"))
    payload["candidate_count"] = 0
    marker.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(RuntimeError, match="interval/count mismatch"):
        viewer._cash_disclosures(tmp_path)


def test_cash_disclosure_loader_matches_producer_listed_candidate_scope(
    tmp_path,
):
    fallback = "20150102900228"
    unlisted = "20150102900229"
    direct = "20150102900230"
    _write_corp_codes(tmp_path, (("001", "005930"),))
    _write_disclosures(tmp_path, [{
        "rcept_no": fallback,
        "corp_code": "001",
        "stock_code": "",
        "report_nm": "현금ㆍ현물배당결정",
    }, {
        "rcept_no": unlisted,
        "corp_code": "002",
        "stock_code": "",
        "report_nm": "현금ㆍ현물배당결정",
    }, {
        "rcept_no": direct,
        "corp_code": "003",
        "stock_code": "000660",
        "report_nm": "현금ㆍ현물배당결정",
    }])

    disclosures = viewer._cash_disclosures(tmp_path)

    assert set(disclosures) == {fallback, direct}
    marker = json.loads((
        tmp_path / "corporate_actions/dart/manifests"
        / "from=20150101/to=20150131/documents_complete_v5.json"
    ).read_text(encoding="utf-8"))
    assert marker["candidate_count"] == 2


def _evidence(receipt, classification, *, family, amount=150.0, record=None):
    return ViewerReceiptEvidence(
        receipt_no=receipt,
        dcm_no="1",
        dtd="HTML",
        current_selector="FAMILY",
        attachment_keys=(),
        correction_of_receipt_no=(family[-2] if len(family) > 1 else None),
        revision_root_receipt_no=family[0],
        family_receipt_nos=tuple(family),
        official_family_order=tuple(reversed(family)),
        revision_kind="ECONOMIC_REVISION",
        economic_body_receipt_no=receipt,
        economic_body_dcm_no="1",
        economic_body_dtd="HTML",
        economic_main_path="main.html",
        economic_main_content_length=1,
        economic_main_sha256="a" * 64,
        economic_classification=classification,
        common_cash_amount=amount,
        record_date=record,
        main_path="main.html",
        main_content_length=1,
        main_sha256="a" * 64,
        viewer_path="viewer.html",
        viewer_content_length=1,
        viewer_sha256="b" * 64,
        economic_viewer_path="viewer.html",
        economic_viewer_content_length=1,
        economic_viewer_sha256="b" * 64,
    )


def test_incremental_manifest_seed_reuses_old_evidence_and_returns_delta(
    tmp_path,
):
    old_receipt = "20250102000001"
    new_receipt = "20250103000002"
    evidence = _evidence(
        old_receipt, "ECONOMIC_DECISION",
        family=(old_receipt,), record="2024-12-31",
    )
    payload = {
        "schema_version": viewer.SCHEMA_VERSION,
        "source_contract": viewer.SOURCE_CONTRACT,
        "seed_coverage_start": "2015-01-01",
        "seed_coverage_end": "2025-01-02",
        "complete": True,
        "seed_receipts": [old_receipt],
        "receipts": [evidence.__dict__],
        "dependency_probes": [],
    }
    manifest = tmp_path / viewer.MANIFEST_RELATIVE_PATH
    manifest.parent.mkdir(parents=True)
    manifest.write_bytes(json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
    ).encode())

    reused, probes, delta = viewer._incremental_manifest_seed(
        tmp_path,
        coverage_start=date(2015, 1, 1),
        coverage_end=date(2025, 1, 3),
        ordered_seeds=(old_receipt, new_receipt),
    )

    assert reused == [evidence]
    assert probes == []
    assert delta == {new_receipt}


def test_official_family_allows_incomplete_intermediate_when_terminal_complete():
    family = ("20250228801790", "20250304800639")
    rows = [
        _evidence(
            family[0], "POSITIVE_PENDING_RECORD_DATE", family=family,
        ),
        _evidence(
            family[1], "ECONOMIC_DECISION", family=family,
            record="2025-03-19",
        ),
    ]
    disclosures = {
        receipt: {"report_nm": "[기재정정]현금ㆍ현물배당결정"}
        for receipt in family
    }

    _validate_terminal_families(rows, disclosures)


def test_pending_family_adds_plain_terminal_as_dynamic_dependency():
    family = (
        "20250228800605", "20250228801700", "20250228801790",
        "20250304800639",
    )
    pending = _evidence(
        "20250228801790",
        "POSITIVE_PENDING_RECORD_DATE",
        family=family,
    )
    disclosures = {
        receipt: {
            "report_nm": (
                "현금ㆍ현물배당결정"
                if receipt == "20250304800639"
                else "[기재정정]현금ㆍ현물배당결정"
            )
        }
        for receipt in family
    }

    assert _pending_terminal_dependencies([pending], disclosures) == (
        "20250304800639",
    )


def test_official_family_rejects_incomplete_terminal():
    receipt = "20250228801790"
    row = _evidence(
        receipt, "POSITIVE_PENDING_RECORD_DATE", family=(receipt,),
    )

    with pytest.raises(RuntimeError, match="terminal economic revision is incomplete"):
        _validate_terminal_families(
            [row], {receipt: {"report_nm": "[기재정정]현금ㆍ현물배당결정"}},
        )


def test_fetch_one_refetches_official_selector_despite_legacy_cache(
    tmp_path, monkeypatch,
):
    receipt = "20220802900375"
    directory = (
        tmp_path / "corporate_actions" / "dart" / "viewer_corrections"
        / f"receipt={receipt}"
    )
    directory.mkdir(parents=True)
    directory.joinpath("main.html").write_bytes(f"""
    <script>
      viewDoc("{receipt}", "999", "0", "0", "0", "HTML", "");
    </script>
    <select id="family">
      <option value="rcpNo={receipt}" selected>current</option>
    </select>
    <select id="att"></select>
    """.encode())
    directory.joinpath("viewer.dtd=HTML.html").write_bytes(_economic_viewer())
    calls: list[str] = []

    def fetch(url, **_kwargs):
        calls.append(url)
        if urlparse(url).path.endswith("/main.do"):
            return directory.joinpath("main.html").read_bytes()
        return _economic_viewer()

    monkeypatch.setattr(viewer, "_get", fetch)

    evidence = viewer._fetch_one(
        tmp_path,
        receipt,
        tries=1,
        timeout=1,
        rate_limiter=viewer._RateLimiter(1.0, 0.0),
        report_name="현금ㆍ현물배당결정",
    )

    assert evidence.economic_classification == "ECONOMIC_DECISION"
    assert evidence.common_cash_amount == 300.0
    assert evidence.record_date == "2022-06-30"
    assert len(calls) == 2
    assert evidence.main_path.startswith(
        viewer.OBJECT_ROOT_RELATIVE_PATH.as_posix() + "/sha256="
    )
    assert evidence.viewer_path.startswith(
        viewer.OBJECT_ROOT_RELATIVE_PATH.as_posix() + "/sha256="
    )


def test_later_correction_refreshes_every_member_of_existing_family(
    tmp_path, monkeypatch,
):
    original = "20220101000001"
    correction_one = "20220102000002"
    correction_two = "20220103000003"
    phase = {"latest": correction_one}
    main_calls: list[str] = []

    def disclosures(include_second: bool) -> list[dict]:
        rows = [{
            "rcept_no": original,
            "stock_code": "005930",
            "report_nm": "현금ㆍ현물배당결정",
        }, {
            "rcept_no": correction_one,
            "stock_code": "005930",
            "report_nm": "[기재정정]현금ㆍ현물배당결정",
        }]
        if include_second:
            rows.append({
                "rcept_no": correction_two,
                "stock_code": "005930",
                "report_nm": "[기재정정]현금ㆍ현물배당결정",
            })
        return rows

    def main_page(receipt: str) -> bytes:
        family = (
            (correction_two, correction_one, original)
            if phase["latest"] == correction_two
            else (correction_one, original)
        )
        options = "".join(
            f'<option value="rcpNo={member}"'
            f'{" selected" if member == receipt else ""}>{member}</option>'
            for member in family
        )
        return (
            f'<script>viewDoc("{receipt}", "{int(receipt[-2:])}", '
            '"0", "0", "0", "HTML", "");</script>'
            f'<select id="family">{options}</select>'
            '<select id="att"></select>'
        ).encode()

    def fetch(url, **_kwargs):
        parsed = urlparse(url)
        receipt = parse_qs(parsed.query)["rcpNo"][0]
        if parsed.path.endswith("/main.do"):
            main_calls.append(receipt)
            return main_page(receipt)
        return _economic_viewer()

    monkeypatch.setattr(viewer, "_get", fetch)
    _write_disclosures(tmp_path, disclosures(False))
    first = _collect(
        str(tmp_path), apply=True, workers=1,
        request_interval_seconds=0.001,
    )
    assert first.receipts[0].official_family_order == (
        correction_one, original,
    )

    phase["latest"] = correction_two
    _write_disclosures(tmp_path, disclosures(True))
    main_calls.clear()
    second = _collect(
        str(tmp_path), apply=True, workers=1,
        request_interval_seconds=0.001,
    )

    assert main_calls == [correction_one, correction_two]
    assert {item.official_family_order for item in second.receipts} == {
        (correction_two, correction_one, original),
    }


def test_postcoverage_terminal_closes_seed_family_without_becoming_seed(
    tmp_path, monkeypatch,
):
    root = "20260701000001"
    seed_correction = "20260801000002"
    terminal = "20260812000003"
    rows = [
        {
            "rcept_no": root,
            "rcept_dt": "20260701",
            "stock_code": "005930",
            "report_nm": "현금ㆍ현물배당결정",
        },
        {
            "rcept_no": seed_correction,
            "rcept_dt": "20260801",
            "stock_code": "005930",
            "report_nm": "[기재정정]현금ㆍ현물배당결정",
        },
        {
            "rcept_no": terminal,
            "rcept_dt": "20260812",
            "stock_code": "005930",
            "report_nm": "[기재정정]현금ㆍ현물배당결정",
        },
    ]
    _write_disclosures(
        tmp_path, rows[:2], start="20260701", end="20260801",
    )
    _write_disclosures(
        tmp_path, rows[2:], start="20260812", end="20260812",
    )
    family = (terminal, seed_correction, root)

    def main_page(receipt: str) -> bytes:
        options = "".join(
            f'<option value="rcpNo={member}"'
            f'{" selected" if member == receipt else ""}>{member}</option>'
            for member in family
        )
        dcm = str(1_000_000 + int(receipt[-6:]))
        return (
            f'<script>viewDoc("{receipt}", "{dcm}", "0", "0", "0", '
            f'"HTML", "");</script><select id="family">{options}</select>'
            '<select id="att"></select>'
        ).encode()

    def fetch(url, **_kwargs):
        parsed = urlparse(url)
        receipt = parse_qs(parsed.query)["rcpNo"][0]
        if parsed.path.endswith("/main.do"):
            return main_page(receipt)
        return _economic_viewer(amount="300", record_date="2026-12-31")

    monkeypatch.setattr(viewer, "_get", fetch)
    coverage_end = date(2026, 8, 10)

    required = _required(str(tmp_path), coverage_end=coverage_end)
    verified = _collect(
        str(tmp_path),
        coverage_end=coverage_end,
        apply=True,
        workers=1,
        request_interval_seconds=0.001,
    )

    assert required == (seed_correction,)
    assert verified.seed_coverage_end == coverage_end
    assert {item.receipt_no for item in verified.receipts} == {
        seed_correction, terminal,
    }
    terminal_rows = [
        item for item in verified.receipts
        if item.economic_body_receipt_no == terminal
    ]
    assert len(terminal_rows) == 1
    assert all(item.common_cash_amount == 300 for item in terminal_rows)
    assert _verify(
        str(tmp_path), required_end=coverage_end,
    ) == verified
    with pytest.raises(RuntimeError, match="seed coverage mismatch"):
        _verify(str(tmp_path), required_end=date(2026, 8, 11))


def test_postcoverage_first_correction_is_exact_dependency_not_seed(
    tmp_path, monkeypatch,
):
    linked_root = "20260715800495"
    linked_correction = "20260812800390"
    unrelated_root = "20260811000001"
    unrelated_correction = "20260812000002"
    _write_disclosures(tmp_path, [{
        "rcept_no": linked_root,
        "rcept_dt": "20260715",
        "stock_code": "018670",
        "report_nm": "현금ㆍ현물배당결정",
    }], start="20260715", end="20260715")
    _write_disclosures(tmp_path, [{
        "rcept_no": linked_correction,
        "rcept_dt": "20260812",
        "stock_code": "018670",
        "report_nm": "[기재정정]현금ㆍ현물배당결정",
    }, {
        "rcept_no": unrelated_root,
        "rcept_dt": "20260811",
        "stock_code": "000660",
        "report_nm": "현금ㆍ현물배당결정",
    }, {
        "rcept_no": unrelated_correction,
        "rcept_dt": "20260812",
        "stock_code": "000660",
        "report_nm": "[기재정정]현금ㆍ현물배당결정",
    }], start="20260811", end="20260812")
    families = {
        linked_correction: (linked_correction, linked_root),
        unrelated_correction: (unrelated_correction, unrelated_root),
    }
    main_calls: list[str] = []

    def main_page(receipt: str) -> bytes:
        family = families[receipt]
        options = "".join(
            f'<option value="rcpNo={member}"'
            f'{" selected" if member == receipt else ""}>{member}</option>'
            for member in family
        )
        return (
            f'<script>viewDoc("{receipt}", "1234567", "0", "0", "0", '
            f'"HTML", "");</script><select id="family">{options}</select>'
            '<select id="att"></select>'
        ).encode()

    def fetch(url, **_kwargs):
        parsed = urlparse(url)
        receipt = parse_qs(parsed.query)["rcpNo"][0]
        if parsed.path.endswith("/main.do"):
            main_calls.append(receipt)
            return main_page(receipt)
        assert receipt == linked_correction
        return _economic_viewer(amount="325", record_date="2026-12-31")

    monkeypatch.setattr(viewer, "_get", fetch)
    coverage_end = date(2026, 8, 10)

    preview = _collect(
        str(tmp_path), coverage_end=coverage_end, apply=False,
    )
    verified = _collect(
        str(tmp_path), coverage_end=coverage_end, apply=True, workers=1,
        request_interval_seconds=0.001,
    )

    assert preview["seed_receipts"] == []
    assert preview["provisional_outside_candidates"] == sorted([
        linked_correction, unrelated_correction,
    ])
    assert sorted(main_calls) == sorted([
        linked_correction, unrelated_correction,
    ])
    assert verified.seed_receipt_count == 0
    assert verified.dependency_receipt_count == 1
    assert [item.receipt_no for item in verified.receipts] == [
        linked_correction,
    ]
    manifest = json.loads(Path(verified.manifest_path).read_text())
    assert manifest["seed_receipts"] == []
    assert manifest["dependency_receipts"] == [linked_correction]
    assert unrelated_correction not in manifest["required_receipts"]

    # An unrelated result is still a required official-selector probe: without
    # it an offline verifier could not distinguish exclusion from omission.
    manifest_path = Path(verified.manifest_path)
    manifest["dependency_probes"] = [
        row for row in manifest["dependency_probes"]
        if row["receipt_no"] != unrelated_correction
    ]
    manifest["dependency_probe_count"] = 1
    only_probe = viewer.ViewerDependencyProbe(**{
        **manifest["dependency_probes"][0],
        "family_receipt_nos": tuple(
            manifest["dependency_probes"][0]["family_receipt_nos"]
        ),
        "attachment_keys": tuple(
            manifest["dependency_probes"][0]["attachment_keys"]
        ),
    })
    manifest["dependency_probe_digest"] = viewer._dependency_probe_digest(
        [only_probe]
    )
    manifest_path.write_bytes(json.dumps(
        manifest,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode())
    with pytest.raises(RuntimeError, match="probe candidate set changed"):
        _verify(str(tmp_path), required_end=coverage_end)


def test_required_receipts_include_all_issuer_corrections_but_not_subsidiary(
    tmp_path,
):
    _write_disclosures(tmp_path, [
        {
            "rcept_no": "20240102800001",
            "stock_code": "005930",
            "report_nm": "[기재정정]현금ㆍ현물배당결정",
        },
        {
            "rcept_no": "20240102800002",
            "stock_code": "005930",
            "report_nm": "[기재정정]현금ㆍ현물배당결정(자회사의 주요경영사항)",
        },
        {
            "rcept_no": "20240102800003",
            "stock_code": "005930",
            "report_nm": "현금ㆍ현물배당결정",
        },
    ])

    assert _required(str(tmp_path)) == ("20240102800001",)


def test_unavailable_body_must_really_be_status_014(tmp_path):
    receipt = "20240102800001"
    _write_disclosures(tmp_path, [{
        "rcept_no": receipt,
        "stock_code": "005930",
        "report_nm": "[첨부정정]현금ㆍ현물배당결정",
    }])
    unavailable = (
        tmp_path / "corporate_actions" / "dart" / "documents_unavailable"
        / "year=2024" / "corp=000001" / f"rcept={receipt}.xml"
    )
    unavailable.parent.mkdir(parents=True)
    unavailable.write_text("<result><status>999</status></result>")

    with pytest.raises(RuntimeError, match="not status 014"):
        _required(str(tmp_path))


def test_main_serializes_verified_coverage_dates(monkeypatch, capsys):
    verified = viewer.VerifiedViewerCorrectionSnapshot(
        base="/snapshot",
        manifest_path="/snapshot/manifest.json",
        manifest_sha256="a" * 64,
        seed_coverage_start=date(2015, 1, 1),
        seed_coverage_end=date(2026, 8, 10),
        seed_receipt_count=0,
        seed_receipt_digest="b" * 64,
        dependency_receipt_count=0,
        dependency_receipt_digest="c" * 64,
        dependency_probe_count=0,
        dependency_probe_digest="d" * 64,
        receipt_count=0,
        receipt_digest="e" * 64,
        dependency_probes=(),
        receipts=(),
    )
    monkeypatch.setattr(
        viewer, "collect_viewer_corrections", lambda *_args, **_kwargs: verified,
    )
    monkeypatch.setattr(sys, "argv", [
        "dart_viewer_corrections",
        "--base", "/snapshot",
        "--coverage-start", "2015-01-01",
        "--coverage-end", "2026-08-10",
        "--apply",
    ])

    viewer.main()

    output = json.loads(capsys.readouterr().out)
    assert output["seed_coverage_start"] == "2015-01-01"
    assert output["seed_coverage_end"] == "2026-08-10"
