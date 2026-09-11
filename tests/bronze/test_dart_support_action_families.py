import json
from datetime import date
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pytest

from pipeline.bronze import dart_support_action_families as families
from pipeline.bronze import financials


TEST_COVERAGE_START = date(2015, 1, 1)
TEST_COVERAGE_END = date(2099, 12, 31)


def _collect(base, **kwargs):
    kwargs.setdefault("coverage_start", TEST_COVERAGE_START)
    kwargs.setdefault("coverage_end", TEST_COVERAGE_END)
    return families.collect_support_action_families(base, **kwargs)


def _verify(base, **kwargs):
    kwargs.setdefault("required_start", TEST_COVERAGE_START)
    kwargs.setdefault("required_end", TEST_COVERAGE_END)
    return families.verify_support_action_families(base, **kwargs)


def _external(base, **kwargs):
    kwargs.setdefault("required_start", TEST_COVERAGE_START)
    kwargs.setdefault("required_end", TEST_COVERAGE_END)
    return families.external_evidence_paths(base, **kwargs)


def _snapshot(base, **kwargs):
    kwargs.setdefault("coverage_start", TEST_COVERAGE_START)
    kwargs.setdefault("coverage_end", TEST_COVERAGE_END)
    return families._load_fresh_snapshot(base, **kwargs)


def _dart_fixture(name: str) -> Path:
    return Path(__file__).parents[1] / "fixtures" / "dart" / name


def _write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, ensure_ascii=False, separators=(",", ":")),
        encoding="utf-8",
    )


def _write_corp_codes(root: Path, pairs=()) -> None:
    entries = "".join(
        "<list><corp_code>" + corp + "</corp_code><stock_code>"
        + ticker + "</stock_code></list>"
        for corp, ticker in pairs
    )
    path = root / financials.CORPCODE_BRONZE_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"<result>{entries}</result>", encoding="utf-8")


def _write_interval(
    root: Path,
    rows: list[dict],
    *,
    start: str = "20211201",
    end: str = "20211231",
    marker_version: int = 5,
) -> None:
    corp_code_path = root / financials.CORPCODE_BRONZE_PATH
    if not corp_code_path.is_file():
        _write_corp_codes(root)
    corp_to_stock = dict(financials.load_listed_corps_from_bronze(str(root)))
    interval = (
        root / "corporate_actions" / "dart" / "manifests"
        / f"from={start}" / f"to={end}"
    )
    _write_json(interval / "disclosures_v3.json", rows)
    marker = {"status": "COMPLETE", "fromdate": start, "todate": end}
    structured_queries = families._structured_query_keys(rows, corp_to_stock)
    document_candidates = families._document_candidate_receipts(
        rows, corp_to_stock,
    )
    _write_json(
        interval / "structured_complete_v3.json",
        {**marker, "query_count": len(structured_queries)},
    )
    _write_json(
        interval / f"documents_complete_v{marker_version}.json",
        {**marker, "candidate_count": len(document_candidates)},
    )
    for row in rows:
        receipt = row["rcept_no"]
        ticker = families._listed_disclosure_ticker(
            row, corp_to_stock,
        ).zfill(6)
        if receipt not in document_candidates:
            continue
        rendered_date = (
            f"{row['rcept_dt'][:4]}-{row['rcept_dt'][4:6]}-"
            f"{row['rcept_dt'][6:8]}"
        )
        _write_json(
            root / "corporate_actions" / "dart" / "disclosures"
            / f"year={receipt[:4]}" / f"date={rendered_date}"
            / f"corp={ticker}" / f"rcept={receipt}.json",
            row,
        )


def _stock_row(
    receipt: str,
    *,
    ticker: str = "090150",
    report: str = "주식배당결정",
    **extra,
) -> dict:
    return {
        "rcept_no": receipt,
        "rcept_dt": receipt[:8],
        "stock_code": ticker,
        "corp_code": f"corp-{ticker}",
        "report_nm": report,
        **extra,
    }


def _bonus_row(
    receipt: str,
    *,
    ticker: str = "090150",
    report: str = "주요사항보고서(무상증자결정)",
    **extra,
) -> dict:
    return _stock_row(
        receipt, ticker=ticker, report=report, **extra,
    )


def _write_bonus_structured(
    root: Path,
    receipt: str,
    *,
    ticker: str = "090150",
    ratio: str = "1.0",
    effective: str = "20211229",
) -> Path:
    path = (
        root / "corporate_actions" / "dart" / "structured"
        / "event=bonus_issue" / f"year={receipt[:4]}"
        / f"corp={ticker}" / f"rcept={receipt}.json"
    )
    _write_json(path, {
        "rcept_no": receipt,
        "nstk_ascnt_ps_ostk": ratio,
        "nstk_asstd": effective,
    })
    return path


def _stock_body(ratio: str = "0.1", *, origin: str | None = None) -> bytes:
    correction = (
        f"<p>정정 관련 공시서류 제출일 : {origin}</p>" if origin else ""
    )
    return (
        "<html><body>" + correction
        + "<table><tr><td>배당기준일</td><td>2021-12-31</td></tr>"
        + "<tr><td>1. 1주당 배당주식수 (주)</td><td>보통주식</td>"
        + f"<td>{ratio}</td></tr>"
        + "</table></body></html>"
    ).encode()


def _bonus_body(ratio: str = "0.1", *, record_date: str = "2021-12-31") -> bytes:
    return (
        "<html><body><table>"
        "<tr><td>1. 신주의 종류와 수</td><td>보통주식 (주)</td>"
        "<td>9,999,999</td></tr>"
        f"<tr><td>4. 신주배정기준일</td><td>{record_date}</td></tr>"
        "<tr><td>5. 1주당 신주배정 주식수</td>"
        f"<td>보통주식 (주)</td><td>{ratio}</td></tr>"
        "<tr><td>5. 1주당 신주배정 주식수</td>"
        "<td>기타주식 (주)</td><td>7.5</td></tr>"
        "</table></body></html>"
    ).encode()


class _Remote:
    def __init__(
        self,
        families_by_receipt: dict[str, tuple[str, ...]],
        reports: dict[str, str],
        bodies: dict[str, bytes],
        *,
        attachments_by_receipt: dict[
            str, tuple[tuple[str, str], ...]
        ] | None = None,
        dcms: dict[str, str] | None = None,
        dtds: dict[str, str] | None = None,
    ) -> None:
        self.families_by_receipt = families_by_receipt
        self.reports = reports
        self.bodies = bodies
        self.attachments_by_receipt = attachments_by_receipt or {}
        self.dcms = dcms or {}
        self.dtds = dtds or {}
        self.calls: list[str] = []

    def dcm(self, receipt: str) -> str:
        return self.dcms.get(
            receipt, str(1_000_000 + int(receipt[-6:])),
        )

    def dtd(self, receipt: str) -> str:
        return self.dtds.get(receipt, "HTML")

    def main(self, receipt: str) -> bytes:
        order = self.families_by_receipt[receipt]
        attachment_options = self.attachments_by_receipt.get(receipt, ())
        attachment_current = any(
            member == receipt and dcm == self.dcm(receipt)
            for member, dcm in attachment_options
        ) and receipt not in order
        options = []
        for member in order:
            selected = (
                " selected"
                if member == receipt and not attachment_current else ""
            )
            options.append(
                f'<option value="rcpNo={member}"{selected} '
                f'title="{self.reports[member]}">'
                f'{self.reports[member]}</option>'
            )
        attachments = []
        for member, dcm in attachment_options:
            selected = (
                " selected"
                if attachment_current
                and member == receipt
                and dcm == self.dcm(receipt)
                else ""
            )
            attachments.append(
                f'<option value="rcpNo={member}&amp;dcmNo={dcm}"{selected}>'
                f'attachment {member}</option>'
            )
        return (
            f'<script>viewDoc("{receipt}", "{self.dcm(receipt)}", '
            f'"0", "0", "0", "{self.dtd(receipt)}", "");</script>'
            '<select id="family"><option value="null">select</option>'
            + "".join(options)
            + '</select><select id="att"><option value="null">attach</option>'
            + "".join(attachments)
            + "</select>"
        ).encode()

    def __call__(self, url: str) -> bytes:
        self.calls.append(url)
        parsed = urlparse(url)
        query = parse_qs(parsed.query)
        receipt = query["rcpNo"][0]
        if parsed.path.endswith("/main.do"):
            return self.main(receipt)
        assert query["dcmNo"] == [self.dcm(receipt)]
        assert query["dtd"] == [self.dtd(receipt)]
        return self.bodies[receipt]


def _stock_fixture(root: Path):
    original = "20211217000001"
    correction = "20211224000002"
    rows = [
        _stock_row(original),
        _stock_row(correction, report="[기재정정] 주식배당결정"),
    ]
    _write_interval(root, rows)
    order = (correction, original)
    remote = _Remote(
        {receipt: order for receipt in order},
        {row["rcept_no"]: row["report_nm"] for row in rows},
        {
            original: _stock_body("0.1"),
            correction: _stock_body("0.2", origin="2021년 12월 17일"),
        },
    )
    return original, correction, remote


def test_real_006800_labelled_row_rejects_adjacent_share_count_traps():
    fixture = _dart_fixture("20260313800897-stock-dividend-table.html")

    ratio = families.stock_dividend_common_ratio_from_body(
        fixture.read_bytes(),
    )

    assert ratio == pytest.approx(0.0073206, rel=0, abs=0)
    assert ratio not in {4_250_472.0, 555_316_408.0, 150_575_750.0}


def test_complete_current_manifest_is_atomically_refreshed_for_new_interval(
    tmp_path,
):
    original, correction, remote = _stock_fixture(tmp_path)
    first = _collect(
        tmp_path, apply=True, fetcher=remote,
    )
    manifest = tmp_path / families.MANIFEST_RELATIVE_PATH
    first_bytes = manifest.read_bytes()

    # A later complete collector interval can observe the same immutable
    # receipts again.  The current manifest must bind that newer canonical
    # observation instead of permanently blocking every subsequent daily run.
    _write_interval(
        tmp_path,
        [
            _stock_row(original),
            _stock_row(correction, report="[기재정정] 주식배당결정"),
        ],
        start="20211215",
        end="20220115",
    )
    refreshed = _collect(
        tmp_path, apply=True, fetcher=remote,
    )

    assert refreshed.candidate_count == first.candidate_count
    assert manifest.read_bytes() != first_bytes
    assert _verify(tmp_path) == refreshed


def test_coverage_extension_reuses_unchanged_support_families(tmp_path):
    _, _, remote = _stock_fixture(tmp_path)
    first_end = date(2021, 12, 31)
    second_end = date(2022, 1, 31)
    first = _collect(
        tmp_path, coverage_end=first_end, apply=True, fetcher=remote,
    )
    first_call_count = len(remote.calls)

    second = _collect(
        tmp_path, coverage_end=second_end, apply=True, fetcher=remote,
    )

    assert second.entries == first.entries
    assert len(remote.calls) == first_call_count
    assert _verify(tmp_path, required_end=second_end) == second


def test_stock_ratio_missing_or_preferred_only_fails_closed():
    no_ratio = b"""
      <table>
        <tr><td>2. total dividend shares</td><td>ordinary</td><td>4250472</td></tr>
        <tr><td>3. issued shares</td><td>ordinary</td><td>555316408</td></tr>
      </table>
    """
    preferred_only = """
      <table><tr><td>1. 1주당 배당주식수 (주)</td>
      <td>종류주식</td><td>0.8</td></tr></table>
    """.encode()

    assert families.stock_dividend_common_ratio_from_body(no_ratio) is None
    assert (
        families.stock_dividend_common_ratio_from_body(preferred_only) is None
    )


def test_stock_ratio_ambiguous_exact_ordinary_rows_fail_closed():
    body = """
      <table>
        <tr><td>1. 1주당 배당주식수 (주)</td>
        <td>보통주식</td><td>0.1</td></tr>
        <tr><td>1. 1주당 배당주식수 (주)</td>
        <td>보통주식</td><td>0.2</td></tr>
      </table>
    """.encode()

    with pytest.raises(RuntimeError, match="ratio is ambiguous"):
        families.stock_dividend_common_ratio_from_body(body)


@pytest.mark.parametrize(
    ("ratio", "record_date"),
    [
        ("0.02", "2017년 01월 01일"),  # actual 001060 table shape
        ("1", "2017년 10월 01일"),  # actual 060560 correction shape
    ],
)
def test_bonus_common_terms_parse_exact_actual_table_shapes(
    ratio, record_date,
):
    body = _bonus_body(ratio, record_date=record_date)

    terms = families.bonus_issue_common_terms_from_body(body)

    assert terms == families.BonusIssueCommonTerms(
        common_ratio=float(ratio),
        record_date=families.parse_dart_date(record_date),
    )


def test_bonus_common_terms_ignore_numeric_and_other_security_traps():
    terms = families.bonus_issue_common_terms_from_body(
        _bonus_body("0.15", record_date="2015년 04월 09일")
    )

    assert terms == families.BonusIssueCommonTerms(
        common_ratio=0.15,
        record_date="2015-04-09",
    )
    assert terms.common_ratio not in {7.5, 9_999_999}


@pytest.mark.parametrize(
    "body",
    [
        b"<html><body>no labelled bonus terms</body></html>",
        (
            "<table><tr><td>5. 1주당 신주배정 주식수</td>"
            "<td>기타주식 (주)</td><td>0.8</td></tr></table>"
        ).encode(),
    ],
)
def test_bonus_common_terms_return_none_without_exact_common_terms(body):
    assert families.bonus_issue_common_terms_from_body(body) is None


@pytest.mark.parametrize(
    "body",
    [
        (
            "<table><tr><td>5. 1주당 신주배정 주식수</td>"
            "<td>보통주식 (주)</td><td>0.1</td></tr></table>"
        ).encode(),
        _bonus_body("0.1") + _bonus_body("0.2"),
        _bonus_body("0.1") + _bonus_body("0.1", record_date="2022-01-01"),
    ],
)
def test_bonus_common_terms_partial_or_ambiguous_rows_fail_closed(body):
    with pytest.raises(RuntimeError, match="missing/ambiguous"):
        families.bonus_issue_common_terms_from_body(body)


@pytest.mark.parametrize(
    ("rendered", "expected"),
    [
        ("20241209", "2024-12-09"),
        ("2024년 12월 9일", "2024-12-09"),
        ("2024년 7월 1일", "2024-07-01"),
        ("2024-7-1", "2024-07-01"),
        ("2024.07.01", "2024-07-01"),
    ],
)
def test_dart_date_normalizes_compact_and_non_padded_dates(
    rendered, expected,
):
    assert families.parse_dart_date(rendered) == expected


@pytest.mark.parametrize("rendered", ["2025-02-30", "202471", "date=2024-7-1"])
def test_dart_date_rejects_invalid_or_non_exact_values(rendered):
    assert families.parse_dart_date(rendered) is None


def test_correction_origin_accepts_actual_korean_non_padded_date_shape():
    body = """
      <table><tr><td>최초 제출일</td><td>2024년 12월 9일</td></tr></table>
    """.encode()
    assert families._correction_origin_date(body) == "2024-12-09"


def test_correction_origin_rejects_invalid_calendar_date():
    body = """
      <table><tr><td>최초 제출일</td><td>2025년 2월 30일</td></tr></table>
    """.encode()
    with pytest.raises(RuntimeError, match="missing/ambiguous"):
        families._correction_origin_date(body)


def test_live_attachment_main_binds_family_att_dcm_and_dtd_exactly():
    attachment = families.parse_official_dart_main_page(
        "20241209000064",
        _dart_fixture("20241209000064-attachment-main.html").read_bytes(),
        expected_attachment_only=True,
    )
    root = families.parse_official_dart_main_page(
        "20241209000042",
        _dart_fixture("20241209000042-family-main.html").read_bytes(),
        expected_attachment_only=False,
    )

    assert attachment.current_selector == "ATTACHMENT"
    assert attachment.family_receipts == ("20241209000042",)
    assert attachment.family_root_receipt_no == "20241209000042"
    assert attachment.dcm_no == "10216564"
    assert attachment.dtd == "dart4.xsd"
    assert attachment.attachment_keys == (
        "20241209000064:10216564",
        "20241209000042:10216522",
        "20241209000042:10216523",
    )
    assert root.current_selector == "FAMILY"
    assert root.family_receipts == attachment.family_receipts
    assert root.attachment_keys == attachment.attachment_keys
    assert root.dtd == "dart4.xsd"
    assert "dtd=dart4.xsd" in families.official_dart_viewer_url(
        attachment.receipt_no, attachment.dcm_no, attachment.dtd,
    )


def test_attachment_bonus_end_to_end_uses_official_dtd_and_no_fake_origin(
    tmp_path,
):
    root_receipt = "20241209000042"
    attachment_receipt = "20241209000064"
    rows = [
        _bonus_row(root_receipt),
        _bonus_row(
            attachment_receipt,
            report="[첨부정정]주요사항보고서(무상증자결정)",
        ),
    ]
    _write_interval(
        tmp_path, rows, start="20241201", end="20241231",
    )
    _write_bonus_structured(
        tmp_path, root_receipt, ratio="0.25", effective="20241231",
    )
    mains = {
        root_receipt: _dart_fixture(
            "20241209000042-family-main.html"
        ).read_bytes(),
        attachment_receipt: _dart_fixture(
            "20241209000064-attachment-main.html"
        ).read_bytes(),
    }
    attachment_body = _dart_fixture(
        "20241209000064-attachment-dart4.html"
    ).read_bytes()
    calls: list[str] = []

    def fetch(url: str) -> bytes:
        calls.append(url)
        parsed = urlparse(url)
        query = parse_qs(parsed.query)
        receipt = query["rcpNo"][0]
        if parsed.path.endswith("/main.do"):
            return mains[receipt]
        assert query["dtd"] == ["dart4.xsd"]
        if receipt == attachment_receipt:
            return attachment_body
        return b"<html><body>official bonus decision</body></html>"

    verified = _collect(
        tmp_path, apply=True, fetcher=fetch,
    )

    entry = verified.entries[0]
    assert entry.ordered_family_receipts == (
        attachment_receipt, root_receipt,
    )
    assert entry.root_receipt_no == root_receipt
    assert entry.terminal_receipt_no == attachment_receipt
    assert entry.terminal_economic_receipt_no == root_receipt
    assert entry.terminal_status == "ACTIVE"
    assert entry.terminal_ratio == pytest.approx(0.25)
    attachment_source = entry.sources[0]
    assert attachment_source.current_selector == "ATTACHMENT"
    assert attachment_source.correction_origin_date is None
    assert attachment_source.correction_of_receipt_no is None
    assert (
        attachment_source.attachment_family_root_receipt_no == root_receipt
    )
    assert attachment_source.dtd == "dart4.xsd"
    assert attachment_source.body_sha256 == families._sha256_bytes(
        attachment_body
    )
    assert all("dtd=HTML" not in url for url in calls)


@pytest.mark.parametrize("corruption", ["dual_selected", "dcm", "dtd"])
def test_attachment_main_corruption_fails_closed(corruption):
    receipt = "20241209000064"
    rendered = _dart_fixture(
        "20241209000064-attachment-main.html"
    ).read_text(encoding="utf-8")
    if corruption == "dual_selected":
        rendered = rendered.replace(
            'value="rcpNo=20241209000042" title=',
            'value="rcpNo=20241209000042" selected title=',
        )
    elif corruption == "dcm":
        rendered = rendered.replace(
            "dcmNo=10216564\" selected", "dcmNo=10216565\" selected",
        )
    else:
        rendered = rendered.replace('"dart4.xsd", ""', '"", ""')

    with pytest.raises(RuntimeError):
        families.parse_official_dart_main_page(
            receipt,
            rendered.encode(),
            expected_attachment_only=True,
        )


def test_family_pages_with_different_attachment_keys_fail_closed(tmp_path):
    root_receipt = "20241209000042"
    attachment_receipt = "20241209000064"
    rows = [
        _bonus_row(root_receipt),
        _bonus_row(
            attachment_receipt,
            report="[첨부정정]주요사항보고서(무상증자결정)",
        ),
    ]
    _write_interval(
        tmp_path, rows, start="20241201", end="20241231",
    )
    _write_bonus_structured(tmp_path, root_receipt, ratio="0.25")
    attachment_main = _dart_fixture(
        "20241209000064-attachment-main.html"
    ).read_bytes()
    root_main = _dart_fixture(
        "20241209000042-family-main.html"
    ).read_text(encoding="utf-8").replace(
        '<option value="rcpNo=20241209000042&amp;dcmNo=10216523">'
        '이사회의사록등증빙서류</option>',
        "",
    ).encode()

    def fetch(url: str) -> bytes:
        parsed = urlparse(url)
        query = parse_qs(parsed.query)
        receipt = query["rcpNo"][0]
        if parsed.path.endswith("/main.do"):
            return (
                attachment_main if receipt == attachment_receipt else root_main
            )
        return b"<html><body>body</body></html>"

    with pytest.raises(RuntimeError, match="attachment selectors disagree"):
        _collect(
            tmp_path, apply=True, fetcher=fetch,
        )


def test_dry_run_has_no_network_or_writes(tmp_path):
    _, _, remote = _stock_fixture(tmp_path)

    preview = _collect(
        tmp_path, apply=False, fetcher=remote,
    )

    assert preview["status"] == "PREVIEW"
    assert preview["candidate_count"] == 2
    assert preview["network_requests_made"] == 0
    assert preview["writes_made"] == 0
    assert remote.calls == []
    assert not (tmp_path / families.MANIFEST_RELATIVE_PATH).exists()
    assert not (tmp_path / families.OBJECT_ROOT_RELATIVE_PATH).exists()


def test_fresh_snapshot_includes_corpcode_fallback_and_excludes_unlisted(
    tmp_path,
):
    fallback = _stock_row(
        "20211201000001", ticker="", corp_code="listed-corp",
    )
    unlisted = _stock_row(
        "20211202000002", ticker="", corp_code="unlisted-corp",
    )
    direct = _stock_row("20211203000003", ticker="000660")
    _write_corp_codes(tmp_path, (("listed-corp", "005930"),))
    _write_interval(tmp_path, [fallback, unlisted, direct])

    snapshot = _snapshot(tmp_path)

    assert set(snapshot.disclosures) == {
        fallback["rcept_no"], direct["rcept_no"],
    }
    assert snapshot.disclosures[fallback["rcept_no"]].ticker == "005930"
    assert set(snapshot.candidates) == {
        fallback["rcept_no"], direct["rcept_no"],
    }


def test_collects_official_family_and_roundtrip_verifies(tmp_path):
    original, correction, remote = _stock_fixture(tmp_path)

    verified = _collect(
        tmp_path, apply=True, fetcher=remote,
    )

    assert verified.candidate_count == 2
    assert verified.entry_count == 1
    entry = verified.entries[0]
    assert entry.ticker == "090150"
    assert entry.action_type == "stock_dividend"
    assert entry.root_receipt_no == original
    assert entry.terminal_receipt_no == correction
    assert entry.terminal_economic_receipt_no == correction
    assert entry.ordered_family_receipts == (correction, original)
    assert entry.original_submission_date == "2021-12-17"
    assert entry.terminal_status == "ACTIVE"
    assert entry.terminal_admissible
    assert entry.terminal_ratio == pytest.approx(0.2)
    assert entry.sources[0].correction_of_receipt_no == original
    assert entry.sources[0].correction_origin_date == "2021-12-17"
    assert entry.sources[1].revision_kind == "ORIGINAL"
    assert all(
        source.main_path.startswith(
            families.OBJECT_ROOT_RELATIVE_PATH.as_posix()
        )
        and source.body_path.startswith(
            families.OBJECT_ROOT_RELATIVE_PATH.as_posix()
        )
        for source in entry.sources
    )
    assert _verify(tmp_path) == verified
    external = _external(tmp_path)
    assert families.MANIFEST_RELATIVE_PATH.as_posix() in external
    assert entry.sources[0].main_path in external
    assert entry.sources[0].body_path in external


def test_precoverage_root_is_exact_source_but_not_candidate_coverage(
    tmp_path,
):
    root_receipt = "20141203800367"
    unrelated_bonus = "20141203000012"
    correction = "20150312800008"
    root_row = _stock_row(root_receipt, ticker="037620")
    unrelated_row = _bonus_row(unrelated_bonus, ticker="034590")
    correction_row = _stock_row(
        correction,
        ticker="037620",
        report="[기재정정]주식배당결정",
    )
    _write_interval(
        tmp_path,
        [unrelated_row, root_row],
        start="20141203",
        end="20141203",
    )
    _write_interval(
        tmp_path,
        [correction_row],
        start="20150312",
        end="20150312",
    )
    family = (correction, root_receipt)
    remote = _Remote(
        {receipt: family for receipt in family},
        {
            root_receipt: root_row["report_nm"],
            correction: correction_row["report_nm"],
        },
        {
            root_receipt: _stock_body("0.05"),
            correction: _stock_body("0.06", origin="2014-12-03"),
        },
    )

    snapshot = _snapshot(tmp_path)
    verified = _collect(
        tmp_path, apply=True, fetcher=remote,
    )

    # The complete 2014 list row is retained as exact lineage provenance, but
    # neither it nor an unrelated 2014 bonus decision enlarges the certified
    # candidate set.  Only the 2015 correction can seed the official family.
    assert set(snapshot.disclosures) == {
        unrelated_bonus, root_receipt, correction,
    }
    assert snapshot.candidates == {
        correction: ("037620", "stock_dividend"),
    }
    assert verified.candidate_count == 1
    assert verified.entry_count == 1
    entry = verified.entries[0]
    assert entry.ordered_family_receipts == family
    assert entry.root_receipt_no == root_receipt
    assert entry.terminal_receipt_no == correction
    root_source = entry.sources[1]
    assert root_source.receipt_no == root_receipt
    assert root_source.receipt_date == "2014-12-03"
    assert root_source.disclosure_manifest_path.endswith(
        "from=20141203/to=20141203/disclosures_v3.json"
    )
    assert root_source.disclosure_path.endswith(
        "year=2014/date=2014-12-03/corp=037620/"
        f"rcept={root_receipt}.json"
    )
    assert all(unrelated_bonus not in call for call in remote.calls)
    assert _verify(tmp_path) == verified


def test_receipt_number_prefix_is_not_assumed_to_be_list_acceptance_date(
    tmp_path,
):
    # Real OpenDART identity shape: this receipt's public list date is the next
    # calendar day.  The receipt remains the opaque selector identity while
    # the exact rcept_dt controls its individual disclosure path/provenance.
    receipt = "20170110000774"
    row = _bonus_row(receipt, ticker="071280", rcept_dt="20170111")
    _write_interval(
        tmp_path, [row], start="20170111", end="20170111",
    )
    structured = _write_bonus_structured(
        tmp_path, receipt, ticker="071280", ratio="0.5",
    )
    remote = _Remote(
        {receipt: (receipt,)},
        {receipt: row["report_nm"]},
        {receipt: b"<html><body>official bonus decision</body></html>"},
    )

    verified = _collect(
        tmp_path, apply=True, fetcher=remote,
    )

    source = verified.entries[0].sources[0]
    assert source.receipt_no == receipt
    assert source.receipt_date == "2017-01-11"
    assert source.current_selector == "FAMILY"
    assert source.main_family_receipts == (receipt,)
    assert source.disclosure_row_sha256 == families._row_digest(row)
    assert source.disclosure_path.endswith(
        f"year=2017/date=2017-01-11/corp=071280/rcept={receipt}.json"
    )
    assert source.structured_path == structured.relative_to(
        tmp_path
    ).as_posix()
    assert _verify(tmp_path) == verified


def test_candidate_floor_uses_fresh_list_date_not_receipt_prefix(tmp_path):
    coverage_candidate = "20141231000001"
    precoverage_dependency = "20150101000002"
    candidate_row = _stock_row(
        coverage_candidate, ticker="005930", rcept_dt="20150101",
    )
    dependency_row = _stock_row(
        precoverage_dependency, ticker="000660", rcept_dt="20141231",
    )
    _write_interval(
        tmp_path,
        [dependency_row],
        start="20141231",
        end="20141231",
    )
    _write_interval(
        tmp_path,
        [candidate_row],
        start="20150101",
        end="20150101",
    )
    remote = _Remote(
        {coverage_candidate: (coverage_candidate,)},
        {coverage_candidate: candidate_row["report_nm"]},
        {coverage_candidate: _stock_body("0.1")},
    )

    snapshot = _snapshot(tmp_path)
    verified = _collect(
        tmp_path, apply=True, fetcher=remote,
    )

    assert snapshot.candidates == {
        coverage_candidate: ("005930", "stock_dividend"),
    }
    assert verified.candidate_count == 1
    assert verified.entries[0].root_receipt_no == coverage_candidate
    assert verified.entries[0].sources[0].receipt_date == "2015-01-01"
    assert all(precoverage_dependency not in call for call in remote.calls)
    assert _verify(tmp_path) == verified


def test_precoverage_structured_bonus_root_remains_terminal_dependency(
    tmp_path,
):
    root_receipt = "20141203000012"
    attachment = "20150102000003"
    root_row = _bonus_row(
        root_receipt, ticker="034590", rcept_dt="20141203",
    )
    attachment_row = _bonus_row(
        attachment,
        ticker="034590",
        rcept_dt="20150102",
        report="[첨부정정]주요사항보고서(무상증자결정)",
    )
    _write_interval(
        tmp_path, [root_row], start="20141203", end="20141203",
    )
    _write_interval(
        tmp_path, [attachment_row], start="20150102", end="20150102",
    )
    structured = _write_bonus_structured(
        tmp_path,
        root_receipt,
        ticker="034590",
        ratio="0.25",
        effective="20141231",
    )
    official_family = (root_receipt,)
    all_receipts = (attachment, root_receipt)
    attachment_key = (
        attachment,
        str(1_000_000 + int(attachment[-6:])),
    )
    remote = _Remote(
        {receipt: official_family for receipt in all_receipts},
        {
            root_receipt: root_row["report_nm"],
            attachment: attachment_row["report_nm"],
        },
        {
            root_receipt: b"<html><body>official bonus root</body></html>",
            attachment: b"<html><body>official attachment</body></html>",
        },
        attachments_by_receipt={
            receipt: (attachment_key,) for receipt in all_receipts
        },
    )

    snapshot = _snapshot(tmp_path)
    verified = _collect(
        tmp_path, apply=True, fetcher=remote,
    )

    assert snapshot.candidates == {
        attachment: ("034590", "bonus_issue"),
    }
    assert root_receipt in snapshot.structured
    assert snapshot.structured[root_receipt].body.path == structured.relative_to(
        tmp_path
    ).as_posix()
    entry = verified.entries[0]
    assert entry.ordered_family_receipts == (attachment, root_receipt)
    assert entry.terminal_receipt_no == attachment
    assert entry.terminal_economic_receipt_no == root_receipt
    assert entry.terminal_ratio == pytest.approx(0.25)
    assert entry.sources[0].structured_path is None
    assert entry.sources[1].structured_path == structured.relative_to(
        tmp_path
    ).as_posix()
    assert _verify(tmp_path) == verified


def test_postcoverage_correction_is_latest_terminal_not_candidate_seed(
    tmp_path,
):
    root_receipt = "20260715000358"
    correction = "20260812000624"
    root_row = _bonus_row(
        root_receipt, ticker="002070", rcept_dt="20260715",
    )
    correction_row = _bonus_row(
        correction,
        ticker="002070",
        rcept_dt="20260812",
        report="[기재정정]주요사항보고서(무상증자결정)",
    )
    _write_interval(
        tmp_path, [root_row], start="20260715", end="20260715",
    )
    _write_interval(
        tmp_path, [correction_row], start="20260812", end="20260812",
    )
    _write_bonus_structured(
        tmp_path, root_receipt, ticker="002070", ratio="1.0",
        effective="20260803",
    )
    terminal_structured = _write_bonus_structured(
        tmp_path, correction, ticker="002070", ratio="2.0",
        effective="20260803",
    )
    family = (correction, root_receipt)
    remote = _Remote(
        {receipt: family for receipt in family},
        {
            root_receipt: root_row["report_nm"],
            correction: correction_row["report_nm"],
        },
        {
            root_receipt: b"<html><body>official bonus root</body></html>",
            correction: (
                "<html><body>정정 관련 공시서류 제출일 "
                "2026-07-15</body></html>"
            ).encode(),
        },
    )
    coverage_end = date(2026, 8, 10)

    snapshot = _snapshot(tmp_path, coverage_end=coverage_end)
    verified = _collect(
        tmp_path,
        coverage_end=coverage_end,
        apply=True,
        fetcher=remote,
    )

    assert snapshot.candidates == {
        root_receipt: ("002070", "bonus_issue"),
    }
    assert correction in snapshot.disclosures
    assert correction in snapshot.structured
    assert verified.seed_coverage_end == coverage_end
    assert verified.candidate_count == 1
    entry = verified.entries[0]
    assert entry.ordered_family_receipts == family
    assert entry.terminal_receipt_no == correction
    assert entry.terminal_economic_receipt_no == correction
    assert entry.terminal_ratio == pytest.approx(2.0)
    assert entry.sources[0].receipt_date == "2026-08-12"
    assert entry.sources[0].structured_path == terminal_structured.relative_to(
        tmp_path
    ).as_posix()
    assert _verify(tmp_path, required_end=coverage_end) == verified
    with pytest.raises(RuntimeError, match="seed coverage mismatch"):
        _verify(tmp_path, required_end=date(2026, 8, 11))


def test_same_date_independent_roots_are_not_merged(tmp_path):
    root_one = "20211217000001"
    root_two = "20211217000002"
    rows = [
        _stock_row(root_one, ticker="000001"),
        _stock_row(root_two, ticker="000001"),
    ]
    _write_interval(tmp_path, rows)
    family_one = (root_one,)
    family_two = (root_two,)
    remote = _Remote(
        {
            root_one: family_one,
            root_two: family_two,
        },
        {row["rcept_no"]: row["report_nm"] for row in rows},
        {
            root_one: _stock_body("0.1"),
            root_two: _stock_body("0.3"),
        },
    )

    verified = _collect(
        tmp_path, apply=True, fetcher=remote,
    )

    assert verified.entry_count == 2
    assert {entry.root_receipt_no for entry in verified.entries} == {
        root_one, root_two,
    }
    assert {
        entry.root_receipt_no: entry.ordered_family_receipts
        for entry in verified.entries
    } == {
        root_one: family_one,
        root_two: family_two,
    }


def test_same_date_independent_originals_do_not_override_official_selector(
    tmp_path,
):
    root_one = "20211217000001"
    root_two = "20211217000002"
    correction = "20211224000003"
    rows = [
        _stock_row(root_one, ticker="000001"),
        _stock_row(root_two, ticker="000001"),
        _stock_row(
            correction,
            ticker="000001",
            report="[기재정정]주식배당결정",
        ),
    ]
    _write_interval(tmp_path, rows)
    family_one = (correction, root_one)
    family_two = (root_two,)
    remote = _Remote(
        {
            root_one: family_one,
            correction: family_one,
            root_two: family_two,
        },
        {row["rcept_no"]: row["report_nm"] for row in rows},
        {
            root_one: _stock_body("0.1"),
            correction: _stock_body("0.2", origin="2021-12-17"),
            root_two: _stock_body("0.3"),
        },
    )

    verified = _collect(
        tmp_path, apply=True, fetcher=remote,
    )

    assert verified.entry_count == 2
    corrected = next(
        entry for entry in verified.entries
        if entry.root_receipt_no == root_one
    )
    assert corrected.ordered_family_receipts == family_one
    assert corrected.sources[0].correction_of_receipt_no == root_one
    assert corrected.sources[0].correction_origin_date == "2021-12-17"


def test_actual_non_decision_bonus_notice_shapes_are_not_action_seeds(tmp_path):
    root = "20150127000090"
    first_correction = "20150127000121"
    trading_halt = "20150127600156"
    terminal = "20150130000026"
    later_trading_halt = "20190628600699"
    non_decision_notices = {
        trading_halt,
        later_trading_halt,
        "20160316000224",
        "20160523000076",
        "20180330002445",
        "20190906000418",
        "20210105900743",
        "20210315901556",
        "20220215000621",
        "20240202900879",
        "20240416000288",
        "20150127000269",
        "20150217000164",
        "20150127000371",
        "20211028000340",
    }
    rows = [
        _bonus_row(root, ticker="189700"),
        _bonus_row(
            first_correction,
            ticker="189700",
            report="[기재정정]주요사항보고서(무상증자결정)",
        ),
        _bonus_row(
            trading_halt,
            ticker="189700",
            report="주권매매거래정지(무상증자 결정)",
        ),
        _bonus_row(
            terminal,
            ticker="189700",
            report="[기재정정]주요사항보고서(무상증자결정)",
        ),
        _bonus_row(
            later_trading_halt,
            ticker="215570",
            report="주권매매거래정지(무상증자 결정)",
        ),
        _bonus_row(
            "20160316000224", ticker="195940", report="무상증자결정", rm="공",
        ),
        _bonus_row(
            "20160523000076", ticker="241560", report="무상증자결정", rm="공",
        ),
        _bonus_row(
            "20180330002445", ticker="293490", report="무상증자결정", rm="공",
        ),
        _bonus_row(
            "20190906000418", ticker="237820", report="무상증자결정", rm="공",
        ),
        _bonus_row(
            "20210105900743",
            ticker="072520",
            report="기타주요경영사항(유무상증자 결정 철회)",
            rm="코",
        ),
        _bonus_row(
            "20210315901556",
            ticker="066110",
            report="기타주요경영사항(무상증자결정철회)",
            rm="코",
        ),
        _bonus_row(
            "20220215000621", ticker="417970", report="무상증자결정", rm="공",
        ),
        _bonus_row(
            "20240202900879",
            ticker="196300",
            report="기타주요경영사항(유무상증자결정 철회)",
            rm="코",
        ),
        _bonus_row(
            "20240416000288", ticker="482630", report="무상증자결정", rm="공",
        ),
        _bonus_row(
            "20150127000269",
            ticker="087220",
            report="주요사항보고서(유무상증자결정)",
            rm="정",
        ),
        _bonus_row(
            "20150217000164",
            ticker="087220",
            report="[기재정정]주요사항보고서(유무상증자결정)",
            rm="정",
        ),
        _bonus_row(
            "20150127000371",
            ticker="087220",
            report="[첨부정정]주요사항보고서(유무상증자결정)",
        ),
        _bonus_row(
            "20211028000340",
            ticker="294090",
            report="[첨부추가]주요사항보고서(유무상증자결정)",
            rm="정",
        ),
    ]
    _write_interval(
        tmp_path, rows, start="20150101", end="20240416",
    )
    _write_bonus_structured(
        tmp_path,
        terminal,
        ticker="189700",
        ratio="0.390556",
        effective="20150211",
    )
    official_family = (terminal, first_correction, root)
    remote = _Remote(
        {receipt: official_family for receipt in official_family},
        {
            row["rcept_no"]: row["report_nm"]
            for row in rows
            if row["rcept_no"] in official_family
        },
        {
            root: b"<html><body>official bonus root</body></html>",
            first_correction: (
                "<html><body>정정 관련 공시서류 제출일 "
                "2015-01-27</body></html>"
            ).encode(),
            terminal: (
                "<html><body>정정 관련 공시서류 제출일 "
                "2015-01-27</body></html>"
            ).encode(),
        },
    )

    snapshot = _snapshot(
        tmp_path,
        coverage_end=date(2024, 4, 16),
    )
    verified = _collect(
        tmp_path,
        coverage_end=date(2024, 4, 16),
        apply=True,
        fetcher=remote,
    )

    assert set(snapshot.candidates) == set(official_family)
    assert non_decision_notices.isdisjoint(snapshot.disclosures)
    assert verified.candidate_count == 3
    assert verified.entry_count == 1
    entry = verified.entries[0]
    assert entry.root_receipt_no == root
    assert entry.ordered_family_receipts == official_family
    assert entry.terminal_economic_receipt_no == terminal
    assert entry.terminal_ratio == pytest.approx(0.390556)


def test_active_bonus_binds_exact_terminal_structured_row(tmp_path):
    original = "20211217000406"
    correction = "20211224000781"
    rows = [
        _bonus_row(original),
        _bonus_row(
            correction,
            report="[기재정정]주요사항보고서(무상증자결정)",
        ),
    ]
    _write_interval(tmp_path, rows)
    structured = _write_bonus_structured(
        tmp_path, correction, ratio="1.0", effective="20211229",
    )
    order = (correction, original)
    remote = _Remote(
        {receipt: order for receipt in order},
        {row["rcept_no"]: row["report_nm"] for row in rows},
        {
            original: b"<html><body>bonus original</body></html>",
            correction: (
                b"<html><body>correction original submission date "
                b"2021-12-17</body></html>"
            ),
        },
    )
    remote.bodies[correction] = (
        "<p>정정 관련 공시서류 제출일 2021-12-17</p>".encode()
        + _bonus_body("1.0", record_date="2021-12-29")
    )

    verified = _collect(
        tmp_path, apply=True, fetcher=remote,
    )

    entry = verified.entries[0]
    assert entry.action_type == "bonus_issue"
    assert entry.terminal_ratio == pytest.approx(1.0)
    assert entry.terminal_economic_receipt_no == correction
    assert entry.sources[0].structured_path == structured.relative_to(
        tmp_path
    ).as_posix()
    assert entry.sources[1].structured_path is None


@pytest.mark.parametrize(
    ("body_ratio", "body_date"),
    [("0.5", "2021-12-29"), ("1.0", "2021-12-30")],
)
def test_structured_bonus_and_parseable_viewer_terms_require_exact_parity(
    tmp_path, body_ratio, body_date,
):
    receipt = "20211217000406"
    row = _bonus_row(receipt)
    _write_interval(tmp_path, [row])
    _write_bonus_structured(
        tmp_path, receipt, ratio="1.0", effective="20211229",
    )
    remote = _Remote(
        {receipt: (receipt,)},
        {receipt: row["report_nm"]},
        {receipt: _bonus_body(body_ratio, record_date=body_date)},
    )

    with pytest.raises(RuntimeError, match="structured/viewer"):
        _collect(tmp_path, apply=True, fetcher=remote)


def test_structured_bonus_is_not_blocked_by_nested_optional_viewer_table(
    tmp_path,
):
    receipt = "20211217000406"
    row = _bonus_row(receipt)
    _write_interval(tmp_path, [row])
    _write_bonus_structured(
        tmp_path, receipt, ratio="1.0", effective="20211229",
    )
    nested = (
        "<table><tr><td>outer<table><tr><td>nested</td></tr>"
        "</table></td></tr></table>"
    ).encode()
    remote = _Remote(
        {receipt: (receipt,)},
        {receipt: row["report_nm"]},
        {receipt: nested},
    )

    entry = _collect(tmp_path, apply=True, fetcher=remote).entries[0]

    assert entry.terminal_status == "ACTIVE"
    assert entry.terminal_admissible
    assert entry.terminal_ratio == pytest.approx(1.0)


def test_body_labelled_bonus_withdrawal_without_title_marker_is_visible(
    tmp_path,
):
    original = "20180611000071"
    terminal = "20210315000952"
    rows = [
        _bonus_row(original),
        _bonus_row(
            terminal,
            report="[기재정정]주요사항보고서(무상증자결정)",
        ),
    ]
    _write_interval(tmp_path, rows, start="20180601", end="20210331")
    family = (terminal, original)
    withdrawal_body = """
      <table>
        <tr><td>항 목</td><td>정정사유</td><td>정 정 전</td><td>정 정 후</td></tr>
        <tr><td>-</td><td>무상증자 결정 철회</td>
        <td>무상증자 결정</td><td>무상증자 철회</td></tr>
        <tr><td>4. 신주배정기준일</td><td>-</td></tr>
        <tr><td>5. 1주당 신주배정 주식수</td>
        <td>보통주식 (주)</td><td>-</td></tr>
      </table>
    """.encode()
    remote = _Remote(
        {receipt: family for receipt in family},
        {row["rcept_no"]: row["report_nm"] for row in rows},
        {original: _bonus_body("1", record_date="2018-06-30"),
         terminal: withdrawal_body},
    )

    verified = _collect(tmp_path, apply=True, fetcher=remote)
    entry = verified.entries[0]

    assert entry.terminal_status == "WITHDRAWN"
    assert not entry.terminal_admissible
    assert entry.terminal_ratio is None
    assert entry.terminal_economic_receipt_no == original
    assert entry.sources[0].revision_kind == "WITHDRAWN"
    assert _verify(tmp_path) == verified


@pytest.mark.parametrize(
    ("report", "expected_status"),
    [
        ("[철회]주식배당결정", "WITHDRAWN"),
        ("[취소]주식배당결정", "CANCELLED"),
        ("[부결]주식배당결정", "DENIED"),
    ],
)
def test_terminal_termination_is_visible_but_inadmissible(
    tmp_path, report, expected_status,
):
    original = "20211217000001"
    terminal = "20211224000002"
    rows = [_stock_row(original), _stock_row(terminal, report=report)]
    _write_interval(tmp_path, rows)
    order = (terminal, original)
    remote = _Remote(
        {receipt: order for receipt in order},
        {row["rcept_no"]: row["report_nm"] for row in rows},
        {
            original: _stock_body("0.1"),
            terminal: (
                "<html>정정 관련 공시서류 제출일 2021-12-17</html>"
            ).encode(),
        },
    )

    entry = _collect(
        tmp_path, apply=True, fetcher=remote,
    ).entries[0]

    assert entry.terminal_status == expected_status
    assert not entry.terminal_admissible
    assert entry.terminal_ratio is None
    assert entry.terminal_economic_receipt_no == original


def test_zero_ratio_is_visible_but_inadmissible(tmp_path):
    receipt = "20211217000406"
    row = _bonus_row(receipt)
    _write_interval(tmp_path, [row])
    _write_bonus_structured(tmp_path, receipt, ratio="0")
    remote = _Remote(
        {receipt: (receipt,)},
        {receipt: row["report_nm"]},
        {receipt: b"<html><body>bonus zero</body></html>"},
    )

    entry = _collect(
        tmp_path, apply=True, fetcher=remote,
    ).entries[0]

    assert entry.terminal_status == "ZERO_RATIO"
    assert entry.terminal_ratio == 0
    assert not entry.terminal_admissible


@pytest.mark.parametrize(
    ("termination_report", "expected_status"),
    [
        ("[철회]주식배당결정", "WITHDRAWN"),
        ("[취소]주식배당결정", "CANCELLED"),
        ("[부결]주식배당결정", "DENIED"),
    ],
)
def test_attachment_only_terminal_inherits_prior_termination(
    tmp_path, termination_report, expected_status,
):
    original = "20211217000001"
    withdrawal = "20211223000002"
    attachment = "20211224000003"
    rows = [
        _stock_row(original),
        _stock_row(withdrawal, report=termination_report),
        _stock_row(attachment, report="[첨부정정]주식배당결정"),
    ]
    _write_interval(tmp_path, rows)
    official_order = (withdrawal, original)
    all_receipts = (attachment, withdrawal, original)
    attachment_key = (attachment, str(1_000_000 + int(attachment[-6:])))
    remote = _Remote(
        {receipt: official_order for receipt in all_receipts},
        {row["rcept_no"]: row["report_nm"] for row in rows},
        {
            original: _stock_body("0.1"),
            withdrawal: (
                "<html>정정 관련 공시서류 제출일 2021-12-17</html>"
            ).encode(),
            attachment: (
                "<html>대표이사 등의 확인 첨부정정</html>"
            ).encode(),
        },
        attachments_by_receipt={
            receipt: (attachment_key,) for receipt in all_receipts
        },
    )

    entry = _collect(
        tmp_path, apply=True, fetcher=remote,
    ).entries[0]

    assert entry.terminal_receipt_no == attachment
    assert entry.terminal_economic_receipt_no == original
    assert entry.terminal_status == expected_status
    assert not entry.terminal_admissible
    assert entry.terminal_ratio is None


def test_alphanumeric_krx_ticker_is_preserved(tmp_path):
    receipt = "20211217000001"
    row = _stock_row(receipt, ticker="0008Z0")
    _write_interval(tmp_path, [row])
    remote = _Remote(
        {receipt: (receipt,)},
        {receipt: row["report_nm"]},
        {receipt: _stock_body("0.1")},
    )

    entry = _collect(
        tmp_path, apply=True, fetcher=remote,
    ).entries[0]

    assert entry.ticker == "0008Z0"


def test_bonus_disclosure_without_structured_row_uses_exact_viewer_terms(
    tmp_path,
):
    original = "20211217000406"
    rows = [_bonus_row(original)]
    _write_interval(tmp_path, rows)
    order = (original,)
    remote = _Remote(
        {receipt: order for receipt in order},
        {row["rcept_no"]: row["report_nm"] for row in rows},
        {original: _bonus_body("0.02", record_date="2017-01-01")},
    )

    preview = _collect(tmp_path)
    assert preview["candidate_receipts"] == [original]
    verified = _collect(
        tmp_path, apply=True, fetcher=remote,
    )
    entry = verified.entries[0]
    assert entry.terminal_status == "ACTIVE"
    assert entry.terminal_admissible
    assert entry.terminal_ratio == pytest.approx(0.02)
    assert entry.sources[0].structured_path is None
    assert _verify(tmp_path) == verified
    body_path = tmp_path / entry.sources[0].body_path
    body_path.write_bytes(body_path.read_bytes().replace(b"0.02", b"0.03"))
    with pytest.raises(RuntimeError, match="evidence changed"):
        _verify(tmp_path)


def test_bonus_withdrawal_with_original_structured_row_is_visible(tmp_path):
    original = "20211217000406"
    withdrawal = "20211224000781"
    rows = [
        _bonus_row(original),
        _bonus_row(
            withdrawal,
            report="[철회]주요사항보고서(무상증자결정)",
        ),
    ]
    _write_interval(tmp_path, rows)
    _write_bonus_structured(tmp_path, original, ratio="1.0")
    order = (withdrawal, original)
    remote = _Remote(
        {receipt: order for receipt in order},
        {row["rcept_no"]: row["report_nm"] for row in rows},
        {
            original: b"<html>bonus original</html>",
            withdrawal: (
                "<html>정정 관련 공시서류 제출일 2021-12-17</html>"
            ).encode(),
        },
    )

    entry = _collect(
        tmp_path, apply=True, fetcher=remote,
    ).entries[0]

    assert entry.terminal_status == "WITHDRAWN"
    assert not entry.terminal_admissible
    assert entry.terminal_economic_receipt_no == original


def test_correction_origin_is_provenance_not_official_family_key(tmp_path):
    original, correction, remote = _stock_fixture(tmp_path)
    remote.bodies[correction] = _stock_body(
        "0.2", origin="2021-12-18",
    )

    entry = _collect(
        tmp_path, apply=True, fetcher=remote,
    ).entries[0]

    assert entry.root_receipt_no == original
    assert entry.sources[0].correction_of_receipt_no == original
    assert entry.sources[0].correction_origin_date == "2021-12-18"


@pytest.mark.parametrize(
    ("origin", "expected"),
    [
        ("2021-12-10", "2021-12-10"),  # older official/external member
        ("2021-12-24", "2021-12-24"),  # correction's own date
        ("2021-12-16", "2021-12-16"),  # selector receipt-prefix date
        ("not-a-calendar-date", None),
    ],
)
def test_correction_origin_shapes_are_bound_as_optional_provenance(
    tmp_path, origin, expected,
):
    original, correction, remote = _stock_fixture(tmp_path)
    if expected is None:
        remote.bodies[correction] = (
            "<p>정정 관련 공시서류 제출일 not-a-calendar-date</p>".encode()
            + _stock_body("0.2")
        )
    else:
        remote.bodies[correction] = _stock_body("0.2", origin=origin)

    verified = _collect(tmp_path, apply=True, fetcher=remote)
    source = verified.entries[0].sources[0]

    assert source.correction_of_receipt_no == original
    assert source.correction_origin_date == expected
    assert _verify(tmp_path) == verified


def test_actual_cj_cross_class_stock_distribution_is_inadmissible(tmp_path):
    original = "20181220800750"
    terminal = "20181221800001"
    rows = [
        _stock_row(original, ticker="001040"),
        _stock_row(
            terminal,
            ticker="001040",
            report="[기재정정]주식배당결정",
        ),
    ]
    _write_interval(tmp_path, rows, start="20181201", end="20181231")
    family = (terminal, original)
    terminal_body = """
      <table>
        <tr><td>1. 1주당 배당주식수 (주)</td>
        <td>보통주식</td><td>-</td></tr>
      </table>
      <table>
        <tr><td>종류주식명</td><td>종류주식구분</td>
        <td>1주당 배당주식수(주)</td><td>배당주식 총수(주)</td></tr>
        <tr><td>CJ우</td><td>우선주</td><td>0.15</td><td>338,865</td></tr>
      </table>
    """.encode()
    remote = _Remote(
        {receipt: family for receipt in family},
        {row["rcept_no"]: row["report_nm"] for row in rows},
        {original: _stock_body("0.1"), terminal: terminal_body},
    )

    entry = _collect(tmp_path, apply=True, fetcher=remote).entries[0]

    assert entry.terminal_economic_receipt_no == terminal
    assert entry.terminal_status == "CROSS_CLASS_DISTRIBUTION"
    assert not entry.terminal_admissible
    assert entry.terminal_ratio is None


def test_family_member_missing_from_fresh_disclosures_fails_closed(tmp_path):
    original, correction, remote = _stock_fixture(tmp_path)
    missing = "20211223000009"
    order = (correction, missing, original)
    for receipt in order:
        remote.families_by_receipt[receipt] = order
    remote.reports[missing] = "[기재정정]주식배당결정"
    remote.bodies[missing] = _stock_body("0.15", origin="2021-12-17")

    with pytest.raises(RuntimeError, match="absent from fresh disclosures"):
        _collect(
            tmp_path, apply=True, fetcher=remote,
        )


def test_ambiguous_main_metadata_fails_closed(tmp_path):
    _, correction, remote = _stock_fixture(tmp_path)
    original_main = remote.main

    def ambiguous(receipt):
        payload = original_main(receipt).decode()
        if receipt == correction:
            payload = payload.replace(
                f'value="rcpNo={remote.families_by_receipt[receipt][1]}"',
                f'value="rcpNo={remote.families_by_receipt[receipt][1]}" selected',
            )
        return payload.encode()

    remote.main = ambiguous
    with pytest.raises(RuntimeError, match="selection is missing/ambiguous"):
        _collect(
            tmp_path, apply=True, fetcher=remote,
        )


def test_old_document_completion_marker_is_rejected(tmp_path):
    row = _stock_row("20211217000001")
    _write_interval(tmp_path, [row], marker_version=4)

    with pytest.raises(RuntimeError, match="no documents_complete_v5"):
        _collect(tmp_path)


def test_same_end_mutable_disclosure_uses_latest_start(tmp_path):
    receipt = "20211217000001"
    original = _stock_row(receipt, rm="original")
    _write_interval(
        tmp_path, [original], start="20211201", end="20211231",
    )
    changed = _stock_row(receipt, rm="changed")
    _write_interval(
        tmp_path, [changed], start="20211215", end="20211231",
    )

    result = _collect(tmp_path)
    assert result


def test_mutable_disclosure_uses_latest_coverage_and_binds_all_observations(
    tmp_path,
):
    receipt = "20211217000001"
    old = _stock_row(receipt, rm="old-display")
    latest = _stock_row(receipt, rm="latest-display")
    _write_interval(
        tmp_path, [old], start="20211201", end="20211220",
    )
    _write_interval(
        tmp_path, [latest], start="20211215", end="20211231",
    )
    remote = _Remote(
        {receipt: (receipt,)},
        {receipt: latest["report_nm"]},
        {receipt: _stock_body("0.1")},
    )
    verified = _collect(
        tmp_path, apply=True, fetcher=remote,
    )
    source = verified.entries[0].sources[0]
    assert source.disclosure_row_sha256 == families._row_digest(latest)

    old_manifest = (
        tmp_path / "corporate_actions" / "dart" / "manifests"
        / "from=20211201" / "to=20211220" / "disclosures_v3.json"
    )
    mutated = dict(old)
    mutated["rm"] = "another-old-display"
    _write_json(old_manifest, [mutated])
    with pytest.raises(RuntimeError, match="derived manifest row changed"):
        _verify(tmp_path)


def test_marker_count_parity_is_required(tmp_path):
    row = _stock_row("20211217000001")
    _write_interval(tmp_path, [row])
    marker = (
        tmp_path / "corporate_actions" / "dart" / "manifests"
        / "from=20211201" / "to=20211231"
        / "documents_complete_v5.json"
    )
    payload = json.loads(marker.read_text(encoding="utf-8"))
    payload["candidate_count"] = 0
    _write_json(marker, payload)

    with pytest.raises(RuntimeError, match="candidate_count parity mismatch"):
        _collect(tmp_path)


def test_content_addressed_object_path_is_enforced(tmp_path):
    _, _, remote = _stock_fixture(tmp_path)
    _collect(
        tmp_path, apply=True, fetcher=remote,
    )
    manifest = tmp_path / families.MANIFEST_RELATIVE_PATH
    payload = json.loads(manifest.read_text(encoding="utf-8"))
    payload["entries"][0]["sources"][0]["main_path"] = (
        "corporate_actions/dart/support_action_families/not-content-addressed.html"
    )
    payload["entry_digest"] = families._sha256_bytes(
        families._canonical_bytes(payload["entries"])
    )
    manifest.write_bytes(families._canonical_bytes(payload))

    with pytest.raises(RuntimeError, match="not content-addressed"):
        _verify(tmp_path)


def test_manifest_must_use_canonical_json_bytes(tmp_path):
    _, _, remote = _stock_fixture(tmp_path)
    _collect(
        tmp_path, apply=True, fetcher=remote,
    )
    manifest = tmp_path / families.MANIFEST_RELATIVE_PATH
    payload = json.loads(manifest.read_text(encoding="utf-8"))
    manifest.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8",
    )

    with pytest.raises(RuntimeError, match="not canonical JSON bytes"):
        _verify(tmp_path)


@pytest.mark.parametrize("target", ["main", "body", "disclosure", "manifest"])
def test_roundtrip_rejects_content_corruption(tmp_path, target):
    _, _, remote = _stock_fixture(tmp_path)
    verified = _collect(
        tmp_path, apply=True, fetcher=remote,
    )
    source = verified.entries[0].sources[0]
    if target == "main":
        path = tmp_path / source.main_path
        path.write_bytes(path.read_bytes() + b"corrupt")
    elif target == "body":
        path = tmp_path / source.body_path
        path.write_bytes(path.read_bytes() + b"corrupt")
    elif target == "disclosure":
        path = tmp_path / source.disclosure_path
        row = json.loads(path.read_text(encoding="utf-8"))
        row["rm"] = "mutated"
        _write_json(path, row)
    else:
        path = tmp_path / families.MANIFEST_RELATIVE_PATH
        payload = json.loads(path.read_text(encoding="utf-8"))
        payload["entries"][0]["terminal_status"] = "WITHDRAWN"
        _write_json(path, payload)

    with pytest.raises(RuntimeError):
        _verify(tmp_path)


def test_network_failure_never_publishes_partial_manifest(tmp_path):
    original, correction, remote = _stock_fixture(tmp_path)

    def failing(url: str) -> bytes:
        if urlparse(url).path.endswith("viewer.do") and (
            parse_qs(urlparse(url).query)["rcpNo"] == [correction]
        ):
            raise RuntimeError("injected network failure")
        return remote(url)

    with pytest.raises(RuntimeError, match="injected network failure"):
        _collect(
            tmp_path, apply=True, fetcher=failing,
        )
    assert not (tmp_path / families.MANIFEST_RELATIVE_PATH).exists()
    assert not list(
        (tmp_path / families.MANIFEST_RELATIVE_PATH.parent).glob(".manifest.*")
    )


@pytest.mark.parametrize(
    "mutation",
    ["current_dcm", "current_dtd", "family", "attachment"],
)
def test_final_main_selector_reread_change_never_publishes_manifest(
    tmp_path, mutation,
):
    original, correction, remote = _stock_fixture(tmp_path)
    main_reads = {original: 0, correction: 0}

    def changing(url: str) -> bytes:
        parsed = urlparse(url)
        query = parse_qs(parsed.query)
        receipt = query["rcpNo"][0]
        if not parsed.path.endswith("/main.do"):
            return remote(url)
        main_reads[receipt] += 1
        payload = remote.main(receipt).decode()
        if receipt != correction or main_reads[receipt] == 1:
            return payload.encode()
        if mutation == "current_dcm":
            old = f'viewDoc("{receipt}", "{remote.dcm(receipt)}"'
            new = f'viewDoc("{receipt}", "{int(remote.dcm(receipt)) + 1}"'
            payload = payload.replace(old, new)
        elif mutation == "current_dtd":
            payload = payload.replace('"HTML", "");', '"dart4.xsd", "");')
        elif mutation == "family":
            payload = payload.replace(
                f'<option value="rcpNo={original}"',
                '<option value="rcpNo=20211216000099"',
            )
        else:
            payload = payload.replace(
                '<option value="null">attach</option>',
                '<option value="null">attach</option>'
                '<option value="rcpNo=20211225000003&amp;dcmNo=1999999">'
                'new attachment</option>',
            )
        return payload.encode()

    with pytest.raises(RuntimeError, match="changed during collection"):
        _collect(tmp_path, apply=True, fetcher=changing)

    assert main_reads == {original: 2, correction: 2}
    assert not (tmp_path / families.MANIFEST_RELATIVE_PATH).exists()


def test_final_main_selector_reread_network_failure_preserves_manifest(
    tmp_path,
):
    original, correction, remote = _stock_fixture(tmp_path)
    initial = _collect(tmp_path, apply=True, fetcher=remote)
    manifest = tmp_path / families.MANIFEST_RELATIVE_PATH
    previous = manifest.read_bytes()
    main_reads = {original: 0, correction: 0}

    def failing(url: str) -> bytes:
        parsed = urlparse(url)
        query = parse_qs(parsed.query)
        receipt = query["rcpNo"][0]
        if parsed.path.endswith("/main.do"):
            main_reads[receipt] += 1
            if receipt == correction and main_reads[receipt] == 2:
                raise RuntimeError("injected final selector failure")
        return remote(url)

    with pytest.raises(RuntimeError, match="could not be revalidated"):
        _collect(tmp_path, apply=True, fetcher=failing)

    assert manifest.read_bytes() == previous
    assert _verify(tmp_path) == initial
