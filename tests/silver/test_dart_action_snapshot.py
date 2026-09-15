import hashlib
import json
import zipfile
from datetime import date
from types import SimpleNamespace

import pytest

from pipeline.bronze import financials
from pipeline.silver import dart_action_snapshot as snapshot_module
from pipeline.silver.dart_action_snapshot import (
    MANIFEST_RELATIVE_PATH,
    build_snapshot_manifest,
    verify_snapshot_manifest,
)


def _write_corp_codes(root, pairs=()):
    entries = "".join(
        "<list><corp_code>" + corp + "</corp_code><stock_code>"
        + ticker + "</stock_code></list>"
        for corp, ticker in pairs
    )
    path = root / financials.CORPCODE_BRONZE_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"<result>{entries}</result>", encoding="utf-8")


def _write_zip(path, *, body: str = "<document>fixture</document>"):
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("document.xml", body)


def _complete_interval(root, start: str, end: str):
    corp_code_path = root / financials.CORPCODE_BRONZE_PATH
    if not corp_code_path.is_file():
        _write_corp_codes(root)
    interval = (
        root
        / "corporate_actions"
        / "dart"
        / "manifests"
        / f"from={start}"
        / f"to={end}"
    )
    interval.mkdir(parents=True, exist_ok=True)
    (interval / "disclosures_v3.json").write_text("[]", encoding="utf-8")
    marker = {
        "status": "COMPLETE",
        "fromdate": start,
        "todate": end,
        "candidate_count": 0,
    }
    (interval / "structured_complete_v3.json").write_text(
        json.dumps({**marker, "query_count": 0}), encoding="utf-8",
    )
    (interval / "documents_complete_v5.json").write_text(
        json.dumps(marker), encoding="utf-8",
    )


def test_snapshot_manifest_hashes_every_body_and_verifies_continuous_coverage(
    tmp_path,
):
    _complete_interval(tmp_path, "20150101", "20151231")
    _complete_interval(tmp_path, "20160101", "20161231")
    interval = (
        tmp_path / "corporate_actions/dart/manifests"
        / "from=20160101/to=20161231"
    )
    receipt = "20160601000001"
    row = {
        "rcept_no": receipt,
        "rcept_dt": "20160601",
        "stock_code": "005930",
        "corp_code": "00126380",
        "report_nm": "현금ㆍ현물배당결정",
    }
    interval.joinpath("disclosures_v3.json").write_text(
        json.dumps([row], ensure_ascii=False), encoding="utf-8",
    )
    interval.joinpath("documents_complete_v5.json").write_text(
        json.dumps({
            "status": "COMPLETE",
            "fromdate": "20160101",
            "todate": "20161231",
            "candidate_count": 1,
        }),
        encoding="utf-8",
    )
    body = (
        tmp_path / "corporate_actions/dart/documents/year=2016"
        / "corp=005930" / f"rcept={receipt}.zip"
    )
    _write_zip(body, body="<document>immutable-source-body</document>")

    built = build_snapshot_manifest(
        str(tmp_path), coverage_end=date(2016, 12, 31)
    )
    verified = verify_snapshot_manifest(
        str(tmp_path), required_end=date(2016, 12, 31)
    )

    assert verified == built
    payload = json.loads((tmp_path / MANIFEST_RELATIVE_PATH).read_text())
    entry = next(
        item for item in payload["objects"]
        if item["path"].endswith(f"rcept={receipt}.zip")
    )
    assert entry["sha256"] == hashlib.sha256(body.read_bytes()).hexdigest()
    assert verified.body_count == len(payload["objects"])


def test_snapshot_reuses_authenticated_hash_until_local_file_changes(
    tmp_path, monkeypatch,
):
    _complete_interval(tmp_path, "20150101", "20151231")
    built = build_snapshot_manifest(
        str(tmp_path), coverage_end=date(2015, 12, 31),
    )
    payload = json.loads((tmp_path / MANIFEST_RELATIVE_PATH).read_text())

    snapshot_module.seed_body_hash_cache(tmp_path, payload["objects"])
    original = snapshot_module._sha256
    hashes: list[str] = []

    def observed(path):
        hashes.append(path.relative_to(tmp_path).as_posix())
        return original(path)

    monkeypatch.setattr(snapshot_module, "_sha256", observed)
    assert verify_snapshot_manifest(
        str(tmp_path), required_end=date(2015, 12, 31),
    ) == built
    assert hashes == []

    body_entry = next(
        entry for entry in payload["objects"]
        if entry["path"].endswith("disclosures_v3.json")
    )
    body = tmp_path / body_entry["path"]
    body.write_bytes(body.read_bytes() + b"\n")
    with pytest.raises(RuntimeError, match="SHA/content length mismatch"):
        verify_snapshot_manifest(
            str(tmp_path), required_end=date(2015, 12, 31),
        )
    assert body.relative_to(tmp_path).as_posix() in hashes


def test_precoverage_dependency_interval_does_not_expand_declared_coverage(
    tmp_path,
):
    _complete_interval(tmp_path, "20141203", "20141203")
    _complete_interval(tmp_path, "20150101", "20151231")

    verified = build_snapshot_manifest(
        str(tmp_path), coverage_end=date(2015, 12, 31),
    )
    payload = json.loads((tmp_path / MANIFEST_RELATIVE_PATH).read_text())

    assert verified.coverage_start == date(2015, 1, 1)
    assert verified.coverage_intervals == (
        (date(2015, 1, 1), date(2015, 12, 31)),
    )
    assert payload["coverage_intervals"] == [
        {"from": "2015-01-01", "to": "2015-12-31"},
    ]


def test_orphan_files_and_unrelated_future_interval_do_not_change_body_set(
    tmp_path,
):
    _complete_interval(tmp_path, "20150101", "20151231")
    built = build_snapshot_manifest(
        str(tmp_path), coverage_end=date(2015, 12, 31),
    )
    original = json.loads((tmp_path / MANIFEST_RELATIVE_PATH).read_text())
    original_paths = {item["path"] for item in original["objects"]}

    orphan = (
        tmp_path / "corporate_actions/dart/viewer_corrections/objects"
        / f"sha256={'a' * 64}.html"
    )
    orphan.parent.mkdir(parents=True, exist_ok=True)
    orphan.write_bytes(b"unreferenced legacy viewer cache")
    orphan_disclosure = (
        tmp_path / "corporate_actions/dart/disclosures/year=2016"
        / "date=2016-01-02/corp=005930/rcept=20160102000001.json"
    )
    orphan_disclosure.parent.mkdir(parents=True, exist_ok=True)
    orphan_disclosure.write_text(
        json.dumps({
            "rcept_no": "20160102000001",
            "rcept_dt": "20160102",
            "stock_code": "005930",
            "report_nm": "현금ㆍ현물배당결정",
        }, ensure_ascii=False),
        encoding="utf-8",
    )
    future = (
        tmp_path / "corporate_actions/dart/manifests"
        / "from=20160102/to=20160102"
    )
    future.mkdir(parents=True)
    future.joinpath("disclosures_v3.json").write_text(
        json.dumps([{
            "rcept_no": "20160102000002",
            "rcept_dt": "20160102",
            "stock_code": "005930",
            "report_nm": "사업보고서",
        }], ensure_ascii=False),
        encoding="utf-8",
    )

    rebuilt = build_snapshot_manifest(
        str(tmp_path), coverage_end=date(2015, 12, 31),
    )
    current = json.loads((tmp_path / MANIFEST_RELATIVE_PATH).read_text())

    assert rebuilt.body_digest == built.body_digest
    assert {item["path"] for item in current["objects"]} == original_paths
    assert orphan.relative_to(tmp_path).as_posix() not in original_paths
    assert orphan_disclosure.relative_to(tmp_path).as_posix() not in original_paths


def test_in_range_non_action_receipt_body_is_not_snapshot_evidence(tmp_path):
    _complete_interval(tmp_path, "20150101", "20151231")
    interval = (
        tmp_path / "corporate_actions/dart/manifests"
        / "from=20150101/to=20151231"
    )
    receipt = "20150601000099"
    interval.joinpath("disclosures_v3.json").write_text(
        json.dumps([{
            "rcept_no": receipt,
            "rcept_dt": "20150601",
            "stock_code": "005930",
            "corp_code": "00126380",
            "report_nm": "사업보고서",
        }], ensure_ascii=False),
        encoding="utf-8",
    )
    unused = (
        tmp_path / "corporate_actions/dart/disclosures/year=2015"
        / f"date=2015-06-01/corp=005930/rcept={receipt}.json"
    )
    unused.parent.mkdir(parents=True, exist_ok=True)
    unused.write_text("{}", encoding="utf-8")

    build_snapshot_manifest(
        str(tmp_path), coverage_end=date(2015, 12, 31),
    )
    payload = json.loads((tmp_path / MANIFEST_RELATIVE_PATH).read_text())
    paths = {item["path"] for item in payload["objects"]}

    assert unused.relative_to(tmp_path).as_posix() not in paths


def test_viewer_probe_main_and_outside_list_proofs_are_exact_evidence(
    tmp_path, monkeypatch,
):
    _complete_interval(tmp_path, "20150101", "20151231")
    outside = (
        tmp_path / "corporate_actions/dart/manifests"
        / "from=20160102/to=20160102"
    )
    outside.mkdir(parents=True)
    probe_receipts = ("20160102000001", "20160102000002")
    outside.joinpath("disclosures_v3.json").write_text(
        json.dumps([{
            "rcept_no": receipt,
            "rcept_dt": "20160102",
            "stock_code": "005930",
            "corp_code": "00126380",
            "report_nm": "[기재정정] 현금ㆍ현물배당결정",
        } for receipt in probe_receipts], ensure_ascii=False),
        encoding="utf-8",
    )
    marker = {
        "status": "COMPLETE",
        "fromdate": "20160102",
        "todate": "20160102",
    }
    outside.joinpath("structured_complete_v3.json").write_text(
        json.dumps({**marker, "query_count": 0}), encoding="utf-8",
    )
    outside.joinpath("documents_complete_v5.json").write_text(
        json.dumps({**marker, "candidate_count": 2}), encoding="utf-8",
    )
    manifest = tmp_path / snapshot_module.VIEWER_MANIFEST_RELATIVE_PATH
    manifest.parent.mkdir(parents=True, exist_ok=True)
    manifest.write_text("{}", encoding="utf-8")
    probes = []
    for index, receipt in enumerate(probe_receipts):
        main = manifest.parent / "objects" / f"sha256={'ab'[index] * 64}.html"
        main.parent.mkdir(parents=True, exist_ok=True)
        main.write_text(f"probe-{receipt}", encoding="utf-8")
        probes.append(SimpleNamespace(
            receipt_no=receipt,
            main_path=main.relative_to(tmp_path).as_posix(),
        ))
    orphan = manifest.parent / "objects" / f"sha256={'c' * 64}.html"
    orphan.write_text("orphan", encoding="utf-8")
    verified = SimpleNamespace(
        manifest_path=str(manifest),
        dependency_probes=tuple(probes),
        receipts=(),
    )
    monkeypatch.setattr(
        snapshot_module, "required_viewer_receipts", lambda *args, **kwargs: (),
    )
    monkeypatch.setattr(
        snapshot_module, "verify_viewer_corrections",
        lambda *args, **kwargs: verified,
    )

    paths = snapshot_module._evidence_paths(
        tmp_path,
        required_start=date(2015, 1, 1),
        required_end=date(2015, 12, 31),
    )
    relative = {path.relative_to(tmp_path).as_posix() for path in paths}

    assert {probe.main_path for probe in probes}.issubset(relative)
    assert outside.joinpath("disclosures_v3.json").relative_to(
        tmp_path
    ).as_posix() in relative
    assert outside.joinpath("structured_complete_v3.json").relative_to(
        tmp_path
    ).as_posix() in relative
    assert outside.joinpath("documents_complete_v5.json").relative_to(
        tmp_path
    ).as_posix() in relative
    assert orphan.relative_to(tmp_path).as_posix() not in relative


def test_snapshot_verification_fails_after_body_tamper(tmp_path):
    _complete_interval(tmp_path, "20150101", "20151231")
    build_snapshot_manifest(str(tmp_path), coverage_end=date(2015, 12, 31))
    disclosure = next(
        (tmp_path / "corporate_actions" / "dart" / "manifests").rglob(
            "disclosures_v3.json"
        )
    )
    disclosure.write_text("[{}]", encoding="utf-8")

    with pytest.raises(RuntimeError, match="SHA/content length"):
        verify_snapshot_manifest(
            str(tmp_path), required_end=date(2015, 12, 31)
        )


def test_snapshot_verification_rejects_noncanonical_manifest_bytes(tmp_path):
    _complete_interval(tmp_path, "20150101", "20151231")
    build_snapshot_manifest(str(tmp_path), coverage_end=date(2015, 12, 31))
    manifest = tmp_path / MANIFEST_RELATIVE_PATH
    payload = json.loads(manifest.read_text(encoding="utf-8"))
    manifest.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8",
    )

    with pytest.raises(RuntimeError, match="not canonical"):
        verify_snapshot_manifest(
            str(tmp_path), required_end=date(2015, 12, 31),
        )


def test_snapshot_verification_requires_exact_requested_coverage(tmp_path):
    _complete_interval(tmp_path, "20150101", "20151231")
    build_snapshot_manifest(str(tmp_path), coverage_end=date(2015, 12, 31))

    with pytest.raises(RuntimeError, match="coverage end mismatch"):
        verify_snapshot_manifest(
            str(tmp_path), required_end=date(2015, 12, 30),
        )


def test_snapshot_build_fails_on_calendar_coverage_gap(tmp_path):
    _complete_interval(tmp_path, "20150101", "20150630")
    _complete_interval(tmp_path, "20150702", "20151231")

    with pytest.raises(RuntimeError, match="coverage gap"):
        build_snapshot_manifest(
            str(tmp_path), coverage_end=date(2015, 12, 31)
        )


def test_incomplete_overlap_cannot_use_corp_class_as_scope_waiver(tmp_path):
    _complete_interval(tmp_path, "20150101", "20151231")
    _write_corp_codes(tmp_path, (("001", "005930"),))
    overlap = (
        tmp_path
        / "corporate_actions"
        / "dart"
        / "manifests"
        / "from=20150601"
        / "to=20150630"
    )
    overlap.mkdir(parents=True)
    overlap.joinpath("disclosures_v3.json").write_text(
        json.dumps([{
            "rcept_no": "20150601000001",
            "corp_code": "001",
            "stock_code": "",
            "corp_cls": "E",
            "report_nm": "현금ㆍ현물배당결정",
        }]),
        encoding="utf-8",
    )
    overlap.joinpath("structured_complete_v3.json").write_text(
        json.dumps({
            "status": "COMPLETE",
            "fromdate": "20150601",
            "todate": "20150630",
        }),
        encoding="utf-8",
    )

    with pytest.raises(RuntimeError, match="incomplete v5 intervals"):
        build_snapshot_manifest(
            str(tmp_path), coverage_end=date(2015, 12, 31)
        )


def test_snapshot_accepts_uppercase_alphanumeric_krx_ticker_identities(
    tmp_path,
):
    _complete_interval(tmp_path, "20150101", "20151231")
    interval = (
        tmp_path / "corporate_actions/dart/manifests"
        / "from=20150101/to=20151231"
    )
    codes = ("0008Z0", "0010V0", "0039P0")
    rows = [
        {
            "rcept_no": f"2015123100000{index}",
            "rcept_dt": "20151231",
            "stock_code": ticker,
            "corp_code": f"0000000{index}",
            "corp_cls": "K",
            "report_nm": "현금ㆍ현물배당결정",
        }
        for index, ticker in enumerate(codes, start=1)
    ]
    interval.joinpath("disclosures_v3.json").write_text(
        json.dumps(rows, ensure_ascii=False), encoding="utf-8",
    )
    interval.joinpath("documents_complete_v5.json").write_text(
        json.dumps({
            "status": "COMPLETE",
            "fromdate": "20150101",
            "todate": "20151231",
            "candidate_count": len(rows),
        }),
        encoding="utf-8",
    )
    for row in rows:
        body = (
            tmp_path / "corporate_actions/dart/documents/year=2015"
            / f"corp={row['stock_code']}"
            / f"rcept={row['rcept_no']}.zip"
        )
        _write_zip(body)

    snapshot = build_snapshot_manifest(
        str(tmp_path), coverage_end=date(2015, 12, 31)
    )

    assert snapshot.coverage_end == date(2015, 12, 31)


def test_snapshot_marker_scope_includes_corpcode_fallback_not_unlisted(
    tmp_path,
):
    _complete_interval(tmp_path, "20150101", "20151231")
    _write_corp_codes(tmp_path, (("listed-corp", "005930"),))
    interval = (
        tmp_path / "corporate_actions/dart/manifests"
        / "from=20150101/to=20151231"
    )
    fallback = "20151231000001"
    unlisted = "20151231000002"
    direct = "20151231000003"
    rows = [{
        "rcept_no": fallback,
        "rcept_dt": "20151231",
        "corp_code": "listed-corp",
        "stock_code": "",
        "report_nm": "현금ㆍ현물배당결정",
    }, {
        "rcept_no": unlisted,
        "rcept_dt": "20151231",
        "corp_code": "unlisted-corp",
        "stock_code": "",
        "report_nm": "현금ㆍ현물배당결정",
    }, {
        "rcept_no": direct,
        "rcept_dt": "20151231",
        "corp_code": "direct-corp",
        "stock_code": "000660",
        "report_nm": "현금ㆍ현물배당결정",
    }]
    interval.joinpath("disclosures_v3.json").write_text(
        json.dumps(rows, ensure_ascii=False), encoding="utf-8",
    )
    interval.joinpath("documents_complete_v5.json").write_text(
        json.dumps({
            "status": "COMPLETE",
            "fromdate": "20150101",
            "todate": "20151231",
            "candidate_count": 2,
        }),
        encoding="utf-8",
    )
    for receipt, ticker in ((fallback, "005930"), (direct, "000660")):
        _write_zip(
            tmp_path / "corporate_actions/dart/documents/year=2015"
            / f"corp={ticker}" / f"rcept={receipt}.zip"
        )

    snapshot = build_snapshot_manifest(
        str(tmp_path), coverage_end=date(2015, 12, 31),
    )

    assert snapshot.coverage_end == date(2015, 12, 31)


def test_incomplete_overlap_non_total_return_receipt_does_not_block(tmp_path):
    _complete_interval(tmp_path, "20150101", "20151231")
    overlap = (
        tmp_path
        / "corporate_actions/dart/manifests"
        / "from=20150601/to=20150630"
    )
    overlap.mkdir(parents=True)
    overlap.joinpath("disclosures_v3.json").write_text(
        json.dumps([{
            "rcept_no": "20150601000001",
            "stock_code": "",
            "corp_cls": "E",
            "report_nm": "유상증자결정",
        }]),
        encoding="utf-8",
    )

    verified = build_snapshot_manifest(
        str(tmp_path), coverage_end=date(2015, 12, 31)
    )

    assert verified.coverage_end == date(2015, 12, 31)


def test_v5_marker_cannot_hide_missing_bonus_decision_document(tmp_path):
    _complete_interval(tmp_path, "20150101", "20151231")
    interval = (
        tmp_path / "corporate_actions/dart/manifests"
        / "from=20150101/to=20151231"
    )
    receipt = "20150601000001"
    interval.joinpath("disclosures_v3.json").write_text(
        json.dumps([{
            "rcept_no": receipt,
            "rcept_dt": "20150601",
            "stock_code": "000001",
            "corp_code": "00000001",
            "corp_cls": "Y",
            "report_nm": "[정정] 무상증자결정",
        }], ensure_ascii=False),
        encoding="utf-8",
    )
    interval.joinpath("structured_complete_v3.json").write_text(
        json.dumps({
            "status": "COMPLETE",
            "fromdate": "20150101",
            "todate": "20151231",
            "query_count": 1,
        }),
        encoding="utf-8",
    )
    interval.joinpath("documents_complete_v5.json").write_text(
        json.dumps({
            "status": "COMPLETE",
            "fromdate": "20150101",
            "todate": "20151231",
            "candidate_count": 1,
        }),
        encoding="utf-8",
    )

    with pytest.raises(RuntimeError, match="MISSING_OR_AMBIGUOUS_BODY"):
        build_snapshot_manifest(
            str(tmp_path), coverage_end=date(2015, 12, 31)
        )


def test_v5_document_unavailable_requires_exact_status_014(tmp_path):
    _complete_interval(tmp_path, "20150101", "20151231")
    interval = (
        tmp_path / "corporate_actions/dart/manifests"
        / "from=20150101/to=20151231"
    )
    receipt = "20150601000001"
    row = {
        "rcept_no": receipt,
        "rcept_dt": "20150601",
        "stock_code": "000001",
        "corp_code": "00000001",
        "corp_cls": "Y",
        "report_nm": "무상증자결정",
    }
    interval.joinpath("disclosures_v3.json").write_text(
        json.dumps([row], ensure_ascii=False), encoding="utf-8",
    )
    interval.joinpath("structured_complete_v3.json").write_text(
        json.dumps({
            "status": "COMPLETE",
            "fromdate": "20150101",
            "todate": "20151231",
            "query_count": 1,
        }),
        encoding="utf-8",
    )
    interval.joinpath("documents_complete_v5.json").write_text(
        json.dumps({
            "status": "COMPLETE",
            "fromdate": "20150101",
            "todate": "20151231",
            "candidate_count": 1,
        }),
        encoding="utf-8",
    )
    unavailable = (
        tmp_path / "corporate_actions/dart/documents_unavailable/year=2015"
        / "corp=000001" / f"rcept={receipt}.xml"
    )
    unavailable.parent.mkdir(parents=True, exist_ok=True)
    unavailable.write_text(
        "<result><status>013</status></result>", encoding="utf-8",
    )

    with pytest.raises(
        RuntimeError, match="INVALID_DOCUMENT_UNAVAILABLE_STATUS",
    ):
        build_snapshot_manifest(
            str(tmp_path), coverage_end=date(2015, 12, 31)
        )

    unavailable.write_text(
        "<result><status>014</status></result>", encoding="utf-8",
    )
    built = build_snapshot_manifest(
        str(tmp_path), coverage_end=date(2015, 12, 31)
    )
    assert built.coverage_end == date(2015, 12, 31)
