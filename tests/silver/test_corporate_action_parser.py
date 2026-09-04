import ast
import hashlib
import json
import zipfile
from datetime import date
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import pytest

from pipeline.silver import corporate_actions


@pytest.mark.parametrize(
    "rendered",
    ["20250310", "2025-03-10", "2025년 3월 10일"],
)
def test_parse_date_accepts_padded_and_non_padded_calendar_dates(rendered):
    assert corporate_actions._parse_date(rendered) == date(2025, 3, 10)


def test_parse_date_rejects_invalid_calendar_date():
    assert corporate_actions._parse_date("2025-02-30") is None


def test_prepare_reuses_only_explicitly_verified_snapshot_cache(
    monkeypatch, tmp_path,
):
    corporate_actions._PREPARE_CACHE.clear()
    calls = {"context": 0}

    def evidence_context(*_args, **_kwargs):
        calls["context"] += 1
        return object()

    monkeypatch.setattr(
        corporate_actions, "_prepare_evidence_context", evidence_context,
    )
    monkeypatch.setattr(
        corporate_actions,
        "_disclosure_rows",
        lambda *_args, **_kwargs: ([], {"observation_count": 0}),
    )
    monkeypatch.setattr(
        corporate_actions, "_verified_lineage_receipts",
        lambda *_args, **_kwargs: set(),
    )
    monkeypatch.setattr(corporate_actions.glob, "glob", lambda *_args: [])
    monkeypatch.setattr(
        corporate_actions,
        "_related_cash_correction_signatures",
        lambda *_args, **_kwargs: set(),
    )
    monkeypatch.setattr(
        corporate_actions, "_assert_prepare_evidence_unchanged",
        lambda *_args, **_kwargs: None,
    )

    kwargs = {
        "coverage_start": date(2015, 1, 1),
        "coverage_end": date(2026, 9, 1),
        "verified_snapshot_sha256": "a" * 64,
    }
    first, first_stats = corporate_actions.prepare(str(tmp_path), **kwargs)
    first_stats["row_count"] = 99
    second, second_stats = corporate_actions.prepare(str(tmp_path), **kwargs)

    assert calls["context"] == 1
    assert first is not second
    assert second_stats["row_count"] == 0

    corporate_actions.prepare(
        str(tmp_path), **{**kwargs, "verified_snapshot_sha256": "b" * 64},
    )
    assert calls["context"] == 2
    corporate_actions._PREPARE_CACHE.clear()


def _write_json(path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _write_document(path, xml):
    path.parent.mkdir(parents=True, exist_ok=True)
    output = BytesIO()
    with zipfile.ZipFile(output, "w") as archive:
        archive.writestr("document.xml", xml)
    path.write_bytes(output.getvalue())


def test_scoped_prepare_admits_only_official_date_or_verified_lineage(
    tmp_path, monkeypatch,
):
    overnight = "20141231999999"
    precoverage = "20141201000001"
    support_dependency = "20260811000002"
    viewer_dependency = "20260812000003"
    unrelated_future = "20260813000004"
    rows = [
        {
            "stock_code": "005930",
            "rcept_no": receipt,
            "rcept_dt": accepted,
            "report_nm": "무상증자결정",
        }
        for receipt, accepted in (
            (overnight, "20150101"),
            (precoverage, "20141201"),
            (support_dependency, "20260811"),
            (viewer_dependency, "20260812"),
            (unrelated_future, "20260813"),
        )
    ]
    _write_json(
        tmp_path / "corporate_actions/dart/manifests"
        / "from=20141201/to=20260813/disclosures_v3.json",
        rows,
    )
    for row in rows:
        receipt = row["rcept_no"]
        _write_json(
            tmp_path / "corporate_actions/dart/structured/event=bonus_issue"
            / f"year={receipt[:4]}/corp=005930/rcept={receipt}.json",
            {
                "rcept_no": receipt,
                "nstk_dividrk": "2026-08-14",
                "nstk_ascnt_ps_ostk": "0.1",
            },
        )

    viewer_calls = []
    viewer_body = b"<html>viewer dependency</html>"
    extra_viewer_body = b"<html>future viewer dependency</html>"
    viewer_evidence = SimpleNamespace(
        receipt_no=viewer_dependency,
        revision_root_receipt_no=overnight,
        economic_body_receipt_no=viewer_dependency,
        family_receipt_nos=(viewer_dependency, overnight),
        official_family_order=(viewer_dependency, overnight),
        attachment_keys=(),
        main_path="viewer/dependency.html",
        main_content_length=len(viewer_body),
        main_sha256=hashlib.sha256(viewer_body).hexdigest(),
        viewer_path="viewer/dependency.html",
        viewer_content_length=len(viewer_body),
        viewer_sha256=hashlib.sha256(viewer_body).hexdigest(),
        economic_viewer_path="viewer/dependency.html",
        economic_viewer_content_length=len(viewer_body),
        economic_viewer_sha256=hashlib.sha256(viewer_body).hexdigest(),
        economic_main_path="viewer/dependency.html",
        economic_main_content_length=len(viewer_body),
        economic_main_sha256=hashlib.sha256(viewer_body).hexdigest(),
    )
    extra_viewer_evidence = SimpleNamespace(
        receipt_no=unrelated_future,
        revision_root_receipt_no=unrelated_future,
        economic_body_receipt_no=unrelated_future,
        family_receipt_nos=(unrelated_future,),
        official_family_order=(unrelated_future,),
        attachment_keys=(),
        main_path="viewer/future.html",
        main_content_length=len(extra_viewer_body),
        main_sha256=hashlib.sha256(extra_viewer_body).hexdigest(),
        viewer_path="viewer/future.html",
        viewer_content_length=len(extra_viewer_body),
        viewer_sha256=hashlib.sha256(extra_viewer_body).hexdigest(),
        economic_viewer_path="viewer/future.html",
        economic_viewer_content_length=len(extra_viewer_body),
        economic_viewer_sha256=hashlib.sha256(extra_viewer_body).hexdigest(),
        economic_main_path="viewer/future.html",
        economic_main_content_length=len(extra_viewer_body),
        economic_main_sha256=hashlib.sha256(extra_viewer_body).hexdigest(),
    )

    viewer_manifest = tmp_path / corporate_actions.VIEWER_MANIFEST_RELATIVE_PATH
    _write_json(viewer_manifest, {})
    viewer_path = tmp_path / viewer_evidence.viewer_path
    viewer_path.parent.mkdir(parents=True, exist_ok=True)
    viewer_path.write_bytes(viewer_body)
    extra_viewer_path = tmp_path / extra_viewer_evidence.viewer_path
    extra_viewer_path.write_bytes(extra_viewer_body)

    def fake_viewer(base, *, required_start, required_end):
        viewer_calls.append((base, required_start, required_end))
        return SimpleNamespace(
            manifest_path=str(viewer_manifest.resolve()),
            manifest_sha256=hashlib.sha256(
                viewer_manifest.read_bytes(),
            ).hexdigest(),
            dependency_probes=(),
            receipts=(viewer_evidence, extra_viewer_evidence),
        )

    support_calls = []

    def support_source(receipt):
        body = f"support:{receipt}".encode()
        relative = f"support/{receipt}.bin"
        destination = tmp_path / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(body)
        for marker in (
            "structured_complete_v3.json", "documents_complete_v5.json",
        ):
            (destination.parent / marker).write_bytes(b"{}")
        digest = hashlib.sha256(body).hexdigest()
        return SimpleNamespace(
            receipt_no=receipt,
            main_path=relative,
            main_content_length=len(body),
            main_sha256=digest,
            body_path=relative,
            body_content_length=len(body),
            body_sha256=digest,
            disclosure_path=relative,
            disclosure_content_length=len(body),
            disclosure_sha256=digest,
            disclosure_manifest_path=relative,
            disclosure_manifest_sha256=digest,
            structured_path=None,
            structured_content_length=None,
            structured_sha256=None,
        )

    support_entry = SimpleNamespace(
        root_receipt_no=overnight,
        terminal_receipt_no=support_dependency,
        terminal_economic_receipt_no=support_dependency,
        ordered_family_receipts=(support_dependency, overnight),
        sources=(
            support_source(support_dependency),
            support_source(overnight),
        ),
    )
    extra_support_entry = SimpleNamespace(
        root_receipt_no=precoverage,
        terminal_receipt_no=precoverage,
        terminal_economic_receipt_no=precoverage,
        ordered_family_receipts=(precoverage,),
        sources=(support_source(precoverage),),
    )

    def fake_support(base, *, required_start, required_end):
        support_calls.append((str(base), required_start, required_end))
        manifest = Path(base) / corporate_actions.SUPPORT_FAMILY_MANIFEST_RELATIVE_PATH
        return SimpleNamespace(
            manifest_path=str(manifest.resolve()),
            manifest_sha256=hashlib.sha256(manifest.read_bytes()).hexdigest(),
            entries=(support_entry, extra_support_entry),
        )

    monkeypatch.setattr(
        corporate_actions, "verify_viewer_corrections", fake_viewer,
    )
    monkeypatch.setattr(
        corporate_actions, "verify_support_action_families", fake_support,
    )
    _write_json(
        tmp_path / corporate_actions.SUPPORT_FAMILY_MANIFEST_RELATIVE_PATH,
        {},
    )

    events, stats = corporate_actions.prepare(
        str(tmp_path),
        coverage_start=date(2015, 1, 1),
        coverage_end=date(2026, 8, 10),
    )

    assert set(events["rcept_no"]) == {
        overnight, support_dependency, viewer_dependency,
    }
    assert events.set_index("rcept_no").loc[
        overnight, "announcement_date"
    ] == date(2015, 1, 1)
    assert stats["scoped_structured_file_count"] == 3
    assert stats["coverage_excluded_structured_file_count"] == 2
    assert stats["coverage_excluded_disclosure_count"] == 2
    assert stats["verified_lineage_receipt_count"] == 3
    assert viewer_calls == [(
        str(tmp_path.resolve()), date(2015, 1, 1), date(2026, 8, 10),
    )]
    assert support_calls == [(
        str(tmp_path.resolve()), date(2015, 1, 1), date(2026, 8, 10),
    )]


def test_prepare_verifies_viewer_once_per_invocation_for_many_cash_rows(
    tmp_path, monkeypatch,
):
    viewer_manifest = tmp_path / corporate_actions.VIEWER_MANIFEST_RELATIVE_PATH
    _write_json(viewer_manifest, {})
    rows = [
        {
            "stock_code": "005930",
            "rcept_no": f"20260701{index:06d}",
            "rcept_dt": "20260701",
            "report_nm": "현금ㆍ현물배당 결정",
        }
        for index in range(64)
    ]
    _write_json(
        tmp_path / "corporate_actions/dart/manifests"
        / "from=20260701/to=20260701/disclosures_v3.json",
        rows,
    )
    calls = []

    def fake_verify(base, *, required_start, required_end):
        calls.append((base, required_start, required_end))
        return SimpleNamespace(
            manifest_path=str(viewer_manifest.resolve()),
            manifest_sha256=hashlib.sha256(
                viewer_manifest.read_bytes(),
            ).hexdigest(),
            dependency_probes=(),
            receipts=(),
        )

    monkeypatch.setattr(
        corporate_actions, "verify_viewer_corrections", fake_verify,
    )
    monkeypatch.setattr(
        corporate_actions,
        "_viewer_index",
        lambda *_args, **_kwargs: pytest.fail(
            "prepare loop must not rebuild the viewer index"
        ),
    )
    for _ in range(2):
        events, _ = corporate_actions.prepare(
            str(tmp_path),
            coverage_start=date(2026, 7, 1),
            coverage_end=date(2026, 7, 1),
        )
        assert len(events) == 64
    assert calls == [
        (str(tmp_path.resolve()), date(2026, 7, 1), date(2026, 7, 1)),
        (str(tmp_path.resolve()), date(2026, 7, 1), date(2026, 7, 1)),
    ]


def test_prepare_does_not_reuse_verified_viewer_object_across_runs(
    tmp_path, monkeypatch,
):
    viewer_manifest = tmp_path / corporate_actions.VIEWER_MANIFEST_RELATIVE_PATH
    _write_json(viewer_manifest, {})
    body_path = tmp_path / "viewer/body.html"
    body_path.parent.mkdir(parents=True, exist_ok=True)
    original = b"<html>original</html>"
    body_path.write_bytes(original)
    evidence = SimpleNamespace(
        receipt_no="20260701000001",
        revision_root_receipt_no="20260701000001",
        economic_body_receipt_no="20260701000001",
        family_receipt_nos=("20260701000001",),
        official_family_order=("20260701000001",),
        attachment_keys=(),
        main_path="viewer/body.html",
        main_content_length=len(original),
        main_sha256=hashlib.sha256(original).hexdigest(),
        viewer_path="viewer/body.html",
        viewer_content_length=len(original),
        viewer_sha256=hashlib.sha256(original).hexdigest(),
        economic_viewer_path="viewer/body.html",
        economic_viewer_content_length=len(original),
        economic_viewer_sha256=hashlib.sha256(original).hexdigest(),
        economic_main_path="viewer/body.html",
        economic_main_content_length=len(original),
        economic_main_sha256=hashlib.sha256(original).hexdigest(),
    )
    calls = []

    def fake_verify(base, *, required_start, required_end):
        calls.append((base, required_start, required_end))
        return SimpleNamespace(
            manifest_path=str(viewer_manifest.resolve()),
            manifest_sha256=hashlib.sha256(
                viewer_manifest.read_bytes(),
            ).hexdigest(),
            dependency_probes=(),
            receipts=(evidence,),
        )

    monkeypatch.setattr(
        corporate_actions, "verify_viewer_corrections", fake_verify,
    )
    corporate_actions.prepare(
        str(tmp_path),
        coverage_start=date(2026, 7, 1),
        coverage_end=date(2026, 7, 1),
    )
    body_path.write_bytes(b"<html>tampered</html>")
    with pytest.raises(RuntimeError, match="changed after verification"):
        corporate_actions.prepare(
            str(tmp_path),
            coverage_start=date(2026, 7, 1),
            coverage_end=date(2026, 7, 1),
        )
    assert len(calls) == 2


def test_prepare_fails_if_economic_viewer_changes_after_verification(
    tmp_path, monkeypatch,
):
    viewer_manifest = tmp_path / corporate_actions.VIEWER_MANIFEST_RELATIVE_PATH
    _write_json(viewer_manifest, {})
    source = b"<html>attachment</html>"
    economic = b"<html>economic</html>"
    source_path = tmp_path / "viewer/source.html"
    economic_path = tmp_path / "viewer/economic.html"
    source_path.parent.mkdir(parents=True, exist_ok=True)
    source_path.write_bytes(source)
    economic_path.write_bytes(economic)
    evidence = SimpleNamespace(
        receipt_no="20260701000001",
        revision_root_receipt_no="20260701000001",
        economic_body_receipt_no="20260630000001",
        family_receipt_nos=("20260701000001", "20260630000001"),
        official_family_order=("20260630000001", "20260701000001"),
        attachment_keys=(),
        main_path="viewer/source.html",
        main_content_length=len(source),
        main_sha256=hashlib.sha256(source).hexdigest(),
        viewer_path="viewer/source.html",
        viewer_content_length=len(source),
        viewer_sha256=hashlib.sha256(source).hexdigest(),
        economic_viewer_path="viewer/economic.html",
        economic_viewer_content_length=len(economic),
        economic_viewer_sha256=hashlib.sha256(economic).hexdigest(),
        economic_main_path="viewer/economic.html",
        economic_main_content_length=len(economic),
        economic_main_sha256=hashlib.sha256(economic).hexdigest(),
    )

    def fake_verify(_base, *, required_start, required_end):
        del required_start, required_end
        economic_path.write_bytes(b"<html>tampered economic</html>")
        return SimpleNamespace(
            manifest_path=str(viewer_manifest.resolve()),
            manifest_sha256=hashlib.sha256(
                viewer_manifest.read_bytes(),
            ).hexdigest(),
            dependency_probes=(),
            receipts=(evidence,),
        )

    monkeypatch.setattr(
        corporate_actions, "verify_viewer_corrections", fake_verify,
    )
    with pytest.raises(RuntimeError, match="changed after verification"):
        corporate_actions.prepare(
            str(tmp_path),
            coverage_start=date(2026, 7, 1),
            coverage_end=date(2026, 7, 1),
        )


def test_prepare_fails_if_viewer_manifest_changes_mid_invocation(
    tmp_path, monkeypatch,
):
    viewer_manifest = tmp_path / corporate_actions.VIEWER_MANIFEST_RELATIVE_PATH
    _write_json(viewer_manifest, {})

    def fake_verify(_base, *, required_start, required_end):
        del required_start, required_end
        return SimpleNamespace(
            manifest_path=str(viewer_manifest.resolve()),
            manifest_sha256=hashlib.sha256(
                viewer_manifest.read_bytes(),
            ).hexdigest(),
            dependency_probes=(),
            receipts=(),
        )

    def mutate_manifest(_base, *, include_audit=False):
        _write_json(viewer_manifest, {"mutated": True})
        result = []
        return (result, {}) if include_audit else result

    monkeypatch.setattr(
        corporate_actions, "verify_viewer_corrections", fake_verify,
    )
    monkeypatch.setattr(corporate_actions, "_disclosure_rows", mutate_manifest)
    with pytest.raises(RuntimeError, match="changed during prepare"):
        corporate_actions.prepare(
            str(tmp_path),
            coverage_start=date(2026, 7, 1),
            coverage_end=date(2026, 7, 1),
        )


def test_prepare_fails_if_dependency_probe_changes_mid_invocation(
    tmp_path, monkeypatch,
):
    viewer_manifest = tmp_path / corporate_actions.VIEWER_MANIFEST_RELATIVE_PATH
    _write_json(viewer_manifest, {})
    probe_body = b"<html>dependency probe</html>"
    probe_path = tmp_path / "viewer/probe.html"
    probe_path.parent.mkdir(parents=True, exist_ok=True)
    probe_path.write_bytes(probe_body)
    probe = SimpleNamespace(
        main_path="viewer/probe.html",
        main_content_length=len(probe_body),
        main_sha256=hashlib.sha256(probe_body).hexdigest(),
    )

    def fake_verify(_base, *, required_start, required_end):
        del required_start, required_end
        return SimpleNamespace(
            manifest_path=str(viewer_manifest.resolve()),
            manifest_sha256=hashlib.sha256(
                viewer_manifest.read_bytes(),
            ).hexdigest(),
            dependency_probes=(probe,),
            receipts=(),
        )

    def mutate_probe(_base, *, include_audit=False):
        probe_path.write_bytes(b"<html>mutated dependency probe</html>")
        result = []
        return (result, {}) if include_audit else result

    monkeypatch.setattr(
        corporate_actions, "verify_viewer_corrections", fake_verify,
    )
    monkeypatch.setattr(corporate_actions, "_disclosure_rows", mutate_probe)
    with pytest.raises(RuntimeError, match="evidence changed during prepare"):
        corporate_actions.prepare(
            str(tmp_path),
            coverage_start=date(2026, 7, 1),
            coverage_end=date(2026, 7, 1),
        )


def test_prepare_fails_if_support_body_changes_mid_invocation(
    tmp_path, monkeypatch,
):
    support_manifest = (
        tmp_path / corporate_actions.SUPPORT_FAMILY_MANIFEST_RELATIVE_PATH
    )
    _write_json(support_manifest, {})
    body = b"support body"
    body_path = tmp_path / "support/body.html"
    body_path.parent.mkdir(parents=True, exist_ok=True)
    body_path.write_bytes(body)
    for marker in (
        "structured_complete_v3.json", "documents_complete_v5.json",
    ):
        (body_path.parent / marker).write_bytes(b"{}")
    digest = hashlib.sha256(body).hexdigest()
    source = SimpleNamespace(
        receipt_no="20260701000001",
        main_path="support/body.html",
        main_content_length=len(body),
        main_sha256=digest,
        body_path="support/body.html",
        body_content_length=len(body),
        body_sha256=digest,
        disclosure_path="support/body.html",
        disclosure_content_length=len(body),
        disclosure_sha256=digest,
        disclosure_manifest_path="support/body.html",
        disclosure_manifest_sha256=digest,
        structured_path=None,
        structured_content_length=None,
        structured_sha256=None,
    )
    entry = SimpleNamespace(
        root_receipt_no=source.receipt_no,
        terminal_receipt_no=source.receipt_no,
        terminal_economic_receipt_no=source.receipt_no,
        ordered_family_receipts=(source.receipt_no,),
        sources=(source,),
    )

    def fake_support(_base, *, required_start, required_end):
        del required_start, required_end
        return SimpleNamespace(
            manifest_path=str(support_manifest.resolve()),
            manifest_sha256=hashlib.sha256(
                support_manifest.read_bytes(),
            ).hexdigest(),
            entries=(entry,),
        )

    def mutate_support(_base, *, include_audit=False):
        body_path.write_bytes(b"mutated support body")
        result = []
        return (result, {}) if include_audit else result

    monkeypatch.setattr(
        corporate_actions, "verify_support_action_families", fake_support,
    )
    monkeypatch.setattr(corporate_actions, "_disclosure_rows", mutate_support)
    with pytest.raises(
        RuntimeError, match="support-family evidence changed during prepare",
    ):
        corporate_actions.prepare(
            str(tmp_path),
            coverage_start=date(2026, 7, 1),
            coverage_end=date(2026, 7, 1),
        )


def test_prepare_rejects_symlinked_evidence_path(
    tmp_path, monkeypatch,
):
    viewer_manifest = tmp_path / corporate_actions.VIEWER_MANIFEST_RELATIVE_PATH
    _write_json(viewer_manifest, {})
    actual = b"viewer body"
    actual_path = tmp_path / "viewer/actual.html"
    link_path = tmp_path / "viewer/link.html"
    actual_path.parent.mkdir(parents=True, exist_ok=True)
    actual_path.write_bytes(actual)
    link_path.symlink_to(actual_path.name)
    digest = hashlib.sha256(actual).hexdigest()
    evidence = SimpleNamespace(
        receipt_no="20260701000001",
        revision_root_receipt_no="20260701000001",
        economic_body_receipt_no="20260701000001",
        family_receipt_nos=("20260701000001",),
        official_family_order=("20260701000001",),
        attachment_keys=(),
        main_path="viewer/link.html",
        main_content_length=len(actual),
        main_sha256=digest,
        viewer_path="viewer/link.html",
        viewer_content_length=len(actual),
        viewer_sha256=digest,
        economic_main_path="viewer/link.html",
        economic_main_content_length=len(actual),
        economic_main_sha256=digest,
        economic_viewer_path="viewer/link.html",
        economic_viewer_content_length=len(actual),
        economic_viewer_sha256=digest,
    )

    def fake_verify(_base, *, required_start, required_end):
        del required_start, required_end
        return SimpleNamespace(
            manifest_path=str(viewer_manifest.resolve()),
            manifest_sha256=hashlib.sha256(
                viewer_manifest.read_bytes(),
            ).hexdigest(),
            dependency_probes=(),
            receipts=(evidence,),
        )

    monkeypatch.setattr(
        corporate_actions, "verify_viewer_corrections", fake_verify,
    )
    with pytest.raises(RuntimeError, match="symlinked evidence path"):
        corporate_actions.prepare(
            str(tmp_path),
            coverage_start=date(2026, 7, 1),
            coverage_end=date(2026, 7, 1),
        )


def test_scoped_prepare_requires_paired_valid_coverage(tmp_path):
    with pytest.raises(ValueError, match="provided together"):
        corporate_actions.prepare(
            str(tmp_path), coverage_start=date(2015, 1, 1),
        )
    with pytest.raises(ValueError, match="precedes"):
        corporate_actions.prepare(
            str(tmp_path),
            coverage_start=date(2015, 1, 2),
            coverage_end=date(2015, 1, 1),
        )


def test_every_production_corporate_action_consumer_declares_exact_coverage():
    pipeline_root = Path(__file__).parents[2] / "pipeline"
    violations = []
    for path in sorted(pipeline_root.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if not (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "prepare"
                and isinstance(node.func.value, ast.Name)
                and node.func.value.id == "corporate_actions"
            ):
                continue
            keywords = {item.arg for item in node.keywords}
            if not {"coverage_start", "coverage_end"}.issubset(keywords):
                violations.append(
                    f"{path.relative_to(pipeline_root.parent)}:{node.lineno}"
                )
    assert violations == []


def test_prepare_normalizes_structured_factor_and_exchange_notice(tmp_path):
    structured = (
        tmp_path
        / "corporate_actions/dart/structured/event=bonus_issue/year=2026"
        / "corp=005930/rcept=20260601000001.json"
    )
    _write_json(structured, {
        "rcept_no": "20260601000001",
        "nstk_dividrk": "2026년 07월 08일",
        "nstk_ascnt_ps_ostk": "1.0",
    })
    manifest = (
        tmp_path
        / "corporate_actions/dart/manifests"
        / "from=20260701/to=20260708/disclosures_v3.json"
    )
    _write_json(manifest, [{
        "stock_code": "005930",
        "rcept_no": "20260707000002",
        "rcept_dt": "20260707",
        "report_nm": "권리락(무상증자)",
    }])

    events, stats = corporate_actions.prepare(str(tmp_path))

    assert len(events) == 2
    bonus = events[events["source"].eq("DART_STRUCTURED")].iloc[0]
    assert bonus["event_type"] == "bonus_issue"
    assert bonus["effective_date"] == date(2026, 7, 8)
    assert bonus["expected_factor"] == pytest.approx(0.5)
    notice = events[events["source"].eq("DART_DISCLOSURE")].iloc[0]
    assert notice["event_type"] == "rights_detachment"
    assert notice["effective_date"] == date(2026, 7, 7)
    assert stats["effective_date_count"] == 2
    assert stats["expected_factor_count"] == 1


def test_cancelled_structured_bonus_remains_issuer_revision_but_not_support(tmp_path):
    structured = (
        tmp_path
        / "corporate_actions/dart/structured/event=bonus_issue/year=2026"
        / "corp=005930/rcept=20260601000009.json"
    )
    _write_json(structured, {
        "rcept_no": "20260601000009",
        "nstk_dividrk": "2026년 07월 08일",
        "nstk_ascnt_ps_ostk": "1.0",
    })
    manifest = (
        tmp_path
        / "corporate_actions/dart/manifests"
        / "from=20260601/to=20260601/disclosures_v3.json"
    )
    _write_json(manifest, [{
        "stock_code": "005930",
        "rcept_no": "20260601000009",
        "rcept_dt": "20260601",
        "report_nm": "무상증자결정(취소)",
    }])

    events, _ = corporate_actions.prepare(str(tmp_path))

    bonus = events[events["event_type"].eq("bonus_issue")].iloc[0]
    assert bonus["action_scope"] == "ISSUER"
    assert not bonus["confirms_price_adjustment"]
    assert not bonus["expects_price_adjustment"]


@pytest.mark.parametrize(
    ("report_name", "scope", "expects"),
    [
        ("[기재정정]주요사항보고서(무상증자결정)", "ISSUER", True),
        (
            "주요사항보고서(무상증자결정)(종속회사의 주요경영사항)",
            "RELATED_COMPANY",
            False,
        ),
    ],
)
def test_structured_revision_and_related_company_scope_are_separate(
    tmp_path, report_name, scope, expects,
):
    receipt = "20260601000019"
    structured = (
        tmp_path
        / "corporate_actions/dart/structured/event=bonus_issue/year=2026"
        / f"corp=005930/rcept={receipt}.json"
    )
    _write_json(structured, {
        "rcept_no": receipt,
        "nstk_dividrk": "2026년 07월 08일",
        "nstk_ascnt_ps_ostk": "1.0",
    })
    manifest = (
        tmp_path
        / "corporate_actions/dart/manifests"
        / "from=20260601/to=20260601/disclosures_v3.json"
    )
    _write_json(manifest, [{
        "stock_code": "005930",
        "rcept_no": receipt,
        "rcept_dt": "20260601",
        "report_nm": report_name,
    }])

    events, _ = corporate_actions.prepare(str(tmp_path))
    bonus = events[events["event_type"].eq("bonus_issue")].iloc[0]

    assert bonus["action_scope"] == scope
    assert bool(bonus["expects_price_adjustment"]) is expects
    assert bool(bonus["confirms_price_adjustment"]) is expects


def test_rights_notice_uses_document_execution_date(tmp_path):
    manifest = (
        tmp_path
        / "corporate_actions/dart/manifests"
        / "from=20260202/to=20260731/disclosures_v3.json"
    )
    _write_json(manifest, [{
        "stock_code": "008830",
        "rcept_no": "20260731901116",
        "rcept_dt": "20260731",
        "report_nm": "권리락 (무상증자)",
    }])
    document = (
        tmp_path
        / "corporate_actions/dart/documents/year=2026/corp=008830"
        / "rcept=20260731901116.zip"
    )
    _write_document(
        document,
        "<document><label>권리락 실시일</label>"
        "<value>2026-08-03</value></document>",
    )

    events, _ = corporate_actions.prepare(str(tmp_path))

    event = events.iloc[0]
    assert event["effective_date"] == date(2026, 8, 3)
    assert event["match_window_days"] == 0


def test_stock_dividend_decision_keeps_record_date_out_of_ex_date(tmp_path):
    interval = (
        tmp_path
        / "corporate_actions/dart/manifests"
        / "from=20261201/to=20261231"
    )
    # Stale v1/v2 manifests must not mask the v3 discovery contract.
    _write_json(interval / "disclosures.json", [])
    _write_json(interval / "disclosures_v3.json", [{
        "stock_code": "001040",
        "rcept_no": "20261220000001",
        "rcept_dt": "20261220",
        "report_nm": "주식배당결정",
    }])
    document = (
        tmp_path
        / "corporate_actions/dart/documents/year=2026/corp=001040"
        / "rcept=20261220000001.zip"
    )
    _write_document(
        document,
        "<document><label>배당기준일</label>"
        "<value>2026-12-31</value><label>주식배당결정</label></document>",
    )

    events, _ = corporate_actions.prepare(str(tmp_path))

    event = events.iloc[0]
    assert event["event_type"] == "stock_dividend"
    assert event["record_date"] == date(2026, 12, 31)
    assert pd.isna(event["effective_date"])
    assert event["match_window_days"] == 0
    assert not event["confirms_price_adjustment"]
    assert event["expects_price_adjustment"]
    published = corporate_actions.normalize_for_publish(events).iloc[0]
    assert pd.isna(published["ex_date"])
    assert published["record_date"] == date(2026, 12, 31)


def test_iwin_stock_dividend_decision_publishes_exact_ordinary_ratio(tmp_path):
    interval = (
        tmp_path
        / "corporate_actions/dart/manifests"
        / "from=20211201/to=20211231"
    )
    _write_json(interval / "disclosures_v3.json", [{
        "stock_code": "090150",
        "rcept_no": "20211224900781",
        "rcept_dt": "20211224",
        "report_nm": "주식배당결정(정정)",
    }])
    document = (
        tmp_path
        / "corporate_actions/dart/documents/year=2021/corp=090150"
        / "rcept=20211224900781.zip"
    )
    _write_document(
        document,
        "<document>주식배당결정 배당기준일 2021-12-31<table>"
        "<tr><td>1주당 주식배당</td><td>보통주식</td>"
        "<td>0.1주</td></tr></table></document>",
    )

    events, _ = corporate_actions.prepare(str(tmp_path))
    event = events.iloc[0]
    published = corporate_actions.normalize_for_publish(events).iloc[0]

    assert event["record_date"] == date(2021, 12, 31)
    assert event["ratio_numerator"] == pytest.approx(0.1)
    assert event["ratio_denominator"] == pytest.approx(1.0)
    assert published["ratio_numerator"] == pytest.approx(0.1)
    assert published["ratio_denominator"] == pytest.approx(1.0)


def test_stock_dividend_ratio_ignores_issued_share_total_lure(tmp_path):
    interval = (
        tmp_path / "corporate_actions/dart/manifests"
        / "from=20260301/to=20260331"
    )
    receipt = "20260313809999"
    _write_json(interval / "disclosures_v3.json", [{
        "stock_code": "006800",
        "rcept_no": receipt,
        "rcept_dt": "20260313",
        "report_nm": "[기재정정]주식배당결정",
    }])
    document = (
        tmp_path / "corporate_actions/dart/documents/year=2026/corp=006800"
        / f"rcept={receipt}.zip"
    )
    _write_document(
        document,
        "<document>주식배당결정 배당기준일 2026-03-17<table>"
        "<tr><td>발행주식총수</td><td>보통주식</td>"
        "<td>555,316,408</td></tr>"
        "<tr><td>1주당 배당주식수(주)</td><td>보통주식</td>"
        "<td>0.0073206</td></tr>"
        "<tr><td>배당주식총수</td><td>보통주식</td>"
        "<td>4,065,108</td></tr></table></document>",
    )

    events, _ = corporate_actions.prepare(str(tmp_path))

    assert len(events) == 1
    assert events.iloc[0]["ratio_numerator"] == pytest.approx(0.0073206)


def test_combined_detachment_requires_exact_date_reference_and_reason(tmp_path):
    manifest = (
        tmp_path / "corporate_actions/dart/manifests"
        / "from=20211201/to=20211231/disclosures_v3.json"
    )
    _write_json(manifest, [{
        "stock_code": "005950",
        "rcept_no": "20211228900755",
        "rcept_dt": "20211228",
        "report_nm": "권배락(무상증자 및 배당)",
    }])
    document = (
        tmp_path / "corporate_actions/dart/documents/year=2021/corp=005950"
        / "rcept=20211228900755.zip"
    )
    _write_document(document, """
        <table>
          <tr><td>권배락 실시일</td><td>2021-12-29</td></tr>
          <tr><td>기준가격</td><td>4,960</td></tr>
          <tr><td>사유</td><td>무상증자 및 배당</td></tr>
        </table>
    """)

    events, _ = corporate_actions.prepare(str(tmp_path))

    event = events.iloc[0]
    assert event["event_type"] == "combined_detachment"
    assert event["effective_date"] == date(2021, 12, 29)
    assert event["confirms_price_adjustment"]
    assert event["action_method"] == "무상증자 및 배당"


def test_combined_detachment_duplicate_reference_field_is_not_confirmed(tmp_path):
    manifest = (
        tmp_path / "corporate_actions/dart/manifests"
        / "from=20211201/to=20211231/disclosures_v3.json"
    )
    _write_json(manifest, [{
        "stock_code": "005950", "rcept_no": "20211228900755",
        "rcept_dt": "20211228", "report_nm": "권배락",
    }])
    document = (
        tmp_path / "corporate_actions/dart/documents/year=2021/corp=005950"
        / "rcept=20211228900755.zip"
    )
    _write_document(document, """
        <table>
          <tr><td>권배락 실시일</td><td>2021-12-29</td></tr>
          <tr><td>기준가격</td><td>5,100</td></tr>
          <tr><td>기준가격</td><td>4,960</td></tr>
          <tr><td>사유</td><td>무상증자 및 배당</td></tr>
        </table>
    """)

    events, _ = corporate_actions.prepare(str(tmp_path))

    assert not events.iloc[0]["confirms_price_adjustment"]


def test_capital_reduction_share_factor_is_not_a_price_factor(tmp_path):
    structured = (
        tmp_path
        / "corporate_actions/dart/structured/event=capital_reduction/year=2026"
        / "corp=005930/rcept=20260601000003.json"
    )
    _write_json(structured, {
        "rcept_no": "20260601000003",
        "cr_std": "2026년 07월 08일",
        "bfcr_tisstk_ostk": "8,000",
        "atcr_tisstk_ostk": "1,000",
        "cr_mth": "보통주식 8대 1 무상감자",
    })

    events, _ = corporate_actions.prepare(str(tmp_path))

    assert events.iloc[0]["event_type"] == "capital_reduction"
    assert pd.isna(events.iloc[0]["expected_factor"])
    assert events.iloc[0]["share_count_factor"] == pytest.approx(8.0)
    assert events.iloc[0]["share_count_before"] == pytest.approx(8_000)
    assert events.iloc[0]["share_count_after"] == pytest.approx(1_000)
    assert events.iloc[0]["share_count_factor_comparable"]
    assert (
        events.iloc[0]["share_count_comparison_reason"]
        == "UNIFORM_REDUCTION"
    )
    assert events.iloc[0]["action_method"] == "보통주식 8대 1 무상감자"


def test_non_uniform_reduction_is_not_share_factor_comparable(tmp_path):
    structured = (
        tmp_path
        / "corporate_actions/dart/structured/event=capital_reduction/year=2026"
        / "corp=005930/rcept=20260601000005.json"
    )
    _write_json(structured, {
        "rcept_no": "20260601000005",
        "cr_std": "2026년 07월 08일",
        "bfcr_tisstk_ostk": "8,000",
        "atcr_tisstk_ostk": "1,000",
        "cr_mth": "최대주주 보유주식만 8대 1 병합",
    })

    events, _ = corporate_actions.prepare(str(tmp_path))

    assert not events.iloc[0]["share_count_factor_comparable"]


def test_combined_offering_does_not_infer_factor_from_bonus_leg_only(tmp_path):
    structured = (
        tmp_path
        / "corporate_actions/dart/structured/event=combined_offering/year=2026"
        / "corp=005930/rcept=20260601000004.json"
    )
    _write_json(structured, {
        "rcept_no": "20260601000004",
        "fric_nstk_asstd": "2026년 07월 08일",
        "fric_nstk_ascnt_ps_ostk": "1.0",
    })

    events, _ = corporate_actions.prepare(str(tmp_path))

    assert events.iloc[0]["event_type"] == "combined_offering"
    assert pd.isna(events.iloc[0]["expected_factor"])


def test_simultaneous_financing_makes_reduction_ratio_not_comparable(tmp_path):
    reduction = (
        tmp_path
        / "corporate_actions/dart/structured/event=capital_reduction/year=2026"
        / "corp=005930/rcept=20260601000010.json"
    )
    financing = (
        tmp_path
        / "corporate_actions/dart/structured/event=paid_increase/year=2026"
        / "corp=005930/rcept=20260601000011.json"
    )
    _write_json(reduction, {
        "rcept_no": "20260601000010",
        "cr_std": "2026년 07월 08일",
        "bfcr_tisstk_ostk": "8,000",
        "atcr_tisstk_ostk": "1,000",
        "cr_mth": "보통주식 8대 1 무상감자",
    })
    _write_json(financing, {
        "rcept_no": "20260601000011",
    })

    events, _ = corporate_actions.prepare(str(tmp_path))

    reduction_event = events[
        events["event_type"].eq("capital_reduction")
    ].iloc[0]
    assert not reduction_event["share_count_factor_comparable"]
    assert (
        reduction_event["share_count_comparison_reason"]
        == "SIMULTANEOUS_FINANCING_DISCLOSURE"
    )


def test_ex_dividend_notice_is_evidence_not_required_price_adjustment(tmp_path):
    manifest = (
        tmp_path
        / "corporate_actions/dart/manifests"
        / "from=20260701/to=20260708/disclosures_v3.json"
    )
    _write_json(manifest, [{
        "stock_code": "005930",
        "rcept_no": "20260707000005",
        "rcept_dt": "20260707",
        "report_nm": "배당락",
    }])
    events, _ = corporate_actions.prepare(str(tmp_path))
    event = events.iloc[0]
    assert event["event_type"] == "ex_dividend"
    assert not event["confirms_price_adjustment"]
    assert not event["expects_price_adjustment"]
    assert pd.isna(event["effective_date"])


def test_ex_dividend_notice_uses_only_actual_execution_date(tmp_path):
    manifest = (
        tmp_path
        / "corporate_actions/dart/manifests"
        / "from=20260701/to=20260708/disclosures_v3.json"
    )
    _write_json(manifest, [{
        "stock_code": "005930",
        "rcept_no": "20260707000006",
        "rcept_dt": "20260707",
        "report_nm": "배당락",
    }])
    document = (
        tmp_path
        / "corporate_actions/dart/documents/year=2026/corp=005930"
        / "rcept=20260707000006.zip"
    )
    _write_document(
        document,
        "<document><label>배당락 실시일</label>"
        "<value>2026-07-06</value></document>",
    )

    events, _ = corporate_actions.prepare(str(tmp_path))

    event = events.iloc[0]
    assert event["effective_date"] == date(2026, 7, 6)
    assert event["confirms_price_adjustment"]


def test_cash_dividend_decision_parses_common_amount_dates_and_frequency(tmp_path):
    manifest = (
        tmp_path / "corporate_actions/dart/manifests"
        / "from=20260724/to=20260724/disclosures_v3.json"
    )
    _write_json(manifest, [{
        "stock_code": "006120",
        "corp_cls": "K",
        "rcept_no": "20260724800658",
        "rcept_dt": "20260724",
        "report_nm": "현금ㆍ현물배당결정",
    }])
    document = (
        tmp_path / "corporate_actions/dart/documents/year=2026/corp=006120"
        / "rcept=20260724800658.zip"
    )
    _write_document(document, """
        <document>1. 배당구분 중간배당 2. 배당종류 현금배당
        3. 1주당 배당금(원) 보통주식 500 종류주식 500
        6. 배당기준일 2026-08-10
        7. 배당금지급 예정일자 2026-08-21
        주석 배당기준일 2026년 8월 10일</document>
    """)

    events, _ = corporate_actions.prepare(str(tmp_path))
    event = events.iloc[0]
    assert event["event_type"] == "cash_dividend"
    assert event["cash_amount"] == pytest.approx(500)
    assert event["record_date"] == date(2026, 8, 10)
    assert event["payment_date"] == date(2026, 8, 21)
    assert event["frequency"] == "interim"
    published = corporate_actions.normalize_for_publish(events).iloc[0]
    assert published["cash_amount"] == pytest.approx(500)
    assert published["adjusted_cash_amount"] is None
    assert published["action_scope"] == "ISSUER"
    assert published["report_name"] == "현금ㆍ현물배당결정"
    assert published["corp_cls"] == "K"


def test_zero_common_dps_normalizes_to_no_common_with_null_amount(tmp_path):
    manifest = (
        tmp_path / "corporate_actions/dart/manifests"
        / "from=20250101/to=20251231/disclosures_v3.json"
    )
    _write_json(manifest, [{
        "stock_code": "010950",
        "rcept_no": "20150227801008",
        "rcept_dt": "20150227",
        "report_nm": "현금ㆍ현물배당결정",
    }])
    document = (
        tmp_path / "corporate_actions/dart/documents/year=2015/corp=010950"
        / "rcept=20150227801008.zip"
    )
    _write_document(
        document,
        "<document>1주당 배당금(원) 보통주식 0 "
        "배당기준일 2014-12-31</document>",
    )

    events, _ = corporate_actions.prepare(str(tmp_path))

    event = events.iloc[0]
    assert event["cash_amount_status"] == "NO_COMMON_CASH_DIVIDEND"
    assert pd.isna(event["cash_amount"])


def test_positive_original_without_record_date_is_explicitly_pending(tmp_path):
    manifest = (
        tmp_path / "corporate_actions/dart/manifests"
        / "from=20240101/to=20241231/disclosures_v3.json"
    )
    _write_json(manifest, [{
        "stock_code": "029780",
        "rcept_no": "20240129800952",
        "rcept_dt": "20240129",
        "report_nm": "현금ㆍ현물배당결정",
    }])
    document = (
        tmp_path / "corporate_actions/dart/documents/year=2024/corp=029780"
        / "rcept=20240129800952.zip"
    )
    _write_document(
        document,
        "<document>1주당 배당금(원) 보통주식 2,500 "
        "배당기준일 -</document>",
    )

    events, _ = corporate_actions.prepare(str(tmp_path))

    event = events.iloc[0]
    assert event["cash_amount_status"] == "POSITIVE_PENDING_RECORD_DATE"
    assert event["cash_amount"] == pytest.approx(2500)
    assert pd.isna(event["record_date"])


@pytest.mark.parametrize(
    ("ticker", "receipt"),
    [
        ("0008Z0", "20260120900486"),
        ("0010V0", "20260206900936"),
        ("0039P0", "20260708900856"),
    ],
)
def test_cash_dividend_parser_preserves_new_alphanumeric_krx_tickers(
    tmp_path, ticker, receipt,
):
    manifest = (
        tmp_path / "corporate_actions/dart/manifests"
        / "from=20260101/to=20260731/disclosures_v3.json"
    )
    _write_json(manifest, [{
        "stock_code": ticker.lower(),
        "corp_cls": "K",
        "rcept_no": receipt,
        "rcept_dt": receipt[:8],
        "report_nm": "현금ㆍ현물배당결정",
    }])
    document = (
        tmp_path / "corporate_actions/dart/documents/year=2026"
        / f"corp={ticker}" / f"rcept={receipt}.zip"
    )
    _write_document(
        document,
        "<document>1주당 배당금(원) 보통주식 150 "
        "배당기준일 2026-07-31</document>",
    )

    events, _ = corporate_actions.prepare(str(tmp_path))

    assert events["identifier"].tolist() == [ticker]
    assert events.iloc[0]["cash_amount"] == pytest.approx(150)
    assert events.iloc[0]["record_date"] == date(2026, 7, 31)


def test_ticker_path_fallback_uppercases_alphanumeric_krx_code():
    assert corporate_actions._ticker_from_path(
        "/structured/year=2026/corp=0008z0/rcept=receipt.json"
    ) == "0008Z0"


def test_subsidiary_cash_dividend_is_not_assigned_to_parent_ticker(tmp_path):
    manifest = (
        tmp_path / "corporate_actions/dart/manifests"
        / "from=20260312/to=20260312/disclosures_v3.json"
    )
    _write_json(manifest, [{
        "stock_code": "128940",
        "rcept_no": "20260312800001",
        "rcept_dt": "20260312",
        "report_nm": "현금ㆍ현물배당 결정(자회사의 주요경영사항)",
    }])
    document = (
        tmp_path / "corporate_actions/dart/documents/year=2026/corp=128940"
        / "rcept=20260312800001.zip"
    )
    _write_document(
        document,
        "<document>자회사인 한미약품의 주요경영사항 "
        "1주당 배당금(원) 보통주식 2,000 "
        "배당기준일 2026-03-31</document>",
    )

    events, _ = corporate_actions.prepare(str(tmp_path))

    assert events.empty


def test_subsidiary_form_body_is_excluded_even_without_title_suffix(tmp_path):
    manifest = (
        tmp_path / "corporate_actions/dart/manifests"
        / "from=20260312/to=20260312/disclosures_v3.json"
    )
    _write_json(manifest, [{
        "stock_code": "128940",
        "rcept_no": "20260312800002",
        "rcept_dt": "20260312",
        "report_nm": "현금ㆍ현물배당 결정",
    }])
    document = (
        tmp_path / "corporate_actions/dart/documents/year=2026/corp=128940"
        / "rcept=20260312800002.zip"
    )
    _write_document(
        document,
        "<document>자회사인 한미약품 주식회사의 주요경영사항신고 "
        "1주당 배당금(원) 보통주식 2,000 "
        "배당기준일 2026-03-31</document>",
    )

    events, _ = corporate_actions.prepare(str(tmp_path))

    assert events.empty


def test_original_cash_filing_is_removed_when_correction_marks_subsidiary(
    tmp_path,
):
    manifest = (
        tmp_path / "corporate_actions/dart/manifests"
        / "from=20230310/to=20230313/disclosures_v3.json"
    )
    _write_json(manifest, [
        {
            "stock_code": "009440",
            "rcept_no": "20230310801178",
            "rcept_dt": "20230310",
            "report_nm": "현금ㆍ현물배당 결정",
        },
        {
            "stock_code": "009440",
            "rcept_no": "20230313800096",
            "rcept_dt": "20230313",
            "report_nm": "[기재정정]현금ㆍ현물배당 결정(자회사의 주요경영사항)",
        },
    ])
    original = (
        tmp_path / "corporate_actions/dart/documents/year=2023/corp=009440"
        / "rcept=20230310801178.zip"
    )
    correction = (
        tmp_path / "corporate_actions/dart/documents/year=2023/corp=009440"
        / "rcept=20230313800096.zip"
    )
    body = (
        "1주당 배당금(원) 보통주식 2,949 "
        "배당기준일 2022-12-31 배당금지급 예정일자 2023-04-28"
    )
    _write_document(original, f"<document>{body}</document>")
    _write_document(
        correction,
        "<document>정정관련 공시서류제출일 2023-03-10 "
        f"{body} 자회사인 케이씨환경서비스의 주요경영사항</document>",
    )

    events, stats = corporate_actions.prepare(str(tmp_path))

    assert events.empty
    assert stats["related_company_correction_excluded_count"] == 1


def test_corrected_cash_dividend_uses_last_body_values_without_joining_numbers(
    tmp_path,
):
    manifest = (
        tmp_path / "corporate_actions/dart/manifests"
        / "from=20260410/to=20260410/disclosures_v3.json"
    )
    _write_json(manifest, [{
        "stock_code": "005930",
        "rcept_no": "20260410800001",
        "rcept_dt": "20260410",
        "report_nm": "[기재정정]현금ㆍ현물배당 결정",
    }])
    document = (
        tmp_path / "corporate_actions/dart/documents/year=2026/corp=005930"
        / "rcept=20260410800001.zip"
    )
    _write_document(document, """
        <document>
          <table class="correction">
            <tr><td>정정전</td><td>1주당 배당금(원)</td>
                <td>보통주식 300</td><td>정정후 500</td></tr>
            <tr><td>배당기준일</td><td>2026-03-30</td>
                <td>2026-03-31</td></tr>
          </table>
          <section class="corrected-body">
            1. 배당구분 분기배당
            3. 1 주당 배당금 ( 원 ) 보통주식 500 종류주식 -
            6. 배당 기준일 : 2026 - 03 - 31
            7. 배당금 지급 예정일자 : 2026.04.25
          </section>
        </document>
    """)

    events, _ = corporate_actions.prepare(str(tmp_path))

    event = events.iloc[0]
    assert event["cash_amount"] == pytest.approx(500)
    assert event["record_date"] == date(2026, 3, 31)
    assert event["payment_date"] == date(2026, 4, 25)
    assert event["frequency"] == "quarterly"


def test_common_issuer_events_are_inherited_by_preferred_share():
    event = pd.DataFrame([{
        "identifier": "001520",
        "event_type": "delisting",
        "rcept_no": "20260707000006",
    }])
    expanded, stats = corporate_actions.inherit_issuer_events(
        event,
        {"001529": "001520"},
    )
    inherited = expanded[expanded["identifier"].eq("001529")].iloc[0]
    assert inherited["issuer_event_inherited"]
    assert inherited["issuer_parent_identifier"] == "001520"
    assert stats["inherited_event_count"] == 1


def test_nontradable_actions_are_explicitly_excluded():
    events = pd.DataFrame([
        {
            "identifier": "005930", "event_type": "bonus_issue",
            "effective_date": date(2026, 8, 1), "announcement_date": None,
        },
        {
            "identifier": "026870", "event_type": "bonus_issue",
            "effective_date": date(2026, 8, 2), "announcement_date": None,
        },
        {
            "identifier": "123456", "event_type": "cash_dividend",
            "effective_date": None, "announcement_date": date(2026, 8, 3),
        },
    ])

    retained, stats = corporate_actions.exclude_nontradable(
        events,
        {"row_count": 3},
        {"005930"},
        {"123456"},
    )

    assert list(retained["identifier"]) == ["005930"]
    assert stats["transformed_rows"] == 1
    assert stats["excluded_rows"] == 2
    assert stats["no_tradable_price_action"]["row_count"] == 1
    assert stats["unsupported_market_action"]["row_count"] == 1


def test_immutable_overlap_disclosure_receipt_fails_closed(tmp_path):
    for start, end, ticker in (
        ("20260101", "20260131", "005930"),
        ("20260115", "20260201", "000660"),
    ):
        manifest = (
            tmp_path / "corporate_actions/dart/manifests"
            / f"from={start}" / f"to={end}/disclosures_v3.json"
        )
        _write_json(manifest, [{
            "stock_code": ticker,
            "rcept_no": "20260102900228",
            "rcept_dt": "20260102",
            "report_nm": "현금ㆍ현물배당결정",
            "rm": "유",
        }])

    with pytest.raises(RuntimeError, match="immutable DART disclosure"):
        corporate_actions._disclosure_rows(str(tmp_path))
