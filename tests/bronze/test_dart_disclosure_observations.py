import pytest

from pipeline.bronze.dart_disclosure_observations import (
    canonicalize_disclosures,
    immutable_disclosure_changes,
)


def test_display_only_disclosure_changes_are_not_immutable():
    original = {
        "rcept_no": "20191210000064",
        "stock_code": "299900",
        "report_nm": "주요사항보고서(무상증자결정)",
        "corp_cls": "K",
    }
    current = {**original, "corp_cls": "E"}

    assert immutable_disclosure_changes(original, current) == ()


def test_economic_identity_disclosure_changes_remain_blocked():
    original = {
        "rcept_no": "20191210000064",
        "stock_code": "299900",
        "report_nm": "주요사항보고서(무상증자결정)",
        "corp_cls": "K",
    }
    changed = {**original, "stock_code": "000000"}

    assert immutable_disclosure_changes(original, changed) == ("stock_code",)


def test_correction_order_report_marker_is_conditionally_mutable(tmp_path):
    original = {
        "rcept_no": "20260826000752",
        "stock_code": "361610",
        "report_nm": "[첨부정정]주요사항보고서(회사합병결정)",
        "corp_cls": "Y",
    }
    current = {
        **original,
        "report_nm": (
            "[정정명령부과][첨부정정]주요사항보고서(회사합병결정)"
        ),
    }
    old_path = (
        tmp_path / "from=20260818" / "to=20260901"
        / "disclosures_v3.json"
    )
    new_path = (
        tmp_path / "from=20260819" / "to=20260902"
        / "disclosures_v3.json"
    )

    assert immutable_disclosure_changes(original, current) == ()
    canonical, audit = canonicalize_disclosures([
        (old_path, original),
        (new_path, current),
    ])

    assert canonical["20260826000752"] == (str(new_path), current)
    assert audit["mutable_conflict_field_counts"] == {"report_nm": 1}
    assert audit["conditional_mutable_fields"] == {
        "report_nm": "leading_[정정명령부과]_display_marker_only",
    }


def test_report_semantics_change_remains_blocked():
    original = {
        "rcept_no": "20260826000752",
        "stock_code": "361610",
        "report_nm": "[첨부정정]주요사항보고서(회사합병결정)",
    }
    changed = {
        **original,
        "report_nm": "[정정명령부과]주요사항보고서(유상증자결정)",
    }

    assert immutable_disclosure_changes(original, changed) == ("report_nm",)


def test_latest_explicit_interval_resolves_mutable_overlap(tmp_path):
    original = {
        "rcept_no": "20260819900668",
        "stock_code": "471050",
        "report_nm": "주권매매거래정지",
        "corp_name": "대신밸런스제17호스팩",
        "corp_cls": "K",
    }
    current = {
        **original,
        "corp_name": "대신밸런스제17호기업인수목적",
        "corp_cls": "E",
    }
    old_path = (
        tmp_path / "from=20260812" / "to=20260831"
        / "disclosures_v3.json"
    )
    new_path = (
        tmp_path / "from=20260817" / "to=20260831"
        / "disclosures_v3.json"
    )

    canonical, audit = canonicalize_disclosures([
        (old_path, original),
        (new_path, current),
    ])

    assert canonical["20260819900668"] == (str(new_path), current)
    assert audit["contract"] == (
        "latest_manifest_interval_mutable_list_fields_v3"
    )


def test_same_explicit_interval_mutable_conflict_fails_closed(tmp_path):
    row = {
        "rcept_no": "20260819900668",
        "stock_code": "471050",
        "report_nm": "주권매매거래정지",
        "corp_name": "old",
    }
    left = (
        tmp_path / "left" / "from=20260817" / "to=20260831"
        / "disclosures_v3.json"
    )
    right = (
        tmp_path / "right" / "from=20260817" / "to=20260831"
        / "disclosures_v3.json"
    )

    with pytest.raises(RuntimeError, match="same latest interval"):
        canonicalize_disclosures([
            (left, row),
            (right, {**row, "corp_name": "new"}),
        ])
