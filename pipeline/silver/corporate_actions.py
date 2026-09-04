"""DART 기업행사 Bronze를 가격 DQ용 표준 이벤트로 변환한다.

원천 공시를 수정하지 않고 다음 증거만 표준화한다.

- DART 구조화 주요사항보고서: 효력일과 계산 가능한 주식수 조정계수
- DART/거래소 공시 목록: 권리락·배당락·액면분할·병합 등 직접 공시일

이 결과는 Silver 테이블에 publish하지 않고 DQ 규칙의 외부 근거로 사용한다.
"""
from __future__ import annotations

import glob
import hashlib
import html
import json
import re
import zipfile
from collections import OrderedDict
from copy import deepcopy
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from types import MappingProxyType
from typing import Mapping
from uuid import UUID

import pandas as pd

from pipeline.bronze.dart_disclosure_observations import (
    canonicalize_disclosures,
)
from pipeline.bronze.dart_support_action_families import (
    MANIFEST_RELATIVE_PATH as SUPPORT_FAMILY_MANIFEST_RELATIVE_PATH,
    stock_dividend_common_ratio_from_body,
    verify_support_action_families,
)
from pipeline.common import db
from pipeline.bronze.dart_viewer_corrections import (
    KNOWN_DAMAGED_DOCUMENT_RECEIPTS,
    MANIFEST_RELATIVE_PATH as VIEWER_MANIFEST_RELATIVE_PATH,
    verify_viewer_corrections,
)
from pipeline.silver.return_contract import (
    acquire_return_writer_transaction_lock,
    invalidate_krx_total_return,
    normalize_krx_ticker,
)
from pipeline.silver.reviewed_dividend_corrections import (
    apply_reviewed_correction,
)


COLUMNS = [
    "identifier",
    "event_type",
    "announcement_date",
    "effective_date",
    "match_window_days",
    "expected_factor",
    "share_count_factor",
    "share_count_before",
    "share_count_after",
    "share_count_factor_comparable",
    "share_count_comparison_reason",
    "action_method",
    "record_date",
    "payment_date",
    "cash_amount",
    "adjusted_cash_amount",
    "ratio_numerator",
    "ratio_denominator",
    "currency",
    "frequency",
    "confirms_price_adjustment",
    "expects_price_adjustment",
    "confidence",
    "rcept_no",
    "report_name",
    "dart_rm",
    "corp_cls",
    "action_scope",
    "cash_amount_status",
    "source_evidence_status",
    "correction_of_action_key",
    "revision_root_action_key",
    "revision_kind",
    "viewer_evidence_sha256",
    "economic_evidence_sha256",
    "reviewed_correction_id",
    "payment_date_quality_status",
    "source_body_sha256",
    "source",
    "source_file",
]


# A closed daily run validates the same content-addressed snapshot once for
# preview and once for publication. Keep the parsed frame in-process so the
# second phase does not reopen and parse the complete historical Bronze set.
# Callers must provide the SHA-256 of a successfully verified snapshot; direct
# and unverified prepare() calls deliberately bypass this cache.
_PREPARE_CACHE_MAX_ENTRIES = 2
_PREPARE_CACHE: OrderedDict[
    tuple[str, str, str | None, str | None, str | None],
    tuple[pd.DataFrame, dict],
] = OrderedDict()


def _cached_prepare(
    key: tuple[str, str, str | None, str | None, str | None],
) -> tuple[pd.DataFrame, dict] | None:
    cached = _PREPARE_CACHE.get(key)
    if cached is None:
        return None
    _PREPARE_CACHE.move_to_end(key)
    frame, stats = cached
    return frame.copy(deep=True), deepcopy(stats)


def _remember_prepare(
    key: tuple[str, str, str | None, str | None, str | None],
    frame: pd.DataFrame,
    stats: dict,
) -> None:
    _PREPARE_CACHE[key] = (frame.copy(deep=True), deepcopy(stats))
    _PREPARE_CACHE.move_to_end(key)
    while len(_PREPARE_CACHE) > _PREPARE_CACHE_MAX_ENTRIES:
        _PREPARE_CACHE.popitem(last=False)

STRUCTURED_DATE_FIELDS = {
    "paid_increase": (),
    "bonus_issue": ("nstk_asstd", "nstk_lstprd", "nstk_dividrk"),
    "combined_offering": (
        "fric_nstk_asstd",
        "fric_nstk_lstprd",
        "fric_nstk_dividrk",
    ),
    "capital_reduction": ("crsc_nstklstprd", "cr_std"),
    "merger": ("mgsc_mgdt", "mgsc_nstklstprd"),
    "company_split": ("abcr_nstklstprd", "abcr_nstkasstd", "dvdt"),
    "split_merger": (
        "abcr_nstklstprd",
        "abcr_nstkasstd",
        "dvmgsc_dvmgdt",
    ),
    "share_exchange": ("extrsc_extrdt", "extrsc_nstklstprd"),
}

PRICE_ADJUSTING_STRUCTURED = {
    "bonus_issue",
    "combined_offering",
    "capital_reduction",
}


@dataclass(frozen=True)
class _PrepareEvidenceContext:
    """Verified, invocation-local evidence used throughout one prepare run."""

    base: str
    viewer_index: Mapping[str, object]
    viewer_text_by_receipt: Mapping[str, str]
    viewer_manifest_path: Path
    viewer_manifest_sha256: str | None
    viewer_object_bindings: tuple[tuple[Path, int, str], ...]
    support_manifest_path: Path
    support_manifest_sha256: str | None
    support_object_bindings: tuple[tuple[Path, int, str], ...]
    support_snapshot: object | None


def _empty() -> pd.DataFrame:
    return pd.DataFrame(columns=COLUMNS)


def _compact(value: object) -> str:
    return re.sub(r"[^0-9A-Za-z가-힣]", "", str(value or ""))


def _parse_date(value: object) -> date | None:
    rendered = str(value or "").strip()
    if not rendered or rendered == "-":
        return None
    compact = re.fullmatch(r"((?:19|20)\d{2})(\d{2})(\d{2})", rendered)
    separated = re.search(
        r"((?:19|20)\d{2})\s*(?:년|[./-])\s*"
        r"(\d{1,2})\s*(?:월|[./-])\s*(\d{1,2})\s*일?",
        rendered,
    )
    if compact is not None:
        year, month, day = map(int, compact.groups())
    elif separated is not None:
        year, month, day = map(int, separated.groups())
    else:
        return None
    try:
        return date(year, month, day)
    except ValueError:
        return None


def _number(value: object) -> float | None:
    rendered = str(value or "").replace(",", "").replace("%", "").strip()
    if not rendered or rendered == "-":
        return None
    try:
        return float(rendered)
    except ValueError:
        return None


def _ticker_from_path(path: str) -> str | None:
    match = re.search(r"/corp=([0-9A-Za-z]{6})/", path.replace("\\", "/"))
    return normalize_krx_ticker(match.group(1)) if match else None


def _event_from_path(path: str) -> str | None:
    match = re.search(r"/event=([^/]+)/", path.replace("\\", "/"))
    return match.group(1) if match else None


def _announcement_date(row: dict) -> date | None:
    rcept_no = str(row.get("rcept_no") or "")
    return _parse_date(rcept_no[:8])


def _first_date(row: dict, fields: tuple[str, ...]) -> date | None:
    for field in fields:
        parsed = _parse_date(row.get(field))
        if parsed is not None:
            return parsed
    return None


def _structured_expected_factor(event_type: str, row: dict) -> float | None:
    if event_type == "bonus_issue":
        ratio = _number(row.get("nstk_ascnt_ps_ostk"))
        if ratio is not None and ratio >= 0:
            return 1 / (1 + ratio)
    # 유무상증자는 유상 신주 비율·발행가와 무상 신주 비율이 함께
    # 이론권리락 가격을 결정한다. 이전 종가까지 필요한 값을 DART
    # 무상분만으로 계산하면 거짓 불일치가 되므로 단독 계수를 만들지 않는다.
    return None


def _structured_share_count_factor(
    event_type: str,
    row: dict,
) -> float | None:
    """DART 감자 전·후 보통주 수 비율.

    이것은 가격 조정계수가 아니다. KRX의 실제 상장주식 수 변화와만
    비교하기 위해 별도 필드로 보존한다.
    """
    if event_type != "capital_reduction":
        return None
    before = _number(row.get("bfcr_tisstk_ostk"))
    after = _number(row.get("atcr_tisstk_ostk"))
    if before is None or after is None or before <= 0 or after <= 0:
        return None
    return before / after


def _share_count_factor_comparable(event_type: str, row: dict) -> bool:
    """감자비율을 실제 전체 상장주식 수 변화와 비교할 수 있는지 판정한다.

    전체 보통주를 같은 비율로 병합하는 경우만 비교한다. 특정 주주/주식의
    소각, 유상감자, 액면가 감소, 동시 주식분할은 DART의 감자 전후 숫자가
    KRX 일별 LIST_SHRS 변화와 같은 경제적 범위를 나타내지 않는다.
    """
    if event_type != "capital_reduction":
        return False
    method = _compact(row.get("cr_mth"))
    if "병합" not in method and "무상감자" not in method:
        return False
    non_comparable = (
        "특정",
        "대주주",
        "최대주주",
        "자기주식",
        "보유주식",
        "유상",
        "액면감소",
        "액면액감소",
        "주식분할",
        "주식수변동없음",
        "출자전환",
    )
    return not any(marker in method for marker in non_comparable)


def _classify_share_count_comparability(
    events: pd.DataFrame,
) -> pd.DataFrame:
    """Exclude reductions whose isolated DART ratio cannot match KRX shares."""
    if events.empty:
        return events
    classified = events.copy()
    classified["share_count_comparison_reason"] = None
    reductions = classified["event_type"].eq("capital_reduction")
    classified.loc[
        reductions
        & ~classified["share_count_factor_comparable"].fillna(False),
        "share_count_comparison_reason",
    ] = "ACTION_METHOD_NOT_UNIFORM"

    by_identifier = {
        str(identifier): group
        for identifier, group in classified.groupby(
            classified["identifier"].astype(str),
            sort=False,
        )
    }
    financing_types = {
        "paid_increase",
        "combined_offering",
        "bonus_issue",
    }
    for index, reduction in classified[reductions].iterrows():
        if not bool(reduction["share_count_factor_comparable"]):
            continue
        peers = by_identifier.get(str(reduction["identifier"]))
        if peers is None:
            continue
        announcement = reduction["announcement_date"]
        effective = reduction["effective_date"]
        simultaneous_financing = peers[
            peers["event_type"].isin(financing_types)
            & peers["announcement_date"].notna()
            & (
                peers["announcement_date"].map(
                    lambda value: abs((value - announcement).days)
                    if pd.notna(announcement)
                    else 9999
                ).le(3)
            )
        ]
        simultaneous_split = peers[
            peers["event_type"].eq("stock_split")
            & (
                peers.apply(
                    lambda row: min(
                        abs((value - effective).days)
                        for value in (
                            row["effective_date"],
                            row["announcement_date"],
                        )
                        if pd.notna(value) and pd.notna(effective)
                    )
                    if (
                        pd.notna(effective)
                        and (
                            pd.notna(row["effective_date"])
                            or pd.notna(row["announcement_date"])
                        )
                    )
                    else 9999,
                    axis=1,
                ).le(30)
            )
        ]
        if not simultaneous_split.empty:
            classified.at[index, "share_count_factor_comparable"] = False
            classified.at[
                index,
                "share_count_comparison_reason",
            ] = "SIMULTANEOUS_STOCK_SPLIT"
        elif not simultaneous_financing.empty:
            classified.at[index, "share_count_factor_comparable"] = False
            classified.at[
                index,
                "share_count_comparison_reason",
            ] = "SIMULTANEOUS_FINANCING_DISCLOSURE"
        else:
            classified.at[
                index,
                "share_count_comparison_reason",
            ] = "UNIFORM_REDUCTION"
    return classified


def _structured_row(
    path: str,
    row: dict,
    report_name: object = None,
    corp_cls: object = None,
    accepted_date: object = None,
) -> dict | None:
    ticker = _ticker_from_path(path)
    event_type = _event_from_path(path)
    if not ticker or event_type not in STRUCTURED_DATE_FIELDS:
        return None
    effective_date = _first_date(
        row,
        STRUCTURED_DATE_FIELDS[event_type],
    )
    compact_report = _compact(report_name)
    related_company_event = (
        "종속회사" in compact_report
        or "자회사" in compact_report
    )
    issuer_action_terminated = any(
        marker in compact_report for marker in ("철회", "취소", "부결")
    )
    bonus_ratio = (
        _number(row.get("nstk_ascnt_ps_ostk"))
        if event_type == "bonus_issue" else None
    )
    return {
        "identifier": ticker,
        "event_type": event_type,
        # The official list acceptance date is authoritative.  A receipt
        # number is a selector identity and can carry the previous calendar
        # day's prefix after overnight DART processing.
        "announcement_date": (
            _parse_date(accepted_date) or _announcement_date(row)
        ),
        "effective_date": effective_date,
        "match_window_days": 7 if effective_date else 0,
        "expected_factor": _structured_expected_factor(event_type, row),
        "ratio_numerator": bonus_ratio,
        "ratio_denominator": 1.0 if bonus_ratio is not None else None,
        "share_count_factor": _structured_share_count_factor(event_type, row),
        "share_count_before": (
            _number(row.get("bfcr_tisstk_ostk"))
            if event_type == "capital_reduction"
            else None
        ),
        "share_count_after": (
            _number(row.get("atcr_tisstk_ostk"))
            if event_type == "capital_reduction"
            else None
        ),
        "share_count_factor_comparable": _share_count_factor_comparable(
            event_type,
            row,
        ),
        "share_count_comparison_reason": None,
        "action_method": row.get("cr_mth") if event_type == "capital_reduction" else None,
        "confirms_price_adjustment": (
            event_type in PRICE_ADJUSTING_STRUCTURED
            and effective_date is not None
            and not related_company_event
            and not issuer_action_terminated
        ),
        "expects_price_adjustment": (
            event_type in PRICE_ADJUSTING_STRUCTURED
            and effective_date is not None
            and not related_company_event
            and not issuer_action_terminated
        ),
        "confidence": "EFFECTIVE_DATE" if effective_date else "ANNOUNCEMENT_ONLY",
        "rcept_no": str(row.get("rcept_no") or ""),
        "report_name": report_name,
        "dart_rm": None,
        "corp_cls": (
            str(corp_cls).strip().upper() if pd.notna(corp_cls) and str(corp_cls).strip()
            else None
        ),
        "action_scope": (
            "RELATED_COMPANY" if related_company_event else "ISSUER"
        ),
        "cash_amount_status": None,
        "source_evidence_status": None,
        "correction_of_action_key": None,
        "revision_root_action_key": None,
        "revision_kind": None,
        "viewer_evidence_sha256": None,
        "economic_evidence_sha256": None,
        "reviewed_correction_id": None,
        "payment_date_quality_status": None,
        "source_body_sha256": hashlib.sha256(
            Path(path).read_bytes()
        ).hexdigest(),
        "source": "DART_STRUCTURED",
        "source_file": path,
    }


def _disclosure_type(report_name: object) -> tuple[str, bool, int] | None:
    title = _compact(report_name)
    # A listed parent also files material events of an unlisted subsidiary.
    # The disclosure row carries the parent's stock code, so treating those
    # rows as the parent's own action silently assigns the subsidiary's DPS,
    # split or delisting to the wrong security.  Bronze retains the source
    # response; Silver admits issuer-level actions only.
    if (
        "자회사의주요경영사항" in title
        or "종속회사의주요경영사항" in title
    ):
        return None
    if "권배락" in title:
        return "combined_detachment", True, 0
    if "권리락" in title:
        return "rights_detachment", True, 3
    if "배당락" in title:
        # 배당락 공시는 현금배당처럼 KRX 기준가격 조정계수가 생기지 않는
        # 경우가 있다. 관측된 기준가 변경의 근거로는 쓰되, 역방향으로
        # 모든 배당락에 가격조정을 요구하지 않는다.
        return "ex_dividend", False, 0
    if "주식배당결정" in title:
        # The decision body supplies the record date and issued-share
        # semantics, but it does not supply the exchange's ex/adjustment
        # date.  Silver therefore retains it as a semantic support action;
        # the cash-scale contract must independently bind the exact KRX
        # adjustment date and matching cash record date.
        return "stock_dividend", True, 0
    if "액면분할" in title or "주식분할" in title:
        executed = "변경상장" in title or "거래정지해제" in title
        cancelled = "철회" in title or "부결" in title
        return (
            "stock_split_cancelled" if cancelled else "stock_split",
            executed and not cancelled,
            10 if executed and not cancelled else 0,
        )
    if "액면병합" in title or "주식병합" in title:
        executed = "변경상장" in title or "거래정지해제" in title
        cancelled = "철회" in title or "부결" in title
        return (
            "reverse_split_cancelled" if cancelled else "reverse_split",
            executed and not cancelled,
            10 if executed and not cancelled else 0,
        )
    if "현금현물배당결정" in title:
        return "cash_dividend", False, 0
    if "변경상장" in title:
        return "listing_change", False, 0
    if "거래정지" in title:
        return "trading_halt", False, 0
    if "상장폐지" in title or "정리매매" in title:
        return "delisting", False, 0
    return None


def _document_effective_date(
    base: str,
    *,
    ticker: str,
    rcept_no: str,
    event_type: str,
) -> date | None:
    """원문 ZIP에서 거래소 공시의 실제 실시일을 읽는다."""
    labels = {
        "combined_detachment": ("권배락 실시일", "권배락실시일"),
        "rights_detachment": ("권리락 실시일",),
        "ex_dividend": ("배당락 실시일",),
        "stock_dividend": ("배당기준일",),
    }.get(event_type, ())
    if not labels:
        return None
    paths = glob.glob(
        f"{base}/corporate_actions/dart/documents/year=*/"
        f"corp={ticker}/rcept={rcept_no}.zip"
    )
    for path in sorted(paths):
        try:
            with zipfile.ZipFile(path) as archive:
                payloads = [archive.read(name) for name in archive.namelist()]
        except (OSError, zipfile.BadZipFile):
            continue
        for payload in payloads:
            text = None
            for encoding in ("utf-8", "euc-kr", "cp949"):
                try:
                    text = payload.decode(encoding)
                    break
                except UnicodeDecodeError:
                    continue
            if text is None:
                text = payload.decode("utf-8", errors="replace")
            visible = html.unescape(re.sub(r"<[^>]+>", " ", text))
            visible = re.sub(r"\s+", " ", visible)
            for label in labels:
                match = re.search(
                    re.escape(label)
                    + r".{0,600}?((?:19|20)\d{2}\s*[년./-]\s*"
                    r"\d{1,2}\s*[월./-]\s*\d{1,2}\s*일?)",
                    visible,
                )
                if match:
                    parsed = _parse_date(match.group(1))
                    if parsed is not None:
                        return parsed
    return None


def _decode_document(payload: bytes) -> str:
    candidates: list[str] = []
    for encoding in ("utf-8", "euc-kr", "cp949"):
        try:
            candidates.append(payload.decode(encoding))
        except UnicodeDecodeError:
            continue
    if not candidates:
        return payload.decode("utf-8", errors="replace")
    # Some DART bodies declare EUC-KR while actually containing UTF-8.  Pick
    # the successful decoding with the most Hangul and least replacement
    # damage instead of trusting a frequently stale meta tag.
    return max(
        candidates,
        key=lambda value: (
            len(re.findall(r"[가-힣]", value)),
            -value.count("�"),
            -value.count("?"),
        ),
    )


def _verified_viewer_index(
    base: str,
    manifest_sha256: str,
    required_start: date,
    required_end: date,
) -> dict[str, object]:
    del manifest_sha256
    verified = verify_viewer_corrections(
        base,
        required_start=required_start,
        required_end=required_end,
    )
    return {item.receipt_no: item for item in verified.receipts}


def _viewer_index(
    base: str,
    *,
    required_start: date | None = None,
    required_end: date | None = None,
) -> dict[str, object]:
    root = Path(base).expanduser().resolve()
    manifest = root / VIEWER_MANIFEST_RELATIVE_PATH
    if not manifest.is_file():
        return {}
    if (required_start is None) != (required_end is None):
        raise ValueError(
            "viewer required_start/required_end must be provided together"
        )
    if required_start is None:
        try:
            payload = json.loads(manifest.read_bytes())
        except (OSError, json.JSONDecodeError) as exc:
            raise RuntimeError(
                f"missing/invalid DART viewer correction manifest: {manifest}"
            ) from exc
        required_start = _parse_date(payload.get("seed_coverage_start"))
        required_end = _parse_date(payload.get("seed_coverage_end"))
        if required_start is None or required_end is None:
            raise RuntimeError(
                "DART viewer correction manifest has no valid seed coverage"
            )
    manifest_sha256 = hashlib.sha256(manifest.read_bytes()).hexdigest()
    return _verified_viewer_index(
        str(root), manifest_sha256, required_start, required_end,
    )


def _path_sha256_if_file(path: Path) -> str | None:
    try:
        payload = path.read_bytes()
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise RuntimeError(f"cannot read evidence manifest: {path}") from exc
    return hashlib.sha256(payload).hexdigest()


def _resolved_snapshot_path(root: Path, value: str) -> Path:
    path = Path(value)
    return path.resolve() if path.is_absolute() else (root / path).resolve()


def _assert_no_evidence_symlink(root: Path, path: Path) -> None:
    """Reject mutable symlink indirection below a verified snapshot root."""
    candidate = path if path.is_absolute() else root / path
    try:
        relative = candidate.relative_to(root)
    except ValueError as exc:
        raise RuntimeError(f"evidence path escapes snapshot root: {path}") from exc
    if ".." in relative.parts:
        raise RuntimeError(f"evidence path escapes snapshot root: {path}")
    current = root
    for part in relative.parts:
        current = current / part
        if current.is_symlink():
            raise RuntimeError(f"symlinked evidence path is forbidden: {current}")


def _prepare_evidence_context(
    base: str,
    *,
    coverage_start: date | None,
    coverage_end: date | None,
) -> _PrepareEvidenceContext:
    root = Path(base).expanduser().resolve()
    viewer_manifest = root / VIEWER_MANIFEST_RELATIVE_PATH
    _assert_no_evidence_symlink(root, viewer_manifest)
    viewer_sha = _path_sha256_if_file(viewer_manifest)
    viewer_index: dict[str, object] = {}
    viewer_text_by_receipt: dict[str, str] = {}
    viewer_object_bindings: tuple[tuple[Path, int, str], ...] = ()
    if viewer_sha is not None:
        required_start = coverage_start
        required_end = coverage_end
        if required_start is None:
            try:
                payload = json.loads(viewer_manifest.read_bytes())
            except (OSError, json.JSONDecodeError) as exc:
                raise RuntimeError(
                    f"missing/invalid DART viewer correction manifest: "
                    f"{viewer_manifest}"
                ) from exc
            required_start = _parse_date(payload.get("seed_coverage_start"))
            required_end = _parse_date(payload.get("seed_coverage_end"))
            if required_start is None or required_end is None:
                raise RuntimeError(
                    "DART viewer correction manifest has no valid seed coverage"
                )
        verified = verify_viewer_corrections(
            str(root),
            required_start=required_start,
            required_end=required_end,
        )
        if (
            _resolved_snapshot_path(root, verified.manifest_path)
            != viewer_manifest
            or
            verified.manifest_sha256 != viewer_sha
            or _path_sha256_if_file(viewer_manifest) != viewer_sha
        ):
            raise RuntimeError(
                "DART viewer correction manifest changed during verification"
            )
        declared_bindings: dict[Path, tuple[int, str]] = {}

        def declare_binding(relative: str, length: int, digest: str) -> None:
            lexical = Path(relative)
            path = lexical if lexical.is_absolute() else root / lexical
            _assert_no_evidence_symlink(root, path)
            path = path.resolve()
            if root not in path.parents:
                raise RuntimeError(
                    f"DART viewer evidence path is unsafe: {relative}"
                )
            identity = (int(length), str(digest))
            existing = declared_bindings.get(path)
            if existing is not None and existing != identity:
                raise RuntimeError(
                    f"conflicting DART viewer evidence identity: {relative}"
                )
            declared_bindings[path] = identity

        for probe in verified.dependency_probes:
            declare_binding(
                probe.main_path,
                probe.main_content_length,
                probe.main_sha256,
            )
        for evidence in verified.receipts:
            for relative, length, digest in (
                (
                    evidence.main_path,
                    evidence.main_content_length,
                    evidence.main_sha256,
                ),
                (
                    evidence.viewer_path,
                    evidence.viewer_content_length,
                    evidence.viewer_sha256,
                ),
                (
                    evidence.economic_main_path,
                    evidence.economic_main_content_length,
                    evidence.economic_main_sha256,
                ),
                (
                    evidence.economic_viewer_path,
                    evidence.economic_viewer_content_length,
                    evidence.economic_viewer_sha256,
                ),
            ):
                declare_binding(relative, length, digest)

        frozen_body_by_path: dict[Path, bytes] = {}
        viewer_object_bindings = tuple(
            (path, length, digest)
            for path, (length, digest) in sorted(
                declared_bindings.items(), key=lambda item: str(item[0]),
            )
        )
        for path, length, digest in viewer_object_bindings:
            try:
                frozen_body = path.read_bytes()
            except OSError as exc:
                raise RuntimeError(
                    f"DART viewer evidence is unreadable: {path}"
                ) from exc
            if (
                len(frozen_body) != length
                or hashlib.sha256(frozen_body).hexdigest() != digest
            ):
                raise RuntimeError(
                    f"DART viewer evidence changed after verification: {path}"
                )
            frozen_body_by_path[path] = frozen_body

        for evidence in verified.receipts:
            body = frozen_body_by_path[(root / evidence.viewer_path).resolve()]
            decoded = _decode_document(body)
            visible = html.unescape(re.sub(r"<[^>]+>", " ", decoded))
            viewer_index[evidence.receipt_no] = evidence
            viewer_text_by_receipt[evidence.receipt_no] = re.sub(
                r"\s+", " ", visible,
            )

    support_manifest = root / SUPPORT_FAMILY_MANIFEST_RELATIVE_PATH
    _assert_no_evidence_symlink(root, support_manifest)
    support_sha = _path_sha256_if_file(support_manifest)
    support_snapshot = None
    support_object_bindings: tuple[tuple[Path, int, str], ...] = ()
    if support_sha is not None and coverage_start is not None:
        support_snapshot = verify_support_action_families(
            root,
            required_start=coverage_start,
            required_end=coverage_end,
        )
        if (
            _resolved_snapshot_path(root, support_snapshot.manifest_path)
            != support_manifest
            or
            support_snapshot.manifest_sha256 != support_sha
            or _path_sha256_if_file(support_manifest) != support_sha
        ):
            raise RuntimeError(
                "DART support-family manifest changed during verification"
            )
        declared_support_bindings: dict[Path, tuple[int | None, str]] = {}

        def declare_support_binding(
            relative: str,
            length: int | None,
            digest: str,
        ) -> None:
            lexical = Path(relative)
            path = lexical if lexical.is_absolute() else root / lexical
            _assert_no_evidence_symlink(root, path)
            path = path.resolve()
            if root not in path.parents:
                raise RuntimeError(
                    f"DART support-family evidence path is unsafe: {relative}"
                )
            identity = (length, str(digest))
            existing = declared_support_bindings.get(path)
            if existing is not None:
                existing_length, existing_digest = existing
                if (
                    existing_digest != identity[1]
                    or (
                        existing_length is not None
                        and identity[0] is not None
                        and existing_length != identity[0]
                    )
                ):
                    raise RuntimeError(
                        "conflicting DART support-family evidence identity: "
                        f"{relative}"
                    )
                if existing_length is None and identity[0] is not None:
                    declared_support_bindings[path] = identity
                return
            declared_support_bindings[path] = identity

        for entry in support_snapshot.entries:
            for source in entry.sources:
                declare_support_binding(
                    source.main_path,
                    source.main_content_length,
                    source.main_sha256,
                )
                declare_support_binding(
                    source.body_path,
                    source.body_content_length,
                    source.body_sha256,
                )
                declare_support_binding(
                    source.disclosure_path,
                    source.disclosure_content_length,
                    source.disclosure_sha256,
                )
                declare_support_binding(
                    source.disclosure_manifest_path,
                    None,
                    source.disclosure_manifest_sha256,
                )
                structured_fields = (
                    source.structured_path,
                    source.structured_content_length,
                    source.structured_sha256,
                )
                if any(value is not None for value in structured_fields) and not all(
                    value is not None for value in structured_fields
                ):
                    raise RuntimeError(
                        "incomplete DART support-family structured identity: "
                        f"{source.receipt_no}"
                    )
                if source.structured_path is not None:
                    declare_support_binding(
                        source.structured_path,
                        source.structured_content_length,
                        source.structured_sha256,
                    )
        completeness_parents = {
            (root / source.disclosure_manifest_path).resolve().parent
            for entry in support_snapshot.entries
            for source in entry.sources
        }
        for parent in completeness_parents:
            for marker_name in (
                "structured_complete_v3.json",
                "documents_complete_v5.json",
            ):
                marker = parent / marker_name
                _assert_no_evidence_symlink(root, marker)
                marker = marker.resolve()
                if root not in marker.parents:
                    raise RuntimeError(
                        f"DART support-family marker path is unsafe: {marker}"
                    )
                try:
                    marker_payload = marker.read_bytes()
                except OSError as exc:
                    raise RuntimeError(
                        f"DART support-family marker is unreadable: {marker}"
                    ) from exc
                declare_support_binding(
                    str(marker.relative_to(root)),
                    len(marker_payload),
                    hashlib.sha256(marker_payload).hexdigest(),
                )
        exact_support_bindings: list[tuple[Path, int, str]] = []
        for path, (declared_length, digest) in sorted(
            declared_support_bindings.items(), key=lambda item: str(item[0]),
        ):
            try:
                payload = path.read_bytes()
            except OSError as exc:
                raise RuntimeError(
                    f"DART support-family evidence is unreadable: {path}"
                ) from exc
            if (
                (declared_length is not None and len(payload) != declared_length)
                or hashlib.sha256(payload).hexdigest() != digest
            ):
                raise RuntimeError(
                    "DART support-family evidence changed after verification: "
                    f"{path}"
                )
            exact_support_bindings.append((path, len(payload), digest))
        support_object_bindings = tuple(exact_support_bindings)
    return _PrepareEvidenceContext(
        base=str(root),
        viewer_index=MappingProxyType(viewer_index),
        viewer_text_by_receipt=MappingProxyType(viewer_text_by_receipt),
        viewer_manifest_path=viewer_manifest,
        viewer_manifest_sha256=viewer_sha,
        viewer_object_bindings=viewer_object_bindings,
        support_manifest_path=support_manifest,
        support_manifest_sha256=support_sha,
        support_object_bindings=support_object_bindings,
        support_snapshot=support_snapshot,
    )


def _assert_prepare_evidence_unchanged(
    context: _PrepareEvidenceContext,
) -> None:
    root = Path(context.base)
    _assert_no_evidence_symlink(root, context.viewer_manifest_path)
    if (
        _path_sha256_if_file(context.viewer_manifest_path)
        != context.viewer_manifest_sha256
    ):
        raise RuntimeError("DART viewer correction manifest changed during prepare")
    for path, length, digest in context.viewer_object_bindings:
        _assert_no_evidence_symlink(root, path)
        try:
            payload = path.read_bytes()
        except OSError as exc:
            raise RuntimeError(
                f"DART viewer evidence changed during prepare: {path}"
            ) from exc
        if (
            len(payload) != length
            or hashlib.sha256(payload).hexdigest() != digest
        ):
            raise RuntimeError(
                f"DART viewer evidence changed during prepare: {path}"
            )
    _assert_no_evidence_symlink(root, context.support_manifest_path)
    if (
        _path_sha256_if_file(context.support_manifest_path)
        != context.support_manifest_sha256
    ):
        raise RuntimeError("DART support-family manifest changed during prepare")
    for path, length, digest in context.support_object_bindings:
        _assert_no_evidence_symlink(root, path)
        try:
            payload = path.read_bytes()
        except OSError as exc:
            raise RuntimeError(
                f"DART support-family evidence changed during prepare: {path}"
            ) from exc
        if (
            len(payload) != length
            or hashlib.sha256(payload).hexdigest() != digest
        ):
            raise RuntimeError(
                f"DART support-family evidence changed during prepare: {path}"
            )


def _document_texts(
    base: str,
    *,
    ticker: str,
    rcept_no: str,
    include_viewer: bool = True,
    evidence_context: _PrepareEvidenceContext | None = None,
) -> list[str]:
    paths = glob.glob(
        f"{base}/corporate_actions/dart/documents/year=*/"
        f"corp={ticker}/rcept={rcept_no}.zip"
    )
    texts: list[str] = []
    for path in sorted(paths):
        try:
            with zipfile.ZipFile(path) as archive:
                payloads = [archive.read(name) for name in archive.namelist()]
        except (OSError, zipfile.BadZipFile):
            continue
        for payload in payloads:
            decoded = _decode_document(payload)
            visible = html.unescape(re.sub(r"<[^>]+>", " ", decoded))
            texts.append(re.sub(r"\s+", " ", visible))
    if include_viewer:
        if evidence_context is not None:
            viewer_text = evidence_context.viewer_text_by_receipt.get(
                str(rcept_no),
            )
        else:
            viewer = _viewer_index(base).get(str(rcept_no))
            viewer_text = None
            if viewer is not None:
                payload = (Path(base).resolve() / viewer.viewer_path).read_bytes()
                decoded = _decode_document(payload)
                visible = html.unescape(re.sub(r"<[^>]+>", " ", decoded))
                viewer_text = re.sub(r"\s+", " ", visible)
        if viewer_text is not None:
            # Appended last on purpose: a verified viewer revision supersedes
            # a stale/corrupted OpenDART body for the same receipt.
            texts.append(viewer_text)
    return texts


def _combined_detachment_details(
    base: str,
    *,
    ticker: str,
    rcept_no: str,
) -> dict[str, object]:
    """Parse the exact KRX base price and combined-action reason."""
    paths = sorted(glob.glob(
        f"{base}/corporate_actions/dart/documents/year=*/"
        f"corp={ticker}/rcept={rcept_no}.zip"
    ))
    if len(paths) > 1:
        raise RuntimeError(f"duplicate combined-detachment bodies: {rcept_no}")
    labelled: dict[str, list[str]] = {"reference": [], "reason": []}
    for path in paths:
        try:
            with zipfile.ZipFile(path) as archive:
                payloads = [archive.read(name) for name in archive.namelist()]
        except (OSError, zipfile.BadZipFile) as exc:
            raise RuntimeError(
                f"invalid combined-detachment document: {rcept_no}"
            ) from exc
        for payload in payloads:
            decoded = _decode_document(payload)
            for cells in _table_rows(decoded):
                for index, cell in enumerate(cells[:-1]):
                    label = _compact(cell)
                    value = re.sub(r"\s+", " ", cells[index + 1]).strip()
                    if label in {"기준가격", "기준가격원"}:
                        labelled["reference"].append(value)
                    elif label == "사유":
                        labelled["reason"].append(value)
    reference_values = labelled["reference"]
    reason_values = labelled["reason"]
    if len(reference_values) != 1 or len(reason_values) != 1:
        return {"reference_price": None, "reason": None}
    reference_price = _number(reference_values[0])
    reason = reason_values[0]
    compact_reason = _compact(reason)
    if (
        reference_price is None or reference_price <= 0
        or "무상증자" not in compact_reason or "배당" not in compact_reason
    ):
        return {"reference_price": None, "reason": None}
    return {
        "reference_price": reference_price,
        "reason": reason,
    }


def _stock_dividend_ratio(
    base: str,
    *,
    ticker: str,
    rcept_no: str,
) -> float | None:
    """Parse one exact labelled ordinary-share distribution ratio."""
    observed: list[float] = []
    paths = sorted(glob.glob(
        f"{base}/corporate_actions/dart/documents/year=*/"
        f"corp={ticker}/rcept={rcept_no}.zip"
    ))
    if len(paths) > 1:
        raise RuntimeError(f"duplicate stock-dividend bodies: {rcept_no}")
    for path in paths:
        try:
            with zipfile.ZipFile(path) as archive:
                payloads = [archive.read(name) for name in archive.namelist()]
        except (OSError, zipfile.BadZipFile) as exc:
            raise RuntimeError(
                f"invalid stock-dividend document: {rcept_no}"
            ) from exc
        for payload in payloads:
            value = stock_dividend_common_ratio_from_body(payload)
            if value is not None:
                observed.append(value)
    unique = sorted(set(observed))
    return unique[0] if len(unique) == 1 else None


def _document_evidence_sha256(
    base: str,
    *,
    ticker: str,
    rcept_no: str,
) -> str | None:
    paths = sorted(glob.glob(
        f"{base}/corporate_actions/dart/documents/year=*/"
        f"corp={ticker}/rcept={rcept_no}.zip"
    ))
    if len(paths) > 1:
        raise RuntimeError(
            f"duplicate DART document bodies for one receipt: {rcept_no}"
        )
    if not paths:
        return None
    return hashlib.sha256(Path(paths[0]).read_bytes()).hexdigest()


def _table_rows(visible: str) -> list[list[str]]:
    rows: list[list[str]] = []
    for row_html in re.findall(
        r"<tr\b[^>]*>(.*?)</tr>", visible,
        flags=re.IGNORECASE | re.DOTALL,
    ):
        cells = [
            re.sub(r"\s+", " ", html.unescape(re.sub(r"<[^>]+>", " ", cell))).strip()
            for cell in re.findall(
                r"<td\b[^>]*>(.*?)</td>", row_html,
                flags=re.IGNORECASE | re.DOTALL,
            )
        ]
        if cells:
            rows.append(cells)
    return rows


def _cash_dividend_details(
    base: str,
    *,
    ticker: str,
    rcept_no: str,
    include_viewer: bool = True,
    evidence_context: _PrepareEvidenceContext | None = None,
) -> dict:
    details = {
        "record_date": None,
        "payment_date": None,
        "cash_amount": None,
        "adjusted_cash_amount": None,
        "currency": "KRW",
        "frequency": None,
        "correction_origin_date": None,
        "related_company_event": False,
        "cash_amount_status": "UNRESOLVED",
    }
    for visible in _document_texts(
        base,
        ticker=ticker,
        rcept_no=rcept_no,
        include_viewer=include_viewer,
        evidence_context=evidence_context,
    ):
        # Keep field boundaries.  Compacting the entire correction document
        # can concatenate the correction table's old/new values and nearby
        # totals into one very large number.  Corrected disclosures contain a
        # correction table first and the complete corrected body last, so the
        # final body occurrence is the canonical common-share DPS.
        normalized = re.sub(r"\s+", " ", visible).strip()
        compact_document = _compact(normalized)
        if (
            re.search(
                r"(?:자회사|종속회사)인.{0,120}의주요경영사항신고",
                compact_document,
            )
            is not None
        ):
            details["related_company_event"] = True
        correction_dates = list(re.finditer(
            r"정정관련\s*공시서류\s*제출일\s*[:：]?\s*"
            r"((?:19|20)\d{2}\s*[년./-]?\s*"
            r"\d{1,2}\s*[월./-]?\s*\d{1,2}\s*일?)",
            normalized,
        ))
        if correction_dates:
            details["correction_origin_date"] = _parse_date(
                correction_dates[-1].group(1),
            )
        amount_matches = list(re.finditer(
            r"1\s*주당\s*배당금\s*"
            r"(?:\(\s*원\s*\)|원)?\s*"
            r"보통주(?:식)?\s*[:：]?\s*"
            r"([0-9][0-9,]*(?:\.[0-9]+)?)",
            normalized,
        ))
        if amount_matches:
            parsed_amount = _number(amount_matches[-1].group(1))
            if parsed_amount is not None and parsed_amount > 0:
                details["cash_amount"] = parsed_amount
                details["cash_amount_status"] = "POSITIVE"
            else:
                # Preserve the exact source bytes through their SHA, but use
                # NULL for the normalized economic amount when common DPS is
                # explicitly zero.  This keeps the no-common state disjoint
                # from an investable positive cash event.
                details["cash_amount"] = None
                details["cash_amount_status"] = "NO_COMMON_CASH_DIVIDEND"
        no_common_matches = list(re.finditer(
            r"1\s*주당\s*배당금\s*"
            r"(?:\(\s*원\s*\)|원)?\s*"
            r"보통주(?:식)?\s*[:：]?\s*"
            r"(?:-|해당\s*없음|없음|미지급)",
            normalized,
        ))
        if no_common_matches and (
            not amount_matches
            or no_common_matches[-1].start() > amount_matches[-1].start()
        ):
            details["cash_amount"] = None
            details["cash_amount_status"] = "NO_COMMON_CASH_DIVIDEND"
        for label_pattern, field in (
            (r"배당\s*기준일", "record_date"),
            (r"배당금\s*지급\s*예정일자", "payment_date"),
        ):
            matches = list(re.finditer(
                label_pattern
                + r"\s*[:：]?\s*"
                + r"((?:19|20)\d{2}\s*[년./-]?\s*"
                + r"\d{1,2}\s*[월./-]?\s*\d{1,2}\s*일?)",
                normalized,
            ))
            if matches:
                parsed_date = _parse_date(matches[-1].group(1))
                if parsed_date is not None:
                    details[field] = parsed_date
        compact = compact_document
        if "분기배당" in compact:
            details["frequency"] = "quarterly"
        elif "중간배당" in compact:
            details["frequency"] = "interim"
        elif "결산배당" in compact:
            details["frequency"] = "annual"
        elif "배당구분" in compact:
            details["frequency"] = "irregular"
    return details


def _assert_viewer_cash_parity(
    *,
    receipt: str,
    viewer_evidence,
    final_details: dict,
    zip_details: dict,
    zip_body_present: bool,
) -> None:
    classification = str(viewer_evidence.economic_classification)
    expected_status = {
        "ECONOMIC_DECISION": "POSITIVE",
        "POSITIVE_PENDING_RECORD_DATE": "POSITIVE_PENDING_RECORD_DATE",
        "NO_COMMON_CASH_DIVIDEND": "NO_COMMON_CASH_DIVIDEND",
        "NO_ECONOMIC_EVENT": "NO_ECONOMIC_EVENT",
    }.get(classification)
    if expected_status is None:
        raise RuntimeError(
            f"unsupported viewer economic classification: {receipt} "
            f"{classification}"
        )
    actual_record = final_details.get("record_date")
    actual_record_text = (
        actual_record.isoformat() if actual_record is not None else None
    )
    actual_amount = final_details.get("cash_amount")
    if (
        final_details.get("cash_amount_status") != expected_status
        or actual_record_text != viewer_evidence.record_date
        or (
            classification in {
                "ECONOMIC_DECISION", "POSITIVE_PENDING_RECORD_DATE",
            }
            and (
                actual_amount is None
                or float(actual_amount)
                != float(viewer_evidence.common_cash_amount)
            )
        )
    ):
        raise RuntimeError(
            "DART viewer/parser economic parity failed: "
            f"receipt={receipt} expected="
            f"{classification}/{viewer_evidence.common_cash_amount}/"
            f"{viewer_evidence.record_date} actual="
            f"{final_details.get('cash_amount_status')}/{actual_amount}/"
            f"{actual_record_text}"
        )

    zip_resolved = zip_details.get("cash_amount_status") in {
        "POSITIVE", "POSITIVE_PENDING_RECORD_DATE",
        "NO_COMMON_CASH_DIVIDEND", "NO_ECONOMIC_EVENT",
    }
    if receipt in KNOWN_DAMAGED_DOCUMENT_RECEIPTS:
        return
    if not zip_resolved:
        # status 014 has no ZIP body and is exactly why viewer fallback exists.
        if not zip_body_present:
            return
        raise RuntimeError(
            f"existing DART ZIP remained economically unresolved: {receipt}"
        )
    comparable_fields = (
        "record_date", "payment_date", "cash_amount",
        "cash_amount_status", "frequency",
    )
    mismatched = {
        field: (zip_details.get(field), final_details.get(field))
        for field in comparable_fields
        if zip_details.get(field) != final_details.get(field)
    }
    if mismatched:
        raise RuntimeError(
            "OpenDART ZIP/viewer economic parity failed: "
            f"receipt={receipt} mismatched={mismatched}"
        )


def _related_cash_correction_signatures(
    base: str,
    disclosure_rows: list[tuple[str, dict]],
    *,
    evidence_context: _PrepareEvidenceContext,
) -> set[tuple[str, date, date, float]]:
    """Identify original filings later corrected as subsidiary disclosures.

    Some DART filings were first published without the subsidiary suffix and
    corrected a few days later.  Looking at each title independently would
    leave the original row attached to the listed parent.  The correction
    document states the original submission date, record date and DPS, which
    together form a narrow, auditable exclusion signature.
    """
    signatures: set[tuple[str, date, date, float]] = set()
    for path, row in disclosure_rows:
        title = _compact(row.get("report_nm"))
        if not (
            "현금현물배당결정" in title
            and (
                "자회사의주요경영사항" in title
                or "종속회사의주요경영사항" in title
            )
        ):
            continue
        ticker = normalize_krx_ticker(
            row.get("stock_code")
        ) or _ticker_from_path(path)
        if not ticker:
            continue
        details = _cash_dividend_details(
            base,
            ticker=ticker,
            rcept_no=str(row.get("rcept_no") or ""),
            evidence_context=evidence_context,
        )
        origin = details.get("correction_origin_date")
        record_date = details.get("record_date")
        amount = details.get("cash_amount")
        if origin is not None and record_date is not None and amount is not None:
            signatures.add((
                normalize_krx_ticker(ticker),
                origin,
                record_date,
                float(amount),
            ))
    return signatures


def _disclosure_row(
    base: str,
    path: str,
    row: dict,
    *,
    evidence_context: _PrepareEvidenceContext,
) -> dict | None:
    event = _disclosure_type(row.get("report_nm"))
    if event is None:
        return None
    event_type, expects_adjustment, window = event
    ticker = normalize_krx_ticker(
        row.get("stock_code")
    ) or _ticker_from_path(path)
    announced = _parse_date(row.get("rcept_dt")) or _announcement_date(row)
    if not ticker or announced is None:
        return None
    document_date = _document_effective_date(
        base,
        ticker=ticker,
        rcept_no=str(row.get("rcept_no") or ""),
        event_type=event_type,
    )
    dividend_details = (
        _cash_dividend_details(
            base,
            ticker=ticker,
            rcept_no=str(row.get("rcept_no") or ""),
            evidence_context=evidence_context,
        )
        if event_type == "cash_dividend"
        else {}
    )
    combined_details = (
        _combined_detachment_details(
            base,
            ticker=ticker,
            rcept_no=str(row.get("rcept_no") or ""),
        )
        if event_type == "combined_detachment"
        else {}
    )
    stock_dividend_ratio = (
        _stock_dividend_ratio(
            base,
            ticker=ticker,
            rcept_no=str(row.get("rcept_no") or ""),
        )
        if event_type == "stock_dividend"
        else None
    )
    if dividend_details.get("related_company_event"):
        return None
    receipt = str(row.get("rcept_no") or "")
    viewer_evidence = evidence_context.viewer_index.get(receipt)
    compact_report = _compact(row.get("report_nm"))
    is_attachment_correction = "첨부정정" in compact_report
    is_revision = any(
        marker in compact_report
        for marker in ("정정", "철회", "취소", "부결")
    )
    is_withdrawn = any(
        marker in compact_report for marker in ("철회", "취소", "부결")
    )
    if event_type == "cash_dividend" and viewer_evidence is not None:
        if is_attachment_correction:
            if viewer_evidence.revision_kind != "ATTACHMENT_ONLY":
                raise RuntimeError(
                    f"DART attachment revision-kind mismatch: {receipt}"
                )
            dividend_details.update({
                "record_date": None,
                "payment_date": None,
                "cash_amount": None,
                "cash_amount_status": "ATTACHMENT_ONLY",
                "frequency": None,
            })
        else:
            if viewer_evidence.revision_kind != "ECONOMIC_REVISION":
                raise RuntimeError(
                    f"DART economic revision-kind mismatch: {receipt}"
                )
            zip_texts = _document_texts(
                base,
                ticker=ticker,
                rcept_no=receipt,
                include_viewer=False,
                evidence_context=evidence_context,
            )
            zip_details = _cash_dividend_details(
                base,
                ticker=ticker,
                rcept_no=receipt,
                include_viewer=False,
                evidence_context=evidence_context,
            )
            if viewer_evidence.economic_classification == (
                "POSITIVE_PENDING_RECORD_DATE"
            ):
                dividend_details["cash_amount"] = (
                    viewer_evidence.common_cash_amount
                )
                dividend_details["cash_amount_status"] = (
                    "POSITIVE_PENDING_RECORD_DATE"
                )
                dividend_details["record_date"] = None
                if (
                    zip_details.get("cash_amount") is not None
                    and zip_details.get("record_date") is None
                ):
                    zip_details["cash_amount_status"] = (
                        "POSITIVE_PENDING_RECORD_DATE"
                    )
            elif viewer_evidence.economic_classification == "NO_ECONOMIC_EVENT":
                dividend_details["cash_amount"] = None
                dividend_details["cash_amount_status"] = "NO_ECONOMIC_EVENT"
                dividend_details["record_date"] = None
            _assert_viewer_cash_parity(
                receipt=receipt,
                viewer_evidence=viewer_evidence,
                final_details=dividend_details,
                zip_details=zip_details,
                zip_body_present=bool(zip_texts),
            )
    if event_type == "cash_dividend" and is_withdrawn:
        dividend_details["cash_amount"] = None
        dividend_details["cash_amount_status"] = "NO_ECONOMIC_EVENT"
    if event_type == "cash_dividend":
        dividend_details = apply_reviewed_correction(
            base,
            ticker=ticker,
            receipt=receipt,
            details=dividend_details,
        )
        # An original decision may announce a positive DPS before fixing the
        # record date, then complete it in a later correction.  Preserve that
        # immutable intermediate evidence explicitly; family canonicalization
        # still requires the terminal economic revision to carry a date.
        if (
            dividend_details.get("cash_amount_status") == "POSITIVE"
            and dividend_details.get("record_date") is None
            and dividend_details.get("cash_amount") is not None
            and float(dividend_details["cash_amount"]) > 0
        ):
            dividend_details["cash_amount_status"] = (
                "POSITIVE_PENDING_RECORD_DATE"
            )
    reviewed_correction_id = dividend_details.get("reviewed_correction_id")
    if event_type != "cash_dividend":
        source_evidence_status = None
    elif viewer_evidence is not None and is_attachment_correction:
        source_evidence_status = "VERIFIED_ATTACHMENT_CORRECTION"
    elif dividend_details.get("reviewed_economic_correction"):
        source_evidence_status = "VERIFIED_REVIEWED_SOURCE_ERRATUM"
    elif viewer_evidence is not None:
        source_evidence_status = "VERIFIED_DART_VIEWER_BODY"
    elif is_revision:
        source_evidence_status = "UNVERIFIED_REVISION_LINEAGE"
    elif _document_texts(
        base,
        ticker=ticker,
        rcept_no=str(row.get("rcept_no") or ""),
        evidence_context=evidence_context,
    ):
        source_evidence_status = "VERIFIED_OPENDART_DOCUMENT"
    else:
        source_evidence_status = "UNRESOLVED_SOURCE_BODY"
    # A DART exchange notice's filing date is only an announcement proxy.  It
    # must never override the KRX T+2 dividend fallback as though it were the
    # actual ex-date.  Only an execution date parsed from the official body is
    # persisted as effective_date for ex-dividend notices.
    if event_type == "ex_dividend":
        effective_date = document_date
        confirms_adjustment = document_date is not None
    elif event_type == "combined_detachment":
        effective_date = document_date
        confirms_adjustment = bool(
            document_date is not None
            and combined_details.get("reference_price") is not None
            and combined_details.get("reason")
        )
    elif event_type == "stock_dividend":
        # ``document_date`` is the 배당기준일, not an ex-date.  Publishing
        # it as ``effective_date`` would silently shift the corporate action
        # onto the wrong market session and permit a loose date match.
        effective_date = None
        confirms_adjustment = False
    else:
        confirms_adjustment = expects_adjustment
        effective_date = document_date or announced if expects_adjustment else None
    match_window_days = (
        0
        if document_date is not None or event_type == "stock_dividend"
        else (3 if event_type == "ex_dividend" else window)
    )
    return {
        "identifier": ticker,
        "event_type": event_type,
        "announcement_date": announced,
        # Announcement dates remain provenance only; actual execution dates
        # are the sole exact-date evidence admitted here.
        "effective_date": effective_date,
        "match_window_days": match_window_days,
        "expected_factor": None,
        "share_count_factor": None,
        "share_count_before": None,
        "share_count_after": None,
        "share_count_factor_comparable": False,
        "share_count_comparison_reason": None,
        "action_method": combined_details.get("reason"),
        "record_date": (
            document_date
            if event_type == "stock_dividend"
            else dividend_details.get("record_date")
        ),
        "payment_date": dividend_details.get("payment_date"),
        "cash_amount": dividend_details.get("cash_amount"),
        "adjusted_cash_amount": dividend_details.get("adjusted_cash_amount"),
        "ratio_numerator": stock_dividend_ratio,
        "ratio_denominator": 1.0 if stock_dividend_ratio is not None else None,
        "currency": dividend_details.get("currency"),
        "frequency": dividend_details.get("frequency"),
        "confirms_price_adjustment": confirms_adjustment,
        "expects_price_adjustment": expects_adjustment,
        "confidence": (
            "EXCHANGE_NOTICE" if confirms_adjustment else "ANNOUNCEMENT_ONLY"
        ),
        "rcept_no": str(row.get("rcept_no") or ""),
        "report_name": row.get("report_nm"),
        "dart_rm": row.get("rm"),
        "corp_cls": (
            str(row.get("corp_cls")).strip().upper()
            if pd.notna(row.get("corp_cls")) and str(row.get("corp_cls")).strip()
            else None
        ),
        "action_scope": "ISSUER",
        "cash_amount_status": (
            dividend_details.get("cash_amount_status")
            if event_type == "cash_dividend" else None
        ),
        "source_evidence_status": source_evidence_status,
        "correction_of_action_key": (
            viewer_evidence.correction_of_receipt_no
            if viewer_evidence is not None else None
        ),
        "revision_root_action_key": (
            viewer_evidence.revision_root_receipt_no
            if viewer_evidence is not None
            else (receipt if event_type == "cash_dividend" else None)
        ),
        "revision_kind": (
            viewer_evidence.revision_kind
            if viewer_evidence is not None
            else (
                "ORIGINAL_DECISION"
                if event_type == "cash_dividend" else None
            )
        ),
        "viewer_evidence_sha256": (
            viewer_evidence.viewer_sha256
            if viewer_evidence is not None else None
        ),
        "economic_evidence_sha256": (
            dividend_details.get("reviewed_evidence_sha256")
            or (
                viewer_evidence.economic_viewer_sha256
                if viewer_evidence is not None else None
            )
            or (
                _document_evidence_sha256(
                    base, ticker=ticker, rcept_no=receipt,
                )
                if event_type == "cash_dividend" else None
            )
        ),
        "reviewed_correction_id": reviewed_correction_id,
        "payment_date_quality_status": dividend_details.get(
            "payment_date_quality_status"
        ),
        "source_body_sha256": (
            _document_evidence_sha256(
                base, ticker=ticker, rcept_no=receipt,
            )
            or hashlib.sha256(Path(path).read_bytes()).hexdigest()
        ),
        "source": "DART_DISCLOSURE",
        "source_file": path,
    }


def _read_json(path: str):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _disclosure_rows(
    base: str,
    *,
    include_audit: bool = False,
) -> list[tuple[str, dict]] | tuple[list[tuple[str, dict]], dict[str, object]]:
    manifests = sorted(glob.glob(
        f"{base}/corporate_actions/dart/manifests/"
        "from=*/to=*/disclosures_v3.json"
    ))
    observations: list[tuple[str, dict]] = []
    for path in manifests:
        payload = _read_json(path)
        if not isinstance(payload, list):
            continue
        for row in payload:
            rcept_no = str(row.get("rcept_no") or "")
            if rcept_no:
                observations.append((path, row))
    if observations:
        rows, audit = canonicalize_disclosures(
            observations, audit_root=base,
        )
        output = list(rows.values())
        return (output, audit) if include_audit else output

    rows: dict[str, tuple[str, dict]] = {}
    for path in sorted(glob.glob(
        f"{base}/corporate_actions/dart/disclosures/"
        "year=*/date=*/corp=*/rcept=*.json"
    )):
        row = _read_json(path)
        rcept_no = str(row.get("rcept_no") or "")
        if rcept_no:
            existing = rows.get(rcept_no)
            if existing is not None and existing[1] != row:
                raise RuntimeError(
                    "conflicting DART disclosure payloads for receipt: "
                    f"{rcept_no} paths={[existing[0], path]}"
                )
            rows.setdefault(rcept_no, (path, row))
    output = list(rows.values())
    audit = {
        "contract": "individual_disclosure_files_no_overlap_v1",
        "mutable_fields": [],
        "observation_count": len(output),
        "unique_receipt_count": len(output),
        "duplicate_receipt_count": 0,
        "mutable_conflict_receipt_count": 0,
        "mutable_conflict_field_counts": {},
        "mutable_conflict_digest": hashlib.sha256(b"[]").hexdigest(),
        "mutable_conflict_samples": [],
    }
    return (output, audit) if include_audit else output


def _verified_lineage_receipts(
    base: str,
    *,
    coverage_start: date,
    coverage_end: date,
    disclosure_rows: list[tuple[str, dict]],
    evidence_context: _PrepareEvidenceContext,
) -> set[str]:
    """Return only exact, independently verified family dependencies.

    Bronze can retain complete intervals outside the published action
    coverage so that an in-coverage correction can bind its official DART
    root.  Those intervals are evidence, not an invitation to publish every
    historical or future filing.  The only out-of-range receipts admitted by
    Silver are identities named by the exact viewer/support-family manifests
    verified for this same coverage window.
    """
    root = Path(base).expanduser().resolve()
    allowed: set[str] = set()
    in_coverage_receipts = {
        str(row.get("rcept_no") or "")
        for _, row in disclosure_rows
        if (
            (accepted := _parse_date(row.get("rcept_dt"))) is not None
            and coverage_start <= accepted <= coverage_end
        )
    }

    def admit_family(values: set[str]) -> None:
        exact = {
            value for value in values if re.fullmatch(r"\d{14}", value)
        }
        # A verified manifest may retain extra immutable evidence, but only a
        # family anchored by an official in-coverage list acceptance can
        # extend the consumer scope beyond either coverage boundary.
        if exact.intersection(in_coverage_receipts):
            allowed.update(exact)

    viewer = evidence_context.viewer_index
    for evidence in viewer.values():
        values = {
            str(evidence.receipt_no),
            str(evidence.revision_root_receipt_no),
            str(evidence.economic_body_receipt_no),
            *(str(value) for value in evidence.family_receipt_nos),
            *(str(value) for value in evidence.official_family_order),
            *(
                str(value).partition(":")[0]
                for value in evidence.attachment_keys
            ),
        }
        admit_family(values)

    support_manifest = root / SUPPORT_FAMILY_MANIFEST_RELATIVE_PATH
    if support_manifest.is_file():
        support = evidence_context.support_snapshot
        if support is None:
            raise RuntimeError("support-family manifest was not verified")
        for entry in support.entries:
            values = {
                str(entry.root_receipt_no),
                str(entry.terminal_receipt_no),
                str(entry.terminal_economic_receipt_no),
                *(str(value) for value in entry.ordered_family_receipts),
                *(str(source.receipt_no) for source in entry.sources),
            }
            admit_family(values)
    return allowed


def _receipt_is_in_action_scope(
    row: dict,
    *,
    coverage_start: date,
    coverage_end: date,
    lineage_receipts: set[str],
) -> bool:
    receipt = str(row.get("rcept_no") or "")
    accepted = _parse_date(row.get("rcept_dt"))
    if accepted is None:
        raise RuntimeError(
            "scoped DART action disclosure has no valid official rcept_dt: "
            f"{receipt or '<missing>'}"
        )
    return (
        coverage_start <= accepted <= coverage_end
        or receipt in lineage_receipts
    )


def prepare(
    base: str,
    *,
    target_date: date | None = None,
    coverage_start: date | None = None,
    coverage_end: date | None = None,
    verified_snapshot_sha256: str | None = None,
) -> tuple[pd.DataFrame, dict]:
    """로컬 Bronze에서 기업행사 증거를 읽고 표준 DataFrame과 통계를 반환한다.

    A paired ``coverage_start``/``coverage_end`` makes publication scope
    explicit.  Rows are then admitted by official list ``rcept_dt`` or by an
    exact verified viewer/support family identity, never by receipt prefix or
    by the mere presence of an out-of-range Bronze interval.
    """
    if (coverage_start is None) != (coverage_end is None):
        raise ValueError("coverage_start/coverage_end must be provided together")
    if (
        coverage_start is not None
        and coverage_end is not None
        and coverage_end < coverage_start
    ):
        raise ValueError("coverage_end precedes coverage_start")
    base = str(Path(base).expanduser().resolve())
    cache_key = None
    if verified_snapshot_sha256 is not None:
        cache_key = (
            base,
            str(verified_snapshot_sha256),
            target_date.isoformat() if target_date is not None else None,
            coverage_start.isoformat() if coverage_start is not None else None,
            coverage_end.isoformat() if coverage_end is not None else None,
        )
        cached = _cached_prepare(cache_key)
        if cached is not None:
            print(
                "[corporate-actions] reused verified snapshot parse cache",
                flush=True,
            )
            return cached
    evidence_context = _prepare_evidence_context(
        base,
        coverage_start=coverage_start,
        coverage_end=coverage_end,
    )
    records: list[dict] = []
    all_disclosure_rows, disclosure_observation_audit = _disclosure_rows(
        base, include_audit=True,
    )
    lineage_receipts: set[str] = set()
    if coverage_start is not None and coverage_end is not None:
        lineage_receipts = _verified_lineage_receipts(
            base,
            coverage_start=coverage_start,
            coverage_end=coverage_end,
            disclosure_rows=all_disclosure_rows,
            evidence_context=evidence_context,
        )
        disclosure_rows = [
            (path, row)
            for path, row in all_disclosure_rows
            if _receipt_is_in_action_scope(
                row,
                coverage_start=coverage_start,
                coverage_end=coverage_end,
                lineage_receipts=lineage_receipts,
            )
        ]
    else:
        disclosure_rows = all_disclosure_rows
    scoped_receipts = {
        str(row.get("rcept_no") or "") for _, row in disclosure_rows
    }
    related_cash_corrections = _related_cash_correction_signatures(
        base,
        disclosure_rows,
        evidence_context=evidence_context,
    )
    all_disclosure_by_receipt = {
        str(row.get("rcept_no") or ""): row
        for _, row in all_disclosure_rows
    }
    structured_files = sorted(glob.glob(
        f"{base}/corporate_actions/dart/structured/"
        "event=*/year=*/corp=*/rcept=*.json"
    ))
    scoped_structured_file_count = 0
    for path in structured_files:
        raw = _read_json(path)
        receipt = str(raw.get("rcept_no") or "")
        if coverage_start is not None and receipt not in scoped_receipts:
            continue
        disclosure = all_disclosure_by_receipt.get(receipt, {})
        parsed = _structured_row(
            path,
            raw,
            disclosure.get("report_nm"),
            disclosure.get("corp_cls"),
            disclosure.get("rcept_dt"),
        )
        if parsed is not None:
            records.append(parsed)
            scoped_structured_file_count += 1

    disclosure_count = 0
    related_correction_excluded = 0
    for path, row in disclosure_rows:
        parsed = _disclosure_row(
            base, path, row, evidence_context=evidence_context,
        )
        if parsed is not None:
            signature = (
                normalize_krx_ticker(parsed["identifier"]),
                parsed["announcement_date"],
                parsed.get("record_date"),
                (
                    float(parsed["cash_amount"])
                    if parsed.get("cash_amount") is not None
                    else None
                ),
            )
            if (
                parsed["event_type"] == "cash_dividend"
                and signature in related_cash_corrections
            ):
                related_correction_excluded += 1
                continue
            records.append(parsed)
            disclosure_count += 1

    if not records:
        _assert_prepare_evidence_unchanged(evidence_context)
        empty = _empty()
        stats = {
            "row_count": 0,
            "structured_file_count": len(structured_files),
            "scoped_structured_file_count": scoped_structured_file_count,
            "coverage_excluded_structured_file_count": (
                len(structured_files) - scoped_structured_file_count
                if coverage_start is not None else 0
            ),
            "coverage_excluded_disclosure_count": (
                len(all_disclosure_rows) - len(disclosure_rows)
            ),
            "verified_lineage_receipt_count": len(lineage_receipts),
            "disclosure_event_count": 0,
            "related_company_correction_excluded_count": (
                related_correction_excluded
            ),
            "disclosure_observation_audit": disclosure_observation_audit,
        }
        if cache_key is not None:
            _remember_prepare(cache_key, empty, stats)
        return empty, stats

    events = pd.DataFrame(records, columns=COLUMNS)
    events["identifier"] = events["identifier"].map(normalize_krx_ticker)
    events = events.drop_duplicates(
        ["identifier", "event_type", "rcept_no", "source"],
        keep="last",
    ).reset_index(drop=True)
    events = _classify_share_count_comparability(events)
    if target_date is not None:
        lower = target_date - pd.Timedelta(days=180)
        upper = target_date + pd.Timedelta(days=30)
        relevant = (
            events["effective_date"].between(lower, upper)
            | events["announcement_date"].between(lower, upper)
        )
        events = events[relevant].reset_index(drop=True)
    _assert_prepare_evidence_unchanged(evidence_context)
    stats = {
        "row_count": len(events),
        "structured_file_count": len(structured_files),
        "scoped_structured_file_count": scoped_structured_file_count,
        "coverage_excluded_structured_file_count": (
            len(structured_files) - scoped_structured_file_count
            if coverage_start is not None else 0
        ),
        "coverage_excluded_disclosure_count": (
            len(all_disclosure_rows) - len(disclosure_rows)
        ),
        "verified_lineage_receipt_count": len(lineage_receipts),
        "disclosure_event_count": disclosure_count,
        "effective_date_count": int(events["effective_date"].notna().sum()),
        "expected_factor_count": int(events["expected_factor"].notna().sum()),
        "share_count_factor_count": int(
            events["share_count_factor"].notna().sum()
        ),
        "related_company_correction_excluded_count": (
            related_correction_excluded
        ),
        "disclosure_observation_audit": disclosure_observation_audit,
    }
    if cache_key is not None:
        _remember_prepare(cache_key, events, stats)
    return events, stats


def exclude_nontradable(
    events: pd.DataFrame,
    stats: dict,
    tradable_identifiers: set[str],
    unsupported_market_identifiers: set[str] | None = None,
) -> tuple[pd.DataFrame, dict]:
    """Keep DART actions only where the KRX price universe can use them.

    DART contains disclosures for KONEX, unlisted and pre-coverage issuers.
    They remain in Bronze; the exclusion is surfaced as an explicit Silver DQ
    modification rather than appearing as an identifier-mapping failure.
    """
    allowed = {str(value) for value in tradable_identifiers}
    unsupported = {
        str(value) for value in (unsupported_market_identifiers or set())
    }
    if events.empty:
        updated = dict(stats)
        for key in (
            "no_tradable_price_action", "unsupported_market_action",
        ):
            updated[key] = {
                "row_count": 0, "ticker_count": 0, "samples": [],
            }
        return events, updated

    identifiers = events["identifier"].astype(str)
    unsupported_excluded = events[
        ~identifiers.isin(allowed) & identifiers.isin(unsupported)
    ].copy()
    no_price_excluded = events[
        ~identifiers.isin(allowed) & ~identifiers.isin(unsupported)
    ].copy()
    retained = events[identifiers.isin(allowed)].reset_index(drop=True)

    def summarize(frame: pd.DataFrame) -> dict:
        if frame.empty:
            return {"row_count": 0, "ticker_count": 0, "samples": []}
        summary = (
            frame.assign(
                event_date=frame["effective_date"].where(
                    frame["effective_date"].notna(),
                    frame["announcement_date"],
                )
            )
            .groupby("identifier", as_index=False)
            .agg(
                row_count=("identifier", "size"),
                first_event_date=("event_date", "min"),
                last_event_date=("event_date", "max"),
            )
            .sort_values(
                ["row_count", "identifier"], ascending=[False, True],
            )
        )
        head = summary.head(20)
        return {
            "row_count": len(frame),
            "ticker_count": int(frame["identifier"].nunique()),
            "samples": (
                head.astype(object)
                .where(pd.notna(head), None)
                .to_dict("records")
            ),
        }

    updated = dict(stats)
    updated["transformed_rows"] = len(retained)
    updated["excluded_rows"] = int(updated.get("excluded_rows", 0)) + (
        len(unsupported_excluded) + len(no_price_excluded)
    )
    updated["no_tradable_price_action"] = summarize(no_price_excluded)
    updated["unsupported_market_action"] = summarize(unsupported_excluded)
    return retained, updated


def inherit_issuer_events(
    events: pd.DataFrame,
    preferred_to_common: dict[str, str],
) -> tuple[pd.DataFrame, dict]:
    """보통주 DART 행사를 동일 발행회사의 우선주에 증거로 복제한다."""
    if events.empty or not preferred_to_common:
        return events, {"preferred_ticker_count": 0, "inherited_event_count": 0}
    inherited = []
    identifiers = events["identifier"].astype(str)
    for preferred, common in sorted(preferred_to_common.items()):
        rows = events[identifiers.eq(str(common))].copy()
        if rows.empty:
            continue
        rows["issuer_parent_identifier"] = str(common)
        rows["issuer_event_inherited"] = True
        rows["identifier"] = str(preferred)
        cash_events = rows["event_type"].eq("cash_dividend")
        for column in ("cash_amount", "adjusted_cash_amount"):
            if column in rows:
                rows.loc[cash_events, column] = None
        inherited.append(rows)
    original = events.copy()
    original["issuer_parent_identifier"] = None
    original["issuer_event_inherited"] = False
    if not inherited:
        return original, {
            "preferred_ticker_count": 0,
            "inherited_event_count": 0,
        }
    expanded = pd.concat([original, *inherited], ignore_index=True)
    return expanded, {
        "preferred_ticker_count": len(inherited),
        "inherited_event_count": len(expanded) - len(original),
    }


PUBLISH_COLUMNS = [
    "asset_id", "source", "action_key", "action_type", "announcement_date",
    "ex_date", "record_date", "payment_date", "cash_amount",
    "adjusted_cash_amount", "currency", "frequency",
    "ratio_numerator", "ratio_denominator", "expected_price_factor",
    "share_count_factor", "status", "confidence", "filing_id",
    "report_name", "dart_rm", "corp_cls", "action_scope",
    "cash_amount_status", "source_evidence_status",
    "correction_of_action_key", "revision_root_action_key",
    "revision_kind", "viewer_evidence_sha256",
    "economic_evidence_sha256",
    "reviewed_correction_id", "payment_date_quality_status",
    "source_body_sha256",
    "quality_run_id",
]


def _action_key(row) -> str:
    receipt = str(getattr(row, "rcept_no", "") or "").strip()
    if receipt:
        return receipt
    material = "|".join(
        str(value or "")
        for value in (
            getattr(row, "identifier", None),
            getattr(row, "event_type", None),
            getattr(row, "announcement_date", None),
            getattr(row, "effective_date", None),
            getattr(row, "source_file", None),
        )
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def normalize_for_publish(candidates: pd.DataFrame) -> pd.DataFrame:
    """Convert DART evidence rows to the persistent corporate-action shape."""
    if candidates.empty:
        return pd.DataFrame(columns=["identifier", *PUBLISH_COLUMNS[1:-1]])
    records = []
    for row in candidates.itertuples(index=False):
        records.append({
            "identifier": str(row.identifier),
            "asset_id": getattr(row, "asset_id", None),
            "source": str(row.source),
            "action_key": _action_key(row),
            "action_type": str(row.event_type),
            "announcement_date": row.announcement_date,
            "ex_date": row.effective_date,
            "record_date": getattr(row, "record_date", None),
            "payment_date": getattr(row, "payment_date", None),
            "cash_amount": getattr(row, "cash_amount", None),
            "adjusted_cash_amount": getattr(
                row, "adjusted_cash_amount", None,
            ),
            "currency": getattr(row, "currency", None) or "KRW",
            "frequency": getattr(row, "frequency", None),
            "ratio_numerator": getattr(row, "ratio_numerator", None),
            "ratio_denominator": getattr(row, "ratio_denominator", None),
            "expected_price_factor": row.expected_factor,
            "share_count_factor": row.share_count_factor,
            "status": (
                "confirmed" if row.effective_date is not None else "announced"
            ),
            "confidence": row.confidence,
            "filing_id": str(row.rcept_no or "") or None,
            "report_name": getattr(row, "report_name", None),
            "dart_rm": getattr(row, "dart_rm", None),
            "corp_cls": getattr(row, "corp_cls", None),
            "action_scope": getattr(row, "action_scope", None) or "UNKNOWN",
            "cash_amount_status": getattr(row, "cash_amount_status", None),
            "source_evidence_status": getattr(
                row, "source_evidence_status", None,
            ),
            "correction_of_action_key": getattr(
                row, "correction_of_action_key", None,
            ),
            "revision_root_action_key": getattr(
                row, "revision_root_action_key", None,
            ),
            "revision_kind": getattr(row, "revision_kind", None),
            "viewer_evidence_sha256": getattr(
                row, "viewer_evidence_sha256", None,
            ),
            "economic_evidence_sha256": getattr(
                row, "economic_evidence_sha256", None,
            ),
            "reviewed_correction_id": getattr(
                row, "reviewed_correction_id", None,
            ),
            "payment_date_quality_status": getattr(
                row, "payment_date_quality_status", None,
            ),
            "source_body_sha256": getattr(
                row, "source_body_sha256", None,
            ),
        })
    return pd.DataFrame(records)


def publish(
    conn,
    candidates: pd.DataFrame,
    identifier_map: dict[str, int],
    quality_run_id: UUID,
) -> int:
    """DQ에만 쓰던 DART 기업행사를 Silver 테이블에도 영속화한다."""
    frame = normalize_for_publish(candidates)
    if frame.empty:
        return 0
    if "asset_id" not in frame or frame["asset_id"].isna().all():
        frame["asset_id"] = frame["identifier"].map(identifier_map)
    if frame["asset_id"].isna().any():
        missing = frame.loc[
            frame["asset_id"].isna(), "identifier"
        ].astype(str).unique().tolist()
        raise RuntimeError(
            "quality gate missed unmapped corporate-action identifiers: "
            f"{sorted(missing)[:20]}"
        )
    frame["asset_id"] = frame["asset_id"].astype("int64")
    frame["quality_run_id"] = quality_run_id
    records = frame.to_dict("records")
    if not records:
        return 0
    rows = list(
        frame[PUBLISH_COLUMNS].astype(object).where(
            pd.notna(frame[PUBLISH_COLUMNS]), None,
        ).itertuples(index=False, name=None)
    )
    base_return_action = (
        frame["source"].eq("DART_DISCLOSURE")
        & frame["action_type"].isin(("cash_dividend", "ex_dividend"))
    )
    scale_support_action = (
        (
            frame["source"].isin(("DART_STRUCTURED", "DART_VIEWER"))
            & frame["action_type"].eq("bonus_issue")
        )
        | (
            frame["source"].eq("DART_DISCLOSURE")
            & frame["action_type"].isin((
                "stock_dividend", "rights_detachment",
                "combined_detachment",
            ))
        )
        | (
            frame["source"].eq("KRX_KIND")
            & frame["action_type"].eq("stock_dividend")
        )
    )
    invalidates_total_return = (
        (base_return_action | scale_support_action)
        & frame["action_scope"].eq("ISSUER")
    ).any()
    if invalidates_total_return:
        acquire_return_writer_transaction_lock(conn)
    count = db.upsert(
        conn,
        "corporate_action",
        PUBLISH_COLUMNS,
        rows,
        conflict=["asset_id", "source", "action_key"],
        update=[
            "action_type", "announcement_date", "ex_date", "record_date",
            "payment_date", "cash_amount", "adjusted_cash_amount", "currency",
            "frequency", "ratio_numerator",
            "ratio_denominator", "expected_price_factor", "share_count_factor",
            "status", "confidence", "filing_id", "report_name",
            "dart_rm", "corp_cls", "action_scope", "cash_amount_status",
            "source_evidence_status", "correction_of_action_key",
            "revision_root_action_key", "revision_kind",
            "viewer_evidence_sha256", "economic_evidence_sha256",
            "reviewed_correction_id", "payment_date_quality_status",
            "source_body_sha256",
            "quality_run_id", "loaded_at",
        ],
        temp_name="_stg_corporate_action_publish",
    )
    print(f"[corporate-actions] corporate_action upsert {count}행")
    if count > 0 and invalidates_total_return:
        invalidate_krx_total_return(
            conn,
            reason="ISSUER_DIVIDEND_ACTION_PUBLISHED",
            quality_run_id=quality_run_id,
        )
    return count
