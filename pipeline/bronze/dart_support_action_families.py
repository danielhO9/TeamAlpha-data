"""Immutable official DART families for non-cash dividend support actions.

The OpenDART list and structured endpoints identify filings, but they do not
publish a correction-family key.  Joining rows by issuer and date can therefore
merge two independent decisions or select a superseded ratio.  This module
captures DART's official ``main.do`` family selector and the corresponding
viewer body for every stock-dividend and structured bonus-issue candidate.

Collection is local-only and opt-in: without ``--apply`` no HTTP request and no
write is performed.  Main/body objects are stored under their SHA-256 names and
the current complete manifest is replaced atomically only after a complete
round-trip verification.  Older content-addressed objects remain immutable.
Silver consumes the verified family identity; it must not reconstruct a family
from dates or receipt-number proximity.
"""
from __future__ import annotations

import argparse
import hashlib
import html
import json
import math
import os
import random
import re
import tempfile
import time
from dataclasses import asdict, dataclass, fields
from datetime import date
from html.parser import HTMLParser
from pathlib import Path
from typing import Callable, Iterable
from urllib.parse import urlencode

import requests

from pipeline.bronze.corporate_actions import (
    _candidate_corp_to_stock,
    _document_candidate_receipts,
    _is_listed_disclosure_candidate,
    _listed_disclosure_ticker,
    _structured_query_keys,
)
from pipeline.bronze.dart_disclosure_observations import (
    canonicalize_disclosures,
    immutable_disclosure_changes,
)


SCHEMA_VERSION = "dart_support_action_family_snapshot_v3"
# The serialized field layout remains v3.  v4 records the stricter semantic
# source interpretation (viewer fallback, optional origin provenance, and
# body-labelled terminal states) without adding mutable manifest fields.
SOURCE_CONTRACT = "dart_official_main_family_attachment_viewer_body_v4"
FAMILY_ORDER_CONTRACT = "OFFICIAL_MAIN_AND_ATTACHMENT_NEWEST_TO_OLDEST"
TERMINAL_RATIO_CONTRACT = (
    "PER_ELIGIBLE_COMMON_SHARE_ENTITLEMENT_NOT_PRICE_DILUTION"
)
# The certified total-return action history starts here.  A correction inside
# that history can legitimately point at an older official DART family root.
# Complete pre-coverage list intervals therefore remain available below as
# exact dependency lookup/provenance, but they must not silently enlarge the
# support candidate set (or its digest) before the certified coverage floor.
SUPPORT_FAMILY_COVERAGE_START = date(2015, 1, 1)
MANIFEST_RELATIVE_PATH = Path(
    "corporate_actions/dart/support_action_families/manifest.json"
)
OBJECT_ROOT_RELATIVE_PATH = Path(
    "corporate_actions/dart/support_action_families/objects"
)
MAIN_URL = "https://dart.fss.or.kr/dsaf001/main.do"
VIEWER_URL = "https://dart.fss.or.kr/report/viewer.do"
_RECEIPT = re.compile(r"^[0-9]{14}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_INTERVAL = re.compile(r"^[0-9]{8}$")
_REVISION_MARKERS = ("정정", "철회", "취소", "부결")
_TERMINATED_MARKERS = (
    ("철회", "WITHDRAWN"),
    ("취소", "CANCELLED"),
    ("부결", "DENIED"),
)


@dataclass(frozen=True)
class SupportActionFamilySource:
    receipt_no: str
    official_position: int
    dcm_no: str
    dtd: str
    current_selector: str
    main_family_receipts: tuple[str, ...]
    main_attachment_keys: tuple[str, ...]
    attachment_family_root_receipt_no: str | None
    report_name: str
    receipt_date: str
    correction_of_receipt_no: str | None
    correction_origin_date: str | None
    revision_kind: str
    main_path: str
    main_content_length: int
    main_sha256: str
    body_path: str
    body_content_length: int
    body_sha256: str
    disclosure_path: str
    disclosure_content_length: int
    disclosure_sha256: str
    disclosure_row_sha256: str
    disclosure_observation_digest: str
    disclosure_manifest_path: str
    disclosure_manifest_sha256: str
    structured_path: str | None
    structured_content_length: int | None
    structured_sha256: str | None
    structured_row_sha256: str | None


@dataclass(frozen=True)
class SupportActionFamilyEntry:
    """One exact official action family.

    ``terminal_ratio`` is the ordinary-share entitlement printed in DART's
    labelled per-eligible-share row.  It is not issued-share dilution and must
    not be transformed into a theoretical price factor with ``1 / (1 + r)``.
    """

    ticker: str
    action_type: str
    root_receipt_no: str
    terminal_receipt_no: str
    terminal_economic_receipt_no: str
    ordered_family_receipts: tuple[str, ...]
    original_submission_date: str
    terminal_status: str
    terminal_admissible: bool
    terminal_ratio: float | None
    fresh_row_bind_digest: str
    sources: tuple[SupportActionFamilySource, ...]


@dataclass(frozen=True)
class VerifiedSupportActionFamilies:
    base: str
    manifest_path: str
    manifest_sha256: str
    seed_coverage_start: date
    seed_coverage_end: date
    candidate_count: int
    candidate_digest: str
    entry_count: int
    entry_digest: str
    entries: tuple[SupportActionFamilyEntry, ...]


@dataclass(frozen=True)
class BonusIssueCommonTerms:
    """Exact ordinary-share entitlement and record date from a DART body."""

    common_ratio: float
    record_date: str


@dataclass(frozen=True)
class StockDividendCommonTerms:
    """Exact common-share entitlement and record date from a DART body."""

    common_ratio: float
    record_date: str


# Closed recovery set reviewed against the fresh official ``main.do`` family
# selector and the content-addressed terminal viewer body.  OpenDART does not
# expose current structured economics for these four corrected decisions, so
# Silver may use the viewer body only when every frozen family/economic field
# below matches exactly.  Keeping this identity in Bronze lets every consumer
# re-apply the same restriction instead of trusting the builder's selection.
VIEWER_STOCK_DIVIDEND_FALLBACKS = frozenset({
    ("032960", "20151216900093", "20151228900387", "2015-12-31", "0.05"),
    ("033540", "20151207900194", "20151228900494", "2015-12-31", "0.0085763"),
    ("045970", "20191202900143", "20191226900105", "2019-12-31", "0.04"),
    ("187270", "20171220900499", "20171222900807", "2017-12-31", "0.02"),
})


def viewer_stock_dividend_fallback_identity(
    ticker: object,
    root_receipt_no: object,
    terminal_receipt_no: object,
    record_date: object,
    common_ratio: object,
) -> tuple[str, str, str, str, str]:
    """Canonical identity used by all four viewer-fallback trust boundaries."""
    return (
        str(ticker).zfill(6),
        str(root_receipt_no),
        str(terminal_receipt_no),
        str(record_date),
        format(float(common_ratio), ".12g"),
    )


VIEWER_STOCK_DIVIDEND_CHILD_IDENTITIES = frozenset(
    (ticker, terminal, record_date, ratio)
    for ticker, _root, terminal, record_date, ratio
    in VIEWER_STOCK_DIVIDEND_FALLBACKS
)


@dataclass(frozen=True)
class _FileBind:
    path: str
    content_length: int
    sha256: str


@dataclass(frozen=True)
class _DisclosureBind:
    receipt_no: str
    ticker: str
    row: dict
    row_sha256: str
    observation_digest: str
    selected_manifest: _FileBind
    body: _FileBind


@dataclass(frozen=True)
class _StructuredBind:
    receipt_no: str
    ticker: str
    row: dict
    row_sha256: str
    body: _FileBind


@dataclass(frozen=True)
class _FreshSnapshot:
    disclosures: dict[str, _DisclosureBind]
    structured: dict[str, _StructuredBind]
    candidates: dict[str, tuple[str, str]]
    candidate_digest: str
    disclosure_audit: dict[str, object]


@dataclass(frozen=True)
class OfficialDartFamilyOption:
    receipt_no: str
    selected: bool
    title: str
    visible_text: str


@dataclass(frozen=True)
class OfficialDartAttachmentOption:
    receipt_no: str
    dcm_no: str
    selected: bool
    visible_text: str

    @property
    def key(self) -> str:
        return f"{self.receipt_no}:{self.dcm_no}"


@dataclass(frozen=True)
class OfficialDartMainPage:
    receipt_no: str
    dcm_no: str
    dtd: str
    current_selector: str
    family_receipts: tuple[str, ...]
    family_options: tuple[OfficialDartFamilyOption, ...]
    attachment_options: tuple[OfficialDartAttachmentOption, ...]

    @property
    def family_root_receipt_no(self) -> str:
        return self.family_receipts[-1]

    @property
    def attachment_keys(self) -> tuple[str, ...]:
        return tuple(option.key for option in self.attachment_options)


@dataclass(frozen=True)
class _Artifact:
    main: bytes
    body: bytes
    parsed_main: OfficialDartMainPage
    main_bind: _FileBind
    body_bind: _FileBind


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _sha256_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _compact(value: object) -> str:
    return re.sub(r"[^0-9A-Za-z가-힣]", "", str(value or ""))


def _row_digest(row: dict) -> str:
    return _sha256_bytes(_canonical_bytes(row))


def _relative(root: Path, path: Path) -> str:
    try:
        return path.resolve().relative_to(root).as_posix()
    except ValueError as exc:
        raise RuntimeError(f"support-family path escaped snapshot: {path}") from exc


def _bound_file(root: Path, path: Path) -> _FileBind:
    if not path.is_file():
        raise RuntimeError(f"support-family source file is missing: {path}")
    return _FileBind(
        path=_relative(root, path),
        content_length=path.stat().st_size,
        sha256=_sha256_path(path),
    )


def _safe_path(root: Path, relative: str) -> Path:
    path = Path(relative)
    if path.is_absolute() or ".." in path.parts:
        raise RuntimeError(f"unsafe support-family relative path: {relative}")
    resolved = (root / path).resolve()
    try:
        resolved.relative_to(root)
    except ValueError as exc:
        raise RuntimeError(
            f"support-family path escaped snapshot: {relative}"
        ) from exc
    return resolved


def _verify_file_bind(root: Path, bind: _FileBind) -> bytes:
    path = _safe_path(root, bind.path)
    if not path.is_file():
        raise RuntimeError(f"support-family evidence is missing: {bind.path}")
    payload = path.read_bytes()
    if len(payload) != bind.content_length or _sha256_bytes(payload) != bind.sha256:
        raise RuntimeError(f"support-family evidence changed: {bind.path}")
    return payload


def _verify_object_bind(root: Path, bind: _FileBind) -> bytes:
    if _SHA256.fullmatch(bind.sha256) is None:
        raise RuntimeError("support-family object has an invalid SHA-256")
    expected = (
        OBJECT_ROOT_RELATIVE_PATH / f"sha256={bind.sha256}.html"
    ).as_posix()
    if bind.path != expected:
        raise RuntimeError(
            "support-family object path is not content-addressed: "
            f"actual={bind.path} expected={expected}"
        )
    return _verify_file_bind(root, bind)


def _atomic_write(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent,
    )
    temporary = Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        try:
            directory_descriptor = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(directory_descriptor)
            finally:
                os.close(directory_descriptor)
        except OSError:
            pass
    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def _store_object(root: Path, payload: bytes) -> _FileBind:
    digest = _sha256_bytes(payload)
    relative = OBJECT_ROOT_RELATIVE_PATH / f"sha256={digest}.html"
    path = root / relative
    if path.is_file():
        if path.read_bytes() != payload:
            raise RuntimeError(f"SHA-256 object collision: {relative}")
    else:
        _atomic_write(path, payload)
    return _FileBind(relative.as_posix(), len(payload), digest)


def _decode(payload: bytes) -> str:
    candidates: list[str] = []
    for encoding in ("utf-8", "euc-kr", "cp949"):
        try:
            candidates.append(payload.decode(encoding))
        except UnicodeDecodeError:
            continue
    if not candidates:
        return payload.decode("utf-8", errors="replace")
    return max(
        candidates,
        key=lambda value: (
            len(re.findall(r"[가-힣]", value)),
            -value.count("�"),
            -value.count("?"),
        ),
    )


class _DartDocumentSelectParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.select_counts = {"family": 0, "att": 0}
        self._selector: str | None = None
        self._current: dict[str, object] | None = None
        self.family_options: list[OfficialDartFamilyOption] = []
        self.attachment_options: list[OfficialDartAttachmentOption] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attributes = {key.lower(): value for key, value in attrs}
        if tag.lower() == "select" and attributes.get("id") in {"family", "att"}:
            selector = str(attributes["id"])
            self.select_counts[selector] += 1
            self._selector = selector
            return
        if tag.lower() != "option" or self._selector is None:
            return
        value = str(attributes.get("value") or "")
        match = re.search(r"(?:^|[?&])rcpNo=(\d{14})(?:&|$)", value, re.I)
        dcm_match = re.search(r"(?:^|[?&])dcmNo=(\d+)(?:&|$)", value, re.I)
        self._current = {
            "receipt": match.group(1) if match else None,
            "dcm": dcm_match.group(1) if dcm_match else None,
            "selector": self._selector,
            "selected": "selected" in attributes,
            "title": str(attributes.get("title") or ""),
            "text": [],
        }

    def handle_data(self, data: str) -> None:
        if self._current is not None:
            text = self._current["text"]
            assert isinstance(text, list)
            text.append(data)

    def handle_endtag(self, tag: str) -> None:
        lowered = tag.lower()
        if lowered == "option" and self._current is not None:
            receipt = self._current["receipt"]
            if receipt:
                visible = re.sub(
                    r"\s+", " ", "".join(self._current["text"]),
                ).strip()
                if self._current["selector"] == "family":
                    if self._current["dcm"] is not None:
                        raise RuntimeError(
                            "DART family option unexpectedly carries dcmNo"
                        )
                    self.family_options.append(OfficialDartFamilyOption(
                        receipt_no=str(receipt),
                        selected=bool(self._current["selected"]),
                        title=str(self._current["title"]),
                        visible_text=visible,
                    ))
                else:
                    if self._current["dcm"] is None:
                        raise RuntimeError(
                            "DART attachment option has no dcmNo"
                        )
                    self.attachment_options.append(OfficialDartAttachmentOption(
                        receipt_no=str(receipt),
                        dcm_no=str(self._current["dcm"]),
                        selected=bool(self._current["selected"]),
                        visible_text=visible,
                    ))
            self._current = None
        elif lowered == "select" and self._selector is not None:
            self._selector = None


class _TableRowParser(HTMLParser):
    """Extract table cell boundaries without flattening unrelated numbers."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._row: list[str] | None = None
        self._cell: list[str] | None = None
        self.rows: list[tuple[str, ...]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        del attrs
        lowered = tag.lower()
        if lowered == "tr":
            if self._row is not None:
                raise RuntimeError("nested DART table rows are unsupported")
            self._row = []
        elif lowered in {"td", "th"} and self._row is not None:
            if self._cell is not None:
                raise RuntimeError("nested DART table cells are unsupported")
            self._cell = []
        elif lowered == "br" and self._cell is not None:
            self._cell.append(" ")

    def handle_data(self, data: str) -> None:
        if self._cell is not None:
            self._cell.append(data)

    def handle_endtag(self, tag: str) -> None:
        lowered = tag.lower()
        if lowered in {"td", "th"} and self._cell is not None:
            assert self._row is not None
            self._row.append(re.sub(r"\s+", " ", "".join(self._cell)).strip())
            self._cell = None
        elif lowered == "tr" and self._row is not None:
            if self._cell is not None:
                raise RuntimeError("unterminated DART table cell")
            if self._row:
                self.rows.append(tuple(self._row))
            self._row = None


def parse_official_dart_main_page(
    receipt: str,
    payload: bytes,
    *,
    expected_attachment_only: bool,
) -> OfficialDartMainPage:
    """Parse the exact official main/attachment selection and viewer target."""
    if _RECEIPT.fullmatch(receipt) is None:
        raise RuntimeError(f"invalid support-family receipt: {receipt}")
    rendered = html.unescape(_decode(payload))
    parser = _DartDocumentSelectParser()
    parser.feed(rendered)
    if parser.select_counts != {"family": 1, "att": 1}:
        raise RuntimeError(
            "DART family/attachment selects are missing/ambiguous: "
            f"receipt={receipt} counts={parser.select_counts}"
        )
    if not parser.family_options:
        raise RuntimeError(
            f"DART official family select is missing/ambiguous: {receipt}"
        )
    ordered = tuple(option.receipt_no for option in parser.family_options)
    if len(ordered) != len(set(ordered)):
        raise RuntimeError(f"DART official family has duplicate receipts: {receipt}")
    if ordered != tuple(sorted(ordered, key=int, reverse=True)):
        raise RuntimeError(
            f"DART official family order is not newest-to-oldest: {receipt}"
        )
    attachment_keys = [option.key for option in parser.attachment_options]
    if len(attachment_keys) != len(set(attachment_keys)):
        raise RuntimeError(
            f"DART official attachment selector has duplicate keys: {receipt}"
        )
    family_selected = [
        option for option in parser.family_options if option.selected
    ]
    attachment_selected = [
        option for option in parser.attachment_options if option.selected
    ]
    if len(family_selected) + len(attachment_selected) != 1:
        raise RuntimeError(
            "DART family-or-attachment current selection is missing/ambiguous: "
            f"receipt={receipt} family_selected="
            f"{[item.receipt_no for item in family_selected]} "
            f"attachment_selected={[item.key for item in attachment_selected]}"
        )
    if family_selected:
        if family_selected[0].receipt_no != receipt:
            raise RuntimeError("DART family selector selected another receipt")
        current_selector = "FAMILY"
    else:
        if attachment_selected[0].receipt_no != receipt:
            raise RuntimeError("DART attachment selector selected another receipt")
        current_selector = "ATTACHMENT"
    if expected_attachment_only != (current_selector == "ATTACHMENT"):
        raise RuntimeError(
            "DART disclosure attachment kind/official selector mismatch: "
            f"receipt={receipt} selector={current_selector}"
        )
    if current_selector == "ATTACHMENT" and receipt in ordered:
        raise RuntimeError(
            f"DART attachment correction leaked into family selector: {receipt}"
        )
    if current_selector == "FAMILY" and receipt not in ordered:
        raise RuntimeError(f"DART family selector omits current receipt: {receipt}")
    view_matches = set(re.findall(
        r"viewDoc\(\s*[\"']"
        + re.escape(receipt)
        + r"[\"']\s*,\s*[\"'](\d+)[\"']\s*,\s*"
        r"[\"']0[\"']\s*,\s*[\"']0[\"']\s*,\s*[\"']0[\"']\s*,\s*"
        r"[\"']([0-9A-Za-z_.-]+)[\"']",
        rendered,
    ))
    if len(view_matches) != 1:
        raise RuntimeError(
            f"DART official viewer dcmNo/dtd is missing/ambiguous: {receipt}"
        )
    dcm_no, dtd = next(iter(view_matches))
    if current_selector == "ATTACHMENT" and (
        attachment_selected[0].dcm_no != dcm_no
    ):
        raise RuntimeError(
            "DART selected attachment/viewer dcmNo mismatch: "
            f"receipt={receipt} selected={attachment_selected[0].dcm_no} "
            f"viewer={dcm_no}"
        )
    return OfficialDartMainPage(
        receipt_no=receipt,
        dcm_no=dcm_no,
        dtd=dtd,
        current_selector=current_selector,
        family_receipts=ordered,
        family_options=tuple(parser.family_options),
        attachment_options=tuple(parser.attachment_options),
    )


def _visible(payload: bytes) -> str:
    rendered = _decode(payload)
    visible = html.unescape(re.sub(r"<[^>]+>", " ", rendered))
    return re.sub(r"\s+", " ", visible).strip()


def parse_dart_date(value: object) -> str | None:
    """Normalize an exact DART calendar date with component validation.

    DART emits both compact API dates and human-rendered dates whose month or
    day is not zero-padded.  Removing every non-digit character is ambiguous
    for the latter (``2024년 7월 1일`` becomes seven digits), so each calendar
    component is parsed independently and validated by ``datetime.date``.
    """
    rendered = str(value or "").strip()
    compact = re.fullmatch(r"((?:19|20)\d{2})(\d{2})(\d{2})", rendered)
    separated = re.fullmatch(
        r"((?:19|20)\d{2})\s*(?:년|[./-])\s*"
        r"(\d{1,2})\s*(?:월|[./-])\s*(\d{1,2})\s*일?",
        rendered,
    )
    match = compact or separated
    if match is None:
        return None
    try:
        return date(*(int(part) for part in match.groups())).isoformat()
    except ValueError:
        return None


def _correction_origin_date(payload: bytes) -> str:
    visible = _visible(payload)
    matches = re.findall(
        r"(?:정정\s*관련\s*공시서류\s*제출일|최초\s*제출일)"
        r"\s*[:：]?\s*((?:19|20)\d{2}\s*(?:년|[./-])?\s*"
        r"\d{1,2}\s*(?:월|[./-])?\s*\d{1,2}\s*일?)",
        visible,
    )
    parsed = {
        value for raw in matches
        if (value := parse_dart_date(raw)) is not None
    }
    if len(parsed) != 1:
        raise RuntimeError("correction original-submission date is missing/ambiguous")
    return next(iter(parsed))


def _number(value: object) -> float | None:
    rendered = str(value or "").replace(",", "").strip()
    try:
        parsed = float(rendered)
    except ValueError:
        return None
    return parsed if math.isfinite(parsed) else None


def stock_dividend_common_ratio_from_body(payload: bytes) -> float | None:
    """Parse only the ordinary-share value in the labelled per-share row.

    DART's stock-dividend body contains much larger values in adjacent rows
    (total dividend shares and issued shares) and in free-form notes.  Numeric
    proximity is therefore unsafe.  The admissible shape is one exact report
    row: ``1주당 배당주식수`` (or the older ``1주당 주식배당`` label), then an
    ordinary-share class cell, then one numeric value cell.
    """
    parser = _TableRowParser()
    parser.feed(_decode(payload))
    allowed_labels = {
        "1주당배당주식수주",
        "1주당배당주식수",
        "1주당주식배당주",
        "1주당주식배당",
    }
    ordinary_classes = {"보통주", "보통주식"}
    observed: list[float] = []
    for row in parser.rows:
        for index, cell in enumerate(row):
            without_item_number = re.sub(
                r"^\s*\d+\s*[.)]\s*", "", cell,
            )
            if _compact(without_item_number) not in allowed_labels:
                continue
            if index + 2 >= len(row):
                continue
            if _compact(row[index + 1]) not in ordinary_classes:
                continue
            rendered_value = re.sub(
                r"\s*주\s*$", "", row[index + 2], flags=re.IGNORECASE,
            ).replace(",", "").strip()
            if re.fullmatch(r"[0-9]+(?:\.[0-9]+)?", rendered_value) is None:
                continue
            value = _number(rendered_value)
            if value is not None and value >= 0:
                observed.append(value)
    unique = set(observed)
    if not unique:
        return None
    if len(unique) != 1:
        raise RuntimeError("terminal stock-dividend ratio is ambiguous")
    return next(iter(unique))


def stock_dividend_common_terms_from_body(
    payload: bytes,
) -> StockDividendCommonTerms | None:
    """Parse one exact common-share stock-dividend ratio and record date.

    The family snapshot already binds the selected official body.  A viewer
    fallback must additionally prove that this one body contains both current
    economics; combining a ratio from one revision with a date from another is
    deliberately inadmissible.
    """
    ratio = stock_dividend_common_ratio_from_body(payload)
    parser = _TableRowParser()
    parser.feed(_decode(payload))
    date_labels = {"배당기준일"}
    record_dates: list[str] = []
    for row in parser.rows:
        for index, cell in enumerate(row[:-1]):
            without_item_number = re.sub(
                r"^\s*\d+\s*[.)]\s*", "", cell,
            )
            if _compact(without_item_number) not in date_labels:
                continue
            parsed = parse_dart_date(row[index + 1])
            if parsed is not None:
                record_dates.append(parsed)
    unique_dates = set(record_dates)
    if ratio is None and not unique_dates:
        return None
    if ratio is None or len(unique_dates) != 1:
        raise RuntimeError(
            "stock-dividend common terms are missing/ambiguous: "
            f"ratio={ratio} dates={sorted(unique_dates)}"
        )
    return StockDividendCommonTerms(
        common_ratio=ratio,
        record_date=next(iter(unique_dates)),
    )


def _stock_dividend_is_cross_class_distribution(payload: bytes) -> bool:
    """Return whether ordinary holders receive an exact preferred-share ratio."""
    parser = _TableRowParser()
    parser.feed(_decode(payload))
    ordinary_missing = False
    preferred_ratios: list[float] = []
    main_labels = {
        "1주당배당주식수주", "1주당배당주식수",
        "1주당주식배당주", "1주당주식배당",
    }
    ordinary_classes = {"보통주", "보통주식"}
    for row in parser.rows:
        for index, cell in enumerate(row):
            label = _compact(re.sub(r"^\s*\d+\s*[.)]\s*", "", cell))
            if label not in main_labels or index + 2 >= len(row):
                continue
            if _compact(row[index + 1]) not in ordinary_classes:
                continue
            if row[index + 2].strip() == "-":
                ordinary_missing = True
    for index, header in enumerate(parser.rows[:-1]):
        compact_header = tuple(_compact(cell) for cell in header)
        if (
            "종류주식구분" not in compact_header
            or "1주당배당주식수주" not in compact_header
        ):
            continue
        class_index = compact_header.index("종류주식구분")
        ratio_index = compact_header.index("1주당배당주식수주")
        row = parser.rows[index + 1]
        if max(class_index, ratio_index) >= len(row):
            continue
        if _compact(row[class_index]) not in {"우선주", "우선주식"}:
            continue
        rendered = row[ratio_index].replace(",", "").strip()
        if re.fullmatch(r"[0-9]+(?:\.[0-9]+)?", rendered):
            ratio = _number(rendered)
            if ratio is not None and ratio > 0:
                preferred_ratios.append(ratio)
    return ordinary_missing and len(set(preferred_ratios)) == 1


def bonus_issue_common_terms_from_body(
    payload: bytes,
) -> BonusIssueCommonTerms | None:
    """Parse one exact common-share bonus ratio and allocation record date.

    Only the labelled decision-table rows are admissible.  Free-form numbers,
    total share counts, and preferred/other-security rows are deliberately
    ignored.  A partial match or conflicting exact rows fails closed; a body
    with neither term (for example an attachment-only document) returns
    ``None`` so the caller can use the economic family member instead.
    """
    parser = _TableRowParser()
    parser.feed(_decode(payload))
    ratio_labels = {"1주당신주배정주식수", "1주당신주배정주식수주"}
    date_labels = {"신주배정기준일"}
    ordinary_classes = {"보통주", "보통주주", "보통주식", "보통주식주"}
    ratios: list[float] = []
    record_dates: list[str] = []
    for row in parser.rows:
        for index, cell in enumerate(row):
            without_item_number = re.sub(
                r"^\s*\d+\s*[.)]\s*", "", cell,
            )
            label = _compact(without_item_number)
            if label in ratio_labels:
                if index + 2 >= len(row):
                    continue
                if _compact(row[index + 1]) not in ordinary_classes:
                    continue
                rendered = re.sub(
                    r"\s*주\s*$", "", row[index + 2], flags=re.IGNORECASE,
                ).replace(",", "").strip()
                if re.fullmatch(r"[0-9]+(?:\.[0-9]+)?", rendered):
                    value = _number(rendered)
                    if value is not None and value >= 0:
                        ratios.append(value)
            elif label in date_labels and index + 1 < len(row):
                parsed = parse_dart_date(row[index + 1])
                if parsed is not None:
                    record_dates.append(parsed)
    unique_ratios = set(ratios)
    unique_dates = set(record_dates)
    if not unique_ratios and not unique_dates:
        return None
    if len(unique_ratios) != 1 or len(unique_dates) != 1:
        raise RuntimeError(
            "bonus-issue common terms are missing/ambiguous: "
            f"ratios={sorted(unique_ratios)} dates={sorted(unique_dates)}"
        )
    return BonusIssueCommonTerms(
        common_ratio=next(iter(unique_ratios)),
        record_date=next(iter(unique_dates)),
    )


def _action_type(report_name: object) -> str | None:
    compact = _compact(report_name)
    if "자회사의주요경영사항" in compact or "종속회사의주요경영사항" in compact:
        return None
    if "주식배당결정" in compact:
        return "stock_dividend"
    # A bonus-action seed must be the issuer's pure bonus decision report.
    # DART list titles also repeat ``무상증자결정`` in independent exchange
    # notices and in ``유무상증자결정`` reports.  The latter belong to the
    # combined-offering structured/event contract, not this bonus-only support
    # schema.  Exclude both shapes before family collection; genuine pure-bonus
    # corrections/attachments retain ``주요사항보고서(무상증자결정)``.
    if (
        "주요사항보고서" in compact
        and "무상증자결정" in compact
        and "유무상증자결정" not in compact
    ):
        return "bonus_issue"
    return None


def _is_revision(report_name: object) -> bool:
    compact = _compact(report_name)
    return any(marker in compact for marker in _REVISION_MARKERS)


def _is_attachment_only(report_name: object) -> bool:
    return "첨부정정" in _compact(report_name)


def _manifest_interval(path: Path) -> tuple[str, str]:
    start = path.parent.parent.name.removeprefix("from=")
    end = path.parent.name.removeprefix("to=")
    if (
        _INTERVAL.fullmatch(start) is None
        or _INTERVAL.fullmatch(end) is None
        or start > end
    ):
        raise RuntimeError(f"invalid DART interval path: {path}")
    return start, end


def _read_json(path: Path) -> object:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"invalid support-family JSON source: {path}") from exc


def _verify_marker(
    path: Path,
    start: str,
    end: str,
    *,
    count_field: str,
    expected_count: int,
) -> None:
    marker = _read_json(path)
    if not isinstance(marker, dict) or marker.get("status") != "COMPLETE":
        raise RuntimeError(f"DART completion marker is not COMPLETE: {path}")
    if str(marker.get("fromdate") or "") != start or str(
        marker.get("todate") or ""
    ) != end:
        raise RuntimeError(f"DART completion marker interval mismatch: {path}")
    try:
        observed_count = int(marker.get(count_field, -1))
    except (TypeError, ValueError) as exc:
        raise RuntimeError(
            f"DART completion marker {count_field} is invalid: {path}"
        ) from exc
    if observed_count != expected_count:
        raise RuntimeError(
            f"DART completion marker {count_field} parity mismatch: "
            f"path={path} observed={observed_count} expected={expected_count}"
        )


def _individual_disclosure_path(root: Path, row: dict, ticker: str) -> Path:
    receipt = str(row.get("rcept_no") or "")
    rendered_date = parse_dart_date(str(row.get("rcept_dt") or ""))
    if _RECEIPT.fullmatch(receipt) is None or rendered_date is None:
        raise RuntimeError(f"invalid DART disclosure identity: {receipt}")
    return (
        root / "corporate_actions" / "dart" / "disclosures"
        / f"year={receipt[:4]}" / f"date={rendered_date}"
        / f"corp={ticker}" / f"rcept={receipt}.json"
    )


def _ticker(value: object) -> str:
    rendered = str(value or "").strip()
    if re.fullmatch(r"[0-9]{1,6}", rendered):
        return rendered.zfill(6)
    rendered = rendered.upper()
    if re.fullmatch(r"[0-9A-Z]{6}", rendered):
        return rendered
    raise RuntimeError(f"invalid DART listed ticker: {value!r}")


def _observation_digest(
    root: Path,
    observations: Iterable[tuple[Path, dict]],
) -> str:
    rows = [
        {
            "path": _relative(root, path),
            "row_sha256": _row_digest(row),
        }
        for path, row in observations
    ]
    rows.sort(key=lambda item: (item["path"], item["row_sha256"]))
    return _sha256_bytes(_canonical_bytes(rows))


def _load_fresh_snapshot(
    root: Path,
    *,
    coverage_start: date,
    coverage_end: date,
) -> _FreshSnapshot:
    if coverage_end < coverage_start:
        raise ValueError("support-family coverage_end precedes coverage_start")
    corp_to_stock: dict[str, str] = {}
    manifest_root = root / "corporate_actions" / "dart" / "manifests"
    complete_observations: list[tuple[Path, dict]] = []
    observations_by_receipt: dict[str, list[tuple[Path, dict]]] = {}
    complete_receipts: set[str] = set()
    incomplete_relevant: dict[str, dict] = {}
    complete_interval_count = 0
    complete_intervals: list[tuple[str, str]] = []
    for path in sorted(manifest_root.glob("from=*/to=*/disclosures_v3.json")):
        start, end = _manifest_interval(path)
        rows = _read_json(path)
        if not isinstance(rows, list):
            raise RuntimeError(f"DART disclosures must be a list: {path}")
        corp_to_stock.update(_candidate_corp_to_stock(str(root), rows))
        relevant = {
            str(row.get("rcept_no") or ""): row
            for row in rows
            if isinstance(row, dict)
            and _is_listed_disclosure_candidate(row, corp_to_stock)
            and _action_type(row.get("report_nm")) is not None
        }
        structured_marker = path.parent / "structured_complete_v3.json"
        document_marker = path.parent / "documents_complete_v5.json"
        if not structured_marker.is_file() or not document_marker.is_file():
            incomplete_relevant.update(relevant)
            continue
        structured_queries = _structured_query_keys(rows, corp_to_stock)
        document_candidates = _document_candidate_receipts(
            rows, corp_to_stock,
        )
        _verify_marker(
            structured_marker,
            start,
            end,
            count_field="query_count",
            expected_count=len(structured_queries),
        )
        _verify_marker(
            document_marker,
            start,
            end,
            count_field="candidate_count",
            expected_count=len(document_candidates),
        )
        complete_interval_count += 1
        complete_intervals.append((start, end))
        for row in rows:
            if (
                not isinstance(row, dict)
                or not _is_listed_disclosure_candidate(row, corp_to_stock)
            ):
                continue
            receipt = str(row.get("rcept_no") or "")
            if not receipt:
                continue
            complete_observations.append((path, row))
            observations_by_receipt.setdefault(receipt, []).append((path, row))
            complete_receipts.add(receipt)
    if complete_interval_count == 0:
        raise RuntimeError("no documents_complete_v5 DART interval")
    uncovered = sorted(set(incomplete_relevant) - complete_receipts)
    if uncovered:
        raise RuntimeError(
            "support-action disclosures occur only in incomplete v5 intervals: "
            f"{uncovered[:20]}"
        )
    canonical, audit = canonicalize_disclosures(
        complete_observations, audit_root=root,
    )
    disclosures: dict[str, _DisclosureBind] = {}
    for receipt, (manifest_path_string, row) in canonical.items():
        if not isinstance(row, dict):
            raise RuntimeError(f"canonical DART disclosure is not an object: {receipt}")
        if _action_type(row.get("report_nm")) is None:
            continue
        ticker = _ticker(_listed_disclosure_ticker(row, corp_to_stock))
        individual = _individual_disclosure_path(root, row, ticker)
        individual_row = _read_json(individual)
        immutable_changes = immutable_disclosure_changes(individual_row, row)
        if immutable_changes:
            raise RuntimeError(
                "individual/canonical DART disclosure mismatch: "
                f"receipt={receipt} fields={list(immutable_changes)} "
                f"path={individual}"
            )
        manifest_path = Path(manifest_path_string)
        disclosures[receipt] = _DisclosureBind(
            receipt_no=receipt,
            ticker=ticker,
            row=row,
            row_sha256=_row_digest(row),
            observation_digest=_observation_digest(
                root, observations_by_receipt.get(receipt, ()),
            ),
            selected_manifest=_bound_file(root, manifest_path),
            body=_bound_file(root, individual),
        )

    structured: dict[str, _StructuredBind] = {}
    structured_root = (
        root / "corporate_actions" / "dart" / "structured"
        / "event=bonus_issue"
    )
    for path in sorted(structured_root.glob("year=*/corp=*/rcept=*.json")):
        row = _read_json(path)
        if not isinstance(row, dict):
            raise RuntimeError(f"DART structured row must be an object: {path}")
        receipt = str(row.get("rcept_no") or "")
        if _RECEIPT.fullmatch(receipt) is None:
            raise RuntimeError(f"invalid structured bonus receipt: {path}")
        ticker_match = re.fullmatch(
            r"corp=([0-9A-Za-z]{6}|[0-9]{1,6})", path.parent.name,
        )
        if ticker_match is None:
            raise RuntimeError(f"invalid structured bonus ticker path: {path}")
        ticker = ticker_match.group(1).zfill(6)
        if receipt in structured:
            raise RuntimeError(f"duplicate structured bonus receipt: {receipt}")
        disclosure = disclosures.get(receipt)
        if disclosure is None:
            # Structured endpoints are queried from the full 2015 history.
            # A row outside every freshly complete list interval is not a
            # candidate for the current certified snapshot.
            receipt_date = receipt[:8]
            if any(start <= receipt_date <= end for start, end in complete_intervals):
                raise RuntimeError(
                    "fresh structured bonus row is absent from completed "
                    f"disclosures: {receipt}"
                )
            continue
        if disclosure.ticker != ticker or _action_type(
            disclosure.row.get("report_nm")
        ) != "bonus_issue":
            raise RuntimeError(
                f"structured/disclosure bonus identity mismatch: {receipt}"
            )
        structured[receipt] = _StructuredBind(
            receipt_no=receipt,
            ticker=ticker,
            row=row,
            row_sha256=_row_digest(row),
            body=_bound_file(root, path),
        )

    candidates: dict[str, tuple[str, str]] = {}
    for receipt, disclosure in disclosures.items():
        action_type = _action_type(disclosure.row.get("report_nm"))
        receipt_date = parse_dart_date(
            str(disclosure.row.get("rcept_dt") or "")
        )
        # The completed fresh-list ``rcept_dt`` is the authoritative public
        # acceptance timeline.  ``rcept_no`` is only the official selector
        # identity: overnight processing can give it an earlier-looking
        # prefix, which must not silently demote a coverage-day candidate to a
        # pre-coverage dependency.  Both raw values remain exact provenance.
        if receipt_date is None:
            raise RuntimeError(f"invalid DART receipt date: {receipt}")
        if (
            action_type in {"stock_dividend", "bonus_issue"}
            and coverage_start <= date.fromisoformat(receipt_date) <= coverage_end
        ):
            candidates[receipt] = (disclosure.ticker, action_type)
    for receipt, item in structured.items():
        disclosure = disclosures[receipt]
        receipt_date = parse_dart_date(
            str(disclosure.row.get("rcept_dt") or "")
        )
        if receipt_date is None:
            raise RuntimeError(f"invalid DART receipt date: {receipt}")
        # Pre-coverage structured rows remain exact family dependencies.  They
        # are intentionally not seeds, but a later attachment/correction can
        # select such a root as its terminal economic bonus source.
        if (
            coverage_start <= date.fromisoformat(receipt_date) <= coverage_end
            and candidates.get(receipt) != (item.ticker, "bonus_issue")
        ):
            raise RuntimeError(
                f"structured bonus candidate identity changed: {receipt}"
            )
    candidate_rows = [
        {
            "receipt_no": receipt,
            "ticker": ticker,
            "action_type": action_type,
            "disclosure_row_sha256": disclosures[receipt].row_sha256,
            "structured_row_sha256": (
                structured[receipt].row_sha256 if receipt in structured else None
            ),
        }
        for receipt, (ticker, action_type) in sorted(candidates.items())
    ]
    return _FreshSnapshot(
        disclosures=disclosures,
        structured=structured,
        candidates=candidates,
        candidate_digest=_sha256_bytes(_canonical_bytes(candidate_rows)),
        disclosure_audit=audit,
    )


def official_dart_viewer_url(receipt: str, dcm_no: str, dtd: str) -> str:
    if (
        _RECEIPT.fullmatch(receipt) is None
        or not dcm_no.isdigit()
        or re.fullmatch(r"[0-9A-Za-z_.-]+", dtd) is None
    ):
        raise RuntimeError("invalid official DART viewer target")
    query = urlencode({
        'rcpNo': receipt,
        'dcmNo': dcm_no,
        'eleId': '0',
        'offset': '0',
        'length': '0',
        'dtd': dtd,
    })
    return f"{VIEWER_URL}?{query}"


def _main_url(receipt: str) -> str:
    return f"{MAIN_URL}?{urlencode({'rcpNo': receipt})}"


def _http_fetch(
    url: str,
    *,
    tries: int,
    timeout: float,
    request_interval_seconds: float,
) -> bytes:
    last_error: Exception | None = None
    headers = {"User-Agent": "TeamAlpha-data support-family audit/1.0"}
    for attempt in range(tries):
        try:
            response = requests.get(url, headers=headers, timeout=timeout)
            response.raise_for_status()
            if not response.content:
                raise RuntimeError(f"empty DART support-family response: {url}")
            if request_interval_seconds:
                time.sleep(
                    request_interval_seconds + random.uniform(0.0, 0.10)
                )
            return response.content
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt + 1 < tries:
                time.sleep(min(30.0, float(2**attempt)))
    raise RuntimeError(f"DART support-family request failed: {url}") from last_error


def _body_bonus_termination_kind(payload: bytes) -> str | None:
    """Recognize an exact labelled body-only bonus withdrawal."""
    parser = _TableRowParser()
    parser.feed(_decode(payload))
    matches: list[str] = []
    for index, header in enumerate(parser.rows[:-1]):
        compact_header = tuple(_compact(cell) for cell in header)
        if "정정사유" not in compact_header or "정정후" not in compact_header:
            continue
        reason_index = compact_header.index("정정사유")
        after_index = compact_header.index("정정후")
        row = parser.rows[index + 1]
        if max(reason_index, after_index) >= len(row):
            continue
        reason = _compact(row[reason_index])
        after = _compact(row[after_index])
        if reason == "무상증자결정철회" and after in {
            "무상증자철회", "무상증자결정철회",
        }:
            matches.append("WITHDRAWN")
    if not matches:
        return None
    if set(matches) != {"WITHDRAWN"}:
        raise RuntimeError("bonus body termination status is ambiguous")
    return "WITHDRAWN"


def _revision_kind(report_name: str, *, root: bool, body: bytes) -> str:
    compact = _compact(report_name)
    if root:
        return "ORIGINAL"
    if "첨부정정" in compact:
        return "ATTACHMENT_ONLY"
    for marker, status in _TERMINATED_MARKERS:
        if marker in compact:
            return status
    try:
        body_status = _body_bonus_termination_kind(body)
    except RuntimeError:
        body_status = None
    if body_status is not None:
        return body_status
    return "ECONOMIC_REVISION"


def _optional_correction_origin_date(payload: bytes) -> str | None:
    """Return unique origin-date provenance without making it a family key."""
    try:
        return _correction_origin_date(payload)
    except RuntimeError:
        return None


def _source(
    root: Path,
    snapshot: _FreshSnapshot,
    receipt: str,
    position: int,
    order: tuple[str, ...],
    artifact: _Artifact,
) -> SupportActionFamilySource:
    disclosure = snapshot.disclosures.get(receipt)
    if disclosure is None:
        raise RuntimeError(
            f"official family receipt is absent from fresh disclosures: {receipt}"
        )
    report_name = str(disclosure.row.get("report_nm") or "")
    official_family = artifact.parsed_main.family_receipts
    official_root = official_family[-1]
    attachment_only = artifact.parsed_main.current_selector == "ATTACHMENT"
    is_root = receipt == official_root
    if is_root and _is_revision(report_name):
        raise RuntimeError(f"official family root is marked as a revision: {receipt}")
    if not is_root and not _is_revision(report_name):
        raise RuntimeError(
            f"official family revision lacks a revision marker: {receipt}"
        )
    receipt_date = parse_dart_date(str(disclosure.row.get("rcept_dt") or ""))
    if receipt_date is None:
        raise RuntimeError(f"invalid DART receipt date: {receipt}")
    # ``rcept_no`` is the official selector identity, not the authoritative
    # public list acceptance date.  Overnight DART processing legitimately
    # yields e.g. receipt 20170110000774 with rcept_dt 20170111.  Preserve and
    # bind both exact values; never infer one from the other.  Numeric ordering
    # within the official family selector remains a separate source contract.
    origin = (
        None
        if is_root or attachment_only
        else _optional_correction_origin_date(artifact.body)
    )
    root_disclosure = snapshot.disclosures.get(official_root)
    if root_disclosure is None:
        raise RuntimeError(
            f"official family root disclosure is missing: {official_root}"
        )
    # The body date is retained as exact provenance, but it is not a family
    # key.  Actual DART corrections variously print the selector root date, an
    # older member date, the correction's own date, a receipt-prefix date, an
    # external filing date, or malformed text.  The exact official selector
    # and its newest-to-oldest order are the lineage authority; every body is
    # still immutably bound by its content SHA even when origin is ``None``.
    if attachment_only:
        # The att selector proves membership in this official family root, but
        # it does not identify which economic revision's supplemental file was
        # directly corrected.  Do not overload correction_of with a guess.
        correction_of = None
    elif is_root:
        correction_of = None
    else:
        family_position = official_family.index(receipt)
        if family_position + 1 >= len(official_family):
            raise RuntimeError("economic correction has no prior family receipt")
        correction_of = official_family[family_position + 1]
    structured = snapshot.structured.get(receipt)
    return SupportActionFamilySource(
        receipt_no=receipt,
        official_position=position,
        dcm_no=artifact.parsed_main.dcm_no,
        dtd=artifact.parsed_main.dtd,
        current_selector=artifact.parsed_main.current_selector,
        main_family_receipts=official_family,
        main_attachment_keys=artifact.parsed_main.attachment_keys,
        attachment_family_root_receipt_no=(
            official_root if attachment_only else None
        ),
        report_name=report_name,
        receipt_date=receipt_date,
        correction_of_receipt_no=correction_of,
        correction_origin_date=origin,
        revision_kind=_revision_kind(
            report_name, root=is_root, body=artifact.body,
        ),
        main_path=artifact.main_bind.path,
        main_content_length=artifact.main_bind.content_length,
        main_sha256=artifact.main_bind.sha256,
        body_path=artifact.body_bind.path,
        body_content_length=artifact.body_bind.content_length,
        body_sha256=artifact.body_bind.sha256,
        disclosure_path=disclosure.body.path,
        disclosure_content_length=disclosure.body.content_length,
        disclosure_sha256=disclosure.body.sha256,
        disclosure_row_sha256=disclosure.row_sha256,
        disclosure_observation_digest=disclosure.observation_digest,
        disclosure_manifest_path=disclosure.selected_manifest.path,
        disclosure_manifest_sha256=disclosure.selected_manifest.sha256,
        structured_path=(structured.body.path if structured else None),
        structured_content_length=(
            structured.body.content_length if structured else None
        ),
        structured_sha256=(structured.body.sha256 if structured else None),
        structured_row_sha256=(structured.row_sha256 if structured else None),
    )


def _terminal_economics(
    action_type: str,
    sources: tuple[SupportActionFamilySource, ...],
    artifacts: dict[str, _Artifact],
    snapshot: _FreshSnapshot,
) -> tuple[str, str, bool, float | None]:
    # Attachment-only corrections do not create a new economic state.  Skip
    # them, then inherit the latest non-attachment terminal status exactly;
    # an attachment after a withdrawal must never revive the old ratio.
    state_source = next(
        (
            source for source in sources
            if source.revision_kind != "ATTACHMENT_ONLY"
        ),
        None,
    )
    if state_source is None:
        raise RuntimeError("official family has no economic receipt")
    if state_source.revision_kind in {"WITHDRAWN", "CANCELLED", "DENIED"}:
        state_position = sources.index(state_source)
        economic = next(
            (
                source.receipt_no for source in sources[state_position + 1 :]
                if source.revision_kind
                not in {"ATTACHMENT_ONLY", "WITHDRAWN", "CANCELLED", "DENIED"}
            ),
            sources[-1].receipt_no,
        )
        return economic, state_source.revision_kind, False, None
    economic_source = state_source
    economic_receipt = economic_source.receipt_no
    if action_type == "stock_dividend":
        ratio = stock_dividend_common_ratio_from_body(
            artifacts[economic_receipt].body,
        )
        if ratio is None and _stock_dividend_is_cross_class_distribution(
            artifacts[economic_receipt].body,
        ):
            return (
                economic_receipt,
                "CROSS_CLASS_DISTRIBUTION",
                False,
                None,
            )
    elif action_type == "bonus_issue":
        structured = snapshot.structured.get(economic_receipt)
        if structured is not None:
            ratio = _number(structured.row.get("nstk_ascnt_ps_ostk"))
            # The OpenDART structured row is authoritative when present.
            # Some official viewer documents contain nested legacy tables that
            # the exact fallback parser deliberately rejects.  A complete,
            # parseable viewer pair is useful parity evidence; an unsupported
            # optional viewer shape must not invalidate exact structured data.
            try:
                body_terms = bonus_issue_common_terms_from_body(
                    artifacts[economic_receipt].body,
                )
            except RuntimeError:
                body_terms = None
            if body_terms is not None:
                structured_date = parse_dart_date(
                    structured.row.get("nstk_asstd")
                )
                if (
                    ratio is None
                    or structured_date != body_terms.record_date
                    or not math.isclose(
                        ratio,
                        body_terms.common_ratio,
                        rel_tol=0,
                        abs_tol=5e-13,
                    )
                ):
                    raise RuntimeError(
                        "structured/viewer bonus common terms mismatch: "
                        f"receipt={economic_receipt} structured_ratio={ratio} "
                        f"structured_date={structured_date} "
                        f"viewer={body_terms}"
                    )
        else:
            body_terms = bonus_issue_common_terms_from_body(
                artifacts[economic_receipt].body,
            )
            if body_terms is None:
                raise RuntimeError(
                    "active bonus family terminal has neither structured nor "
                    f"exact viewer common terms: {economic_receipt}"
                )
            ratio = body_terms.common_ratio
    else:  # pragma: no cover - guarded by source discovery
        raise RuntimeError(f"unsupported support action type: {action_type}")
    if ratio is None:
        raise RuntimeError(
            f"terminal {action_type} ratio is missing: {economic_receipt}"
        )
    if ratio < 0:
        raise RuntimeError(
            f"terminal {action_type} ratio is negative: {economic_receipt}"
        )
    if ratio == 0:
        return economic_receipt, "ZERO_RATIO", False, 0.0
    return economic_receipt, "ACTIVE", True, ratio


def _derive_entry(
    root: Path,
    snapshot: _FreshSnapshot,
    action_type: str,
    order: tuple[str, ...],
    artifacts: dict[str, _Artifact],
) -> SupportActionFamilyEntry:
    if not order:
        raise RuntimeError("empty official support-action family")
    if order != tuple(sorted(order, key=int, reverse=True)):
        raise RuntimeError("official support-action family order changed")
    official_orders: set[tuple[str, ...]] = set()
    attachment_key_orders: set[tuple[str, ...]] = set()
    for receipt in order:
        artifact = artifacts.get(receipt)
        if artifact is None:
            raise RuntimeError(f"official family artifact is missing: {receipt}")
        official_orders.add(artifact.parsed_main.family_receipts)
        attachment_key_orders.add(artifact.parsed_main.attachment_keys)
    if len(official_orders) != 1:
        raise RuntimeError("official family pages disagree")
    if len(attachment_key_orders) != 1:
        raise RuntimeError("official attachment selectors disagree")
    official_family = next(iter(official_orders))
    attachment_keys = next(iter(attachment_key_orders))
    official_set = set(official_family)
    attachment_receipts = {
        key.split(":", 1)[0] for key in attachment_keys
    } - official_set
    for attachment_receipt in attachment_receipts:
        disclosure = snapshot.disclosures.get(attachment_receipt)
        if disclosure is None:
            raise RuntimeError(
                "official attachment receipt is absent from fresh disclosures: "
                f"{attachment_receipt}"
            )
        if (
            _action_type(disclosure.row.get("report_nm")) != action_type
            or not _is_attachment_only(disclosure.row.get("report_nm"))
        ):
            raise RuntimeError(
                "official attachment receipt has another disclosure identity: "
                f"{attachment_receipt}"
            )
    expected_order = tuple(sorted(
        official_set | attachment_receipts, key=int, reverse=True,
    ))
    if order != expected_order:
        raise RuntimeError(
            "official family+attachment union changed: "
            f"actual={list(order)} expected={list(expected_order)}"
        )
    if order[-1] != official_family[-1]:
        raise RuntimeError("official family root/union root mismatch")
    for receipt in order:
        parsed = artifacts[receipt].parsed_main
        disclosure = snapshot.disclosures[receipt]
        attachment_only = _is_attachment_only(
            disclosure.row.get("report_nm")
        )
        if attachment_only:
            if (
                parsed.current_selector != "ATTACHMENT"
                or f"{receipt}:{parsed.dcm_no}" not in attachment_keys
                or receipt in official_set
            ):
                raise RuntimeError(
                    f"attachment does not bind official family root: {receipt}"
                )
        elif parsed.current_selector != "FAMILY" or receipt not in official_set:
            raise RuntimeError(
                f"economic receipt does not bind official family: {receipt}"
            )
    sources = tuple(
        _source(root, snapshot, receipt, position, order, artifacts[receipt])
        for position, receipt in enumerate(order)
    )
    ticker_values = {
        snapshot.disclosures[source.receipt_no].ticker for source in sources
    }
    action_values = {
        _action_type(snapshot.disclosures[source.receipt_no].row.get("report_nm"))
        for source in sources
    }
    if len(ticker_values) != 1 or action_values != {action_type}:
        raise RuntimeError(
            "official family crosses issuer/action identities: "
            f"receipts={list(order)} tickers={sorted(ticker_values)} "
            f"actions={sorted(str(value) for value in action_values)}"
        )
    ticker = next(iter(ticker_values))
    economic, status, admissible, ratio = _terminal_economics(
        action_type, sources, artifacts, snapshot,
    )
    original_date = sources[-1].receipt_date
    bind_rows = [
        {
            "receipt_no": source.receipt_no,
            "disclosure_row_sha256": source.disclosure_row_sha256,
            "disclosure_observation_digest": source.disclosure_observation_digest,
            "structured_row_sha256": source.structured_row_sha256,
        }
        for source in sources
    ]
    return SupportActionFamilyEntry(
        ticker=ticker,
        action_type=action_type,
        root_receipt_no=order[-1],
        terminal_receipt_no=order[0],
        terminal_economic_receipt_no=economic,
        ordered_family_receipts=order,
        original_submission_date=original_date,
        terminal_status=status,
        terminal_admissible=admissible,
        terminal_ratio=ratio,
        fresh_row_bind_digest=_sha256_bytes(_canonical_bytes(bind_rows)),
        sources=sources,
    )


def _entry_digest(entries: Iterable[SupportActionFamilyEntry]) -> str:
    return _sha256_bytes(_canonical_bytes([asdict(item) for item in entries]))


def _fetch_artifacts(
    root: Path,
    snapshot: _FreshSnapshot,
    fetcher: Callable[[str], bytes],
) -> tuple[tuple[SupportActionFamilyEntry, ...], dict[str, _Artifact]]:
    pending = list(sorted(snapshot.candidates))
    main_payloads: dict[str, bytes] = {}
    parsed: dict[str, OfficialDartMainPage] = {}
    while pending:
        receipt = pending.pop(0)
        if receipt in parsed:
            continue
        disclosure = snapshot.disclosures.get(receipt)
        if disclosure is None:
            raise RuntimeError(
                f"official family receipt is absent from fresh disclosures: {receipt}"
            )
        main = fetcher(_main_url(receipt))
        parsed_main = parse_official_dart_main_page(
            receipt,
            main,
            expected_attachment_only=_is_attachment_only(
                disclosure.row.get("report_nm")
            ),
        )
        main_payloads[receipt] = main
        parsed[receipt] = parsed_main
        for member in parsed_main.family_receipts:
            if member not in parsed and member not in pending:
                pending.append(member)
    artifacts: dict[str, _Artifact] = {}
    for receipt in sorted(parsed):
        body = fetcher(official_dart_viewer_url(
            receipt, parsed[receipt].dcm_no, parsed[receipt].dtd,
        ))
        if not body:
            raise RuntimeError(f"empty DART support-action body: {receipt}")
        main_bind = _store_object(root, main_payloads[receipt])
        body_bind = _store_object(root, body)
        artifacts[receipt] = _Artifact(
            main=main_payloads[receipt],
            body=body,
            parsed_main=parsed[receipt],
            main_bind=main_bind,
            body_bind=body_bind,
        )
    families: dict[str, set[tuple[str, str]]] = {}
    for receipt, identity in snapshot.candidates.items():
        if receipt not in parsed:
            raise RuntimeError(f"candidate was not fetched: {receipt}")
        families.setdefault(
            parsed[receipt].family_root_receipt_no, set(),
        ).add(identity)
    entries: list[SupportActionFamilyEntry] = []
    used_receipts: set[str] = set()
    covered_candidates: set[str] = set()
    for family_root, identities in sorted(families.items()):
        if len(identities) != 1:
            raise RuntimeError(
                f"official family has ambiguous candidate identities: {identities}"
            )
        _, action_type = next(iter(identities))
        group_candidates = {
            receipt
            for receipt in snapshot.candidates
            if parsed[receipt].family_root_receipt_no == family_root
        }
        official_receipts = {
            receipt
            for candidate in group_candidates
            for receipt in parsed[candidate].family_receipts
        }
        attachment_receipts = {
            option.receipt_no
            for candidate in group_candidates
            for option in parsed[candidate].attachment_options
            if option.receipt_no not in official_receipts
        }
        order = tuple(sorted(
            official_receipts | attachment_receipts,
            key=int,
            reverse=True,
        ))
        overlap = used_receipts.intersection(order)
        if overlap:
            raise RuntimeError(
                f"official support-action families overlap: {sorted(overlap)}"
            )
        used_receipts.update(order)
        entry = _derive_entry(root, snapshot, action_type, order, artifacts)
        entries.append(entry)
        covered_candidates.update(set(order).intersection(snapshot.candidates))
    if covered_candidates != set(snapshot.candidates):
        raise RuntimeError(
            "support-action candidates do not belong to exactly one official family"
        )
    entries.sort(key=lambda item: (
        item.ticker, item.action_type, item.root_receipt_no,
    ))
    return tuple(entries), artifacts


def _official_main_selector_key(
    page: OfficialDartMainPage,
) -> tuple[str, str, str, tuple[str, ...], tuple[str, ...]]:
    """Return the exact mutable selector identity used by the snapshot."""
    return (
        page.current_selector,
        page.dcm_no,
        page.dtd,
        page.family_receipts,
        page.attachment_keys,
    )


def _revalidate_official_main_selectors(
    snapshot: _FreshSnapshot,
    entries: tuple[SupportActionFamilyEntry, ...],
    artifacts: dict[str, _Artifact],
    fetcher: Callable[[str], bytes],
) -> None:
    """Close the mutable-main TOCTOU window before manifest publication."""
    declared_receipts = sorted({
        source.receipt_no
        for entry in entries
        for source in entry.sources
    })
    for receipt in declared_receipts:
        disclosure = snapshot.disclosures.get(receipt)
        artifact = artifacts.get(receipt)
        if disclosure is None or artifact is None:
            raise RuntimeError(
                "declared support-family selector has no cached source: "
                f"{receipt}"
            )
        try:
            refreshed = parse_official_dart_main_page(
                receipt,
                fetcher(_main_url(receipt)),
                expected_attachment_only=_is_attachment_only(
                    disclosure.row.get("report_nm")
                ),
            )
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(
                "official DART main selector could not be revalidated "
                f"before manifest publication: receipt={receipt}"
            ) from exc
        if (
            _official_main_selector_key(refreshed)
            != _official_main_selector_key(artifact.parsed_main)
        ):
            raise RuntimeError(
                "official DART main selector changed during collection; "
                f"manifest was not published: receipt={receipt}"
            )


def _manifest_payload(
    snapshot: _FreshSnapshot,
    entries: tuple[SupportActionFamilyEntry, ...],
    *,
    coverage_start: date,
    coverage_end: date,
) -> dict[str, object]:
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "COMPLETE",
        "source_contract": SOURCE_CONTRACT,
        "family_order": FAMILY_ORDER_CONTRACT,
        "terminal_ratio_contract": TERMINAL_RATIO_CONTRACT,
        "seed_coverage_start": coverage_start.isoformat(),
        "seed_coverage_end": coverage_end.isoformat(),
        "candidate_count": len(snapshot.candidates),
        "candidate_digest": snapshot.candidate_digest,
        "entry_count": len(entries),
        "entry_digest": _entry_digest(entries),
        "disclosure_observation_audit": snapshot.disclosure_audit,
        "entries": [asdict(item) for item in entries],
    }


def _strict_dataclass(value: object, cls: type, *, label: str):
    if not isinstance(value, dict):
        raise RuntimeError(f"{label} must be an object")
    names = {item.name for item in fields(cls)}
    if set(value) != names:
        raise RuntimeError(
            f"{label} fields changed: missing={sorted(names - set(value))} "
            f"extra={sorted(set(value) - names)}"
        )
    return cls(**value)


def _parse_entries(value: object) -> tuple[SupportActionFamilyEntry, ...]:
    if not isinstance(value, list):
        raise RuntimeError("support-family manifest entries must be a list")
    result: list[SupportActionFamilyEntry] = []
    for index, raw in enumerate(value):
        if not isinstance(raw, dict):
            raise RuntimeError(f"support-family entry {index} must be an object")
        source_values = raw.get("sources")
        if not isinstance(source_values, list):
            raise RuntimeError(f"support-family entry {index} sources must be a list")
        parsed_sources: list[SupportActionFamilySource] = []
        for source_index, source in enumerate(source_values):
            if not isinstance(source, dict):
                raise RuntimeError(
                    f"support-family source {index}:{source_index} must be an object"
                )
            source_rendered = dict(source)
            source_rendered["main_family_receipts"] = tuple(
                source_rendered.get("main_family_receipts") or ()
            )
            source_rendered["main_attachment_keys"] = tuple(
                source_rendered.get("main_attachment_keys") or ()
            )
            parsed_sources.append(_strict_dataclass(
                source_rendered,
                SupportActionFamilySource,
                label=f"support-family source {index}:{source_index}",
            ))
        sources = tuple(parsed_sources)
        rendered = dict(raw)
        rendered["ordered_family_receipts"] = tuple(
            rendered.get("ordered_family_receipts") or ()
        )
        rendered["sources"] = sources
        result.append(_strict_dataclass(
            rendered, SupportActionFamilyEntry,
            label=f"support-family entry {index}",
        ))
    return tuple(result)


def _rederive_cached_entry(
    root: Path,
    snapshot: _FreshSnapshot,
    entry: SupportActionFamilyEntry,
) -> SupportActionFamilyEntry:
    """Rebind fresh list provenance without refetching immutable bodies."""
    artifacts: dict[str, _Artifact] = {}
    for source in entry.sources:
        main_bind = _FileBind(
            source.main_path, source.main_content_length, source.main_sha256,
        )
        body_bind = _FileBind(
            source.body_path, source.body_content_length, source.body_sha256,
        )
        main = _verify_object_bind(root, main_bind)
        body = _verify_object_bind(root, body_bind)
        disclosure = snapshot.disclosures.get(source.receipt_no)
        if disclosure is None:
            raise RuntimeError(
                "cached support-family source disappeared: "
                f"{source.receipt_no}"
            )
        parsed = parse_official_dart_main_page(
            source.receipt_no,
            main,
            expected_attachment_only=_is_attachment_only(
                disclosure.row.get("report_nm")
            ),
        )
        artifacts[source.receipt_no] = _Artifact(
            main=main,
            body=body,
            parsed_main=parsed,
            main_bind=main_bind,
            body_bind=body_bind,
        )
    return _derive_entry(
        root,
        snapshot,
        entry.action_type,
        entry.ordered_family_receipts,
        artifacts,
    )


def verify_support_action_families(
    base: str | Path,
    *,
    required_start: date,
    required_end: date,
) -> VerifiedSupportActionFamilies:
    if required_end < required_start:
        raise ValueError("support-family required_end precedes required_start")
    root = Path(base).expanduser().resolve()
    manifest_path = root / MANIFEST_RELATIVE_PATH
    manifest_bytes = manifest_path.read_bytes() if manifest_path.is_file() else b""
    payload = _read_json(manifest_path)
    if not isinstance(payload, dict):
        raise RuntimeError("support-family manifest must be an object")
    if manifest_bytes != _canonical_bytes(payload):
        raise RuntimeError("support-family manifest is not canonical JSON bytes")
    if (
        payload.get("schema_version") != SCHEMA_VERSION
        or payload.get("status") != "COMPLETE"
        or payload.get("source_contract") != SOURCE_CONTRACT
        or payload.get("family_order") != FAMILY_ORDER_CONTRACT
        or payload.get("terminal_ratio_contract")
        != TERMINAL_RATIO_CONTRACT
    ):
        raise RuntimeError("unsupported/incomplete support-family manifest")
    if (
        payload.get("seed_coverage_start") != required_start.isoformat()
        or payload.get("seed_coverage_end") != required_end.isoformat()
    ):
        raise RuntimeError(
            "support-family seed coverage mismatch: "
            f"manifest={payload.get('seed_coverage_start')}.."
            f"{payload.get('seed_coverage_end')} required="
            f"{required_start.isoformat()}..{required_end.isoformat()}"
        )
    snapshot = _load_fresh_snapshot(
        root, coverage_start=required_start, coverage_end=required_end,
    )
    if (
        int(payload.get("candidate_count", -1)) != len(snapshot.candidates)
        or payload.get("candidate_digest") != snapshot.candidate_digest
        or payload.get("disclosure_observation_audit")
        != snapshot.disclosure_audit
    ):
        raise RuntimeError("support-family fresh candidate snapshot changed")
    entries = _parse_entries(payload.get("entries"))
    if int(payload.get("entry_count", -1)) != len(entries):
        raise RuntimeError("support-family entry count mismatch")
    if payload.get("entry_digest") != _entry_digest(entries):
        raise RuntimeError("support-family entry digest mismatch")
    used_receipts: set[str] = set()
    covered_candidates: set[str] = set()
    derived_entries: list[SupportActionFamilyEntry] = []
    for entry in entries:
        artifacts: dict[str, _Artifact] = {}
        order = entry.ordered_family_receipts
        if tuple(source.receipt_no for source in entry.sources) != order:
            raise RuntimeError("support-family source order mismatch")
        for source in entry.sources:
            main_bind = _FileBind(
                source.main_path, source.main_content_length, source.main_sha256,
            )
            body_bind = _FileBind(
                source.body_path, source.body_content_length, source.body_sha256,
            )
            main = _verify_object_bind(root, main_bind)
            body = _verify_object_bind(root, body_bind)
            disclosure = snapshot.disclosures.get(source.receipt_no)
            if disclosure is None:
                raise RuntimeError(
                    "support-family source disappeared from fresh disclosures: "
                    f"{source.receipt_no}"
                )
            parsed = parse_official_dart_main_page(
                source.receipt_no,
                main,
                expected_attachment_only=_is_attachment_only(
                    disclosure.row.get("report_nm")
                ),
            )
            artifacts[source.receipt_no] = _Artifact(
                main=main,
                body=body,
                parsed_main=parsed,
                main_bind=main_bind,
                body_bind=body_bind,
            )
        overlap = used_receipts.intersection(order)
        if overlap:
            raise RuntimeError(
                f"support-family receipt is reused: {sorted(overlap)}"
            )
        used_receipts.update(order)
        derived = _derive_entry(
            root, snapshot, entry.action_type, order, artifacts,
        )
        if derived != entry:
            raise RuntimeError(
                f"support-family derived manifest row changed: {entry.root_receipt_no}"
            )
        derived_entries.append(derived)
        covered_candidates.update(set(order).intersection(snapshot.candidates))
    if covered_candidates != set(snapshot.candidates):
        raise RuntimeError("support-family manifest does not cover every candidate")
    normalized = tuple(sorted(
        derived_entries,
        key=lambda item: (item.ticker, item.action_type, item.root_receipt_no),
    ))
    if normalized != entries:
        raise RuntimeError("support-family entries are not canonically ordered")
    return VerifiedSupportActionFamilies(
        base=str(root),
        manifest_path=_relative(root, manifest_path),
        manifest_sha256=_sha256_path(manifest_path),
        seed_coverage_start=required_start,
        seed_coverage_end=required_end,
        candidate_count=len(snapshot.candidates),
        candidate_digest=snapshot.candidate_digest,
        entry_count=len(entries),
        entry_digest=_entry_digest(entries),
        entries=entries,
    )


def collect_support_action_families(
    base: str | Path,
    *,
    coverage_end: date,
    coverage_start: date = SUPPORT_FAMILY_COVERAGE_START,
    apply: bool = False,
    fetcher: Callable[[str], bytes] | None = None,
    tries: int = 4,
    timeout: float = 30.0,
    request_interval_seconds: float = 0.8,
) -> VerifiedSupportActionFamilies | dict[str, object]:
    root = Path(base).expanduser().resolve()
    snapshot = _load_fresh_snapshot(
        root, coverage_start=coverage_start, coverage_end=coverage_end,
    )
    if not apply:
        return {
            "status": "PREVIEW",
            "schema_version": SCHEMA_VERSION,
            "terminal_ratio_contract": TERMINAL_RATIO_CONTRACT,
            "seed_coverage_start": coverage_start.isoformat(),
            "seed_coverage_end": coverage_end.isoformat(),
            "candidate_count": len(snapshot.candidates),
            "candidate_digest": snapshot.candidate_digest,
            "candidate_receipts": sorted(snapshot.candidates),
            "manifest_path": str(root / MANIFEST_RELATIVE_PATH),
            "network_requests_made": 0,
            "writes_made": 0,
        }
    if tries < 1 or timeout <= 0 or request_interval_seconds < 0:
        raise ValueError("invalid support-family HTTP settings")
    if fetcher is None:
        fetcher = lambda url: _http_fetch(
            url,
            tries=tries,
            timeout=timeout,
            request_interval_seconds=request_interval_seconds,
        )
    previous_entries: tuple[SupportActionFamilyEntry, ...] = ()
    manifest_path = root / MANIFEST_RELATIVE_PATH
    if manifest_path.is_file():
        try:
            previous_payload = _read_json(manifest_path)
            previous_end = date.fromisoformat(str(
                previous_payload.get("seed_coverage_end")
            ))
            if (
                previous_payload.get("schema_version") == SCHEMA_VERSION
                and previous_payload.get("status") == "COMPLETE"
                and previous_payload.get("source_contract") == SOURCE_CONTRACT
                and previous_payload.get("seed_coverage_start")
                == coverage_start.isoformat()
                and previous_end < coverage_end
            ):
                previous_entries = _parse_entries(
                    previous_payload.get("entries")
                )
        except (AttributeError, TypeError, ValueError, RuntimeError):
            previous_entries = ()
    previous_receipts = {
        source.receipt_no
        for entry in previous_entries
        for source in entry.sources
    }
    new_candidates = {
        receipt: identity
        for receipt, identity in snapshot.candidates.items()
        if receipt not in previous_receipts
    }
    if previous_entries:
        delta_snapshot = _FreshSnapshot(
            disclosures=snapshot.disclosures,
            structured=snapshot.structured,
            candidates=new_candidates,
            candidate_digest=snapshot.candidate_digest,
            disclosure_audit=snapshot.disclosure_audit,
        )
        changed_entries, artifacts = _fetch_artifacts(
            root, delta_snapshot, fetcher,
        ) if new_candidates else ((), {})
        changed_roots = {entry.root_receipt_no for entry in changed_entries}
        entries = tuple(sorted(
            [
                _rederive_cached_entry(root, snapshot, entry)
                for entry in previous_entries
                if entry.root_receipt_no not in changed_roots
            ] + list(changed_entries),
            key=lambda item: (
                item.ticker, item.action_type, item.root_receipt_no,
            ),
        ))
        print(
            "[dart-support-families] incremental evidence "
            f"reused_families={len(previous_entries) - len(changed_roots)} "
            f"changed_families={len(changed_entries)}",
            flush=True,
        )
    else:
        entries, artifacts = _fetch_artifacts(root, snapshot, fetcher)
    manifest = _manifest_payload(
        snapshot,
        entries,
        coverage_start=coverage_start,
        coverage_end=coverage_end,
    )
    manifest_bytes = _canonical_bytes(manifest)
    previous = manifest_path.read_bytes() if manifest_path.is_file() else None
    # DART main.do is mutable.  Bodies can take long enough to collect for a
    # new correction or attachment to appear after the first selector read.
    # Re-read every declared source with the same retry/pacing-aware fetcher
    # immediately before moving the complete manifest pointer.
    if artifacts:
        changed_roots = {
            parsed.family_root_receipt_no
            for parsed in (item.parsed_main for item in artifacts.values())
        }
        revalidate_entries = tuple(
            entry for entry in entries
            if entry.root_receipt_no in changed_roots
        )
        _revalidate_official_main_selectors(
            snapshot, revalidate_entries, artifacts, fetcher,
        )
    try:
        if previous != manifest_bytes:
            _atomic_write(manifest_path, manifest_bytes)
        # Verify from disk after publication.  This catches an incomplete
        # refresh and makes the mutable current pointer safe for daily DART
        # intervals while every referenced body remains content-addressed.
        return verify_support_action_families(
            root, required_start=coverage_start, required_end=coverage_end,
        )
    except Exception:
        if previous is None:
            manifest_path.unlink(missing_ok=True)
        else:
            _atomic_write(manifest_path, previous)
        raise


def external_evidence_paths(
    base: str | Path,
    *,
    required_start: date,
    required_end: date,
) -> tuple[str, ...]:
    """Return every source path that an enclosing action snapshot must bind."""
    verified = verify_support_action_families(
        base, required_start=required_start, required_end=required_end,
    )
    paths = {verified.manifest_path}
    for entry in verified.entries:
        for source in entry.sources:
            paths.update({
                source.main_path,
                source.body_path,
                source.disclosure_path,
                source.disclosure_manifest_path,
            })
            if source.structured_path is not None:
                paths.add(source.structured_path)
    return tuple(sorted(paths))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Collect official DART support-action correction families",
    )
    parser.add_argument("--base", required=True)
    parser.add_argument(
        "--coverage-start",
        type=date.fromisoformat,
        default=SUPPORT_FAMILY_COVERAGE_START,
    )
    parser.add_argument("--coverage-end", type=date.fromisoformat, required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--tries", type=int, default=4)
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--request-interval-seconds", type=float, default=0.8)
    args = parser.parse_args()
    result = collect_support_action_families(
        args.base,
        coverage_start=args.coverage_start,
        coverage_end=args.coverage_end,
        apply=args.apply,
        tries=args.tries,
        timeout=args.timeout,
        request_interval_seconds=args.request_interval_seconds,
    )
    if isinstance(result, VerifiedSupportActionFamilies):
        rendered: object = asdict(result)
    else:
        rendered = result
    print(json.dumps(rendered, ensure_ascii=False, sort_keys=True, default=str))


if __name__ == "__main__":
    main()
