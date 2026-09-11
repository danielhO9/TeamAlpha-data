"""Content-addressed DART viewer fallback for correction receipts.

OpenDART ``document.xml`` can return status 014 for a correction receipt even
though the official DART report viewer exposes both its revision lineage and
the final corrected body.  This collector is deliberately separate from
Silver: it downloads immutable public evidence into Bronze, writes one
completion manifest, and lets Silver parse only that verified snapshot.

Nothing is fetched unless ``--apply`` is supplied.
"""
from __future__ import annotations

import argparse
import hashlib
import html
import json
import os
import random
import re
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path
from typing import Iterable
from urllib.parse import urlencode

import requests

from pipeline.bronze.corporate_actions import (
    _candidate_corp_to_stock,
    _document_candidate_receipts,
    _is_listed_disclosure_candidate,
    _structured_query_keys,
)
from pipeline.bronze.dart_disclosure_observations import (
    canonicalize_disclosures,
)
from pipeline.bronze.dart_support_action_families import (
    official_dart_viewer_url,
    parse_dart_date,
    parse_official_dart_main_page,
)


SCHEMA_VERSION = "dart_viewer_correction_snapshot_v4"
SOURCE_CONTRACT = (
    "dart_official_seed_bounded_main_family_dependency_viewer_body_v4"
)
FAMILY_ORDER_CONTRACT = "OFFICIAL_MAIN_NEWEST_TO_OLDEST_WITH_ATTACHMENT_KEYS"
ATTACHMENT_PARENT_CONTRACT = "OFFICIAL_FAMILY_ROOT_NOT_DIRECT_ATTACHMENT_TARGET"
MAIN_URL = "https://dart.fss.or.kr/dsaf001/main.do"
MANIFEST_RELATIVE_PATH = Path(
    "corporate_actions/dart/viewer_corrections/manifest.json"
)
OBJECT_ROOT_RELATIVE_PATH = MANIFEST_RELATIVE_PATH.parent / "objects"
KNOWN_DAMAGED_DOCUMENT_RECEIPTS = {
    # The cached document body contains literal '?' bytes in every Korean
    # label.  The official viewer body is intact and is required permanently,
    # rather than being an ad-hoc CLI exception for this recovery run.
    "20220802900375": "CACHED_ZIP_LABEL_BYTES_CORRUPTED",
}
DEFAULT_REQUEST_INTERVAL_SECONDS = 1.0
DEFAULT_REQUEST_JITTER_SECONDS = 0.20
_INTERVAL_DATE = re.compile(r"^[0-9]{8}$")


@dataclass(frozen=True)
class ViewerReceiptEvidence:
    """Immutable viewer evidence for one cash disclosure receipt.

    For ``ATTACHMENT_ONLY``, ``correction_of_receipt_no`` is the exact root
    proven by DART's attachment selector.  It is intentionally not represented
    as the unknown direct economic revision corrected by that attachment.
    """

    receipt_no: str
    dcm_no: str
    dtd: str
    current_selector: str
    attachment_keys: tuple[str, ...]
    correction_of_receipt_no: str | None
    revision_root_receipt_no: str
    family_receipt_nos: tuple[str, ...]
    official_family_order: tuple[str, ...]
    revision_kind: str
    economic_body_receipt_no: str
    economic_body_dcm_no: str
    economic_body_dtd: str
    economic_main_path: str
    economic_main_content_length: int
    economic_main_sha256: str
    economic_classification: str
    common_cash_amount: float | None
    record_date: str | None
    main_path: str
    main_content_length: int
    main_sha256: str
    viewer_path: str
    viewer_content_length: int
    viewer_sha256: str
    economic_viewer_path: str
    economic_viewer_content_length: int
    economic_viewer_sha256: str


@dataclass(frozen=True)
class ViewerDependencyProbe:
    """Official selector proof for one provisional out-of-range revision."""

    receipt_no: str
    rcept_dt: str
    current_selector: str
    family_receipt_nos: tuple[str, ...]
    attachment_keys: tuple[str, ...]
    intersects_seed_receipt: bool
    selected_dependency: bool
    main_path: str
    main_content_length: int
    main_sha256: str


@dataclass(frozen=True)
class VerifiedViewerCorrectionSnapshot:
    base: str
    manifest_path: str
    manifest_sha256: str
    seed_coverage_start: date
    seed_coverage_end: date
    seed_receipt_count: int
    seed_receipt_digest: str
    dependency_receipt_count: int
    dependency_receipt_digest: str
    dependency_probe_count: int
    dependency_probe_digest: str
    receipt_count: int
    receipt_digest: str
    dependency_probes: tuple[ViewerDependencyProbe, ...]
    receipts: tuple[ViewerReceiptEvidence, ...]


def _compact(value: object) -> str:
    return re.sub(r"[^0-9A-Za-z가-힣]", "", str(value or ""))


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _sha256_path(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_write(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", dir=path.parent,
    )
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary_name, path)
    finally:
        if os.path.exists(temporary_name):
            os.unlink(temporary_name)


def _cash_disclosures(root: Path) -> dict[str, dict]:
    corp_to_stock: dict[str, str] = {}
    observations: list[tuple[Path, dict]] = []
    complete_receipts: set[str] = set()
    incomplete_relevant: dict[str, dict] = {}
    complete_interval_count = 0
    manifest_root = root / "corporate_actions" / "dart" / "manifests"
    for path in sorted(manifest_root.glob("from=*/to=*/disclosures_v3.json")):
        start = path.parent.parent.name.removeprefix("from=")
        end = path.parent.name.removeprefix("to=")
        if (
            _INTERVAL_DATE.fullmatch(start) is None
            or _INTERVAL_DATE.fullmatch(end) is None
            or start > end
        ):
            raise RuntimeError(f"invalid DART disclosures interval: {path}")
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise RuntimeError(f"invalid DART disclosures body: {path}") from exc
        if not isinstance(payload, list):
            raise RuntimeError(f"DART disclosures must be a list: {path}")
        corp_to_stock.update(_candidate_corp_to_stock(str(root), payload))
        relevant = {
            str(row.get("rcept_no") or ""): row
            for row in payload
            if isinstance(row, dict)
            and _is_listed_disclosure_candidate(row, corp_to_stock)
            and "현금현물배당결정" in _compact(row.get("report_nm"))
        }
        structured_marker = path.parent / "structured_complete_v3.json"
        document_marker = path.parent / "documents_complete_v5.json"
        if not structured_marker.is_file() or not document_marker.is_file():
            incomplete_relevant.update(relevant)
            continue
        structured_queries = _structured_query_keys(payload, corp_to_stock)
        document_candidates = _document_candidate_receipts(
            payload, corp_to_stock,
        )
        for marker_path, count_field, expected_count in (
            (
                structured_marker,
                "query_count",
                len(structured_queries),
            ),
            (
                document_marker,
                "candidate_count",
                len(document_candidates),
            ),
        ):
            try:
                marker = json.loads(marker_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                raise RuntimeError(
                    f"invalid DART completion marker: {marker_path}"
                ) from exc
            if not isinstance(marker, dict):
                raise RuntimeError(
                    f"invalid DART completion marker: {marker_path}"
                )
            try:
                observed_count = int(marker.get(count_field, -1))
            except (TypeError, ValueError) as exc:
                raise RuntimeError(
                    f"invalid DART completion marker: {marker_path}"
                ) from exc
            if (
                marker.get("status") != "COMPLETE"
                or str(marker.get("fromdate") or "") != start
                or str(marker.get("todate") or "") != end
                or observed_count != expected_count
            ):
                raise RuntimeError(
                    "DART completion marker interval/count mismatch: "
                    f"{marker_path}"
                )
        complete_interval_count += 1
        for row in payload:
            if not isinstance(row, dict):
                continue
            receipt = str(row.get("rcept_no") or "")
            if (
                _is_listed_disclosure_candidate(row, corp_to_stock)
                and "현금현물배당결정" in _compact(row.get("report_nm"))
            ):
                observations.append((path, row))
                complete_receipts.add(receipt)
    if complete_interval_count == 0:
        raise RuntimeError("no documents_complete_v5 DART cash interval")
    uncovered = sorted(set(incomplete_relevant) - complete_receipts)
    if uncovered:
        raise RuntimeError(
            "cash disclosures occur only in incomplete v5 intervals: "
            f"{uncovered[:20]}"
        )
    legacy_uncovered: set[str] = set()
    for legacy_path in sorted(
        manifest_root.glob("from=*/to=*/disclosures.json")
    ):
        try:
            legacy_rows = json.loads(legacy_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise RuntimeError(
                f"invalid legacy DART disclosures body: {legacy_path}"
            ) from exc
        if not isinstance(legacy_rows, list):
            raise RuntimeError(
                f"legacy DART disclosures must be a list: {legacy_path}"
            )
        legacy_uncovered.update(
            str(row.get("rcept_no") or "")
            for row in legacy_rows
            if isinstance(row, dict)
            and "현금현물배당결정" in _compact(row.get("report_nm"))
            and re.fullmatch(r"\d{14}", str(row.get("rcept_no") or ""))
            and str(row.get("rcept_no") or "") not in complete_receipts
        )
    if legacy_uncovered:
        raise RuntimeError(
            "legacy cash disclosures are not authenticated by v3/v5: "
            f"{sorted(legacy_uncovered)[:20]}"
        )
    canonical, _ = canonicalize_disclosures(
        observations, audit_root=root,
    )
    return {
        receipt: row
        for receipt, (_, row) in canonical.items()
    }


_REVISION_TITLE_MARKERS = ("정정", "철회", "취소", "부결")
_SUBSIDIARY_TITLE_MARKERS = (
    "자회사의주요경영사항",
    "종속회사의주요경영사항",
)


def _cash_disclosure_date(receipt: str, row: dict) -> date:
    rendered = parse_dart_date(row.get("rcept_dt"))
    if rendered is None:
        raise RuntimeError(f"invalid DART cash receipt date: {receipt}")
    return date.fromisoformat(rendered)


def _is_issuer_cash_disclosure(row: dict) -> bool:
    title = _compact(row.get("report_nm"))
    return not any(marker in title for marker in _SUBSIDIARY_TITLE_MARKERS)


def _seed_cash_receipts(
    disclosures: dict[str, dict],
    *,
    coverage_start: date,
    coverage_end: date,
) -> frozenset[str]:
    seeds: set[str] = set()
    for receipt, row in disclosures.items():
        receipt_date = _cash_disclosure_date(receipt, row)
        if (
            coverage_start <= receipt_date <= coverage_end
            and _is_issuer_cash_disclosure(row)
        ):
            seeds.add(receipt)
    return frozenset(seeds)


def _outside_revision_candidates(
    disclosures: dict[str, dict],
    *,
    coverage_start: date,
    coverage_end: date,
) -> tuple[str, ...]:
    """Return provisional out-of-range corrections, never dependencies yet.

    A complete dependency-day disclosure ZIP can contain unrelated reports.
    Its list date or co-location with a needed support-family correction is
    not lineage evidence. Apply mode refreshes each candidate's official
    ``main.do`` selector and retains it only when that exact selector
    intersects an in-range issuer cash seed.
    """
    candidates: set[str] = set()
    for receipt, row in disclosures.items():
        receipt_date = _cash_disclosure_date(receipt, row)
        title = _compact(row.get("report_nm"))
        if (
            not coverage_start <= receipt_date <= coverage_end
            and _is_issuer_cash_disclosure(row)
            and any(marker in title for marker in _REVISION_TITLE_MARKERS)
        ):
            candidates.add(receipt)
    return tuple(sorted(candidates))


def required_viewer_receipts(
    base: str,
    *,
    coverage_start: date,
    coverage_end: date,
) -> tuple[str, ...]:
    """Return every cash correction plus known damaged source bodies.

    ZIP availability does not close revision lineage: a correction can change
    DPS, record date, or cancel the event.  Every correction/withdrawal is
    therefore bound to the official viewer family and final corrected body.
    """
    root = Path(base).expanduser().resolve()
    if coverage_end < coverage_start:
        raise ValueError("viewer coverage_end precedes coverage_start")
    disclosures = _cash_disclosures(root)
    seed_receipts = _seed_cash_receipts(
        disclosures,
        coverage_start=coverage_start,
        coverage_end=coverage_end,
    )
    seed_disclosures = {
        receipt: disclosures[receipt] for receipt in seed_receipts
    }
    required = set(KNOWN_DAMAGED_DOCUMENT_RECEIPTS).intersection(
        seed_disclosures
    )
    unavailable_root = (
        root / "corporate_actions" / "dart" / "documents_unavailable"
    )
    unavailable: dict[str, Path] = {
        path.stem.removeprefix("rcept="): path
        for path in unavailable_root.glob("year=*/corp=*/rcept=*.xml")
        if path.is_file()
    }
    for receipt, row in seed_disclosures.items():
        title = _compact(row.get("report_nm"))
        is_revision = any(marker in title for marker in _REVISION_TITLE_MARKERS)
        if is_revision:
            required.add(receipt)
        unavailable_path = unavailable.get(receipt)
        if unavailable_path is not None:
            body = unavailable_path.read_text(encoding="utf-8", errors="replace")
            if "<status>014</status>" not in body:
                raise RuntimeError(
                    "DART unavailable evidence is not status 014: "
                    f"{unavailable_path}"
                )
    return tuple(sorted(required))


def _decode_main(payload: bytes) -> str:
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


def _parse_main_page(
    receipt: str,
    payload: bytes,
    *,
    attachment_correction: bool = False,
) -> tuple[
    str, str | None, str, tuple[str, ...], str, tuple[str, ...]
]:
    page = parse_official_dart_main_page(
        receipt,
        payload,
        expected_attachment_only=attachment_correction,
    )
    official_family_order = page.family_receipts
    revision_root = official_family_order[-1]
    if attachment_correction:
        # Cash Silver's source-receipt contract needs a non-null parent.  The
        # official att selector proves family-root membership, not a direct
        # economic-revision parent, so this field deliberately means the
        # verified family root for ATTACHMENT_ONLY rows.
        correction_of = revision_root
        economic_body_receipt = official_family_order[0]
    else:
        position = official_family_order.index(receipt)
        correction_of = (
            official_family_order[position + 1]
            if position + 1 < len(official_family_order)
            else None
        )
        economic_body_receipt = receipt
    family = tuple(sorted(
        set(official_family_order) | {receipt}, key=int,
    ))
    return (
        page.dcm_no,
        correction_of,
        revision_root,
        family,
        economic_body_receipt,
        official_family_order,
    )


def _number(value: str) -> float | None:
    rendered = value.replace(",", "").strip()
    try:
        return float(rendered)
    except ValueError:
        return None


def _parse_viewer_economic_body(
    payload: bytes,
    *,
    report_name: object,
) -> tuple[str, float | None, str | None]:
    rendered = _decode_main(payload)
    if "<table" not in rendered.lower():
        raise RuntimeError("DART viewer body has no report table")
    visible = html.unescape(re.sub(r"<[^>]+>", " ", rendered))
    visible = re.sub(r"\s+", " ", visible)
    compact_title = _compact(report_name)
    if "첨부정정" in compact_title:
        return "ATTACHMENT_CORRECTION", None, None
    if any(marker in compact_title for marker in ("철회", "취소", "부결")):
        return "NO_ECONOMIC_EVENT", None, None

    amount_matches = list(re.finditer(
        r"1\s*주당\s*배당금\s*"
        r"(?:\(\s*원\s*\)|원)?\s*"
        r"보통주(?:식)?\s*[:：]?\s*"
        r"([0-9][0-9,]*(?:\.[0-9]+)?)",
        visible,
    ))
    no_common_matches = list(re.finditer(
        r"1\s*주당\s*배당금\s*"
        r"(?:\(\s*원\s*\)|원)?\s*"
        r"보통주(?:식)?\s*[:：]?\s*"
        r"(?:-|해당\s*없음|없음|미지급)",
        visible,
    ))
    record_matches = list(re.finditer(
        r"배당\s*기준일\s*[:：]?\s*(?:"
        r"(?P<compact>(?:19|20)\d{6})"
        r"|"
        r"(?P<year>(?:19|20)\d{2})\s*(?:년|[./-])\s*"
        r"(?P<month>\d{1,2})\s*(?:월|[./-])\s*"
        r"(?P<day>\d{1,2})\s*일?"
        r")",
        visible,
    ))
    record_date = None
    if record_matches:
        match = record_matches[-1]
        compact = match.group("compact")
        if compact:
            year, month, day = (
                int(compact[:4]), int(compact[4:6]), int(compact[6:8])
            )
        else:
            year, month, day = (
                int(match.group("year")),
                int(match.group("month")),
                int(match.group("day")),
            )
        record_date = parse_dart_date(f"{year}-{month}-{day}")
        if record_date is None:
            raise RuntimeError("DART viewer record date is invalid")
    if no_common_matches and (
        not amount_matches
        or no_common_matches[-1].start() > amount_matches[-1].start()
    ):
        return "NO_COMMON_CASH_DIVIDEND", None, record_date
    if not amount_matches:
        raise RuntimeError("DART viewer economic body has no common-share DPS")
    amount = _number(amount_matches[-1].group(1))
    if amount is None:
        raise RuntimeError("DART viewer common-share DPS is invalid")
    if amount <= 0:
        return "NO_COMMON_CASH_DIVIDEND", None, record_date
    if record_date is None:
        return "POSITIVE_PENDING_RECORD_DATE", amount, None
    return "ECONOMIC_DECISION", amount, record_date


class _RateLimiter:
    def __init__(self, interval_seconds: float, jitter_seconds: float):
        self.interval_seconds = interval_seconds
        self.jitter_seconds = jitter_seconds
        self._lock = threading.Lock()
        self._next_request = 0.0

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            delay = max(0.0, self._next_request - now)
            self._next_request = (
                max(now, self._next_request)
                + self.interval_seconds
                + random.uniform(0.0, self.jitter_seconds)
            )
        if delay:
            time.sleep(delay)


def _get(
    url: str,
    *,
    tries: int,
    timeout: float,
    rate_limiter: _RateLimiter,
) -> bytes:
    last_error: Exception | None = None
    headers = {"User-Agent": "TeamAlpha-data dividend-lineage audit/1.0"}
    for attempt in range(tries):
        try:
            rate_limiter.wait()
            response = requests.get(url, headers=headers, timeout=timeout)
            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")
                if retry_after:
                    try:
                        time.sleep(min(60.0, max(0.0, float(retry_after))))
                    except ValueError:
                        pass
            if response.status_code >= 500:
                raise requests.HTTPError(
                    f"DART viewer server error {response.status_code}",
                    response=response,
                )
            response.raise_for_status()
            payload = response.content
            if len(payload) < 200:
                raise RuntimeError(f"unexpectedly short DART viewer body: {url}")
            return payload
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            if attempt + 1 < tries:
                time.sleep(
                    min(60.0, 2.0 * (2**attempt))
                    + random.uniform(0.0, 1.0)
                )
    raise RuntimeError(f"DART viewer request failed: {url}") from last_error


def _receipt_main_path(root: Path, receipt: str) -> Path:
    """Return the legacy mutable cache path (never trusted by v3)."""
    directory = (
        root / "corporate_actions" / "dart" / "viewer_corrections"
        / f"receipt={receipt}"
    )
    return directory / "main.html"


def _viewer_evidence_path(main_path: Path, *, prefix: str, dtd: str) -> Path:
    if prefix not in {"viewer", "economic_viewer"} or re.fullmatch(
        r"[0-9A-Za-z_.-]+", dtd,
    ) is None:
        raise RuntimeError("invalid DART viewer evidence path identity")
    return main_path.with_name(f"{prefix}.dtd={dtd}.html")


def _content_addressed_evidence_path(root: Path, payload: bytes) -> Path:
    """Store one freshly fetched official page under its SHA-256 identity.

    DART's ``main.do`` selector is mutable: a later correction adds another
    receipt to every member's official family.  A receipt-named cache can
    therefore never prove that the selector was refreshed.  Existing objects
    are accepted only when their complete body still matches the digest
    encoded in the path.
    """
    digest = _sha256_bytes(payload)
    destination = root / OBJECT_ROOT_RELATIVE_PATH / f"sha256={digest}.html"
    if destination.is_file():
        if destination.read_bytes() != payload:
            raise RuntimeError(
                "content-addressed DART viewer object changed: "
                f"{destination}"
            )
        return destination
    _atomic_write(destination, payload)
    return destination


def _fetch_main_payload(
    receipt: str,
    *,
    tries: int,
    timeout: float,
    rate_limiter: _RateLimiter,
) -> bytes:
    main_url = f"{MAIN_URL}?{urlencode({'rcpNo': receipt})}"
    return _get(
        main_url,
        tries=tries,
        timeout=timeout,
        rate_limiter=rate_limiter,
    )


def _fetch_one(
    root: Path,
    receipt: str,
    *,
    tries: int,
    timeout: float,
    rate_limiter: _RateLimiter,
    report_name: object,
    report_names: dict[str, object] | None = None,
    prefetched_main_payload: bytes | None = None,
) -> ViewerReceiptEvidence:
    attachment_correction = "첨부정정" in _compact(report_name)
    # Never reuse a receipt-named main page. Its family and attachment
    # selectors change when DART publishes a later correction.
    main_payload = prefetched_main_payload or _fetch_main_payload(
        receipt,
        tries=tries,
        timeout=timeout,
        rate_limiter=rate_limiter,
    )
    main_path = _content_addressed_evidence_path(root, main_payload)
    (
        dcm_no,
        correction_of,
        revision_root,
        family,
        economic_body_receipt,
        official_family_order,
    ) = _parse_main_page(
        receipt,
        main_payload,
        attachment_correction=attachment_correction,
    )
    source_page = parse_official_dart_main_page(
        receipt,
        main_payload,
        expected_attachment_only=attachment_correction,
    )
    dtd = source_page.dtd
    current_selector = source_page.current_selector
    attachment_keys = source_page.attachment_keys
    viewer_url = official_dart_viewer_url(receipt, dcm_no, dtd)
    viewer_payload = _get(
        viewer_url,
        tries=tries,
        timeout=timeout,
        rate_limiter=rate_limiter,
    )
    viewer_path = _content_addressed_evidence_path(root, viewer_payload)
    if attachment_correction:
        economic_main_url = (
            f"{MAIN_URL}?"
            f"{urlencode({'rcpNo': economic_body_receipt})}"
        )
        economic_main_payload = _get(
            economic_main_url,
            tries=tries,
            timeout=timeout,
            rate_limiter=rate_limiter,
        )
        economic_main_path = _content_addressed_evidence_path(
            root, economic_main_payload,
        )
        economic_page = parse_official_dart_main_page(
            economic_body_receipt,
            economic_main_payload,
            expected_attachment_only=False,
        )
        economic_body_dcm = economic_page.dcm_no
        economic_body_dtd = economic_page.dtd
        if (
            report_names is None
            or economic_body_receipt not in report_names
        ):
            raise RuntimeError(
                "DART attachment economic disclosure title is unavailable: "
                f"source={receipt} economic={economic_body_receipt}"
            )
        economic_report_name = report_names[economic_body_receipt]
        if (
            economic_page.receipt_no != economic_body_receipt
            or economic_page.current_selector != "FAMILY"
            or economic_page.family_receipts != source_page.family_receipts
            or economic_page.attachment_keys != source_page.attachment_keys
        ):
            raise RuntimeError(
                "DART attachment/economic main selectors disagree: "
                f"source={receipt} economic={economic_body_receipt}"
            )
        economic_viewer_url = official_dart_viewer_url(
            economic_body_receipt,
            economic_body_dcm,
            economic_body_dtd,
        )
        economic_viewer_payload = _get(
            economic_viewer_url,
            tries=tries,
            timeout=timeout,
            rate_limiter=rate_limiter,
        )
        economic_viewer_path = _content_addressed_evidence_path(
            root, economic_viewer_payload,
        )
    else:
        economic_main_path = main_path
        economic_main_payload = main_payload
        economic_body_dcm = dcm_no
        economic_body_dtd = dtd
        economic_report_name = report_name
        economic_viewer_path = viewer_path
        economic_viewer_payload = viewer_payload
    try:
        source_classification, _, _ = _parse_viewer_economic_body(
            viewer_payload,
            report_name=report_name,
        )
        if attachment_correction and source_classification != "ATTACHMENT_CORRECTION":
            raise RuntimeError("attachment receipt was not classified as attachment")
        economic_classification, common_cash_amount, record_date = (
            _parse_viewer_economic_body(
                economic_viewer_payload,
                report_name=economic_report_name,
            )
        )
    except RuntimeError as exc:
        raise RuntimeError(
            f"DART viewer economic evidence invalid: receipt={receipt}: {exc}"
        ) from exc
    return ViewerReceiptEvidence(
        receipt_no=receipt,
        dcm_no=dcm_no,
        dtd=dtd,
        current_selector=current_selector,
        attachment_keys=attachment_keys,
        correction_of_receipt_no=correction_of,
        revision_root_receipt_no=revision_root,
        family_receipt_nos=family,
        official_family_order=official_family_order,
        revision_kind=(
            "ATTACHMENT_ONLY" if attachment_correction else "ECONOMIC_REVISION"
        ),
        economic_body_receipt_no=economic_body_receipt,
        economic_body_dcm_no=economic_body_dcm,
        economic_body_dtd=economic_body_dtd,
        economic_main_path=economic_main_path.relative_to(root).as_posix(),
        economic_main_content_length=len(economic_main_payload),
        economic_main_sha256=_sha256_bytes(economic_main_payload),
        economic_classification=economic_classification,
        common_cash_amount=common_cash_amount,
        record_date=record_date,
        main_path=main_path.relative_to(root).as_posix(),
        main_content_length=len(main_payload),
        main_sha256=_sha256_bytes(main_payload),
        viewer_path=viewer_path.relative_to(root).as_posix(),
        viewer_content_length=len(viewer_payload),
        viewer_sha256=_sha256_bytes(viewer_payload),
        economic_viewer_path=economic_viewer_path.relative_to(root).as_posix(),
        economic_viewer_content_length=len(economic_viewer_payload),
        economic_viewer_sha256=_sha256_bytes(economic_viewer_payload),
    )


def _receipt_digest(receipts: Iterable[ViewerReceiptEvidence]) -> str:
    rendered = json.dumps(
        [asdict(item) for item in receipts],
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(rendered).hexdigest()


def _receipt_identity_digest(receipts: Iterable[str]) -> str:
    rendered = json.dumps(
        sorted(set(receipts)),
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(rendered).hexdigest()


def _receipt_from_manifest(row: dict) -> ViewerReceiptEvidence:
    rendered = dict(row)
    for field in (
        "attachment_keys", "family_receipt_nos", "official_family_order",
    ):
        rendered[field] = tuple(str(value) for value in rendered.get(field) or ())
    return ViewerReceiptEvidence(**rendered)


def _probe_from_manifest(row: dict) -> ViewerDependencyProbe:
    rendered = dict(row)
    for field in ("family_receipt_nos", "attachment_keys"):
        rendered[field] = tuple(str(value) for value in rendered.get(field) or ())
    return ViewerDependencyProbe(**rendered)


def _incremental_manifest_seed(
    root: Path,
    *,
    coverage_start: date,
    coverage_end: date,
    ordered_seeds: tuple[str, ...],
) -> tuple[list[ViewerReceiptEvidence], list[ViewerDependencyProbe], set[str]]:
    """Reuse immutable evidence and return only receipts needing refresh."""
    manifest = root / MANIFEST_RELATIVE_PATH
    if not manifest.is_file():
        return [], [], set(ordered_seeds)
    try:
        raw = manifest.read_bytes()
        payload = json.loads(raw)
        canonical = json.dumps(
            payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        ).encode("utf-8")
        previous_end = date.fromisoformat(str(payload["seed_coverage_end"]))
        compatible = (
            raw == canonical
            and payload.get("schema_version") == SCHEMA_VERSION
            and payload.get("source_contract") == SOURCE_CONTRACT
            and payload.get("seed_coverage_start") == coverage_start.isoformat()
            and previous_end < coverage_end
            and payload.get("complete") is True
        )
        if not compatible:
            return [], [], set(ordered_seeds)
        previous_seeds = set(payload.get("seed_receipts") or ())
        receipts = [
            _receipt_from_manifest(row) for row in payload.get("receipts") or ()
        ]
        probes = [
            _probe_from_manifest(row)
            for row in payload.get("dependency_probes") or ()
        ]
    except (KeyError, TypeError, ValueError, json.JSONDecodeError):
        return [], [], set(ordered_seeds)
    delta = set(ordered_seeds) - previous_seeds
    print(
        "[dart-viewer-corrections] incremental evidence "
        f"reused={len(receipts)} new_seeds={len(delta)}",
        flush=True,
    )
    return receipts, probes, delta


def _dependency_probe_digest(
    probes: Iterable[ViewerDependencyProbe],
) -> str:
    rendered = json.dumps(
        [asdict(item) for item in probes],
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(rendered).hexdigest()


def _discover_outside_family_dependencies(
    root: Path,
    disclosures: dict[str, dict],
    *,
    coverage_start: date,
    coverage_end: date,
    tries: int,
    timeout: float,
    rate_limiter: _RateLimiter,
    workers: int,
    candidates_override: Iterable[str] | None = None,
) -> tuple[dict[str, bytes], tuple[ViewerDependencyProbe, ...]]:
    """Refresh provisional outside corrections and retain exact seed links.

    Every main page is content-addressed and bound as a probe so offline
    verification can prove that no complete-snapshot candidate was omitted.
    Only exact seed-linked pages are also returned as economic dependencies.
    """
    seed_cash_receipts = _seed_cash_receipts(
        disclosures,
        coverage_start=coverage_start,
        coverage_end=coverage_end,
    )
    candidates = tuple(sorted(
        candidates_override
        if candidates_override is not None
        else _outside_revision_candidates(
            disclosures,
            coverage_start=coverage_start,
            coverage_end=coverage_end,
        )
    ))
    if not candidates:
        return {}, ()
    executor = ThreadPoolExecutor(max_workers=workers)
    futures = {
        executor.submit(
            _fetch_main_payload,
            receipt,
            tries=tries,
            timeout=timeout,
            rate_limiter=rate_limiter,
        ): receipt
        for receipt in candidates
    }
    retained: dict[str, bytes] = {}
    probes: list[ViewerDependencyProbe] = []
    try:
        for future in as_completed(futures):
            receipt = futures[future]
            payload = future.result()
            attachment = "첨부정정" in _compact(
                disclosures[receipt].get("report_nm")
            )
            page = parse_official_dart_main_page(
                receipt,
                payload,
                expected_attachment_only=attachment,
            )
            exact_lineage = set(page.family_receipts)
            if page.current_selector == "FAMILY":
                exact_lineage.add(receipt)
            intersects_seed = bool(
                exact_lineage.intersection(seed_cash_receipts)
            )
            if intersects_seed:
                retained[receipt] = payload
            main_path = _content_addressed_evidence_path(root, payload)
            receipt_date = _cash_disclosure_date(
                receipt, disclosures[receipt]
            ).isoformat()
            probes.append(ViewerDependencyProbe(
                receipt_no=receipt,
                rcept_dt=receipt_date,
                current_selector=page.current_selector,
                family_receipt_nos=page.family_receipts,
                attachment_keys=page.attachment_keys,
                intersects_seed_receipt=intersects_seed,
                selected_dependency=intersects_seed,
                main_path=main_path.relative_to(root).as_posix(),
                main_content_length=len(payload),
                main_sha256=_sha256_bytes(payload),
            ))
    except BaseException:
        for future in futures:
            future.cancel()
        executor.shutdown(wait=False, cancel_futures=True)
        raise
    else:
        executor.shutdown(wait=True)
    print(
        "[dart-viewer-corrections] "
        f"provisional_outside={len(candidates)} "
        f"linked_dependencies={len(retained)}",
        flush=True,
    )
    probes.sort(key=lambda item: item.receipt_no)
    return retained, tuple(probes)


_TERMINAL_ECONOMIC_CLASSIFICATIONS = frozenset({
    "ECONOMIC_DECISION",
    "NO_COMMON_CASH_DIVIDEND",
    "NO_ECONOMIC_EVENT",
})


def _validate_terminal_families(
    receipts: Iterable[ViewerReceiptEvidence],
    disclosures: dict[str, dict],
) -> None:
    """Allow incomplete intermediate revisions, never an incomplete terminal.

    The official ``family`` selector provides the order.  Receipt-number
    magnitude is never used as a chronology proxy.  Attachment-only receipts
    are source evidence and resolve to the official economic terminal.
    """
    rows = tuple(receipts)
    by_root: dict[str, list[ViewerReceiptEvidence]] = {}
    for item in rows:
        by_root.setdefault(item.revision_root_receipt_no, []).append(item)
        if (
            not item.official_family_order
            or len(item.official_family_order)
            != len(set(item.official_family_order))
            or not set(item.official_family_order).issubset(
                item.family_receipt_nos
            )
            or item.revision_root_receipt_no
            != item.official_family_order[-1]
            or (
                item.revision_kind == "ATTACHMENT_ONLY"
                and (
                    item.current_selector != "ATTACHMENT"
                    or item.correction_of_receipt_no
                    != item.revision_root_receipt_no
                )
            )
            or (
                item.revision_kind == "ECONOMIC_REVISION"
                and item.current_selector != "FAMILY"
            )
        ):
            raise RuntimeError(
                f"DART official family order is invalid: {item.receipt_no}"
            )

    for root, group in by_root.items():
        official_orders = {item.official_family_order for item in group}
        attachment_orders = {item.attachment_keys for item in group}
        if len(official_orders) != 1 or len(attachment_orders) != 1:
            raise RuntimeError(
                f"DART official family/attachment selectors disagree: {root}"
            )
        official_order = next(iter(official_orders))
        terminal_receipt = official_order[0]
        terminal_candidates = [
            item for item in group
            if item.economic_body_receipt_no == terminal_receipt
        ]
        terminal_values = {
            (
                item.economic_classification,
                item.common_cash_amount,
                item.record_date,
            )
            for item in terminal_candidates
        }
        if len(terminal_values) != 1:
            raise RuntimeError(
                "DART terminal economic receipt evidence is missing/ambiguous: "
                f"root={root} terminal={terminal_receipt}"
            )
        terminal_classification, terminal_amount, terminal_date = next(
            iter(terminal_values)
        )
        if terminal_classification not in _TERMINAL_ECONOMIC_CLASSIFICATIONS:
            raise RuntimeError(
                "DART terminal economic revision is incomplete: "
                f"root={root} terminal={terminal_receipt} "
                f"classification={terminal_classification}"
            )
        if terminal_classification == "ECONOMIC_DECISION" and (
            terminal_amount is None or terminal_amount <= 0
            or terminal_date is None
        ):
            raise RuntimeError(
                f"DART terminal positive decision is incomplete: {terminal_receipt}"
            )
        for item in group:
            if item.economic_classification == "POSITIVE_PENDING_RECORD_DATE":
                if (
                    item.receipt_no not in official_order
                    or official_order.index(terminal_receipt)
                    >= official_order.index(item.receipt_no)
                ):
                    raise RuntimeError(
                        "DART incomplete positive receipt is not superseded by "
                        f"the official family terminal: {item.receipt_no}"
                    )


def _pending_terminal_dependencies(
    receipts: Iterable[ViewerReceiptEvidence],
    disclosures: dict[str, dict],
) -> tuple[str, ...]:
    """Return economic terminals needed to close exact official families."""
    rows = tuple(receipts)
    economic_bodies = {item.economic_body_receipt_no for item in rows}
    dependencies: set[str] = set()
    for item in rows:
        if not item.official_family_order:
            raise RuntimeError(
                f"DART pending family has no economic member: {item.receipt_no}"
            )
        terminal = item.official_family_order[0]
        if terminal not in disclosures:
            raise RuntimeError(
                "DART pending family terminal is absent from the immutable "
                f"disclosure snapshot: {terminal}"
            )
        if terminal not in economic_bodies:
            dependencies.add(terminal)
    return tuple(sorted(dependencies))


def collect_viewer_corrections(
    base: str,
    *,
    coverage_start: date,
    coverage_end: date,
    apply: bool = False,
    workers: int = 4,
    tries: int = 4,
    timeout: float = 30.0,
    request_interval_seconds: float = DEFAULT_REQUEST_INTERVAL_SECONDS,
) -> VerifiedViewerCorrectionSnapshot | dict:
    root = Path(base).expanduser().resolve()
    if coverage_end < coverage_start:
        raise ValueError("viewer coverage_end precedes coverage_start")
    disclosures = _cash_disclosures(root)
    seed_required = set(required_viewer_receipts(
        str(root),
        coverage_start=coverage_start,
        coverage_end=coverage_end,
    ))
    ordered_seeds = tuple(sorted(seed_required))
    provisional = _outside_revision_candidates(
        disclosures,
        coverage_start=coverage_start,
        coverage_end=coverage_end,
    )
    if not apply:
        return {
            "apply": False,
            "schema_version": SCHEMA_VERSION,
            "source_contract": SOURCE_CONTRACT,
            "family_order": FAMILY_ORDER_CONTRACT,
            "attachment_parent_contract": ATTACHMENT_PARENT_CONTRACT,
            "seed_coverage_start": coverage_start.isoformat(),
            "seed_coverage_end": coverage_end.isoformat(),
            "seed_receipt_count": len(ordered_seeds),
            "seed_receipt_digest": _receipt_identity_digest(ordered_seeds),
            "seed_receipts": list(ordered_seeds),
            "provisional_outside_candidate_count": len(provisional),
            "provisional_outside_candidates": list(provisional),
        }
    if workers < 1 or workers > 8:
        raise ValueError("workers must be in [1, 8]")
    if request_interval_seconds <= 0:
        raise ValueError("request_interval_seconds must be positive")
    evidence, reused_probes, refresh_receipts = _incremental_manifest_seed(
        root,
        coverage_start=coverage_start,
        coverage_end=coverage_end,
        ordered_seeds=ordered_seeds,
    )
    report_names = {
        key: row.get("report_nm") for key, row in disclosures.items()
    }
    rate_limiter = _RateLimiter(
        request_interval_seconds,
        DEFAULT_REQUEST_JITTER_SECONDS,
    )
    current_outside = set(_outside_revision_candidates(
        disclosures,
        coverage_start=coverage_start,
        coverage_end=coverage_end,
    ))
    reusable_probes = [
        probe for probe in reused_probes
        if probe.receipt_no in current_outside
    ]
    known_probe_receipts = {probe.receipt_no for probe in reusable_probes}
    prefetched_main, new_dependency_probes = _discover_outside_family_dependencies(
        root,
        disclosures,
        coverage_start=coverage_start,
        coverage_end=coverage_end,
        tries=tries,
        timeout=timeout,
        rate_limiter=rate_limiter,
        workers=workers,
        candidates_override=current_outside - known_probe_receipts,
    )
    dependency_probes = tuple(sorted(
        [*reusable_probes, *new_dependency_probes],
        key=lambda item: item.receipt_no,
    ))
    reused_dependencies = {
        probe.receipt_no for probe in reusable_probes
        if probe.selected_dependency
    }
    required = seed_required | reused_dependencies | set(prefetched_main)
    ordered = tuple(sorted(required))
    evidence = [item for item in evidence if item.receipt_no in required]
    existing_receipts = {item.receipt_no for item in evidence}
    refresh_receipts.update(required - existing_receipts)
    evidence = [
        item for item in evidence
        if item.receipt_no not in refresh_receipts
    ]
    executor = ThreadPoolExecutor(max_workers=workers)
    futures = {
            executor.submit(
                _fetch_one,
                root,
                receipt,
                tries=tries,
                timeout=timeout,
                rate_limiter=rate_limiter,
                report_name=disclosures[receipt].get("report_nm"),
                report_names=report_names,
                prefetched_main_payload=prefetched_main.get(receipt),
            ): receipt
            for receipt in sorted(refresh_receipts)
        }
    try:
        for completed, future in enumerate(as_completed(futures), start=1):
            evidence.append(future.result())
            if completed % 50 == 0 or completed == len(futures):
                print(
                    "[dart-viewer-corrections] "
                    f"downloaded={completed}/{len(futures)}",
                    flush=True,
                )
    except BaseException:
        for future in futures:
            future.cancel()
        executor.shutdown(wait=False, cancel_futures=True)
        raise
    else:
        executor.shutdown(wait=True)
    # A newly observed correction changes the mutable family selector for its
    # older members. Refresh only reused members of those exact families.
    refreshed_receipts = {
        item.receipt_no for item in evidence
        if item.receipt_no in refresh_receipts
    }
    touched_roots = {
        item.revision_root_receipt_no for item in evidence
        if item.receipt_no in refreshed_receipts
    }
    family_refresh = {
        item.receipt_no for item in evidence
        if item.receipt_no not in refreshed_receipts
        and item.revision_root_receipt_no in touched_roots
    }
    if family_refresh:
        refresh_executor = ThreadPoolExecutor(max_workers=workers)
        refresh_futures = {
            refresh_executor.submit(
                _fetch_one,
                root,
                receipt,
                tries=tries,
                timeout=timeout,
                rate_limiter=rate_limiter,
                report_name=disclosures[receipt].get("report_nm"),
                report_names=report_names,
            ): receipt
            for receipt in sorted(family_refresh)
        }
        replacements = [
            future.result() for future in as_completed(refresh_futures)
        ]
        refresh_executor.shutdown(wait=True)
        evidence = [
            item for item in evidence if item.receipt_no not in family_refresh
        ] + replacements
    # An intermediate correction can point to a later plain (non-correction)
    # terminal receipt, which is not in the initial correction-only set.  Add
    # those exact official-family dependencies and repeat until closure.
    while dependencies := _pending_terminal_dependencies(
        evidence, disclosures,
    ):
        dependency_executor = ThreadPoolExecutor(max_workers=workers)
        dependency_futures = {
            dependency_executor.submit(
                _fetch_one,
                root,
                receipt,
                tries=tries,
                timeout=timeout,
                rate_limiter=rate_limiter,
                report_name=disclosures[receipt].get("report_nm"),
                report_names=report_names,
            ): receipt
            for receipt in dependencies
        }
        try:
            evidence.extend(
                future.result() for future in as_completed(dependency_futures)
            )
        except BaseException:
            for future in dependency_futures:
                future.cancel()
            dependency_executor.shutdown(wait=False, cancel_futures=True)
            raise
        else:
            dependency_executor.shutdown(wait=True)
        required.update(dependencies)
    ordered = tuple(sorted(required))
    dependency_receipts = tuple(sorted(required - seed_required))
    evidence.sort(key=lambda item: item.receipt_no)
    _validate_terminal_families(evidence, disclosures)
    payload = {
        "schema_version": SCHEMA_VERSION,
        "source_contract": SOURCE_CONTRACT,
        "family_order": FAMILY_ORDER_CONTRACT,
        "attachment_parent_contract": ATTACHMENT_PARENT_CONTRACT,
        "seed_coverage_start": coverage_start.isoformat(),
        "seed_coverage_end": coverage_end.isoformat(),
        "complete": True,
        "seed_receipts": list(ordered_seeds),
        "seed_receipt_count": len(ordered_seeds),
        "seed_receipt_digest": _receipt_identity_digest(ordered_seeds),
        "dependency_receipts": list(dependency_receipts),
        "dependency_receipt_count": len(dependency_receipts),
        "dependency_receipt_digest": _receipt_identity_digest(
            dependency_receipts
        ),
        "dependency_probe_count": len(dependency_probes),
        "dependency_probe_digest": _dependency_probe_digest(
            dependency_probes
        ),
        "dependency_probes": [
            asdict(item) for item in dependency_probes
        ],
        "required_receipts": list(ordered),
        "receipt_count": len(evidence),
        "receipt_digest": _receipt_digest(evidence),
        "receipts": [asdict(item) for item in evidence],
    }
    manifest = root / MANIFEST_RELATIVE_PATH
    rendered = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    previous = manifest.read_bytes() if manifest.is_file() else None
    _atomic_write(manifest, rendered)
    try:
        return verify_viewer_corrections(
            str(root),
            required_start=coverage_start,
            required_end=coverage_end,
            required_receipts=ordered,
        )
    except BaseException:
        if previous is None:
            manifest.unlink(missing_ok=True)
        else:
            _atomic_write(manifest, previous)
        raise


def _declared_receipt_list(payload: dict, field: str) -> tuple[str, ...]:
    values = payload.get(field)
    if (
        not isinstance(values, list)
        or any(
            not isinstance(value, str)
            or re.fullmatch(r"\d{14}", value) is None
            for value in values
        )
        or values != sorted(set(values))
    ):
        raise RuntimeError(
            f"DART viewer {field} must be a sorted unique receipt list"
        )
    return tuple(values)


def _verify_dependency_probes(
    root: Path,
    payload: dict,
    disclosures: dict[str, dict],
    *,
    required_start: date,
    required_end: date,
    declared_dependencies: tuple[str, ...],
) -> tuple[ViewerDependencyProbe, ...]:
    rows = payload.get("dependency_probes")
    if not isinstance(rows, list):
        raise RuntimeError("DART viewer dependency probes must be a list")
    expected_candidates = _outside_revision_candidates(
        disclosures,
        coverage_start=required_start,
        coverage_end=required_end,
    )
    seed_cash_receipts = _seed_cash_receipts(
        disclosures,
        coverage_start=required_start,
        coverage_end=required_end,
    )
    dependency_set = set(declared_dependencies)
    probes: list[ViewerDependencyProbe] = []
    seen: set[str] = set()
    for row in rows:
        if not isinstance(row, dict):
            raise RuntimeError("invalid DART viewer dependency probe entry")
        try:
            probe = ViewerDependencyProbe(
                receipt_no=str(row.get("receipt_no") or ""),
                rcept_dt=str(row.get("rcept_dt") or ""),
                current_selector=str(row.get("current_selector") or ""),
                family_receipt_nos=tuple(
                    str(value) for value in row.get("family_receipt_nos") or []
                ),
                attachment_keys=tuple(
                    str(value) for value in row.get("attachment_keys") or []
                ),
                intersects_seed_receipt=row["intersects_seed_receipt"],
                selected_dependency=row["selected_dependency"],
                main_path=str(row.get("main_path") or ""),
                main_content_length=int(row.get("main_content_length", -1)),
                main_sha256=str(row.get("main_sha256") or ""),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise RuntimeError(
                "invalid DART viewer dependency probe entry"
            ) from exc
        disclosure = disclosures.get(probe.receipt_no)
        if (
            disclosure is None
            or probe.receipt_no in seen
            or re.fullmatch(r"\d{14}", probe.receipt_no) is None
            or probe.rcept_dt != _cash_disclosure_date(
                probe.receipt_no, disclosure
            ).isoformat()
            or probe.current_selector not in {"FAMILY", "ATTACHMENT"}
            or not probe.family_receipt_nos
            or len(probe.family_receipt_nos)
            != len(set(probe.family_receipt_nos))
            or any(
                re.fullmatch(r"\d{14}", value) is None
                for value in probe.family_receipt_nos
            )
            or len(probe.attachment_keys) != len(set(probe.attachment_keys))
            or any(
                re.fullmatch(r"\d{14}:\d+", value) is None
                for value in probe.attachment_keys
            )
            or not isinstance(probe.intersects_seed_receipt, bool)
            or not isinstance(probe.selected_dependency, bool)
            or re.fullmatch(r"[0-9a-f]{64}", probe.main_sha256) is None
        ):
            raise RuntimeError("invalid DART viewer dependency probe identity")
        seen.add(probe.receipt_no)
        main_path = (root / probe.main_path).resolve()
        expected_path = (
            OBJECT_ROOT_RELATIVE_PATH
            / f"sha256={probe.main_sha256}.html"
        ).as_posix()
        if (
            probe.main_path != expected_path
            or root not in main_path.parents
            or not main_path.is_file()
            or main_path.stat().st_size != probe.main_content_length
            or _sha256_path(main_path) != probe.main_sha256
        ):
            raise RuntimeError(
                "DART viewer dependency probe body changed: "
                f"{probe.receipt_no}"
            )
        attachment = "첨부정정" in _compact(disclosure.get("report_nm"))
        page = parse_official_dart_main_page(
            probe.receipt_no,
            main_path.read_bytes(),
            expected_attachment_only=attachment,
        )
        if (
            page.current_selector != probe.current_selector
            or page.family_receipts != probe.family_receipt_nos
            or page.attachment_keys != probe.attachment_keys
        ):
            raise RuntimeError(
                "DART viewer dependency probe lineage changed: "
                f"{probe.receipt_no}"
            )
        exact_lineage = set(page.family_receipts)
        if page.current_selector == "FAMILY":
            exact_lineage.add(probe.receipt_no)
        expected_selected = bool(exact_lineage.intersection(seed_cash_receipts))
        if (
            probe.intersects_seed_receipt is not expected_selected
            or probe.selected_dependency is not expected_selected
            or (probe.receipt_no in dependency_set) is not expected_selected
        ):
            raise RuntimeError(
                "DART viewer dependency probe selection changed: "
                f"{probe.receipt_no}"
            )
        probes.append(probe)
    probes.sort(key=lambda item: item.receipt_no)
    if tuple(item.receipt_no for item in probes) != expected_candidates:
        raise RuntimeError("DART viewer dependency probe candidate set changed")
    digest = _dependency_probe_digest(probes)
    if payload.get("dependency_probe_digest") != digest:
        raise RuntimeError("DART viewer dependency probe digest mismatch")
    if int(payload.get("dependency_probe_count", -1)) != len(probes):
        raise RuntimeError("DART viewer dependency probe count mismatch")
    return tuple(probes)


def verify_viewer_corrections(
    base: str,
    *,
    required_start: date,
    required_end: date,
    required_receipts: Iterable[str] | None = None,
) -> VerifiedViewerCorrectionSnapshot:
    if required_end < required_start:
        raise ValueError("viewer required_end precedes required_start")
    root = Path(base).expanduser().resolve()
    manifest = root / MANIFEST_RELATIVE_PATH
    try:
        raw = manifest.read_bytes()
        payload = json.loads(raw)
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(
            f"missing/invalid DART viewer correction manifest: {manifest}"
        ) from exc
    canonical = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    if raw != canonical:
        raise RuntimeError("DART viewer correction manifest is not canonical")
    if payload.get("schema_version") != SCHEMA_VERSION:
        raise RuntimeError("unsupported DART viewer correction schema")
    if (
        payload.get("source_contract") != SOURCE_CONTRACT
        or payload.get("family_order") != FAMILY_ORDER_CONTRACT
        or payload.get("attachment_parent_contract")
        != ATTACHMENT_PARENT_CONTRACT
    ):
        raise RuntimeError("unsupported DART viewer correction provenance")
    if payload.get("complete") is not True:
        raise RuntimeError("DART viewer correction snapshot is incomplete")
    if (
        payload.get("seed_coverage_start") != required_start.isoformat()
        or payload.get("seed_coverage_end") != required_end.isoformat()
    ):
        raise RuntimeError("DART viewer seed coverage mismatch")
    declared_seeds = _declared_receipt_list(payload, "seed_receipts")
    declared_dependencies = _declared_receipt_list(
        payload, "dependency_receipts"
    )
    declared_required = _declared_receipt_list(payload, "required_receipts")
    if set(declared_seeds).intersection(declared_dependencies) or (
        tuple(sorted(set(declared_seeds) | set(declared_dependencies)))
        != declared_required
    ):
        raise RuntimeError("DART viewer seed/dependency receipt partition changed")
    disclosures = _cash_disclosures(root)
    automatic_seeds = set(required_viewer_receipts(
        str(root),
        coverage_start=required_start,
        coverage_end=required_end,
    ))
    expected = (
        automatic_seeds
        if required_receipts is None
        else {str(value) for value in required_receipts}
    )
    if automatic_seeds != set(declared_seeds):
        missing = sorted(automatic_seeds - set(declared_seeds))
        extra = sorted(set(declared_seeds) - automatic_seeds)
        raise RuntimeError(
            "DART viewer seed receipt set changed: "
            f"missing={missing[:20]} extra={extra[:20]}"
        )
    if not expected.issubset(declared_required):
        missing = sorted(expected - set(declared_required))
        raise RuntimeError(
            f"DART viewer correction receipts are missing: {missing[:20]}"
        )
    invalid_seeds = sorted(
        receipt for receipt in declared_seeds
        if receipt not in disclosures
        or not _is_issuer_cash_disclosure(disclosures[receipt])
        or not required_start <= _cash_disclosure_date(
            receipt, disclosures[receipt]
        ) <= required_end
    )
    if invalid_seeds:
        raise RuntimeError(
            "DART viewer seed receipt falls outside exact issuer coverage: "
            f"{invalid_seeds[:20]}"
        )
    for field, identities in (
        ("seed", declared_seeds),
        ("dependency", declared_dependencies),
    ):
        if int(payload.get(f"{field}_receipt_count", -1)) != len(identities):
            raise RuntimeError(f"DART viewer {field} receipt count mismatch")
        if payload.get(f"{field}_receipt_digest") != _receipt_identity_digest(
            identities
        ):
            raise RuntimeError(f"DART viewer {field} receipt digest mismatch")
    dependency_probes = _verify_dependency_probes(
        root,
        payload,
        disclosures,
        required_start=required_start,
        required_end=required_end,
        declared_dependencies=declared_dependencies,
    )
    rows = payload.get("receipts")
    if not isinstance(rows, list):
        raise RuntimeError("DART viewer correction receipts must be a list")
    receipts: list[ViewerReceiptEvidence] = []
    seen: set[str] = set()
    for row in rows:
        if not isinstance(row, dict):
            raise RuntimeError("invalid DART viewer correction receipt entry")
        evidence = ViewerReceiptEvidence(
            receipt_no=str(row.get("receipt_no") or ""),
            dcm_no=str(row.get("dcm_no") or ""),
            dtd=str(row.get("dtd") or ""),
            current_selector=str(row.get("current_selector") or ""),
            attachment_keys=tuple(
                str(value) for value in row.get("attachment_keys") or []
            ),
            correction_of_receipt_no=(
                str(row["correction_of_receipt_no"])
                if row.get("correction_of_receipt_no") else None
            ),
            revision_root_receipt_no=str(
                row.get("revision_root_receipt_no") or ""
            ),
            family_receipt_nos=tuple(
                str(value) for value in row.get("family_receipt_nos") or []
            ),
            official_family_order=tuple(
                str(value) for value in row.get("official_family_order") or []
            ),
            revision_kind=str(row.get("revision_kind") or ""),
            economic_body_receipt_no=str(
                row.get("economic_body_receipt_no") or ""
            ),
            economic_body_dcm_no=str(
                row.get("economic_body_dcm_no") or ""
            ),
            economic_body_dtd=str(row.get("economic_body_dtd") or ""),
            economic_main_path=str(row.get("economic_main_path") or ""),
            economic_main_content_length=int(
                row.get("economic_main_content_length", -1)
            ),
            economic_main_sha256=str(
                row.get("economic_main_sha256") or ""
            ),
            economic_classification=str(
                row.get("economic_classification") or ""
            ),
            common_cash_amount=(
                float(row["common_cash_amount"])
                if row.get("common_cash_amount") is not None else None
            ),
            record_date=(
                str(row["record_date"]) if row.get("record_date") else None
            ),
            main_path=str(row.get("main_path") or ""),
            main_content_length=int(row.get("main_content_length", -1)),
            main_sha256=str(row.get("main_sha256") or ""),
            viewer_path=str(row.get("viewer_path") or ""),
            viewer_content_length=int(row.get("viewer_content_length", -1)),
            viewer_sha256=str(row.get("viewer_sha256") or ""),
            economic_viewer_path=str(
                row.get("economic_viewer_path") or ""
            ),
            economic_viewer_content_length=int(
                row.get("economic_viewer_content_length", -1)
            ),
            economic_viewer_sha256=str(
                row.get("economic_viewer_sha256") or ""
            ),
        )
        if evidence.receipt_no in seen:
            raise RuntimeError("duplicate DART viewer correction receipt")
        seen.add(evidence.receipt_no)
        if (
            not re.fullmatch(r"\d{14}", evidence.receipt_no)
            or not evidence.dcm_no.isdigit()
            or re.fullmatch(r"[0-9A-Za-z_.-]+", evidence.dtd) is None
            or evidence.current_selector not in {"FAMILY", "ATTACHMENT"}
            or len(evidence.attachment_keys)
            != len(set(evidence.attachment_keys))
            or any(
                re.fullmatch(r"\d{14}:\d+", value) is None
                for value in evidence.attachment_keys
            )
            or not re.fullmatch(
                r"\d{14}", evidence.economic_body_receipt_no
            )
            or not evidence.economic_body_dcm_no.isdigit()
            or re.fullmatch(
                r"[0-9A-Za-z_.-]+", evidence.economic_body_dtd,
            ) is None
            or evidence.revision_kind not in {
                "ATTACHMENT_ONLY", "ECONOMIC_REVISION",
            }
        ):
            raise RuntimeError("invalid DART viewer receipt identity")
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
            path = (root / relative).resolve()
            if root not in path.parents or not path.is_file():
                raise RuntimeError(
                    f"DART viewer evidence path is missing/unsafe: {relative}"
                )
            if path.stat().st_size != length or _sha256_path(path) != digest:
                raise RuntimeError(
                    f"DART viewer evidence SHA/content length mismatch: {relative}"
                )
        report_name = (disclosures.get(evidence.receipt_no) or {}).get(
            "report_nm"
        )
        title_is_attachment = "첨부정정" in _compact(report_name)
        for relative, digest in (
            (evidence.main_path, evidence.main_sha256),
            (evidence.viewer_path, evidence.viewer_sha256),
            (evidence.economic_main_path, evidence.economic_main_sha256),
            (
                evidence.economic_viewer_path,
                evidence.economic_viewer_sha256,
            ),
        ):
            expected = (
                OBJECT_ROOT_RELATIVE_PATH / f"sha256={digest}.html"
            ).as_posix()
            if relative != expected:
                raise RuntimeError(
                    "DART viewer evidence path is non-canonical: "
                    f"{evidence.receipt_no}"
                )
        source_page = parse_official_dart_main_page(
            evidence.receipt_no,
            (root / evidence.main_path).read_bytes(),
            expected_attachment_only=title_is_attachment,
        )
        (
            parsed_dcm,
            parsed_origin,
            parsed_root,
            parsed_family,
            parsed_economic_receipt,
            parsed_official_order,
        ) = _parse_main_page(
            evidence.receipt_no,
            (root / evidence.main_path).read_bytes(),
            attachment_correction=title_is_attachment,
        )
        if (
            parsed_dcm != evidence.dcm_no
            or parsed_origin != evidence.correction_of_receipt_no
            or parsed_root != evidence.revision_root_receipt_no
            or parsed_family != evidence.family_receipt_nos
            or parsed_economic_receipt != evidence.economic_body_receipt_no
            or parsed_official_order != evidence.official_family_order
            or source_page.dtd != evidence.dtd
            or source_page.current_selector != evidence.current_selector
            or source_page.attachment_keys != evidence.attachment_keys
        ):
            raise RuntimeError(
                f"DART viewer lineage changed: {evidence.receipt_no}"
            )
        economic_page = parse_official_dart_main_page(
            evidence.economic_body_receipt_no,
            (root / evidence.economic_main_path).read_bytes(),
            expected_attachment_only=False,
        )
        if (
            economic_page.dcm_no != evidence.economic_body_dcm_no
            or economic_page.dtd != evidence.economic_body_dtd
            or economic_page.current_selector != "FAMILY"
            or economic_page.family_receipts
            != evidence.official_family_order
            or economic_page.attachment_keys != evidence.attachment_keys
        ):
            raise RuntimeError(
                f"DART viewer economic main changed: {evidence.receipt_no}"
            )
        if title_is_attachment != (
            evidence.revision_kind == "ATTACHMENT_ONLY"
        ):
            raise RuntimeError(
                f"DART viewer revision kind/title mismatch: "
                f"{evidence.receipt_no}"
            )
        source_classification = _parse_viewer_economic_body(
            (root / evidence.viewer_path).read_bytes(),
            report_name=report_name,
        )[0]
        expected_source_classification = (
            "ATTACHMENT_CORRECTION"
            if evidence.revision_kind == "ATTACHMENT_ONLY"
            else evidence.economic_classification
        )
        if source_classification != expected_source_classification:
            raise RuntimeError(
                f"DART viewer source classification changed: "
                f"{evidence.receipt_no}"
            )
        economic_disclosure = disclosures.get(
            evidence.economic_body_receipt_no
        )
        if economic_disclosure is None:
            raise RuntimeError(
                "DART viewer economic disclosure disappeared: "
                f"{evidence.economic_body_receipt_no}"
            )
        economic = _parse_viewer_economic_body(
            (root / evidence.economic_viewer_path).read_bytes(),
            report_name=economic_disclosure.get("report_nm"),
        )
        if economic != (
            evidence.economic_classification,
            evidence.common_cash_amount,
            evidence.record_date,
        ):
            raise RuntimeError(
                f"DART viewer economic evidence changed: {evidence.receipt_no}"
            )
        receipts.append(evidence)
    receipts.sort(key=lambda item: item.receipt_no)
    _validate_terminal_families(receipts, disclosures)
    by_receipt = {item.receipt_no: item for item in receipts}
    selected_probe_receipts = {
        probe.receipt_no
        for probe in dependency_probes
        if probe.selected_dependency
    }
    primary_receipts = set(declared_seeds) | selected_probe_receipts
    if not primary_receipts.issubset(by_receipt):
        missing = sorted(primary_receipts - set(by_receipt))
        raise RuntimeError(
            f"DART viewer primary evidence is missing: {missing[:20]}"
        )
    primary_economic_bodies = {
        by_receipt[receipt].economic_body_receipt_no
        for receipt in primary_receipts
    }
    terminal_dependencies = {
        by_receipt[receipt].official_family_order[0]
        for receipt in primary_receipts
        if by_receipt[receipt].official_family_order[0]
        not in primary_economic_bodies
    }
    expected_dependencies = selected_probe_receipts | (
        terminal_dependencies - set(declared_seeds)
    )
    if set(declared_dependencies) != expected_dependencies:
        raise RuntimeError(
            "DART viewer dependency closure changed: "
            f"expected={sorted(expected_dependencies)[:20]} "
            f"declared={list(declared_dependencies)[:20]}"
        )
    seed_cash_receipts = _seed_cash_receipts(
        disclosures,
        coverage_start=required_start,
        coverage_end=required_end,
    )
    invalid_dependencies = sorted(
        item.receipt_no for item in receipts
        if item.receipt_no in set(declared_dependencies)
        and not set(item.family_receipt_nos).intersection(seed_cash_receipts)
    )
    if invalid_dependencies:
        raise RuntimeError(
            "DART viewer dependency family has no in-range cash seed: "
            f"{invalid_dependencies[:20]}"
        )
    if set(declared_required) != {item.receipt_no for item in receipts}:
        raise RuntimeError("DART viewer manifest receipt set mismatch")
    digest = _receipt_digest(receipts)
    if payload.get("receipt_digest") != digest:
        raise RuntimeError("DART viewer correction receipt digest mismatch")
    if int(payload.get("receipt_count", -1)) != len(receipts):
        raise RuntimeError("DART viewer correction receipt count mismatch")
    return VerifiedViewerCorrectionSnapshot(
        base=str(root),
        manifest_path=str(manifest),
        manifest_sha256=hashlib.sha256(raw).hexdigest(),
        seed_coverage_start=required_start,
        seed_coverage_end=required_end,
        seed_receipt_count=len(declared_seeds),
        seed_receipt_digest=_receipt_identity_digest(declared_seeds),
        dependency_receipt_count=len(declared_dependencies),
        dependency_receipt_digest=_receipt_identity_digest(
            declared_dependencies
        ),
        dependency_probe_count=len(dependency_probes),
        dependency_probe_digest=_dependency_probe_digest(
            dependency_probes
        ),
        receipt_count=len(receipts),
        receipt_digest=digest,
        dependency_probes=dependency_probes,
        receipts=tuple(receipts),
    )


def evidence_by_receipt(
    base: str,
    *,
    required_start: date,
    required_end: date,
) -> dict[str, ViewerReceiptEvidence]:
    verified = verify_viewer_corrections(
        base, required_start=required_start, required_end=required_end,
    )
    return {item.receipt_no: item for item in verified.receipts}


def _cli_json_default(value: object) -> str:
    if isinstance(value, date):
        return value.isoformat()
    raise TypeError(
        f"Object of type {type(value).__name__} is not JSON serializable"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", required=True)
    parser.add_argument("--coverage-start", type=date.fromisoformat, required=True)
    parser.add_argument("--coverage-end", type=date.fromisoformat, required=True)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    result = collect_viewer_corrections(
        args.base,
        coverage_start=args.coverage_start,
        coverage_end=args.coverage_end,
        workers=args.workers,
        apply=args.apply,
    )
    if isinstance(result, dict):
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        # The verified result intentionally keeps the certified coverage
        # bounds as ``date`` values.  Keep the CLI success receipt JSON-safe
        # instead of failing after the manifest has already been published.
        print(json.dumps(
            asdict(result),
            ensure_ascii=False,
            indent=2,
            default=_cli_json_default,
        ))


if __name__ == "__main__":
    main()
