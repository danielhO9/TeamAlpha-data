"""Content-addressed completeness contract for local DART action snapshots.

The native Bronze collector writes one discovery manifest and two completion
markers per requested interval.  This module verifies that those completed
intervals cover every calendar day from the total-return contract start, then
binds every local action body to an immutable SHA-256 manifest.  Silver loaders
must verify this manifest before parsing or publishing any action.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import tempfile
import zipfile
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Iterable, Sequence

from pipeline.bronze import financials
from pipeline.bronze.corporate_actions import (
    _candidate_corp_to_stock,
    _document_candidate_receipts,
    _event_api_for_title,
    _is_listed_disclosure_candidate,
    _listed_disclosure_ticker,
    _structured_query_keys,
)
from pipeline.bronze.dart_disclosure_observations import (
    canonicalize_disclosures,
)
from pipeline.bronze.dart_viewer_corrections import (
    MANIFEST_RELATIVE_PATH as VIEWER_MANIFEST_RELATIVE_PATH,
    required_viewer_receipts,
    verify_viewer_corrections,
)
from pipeline.bronze.dart_support_action_families import (
    MANIFEST_RELATIVE_PATH as SUPPORT_FAMILY_MANIFEST_RELATIVE_PATH,
    external_evidence_paths as support_family_external_evidence_paths,
)
from pipeline.silver.reviewed_dividend_corrections import (
    MANIFEST_RELATIVE_PATH as REVIEWED_CORRECTIONS_RELATIVE_PATH,
    active_corrections as active_reviewed_corrections,
    canonical_manifest_bytes as reviewed_corrections_manifest_bytes,
    external_evidence_paths as reviewed_external_evidence_paths,
)
from pipeline.silver.cash_adjustment_scale_evidence import (
    MANIFEST_RELATIVE_PATH as CASH_SCALE_MANIFEST_RELATIVE_PATH,
    external_evidence_paths as cash_scale_external_evidence_paths,
    verify_source_evidence_manifest,
)
from pipeline.silver.return_contract import is_valid_krx_ticker
from pipeline.silver.corporate_actions import _disclosure_type


SCHEMA_VERSION = "dart_total_return_action_snapshot_v5"
DEFAULT_COVERAGE_START = date(2015, 1, 1)
MANIFEST_RELATIVE_PATH = Path(
    "corporate_actions/dart/action_snapshot_manifest.json"
)
HASH_CACHE_RELATIVE_PATH = Path(
    ".teamalpha/dart_action_snapshot_hash_cache_v1.json"
)
_HASH_CACHE_SCHEMA = "dart_action_snapshot_hash_cache_v1"


@dataclass(frozen=True)
class VerifiedActionSnapshot:
    base: str
    manifest_path: str
    manifest_sha256: str
    body_digest: str
    body_count: int
    coverage_start: date
    coverage_end: date
    coverage_intervals: tuple[tuple[date, date], ...]
    listing_snapshot: dict[str, object]
    disclosure_observation_audit: dict[str, object]
    cash_adjustment_scale_source_evidence: dict[str, object]


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _read_hash_cache(root: Path) -> dict[str, dict[str, object]]:
    path = root / HASH_CACHE_RELATIVE_PATH
    if not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_bytes())
    except (OSError, json.JSONDecodeError):
        return {}
    entries = payload.get("entries")
    if payload.get("schema_version") != _HASH_CACHE_SCHEMA or not isinstance(
        entries, dict,
    ):
        return {}
    return entries


def _write_hash_cache(root: Path, entries: dict[str, dict[str, object]]) -> None:
    path = root / HASH_CACHE_RELATIVE_PATH
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": _HASH_CACHE_SCHEMA,
        "entries": entries,
    }
    rendered = json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
    ).encode("utf-8")
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", dir=path.parent,
    )
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(rendered)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary_name, path)
    finally:
        if os.path.exists(temporary_name):
            os.unlink(temporary_name)


def seed_body_hash_cache(
    base: str | Path,
    entries: Iterable[dict[str, object]],
    *,
    excluded_paths: Iterable[str] = (),
) -> int:
    """Seed hashes from an already authenticated snapshot manifest.

    The caller must authenticate the manifest itself (the published snapshot
    pointer does this by SHA-256). Local size and mtime are bound here so any
    subsequent local replacement falls back to hashing the body again.
    """
    root = Path(base).expanduser().resolve()
    excluded = set(excluded_paths)
    cache = _read_hash_cache(root)
    def seed_one(entry: dict[str, object]):
        relative = str(entry.get("path") or "")
        candidate = Path(relative)
        digest = str(entry.get("sha256") or "")
        try:
            expected_size = int(entry.get("content_length", -1))
        except (TypeError, ValueError):
            return None
        if (
            not relative
            or relative in excluded
            or candidate.is_absolute()
            or ".." in candidate.parts
            or not re.fullmatch(r"[0-9a-f]{64}", digest)
        ):
            return None
        path = root / candidate
        try:
            stat = path.stat()
        except OSError:
            return None
        if not path.is_file() or stat.st_size != expected_size:
            return None
        return relative, {
                "content_length": stat.st_size,
                "mtime_ns": stat.st_mtime_ns,
                "sha256": digest,
            }

    with ThreadPoolExecutor(max_workers=32) as executor:
        seeded_entries = executor.map(seed_one, entries, chunksize=256)
        seeded = 0
        for result in seeded_entries:
            if result is None:
                continue
            relative, cache_entry = result
            cache[relative] = cache_entry
            seeded += 1
    _write_hash_cache(root, cache)
    return seeded


def _listing_snapshot(root: Path) -> dict[str, object]:
    """Bind the exact listing map used by every listed-candidate predicate."""
    path = root / financials.CORPCODE_BRONZE_PATH
    try:
        raw = path.read_bytes()
        pairs = financials._parse_listed_corps(raw)
    except Exception as exc:
        raise RuntimeError(
            f"missing/invalid DART listed-corp snapshot: {path}"
        ) from exc
    canonical_pairs = json.dumps(
        [
            {"corp_code": corp_code, "stock_code": stock_code}
            for corp_code, stock_code in pairs
        ],
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return {
        "contract": "dart_listed_corp_candidates_v1",
        "path": path.relative_to(root).as_posix(),
        "content_length": len(raw),
        "sha256": hashlib.sha256(raw).hexdigest(),
        "listed_corp_count": len(pairs),
        "listed_corp_digest": hashlib.sha256(canonical_pairs).hexdigest(),
    }


def _assert_listing_snapshot_stable(
    root: Path,
    before: dict[str, object],
) -> None:
    if _listing_snapshot(root) != before:
        raise RuntimeError(
            "DART listed-corp snapshot changed during candidate verification"
        )


def _date_value(value: object, *, field: str) -> date:
    rendered = str(value or "").strip()
    try:
        if len(rendered) == 8 and rendered.isdigit():
            return date(
                int(rendered[:4]), int(rendered[4:6]), int(rendered[6:8])
            )
        return date.fromisoformat(rendered)
    except ValueError as exc:
        raise RuntimeError(f"invalid {field}: {value!r}") from exc


def _is_total_return_disclosure(report_name: object) -> bool:
    rendered = str(report_name or "").replace(" ", "")
    return (
        "현금" in rendered
        and "현물" in rendered
        and "배당결정" in rendered
    ) or "배당락" in rendered or "주식배당결정" in rendered or "권배락" in rendered


def _is_consumed_action_row(
    row: dict,
    corp_to_stock: dict[str, str],
) -> bool:
    return (
        _is_listed_disclosure_candidate(row, corp_to_stock)
        and (
            _event_api_for_title(row.get("report_nm")) is not None
            or _disclosure_type(row.get("report_nm")) is not None
        )
    )


def _semantic_receipts_from_paths(paths: Iterable[Path]) -> set[str]:
    receipts: set[str] = set()
    for path in paths:
        match = re.fullmatch(r"rcept=([0-9]{14})\.[^.]+", path.name)
        if match is not None:
            receipts.add(match.group(1))
    return receipts


def _disclosure_observation_audit(
    root: Path,
    *,
    evidence_paths: Iterable[Path],
    required_start: date,
    required_end: date,
) -> dict[str, object]:
    observations: list[tuple[Path, dict]] = []
    selected = set(evidence_paths)
    semantic_receipts = _semantic_receipts_from_paths(selected)
    corp_to_stock: dict[str, str] = {}
    disclosure_paths = sorted(
        path for path in selected if path.name == "disclosures_v3.json"
    )
    decoded: list[tuple[Path, list[dict]]] = []
    for disclosure in disclosure_paths:
        try:
            rows = json.loads(disclosure.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise RuntimeError(
                f"invalid DART disclosures body: {disclosure}"
            ) from exc
        if not isinstance(rows, list):
            raise RuntimeError(f"DART disclosures must be a list: {disclosure}")
        typed_rows = [row for row in rows if isinstance(row, dict)]
        corp_to_stock.update(_candidate_corp_to_stock(str(root), typed_rows))
        decoded.append((disclosure, typed_rows))
    for disclosure, rows in decoded:
        observations.extend(
            (disclosure, row)
            for row in rows
            if (
                (receipt := str(row.get("rcept_no") or "").strip())
                in semantic_receipts
                or (
                    (accepted := _receipt_date(row)) is not None
                    and required_start <= accepted <= required_end
                    and _is_consumed_action_row(row, corp_to_stock)
                )
            )
        )
    _, audit = canonicalize_disclosures(
        observations, audit_root=root,
    )
    return audit


def _dependency_receipt_dates(
    root: Path,
    receipts: Iterable[str],
) -> dict[str, date]:
    action_root = root / "corporate_actions" / "dart" / "disclosures"
    result: dict[str, date] = {}
    for receipt in sorted(set(receipts)):
        if re.fullmatch(r"[0-9]{14}", receipt) is None:
            continue
        matches = sorted(action_root.glob(f"**/rcept={receipt}.json"))
        dates: set[date] = set()
        for path in matches:
            try:
                row = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                raise RuntimeError(
                    f"invalid DART dependency disclosure: {path}"
                ) from exc
            if not isinstance(row, dict) or str(row.get("rcept_no")) != receipt:
                raise RuntimeError(
                    f"DART dependency disclosure identity mismatch: {path}"
                )
            accepted = _receipt_date(row)
            if accepted is None:
                raise RuntimeError(
                    f"DART dependency has no official rcept_dt: {receipt}"
                )
            dates.add(accepted)
        if len(dates) != 1:
            raise RuntimeError(
                "DART dependency disclosure is missing/ambiguous: "
                f"receipt={receipt} matches={len(matches)} dates={sorted(dates)}"
            )
        result[receipt] = next(iter(dates))
    return result


def _native_complete_intervals(
    root: Path,
    *,
    required_start: date,
    required_end: date,
    dependency_receipts: Iterable[str] = (),
) -> list[tuple[date, date]]:
    if required_end < required_start:
        raise ValueError("coverage_end must be on/after coverage_start")
    dependency_receipts = set(dependency_receipts)
    dependency_dates = set(
        _dependency_receipt_dates(root, dependency_receipts).values()
    )
    corp_to_stock: dict[str, str] = {}
    manifest_root = root / "corporate_actions" / "dart" / "manifests"
    intervals: list[tuple[date, date]] = []
    required_document_receipts: set[str] = set()
    complete_disclosure_receipts: set[str] = set()
    incomplete_disclosures: dict[str, dict] = {}
    for disclosure in sorted(
        manifest_root.glob("from=*/to=*/disclosures_v3.json")
    ):
        interval_dir = disclosure.parent
        try:
            start = _date_value(
                interval_dir.parent.name.removeprefix("from="),
                field="native interval start",
            )
            end = _date_value(
                interval_dir.name.removeprefix("to="),
                field="native interval end",
            )
        except RuntimeError:
            continue
        if start > end:
            raise RuntimeError(f"reversed DART interval: {interval_dir}")
        if not (
            start <= required_end and end >= required_start
        ) and not any(start <= value <= end for value in dependency_dates):
            continue
        try:
            disclosures = json.loads(disclosure.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise RuntimeError(f"invalid DART disclosures body: {disclosure}") from exc
        if not isinstance(disclosures, list):
            raise RuntimeError(f"DART disclosures must be a list: {disclosure}")
        corp_to_stock.update(
            _candidate_corp_to_stock(str(root), disclosures)
        )
        semantic_rows = [
            row
            for row in disclosures
            if isinstance(row, dict)
            and (
                str(row.get("rcept_no") or "") in dependency_receipts
                or (
                    (accepted := _receipt_date(row)) is not None
                    and required_start <= accepted <= required_end
                    and _is_consumed_action_row(row, corp_to_stock)
                )
            )
        ]
        semantic_receipts = {
            str(row.get("rcept_no") or "") for row in semantic_rows
        }
        malformed_total_return_rows = [
            {
                "rcept_no": row.get("rcept_no"),
                "stock_code": row.get("stock_code"),
                "corp_code": row.get("corp_code"),
                "corp_cls": row.get("corp_cls"),
                "report_name": row.get("report_nm"),
            }
            for row in semantic_rows
            if _is_total_return_disclosure(row.get("report_nm"))
            and (
                resolved_ticker := _listed_disclosure_ticker(
                    row, corp_to_stock,
                )
            )
            and (
                re.fullmatch(
                    r"[0-9]{14}", str(row.get("rcept_no") or "").strip()
                ) is None
                or not is_valid_krx_ticker(resolved_ticker)
            )
        ]
        if malformed_total_return_rows:
            raise RuntimeError(
                "TR-relevant DART disclosure has no receipt/ticker identity: "
                f"{malformed_total_return_rows[:20]}"
            )
        interval_document_receipts = _document_candidate_receipts(
            disclosures, corp_to_stock,
        )
        interval_disclosure_receipts = {
            str(row.get("rcept_no") or "")
            for row in disclosures
            if isinstance(row, dict)
            and str(row.get("rcept_no") or "")
        }
        marker_paths = [
            interval_dir / "structured_complete_v3.json",
            interval_dir / "documents_complete_v5.json",
        ]
        # Overlapping collector runs are normal.  A v1/in-progress interval is
        # harmless only when every receipt it discovered is also present in a
        # fully completed v2 interval.  Otherwise the snapshot is incomplete.
        if not all(path.is_file() for path in marker_paths):
            incomplete_disclosures.update({
                str(row.get("rcept_no")): row
                for row in semantic_rows
                if str(row.get("rcept_no") or "")
            })
            continue
        # v5 completion covers the full document keyword set, including
        # structured bonus-decision originals/corrections.  Rechecking only
        # the narrower historical TR-title subset would let a required bonus
        # correction ZIP disappear after the completion marker was written.
        required_document_receipts.update(
            interval_document_receipts.intersection(semantic_receipts)
        )
        complete_disclosure_receipts.update(interval_disclosure_receipts)
        for marker_path in marker_paths:
            marker_name = marker_path.name
            try:
                marker = json.loads(marker_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                raise RuntimeError(
                    f"missing/invalid DART completion marker: {marker_path}"
                ) from exc
            if marker.get("status") != "COMPLETE":
                raise RuntimeError(f"DART marker is not COMPLETE: {marker_path}")
            if _date_value(marker.get("fromdate"), field="marker fromdate") != start:
                raise RuntimeError(f"DART marker start mismatch: {marker_path}")
            if _date_value(marker.get("todate"), field="marker todate") != end:
                raise RuntimeError(f"DART marker end mismatch: {marker_path}")
            if (
                marker_name == "documents_complete_v5.json"
                and int(marker.get("candidate_count", -1))
                != len(interval_document_receipts)
            ):
                raise RuntimeError(
                    f"DART document candidate parity mismatch: {marker_path}"
                )
            if marker_name == "structured_complete_v3.json":
                structured_queries = _structured_query_keys(
                    disclosures, corp_to_stock,
                )
                if int(marker.get("query_count", -1)) != len(
                    structured_queries
                ):
                    raise RuntimeError(
                        f"DART structured query parity mismatch: {marker_path}"
                    )
        intervals.append((start, end))
    if not intervals:
        raise RuntimeError("no complete DART corporate-action intervals")
    uncovered_receipts = sorted(
        set(incomplete_disclosures) - complete_disclosure_receipts
    )
    if uncovered_receipts:
        blocking = []
        for receipt in uncovered_receipts:
            row = incomplete_disclosures[receipt]
            relevant = (
                _is_total_return_disclosure(row.get("report_nm"))
                and bool(_listed_disclosure_ticker(row, corp_to_stock))
            )
            # corp_cls is raw provenance, never scope evidence.  Historical
            # listed KOSDAQ issuers are demonstrably labelled E, so every
            # uncovered economically relevant receipt blocks certification.
            if relevant:
                blocking.append({
                    "rcept_no": receipt,
                    "corp_code": row.get("corp_code"),
                    "stock_code": row.get("stock_code"),
                    "corp_cls": row.get("corp_cls"),
                    "report_name": row.get("report_nm"),
                })
        if blocking:
            raise RuntimeError(
                "incomplete DART intervals contain potentially listed action "
                f"receipts absent from every complete interval: {blocking[:20]}"
            )
    document_paths: dict[str, list[Path]] = {}
    unavailable_paths: dict[str, list[Path]] = {}
    for path in root.glob("corporate_actions/dart/documents/**/rcept=*.zip"):
        if path.is_file():
            document_paths.setdefault(
                path.stem.removeprefix("rcept="), []
            ).append(path)
    for path in root.glob(
        "corporate_actions/dart/documents_unavailable/**/rcept=*.xml"
    ):
        if path.is_file():
            unavailable_paths.setdefault(
                path.stem.removeprefix("rcept="), []
            ).append(path)
    invalid_documents = []
    for receipt in sorted(required_document_receipts):
        documents = document_paths.get(receipt, [])
        unavailable = unavailable_paths.get(receipt, [])
        if len(documents) + len(unavailable) != 1:
            invalid_documents.append({
                "receipt": receipt,
                "zip_count": len(documents),
                "status014_count": len(unavailable),
                "reason": "MISSING_OR_AMBIGUOUS_BODY",
            })
            continue
        if documents and not zipfile.is_zipfile(documents[0]):
            invalid_documents.append({
                "receipt": receipt,
                "reason": "INVALID_OPENDART_ZIP",
            })
        if unavailable:
            body = unavailable[0].read_text(encoding="utf-8", errors="replace")
            statuses = re.findall(r"<status>\s*([^<]+)\s*</status>", body)
            if statuses != ["014"]:
                invalid_documents.append({
                    "receipt": receipt,
                    "reason": "INVALID_DOCUMENT_UNAVAILABLE_STATUS",
                })
    if invalid_documents:
        raise RuntimeError(
            "DART document completion marker has missing/invalid bodies: "
            f"{invalid_documents[:20]}"
        )
    return intervals


def _assert_continuous(
    intervals: Iterable[tuple[date, date]],
    *,
    required_start: date,
    required_end: date,
) -> tuple[tuple[date, date], ...]:
    if required_end < required_start:
        raise ValueError("coverage_end must be on/after coverage_start")
    ordered = sorted(set(intervals))
    cursor = required_start
    retained: list[tuple[date, date]] = []
    for start, end in ordered:
        if end < cursor:
            continue
        if start > cursor:
            raise RuntimeError(
                "DART snapshot coverage gap: "
                f"{cursor.isoformat()}~{(start - timedelta(days=1)).isoformat()}"
            )
        retained.append((start, end))
        cursor = max(cursor, end + timedelta(days=1))
        if cursor > required_end:
            break
    if cursor <= required_end:
        raise RuntimeError(
            "DART snapshot coverage ends early: "
            f"covered_through={(cursor - timedelta(days=1)).isoformat()} "
            f"required={required_end.isoformat()}"
        )
    return tuple(retained)


def _receipt_date(row: dict) -> date | None:
    rendered = str(row.get("rcept_dt") or "").strip()
    if re.fullmatch(r"[0-9]{8}", rendered) is None:
        return None
    try:
        return date(
            int(rendered[:4]), int(rendered[4:6]), int(rendered[6:8]),
        )
    except ValueError:
        return None


def _completed_disclosure_observations(
    root: Path,
) -> list[tuple[Path, tuple[date, date], list[dict]]]:
    """Return only list bodies backed by both native completion markers."""
    observations: list[tuple[Path, tuple[date, date], list[dict]]] = []
    manifest_root = root / "corporate_actions" / "dart" / "manifests"
    for disclosure in sorted(
        manifest_root.glob("from=*/to=*/disclosures_v3.json")
    ):
        markers = (
            disclosure.parent / "structured_complete_v3.json",
            disclosure.parent / "documents_complete_v5.json",
        )
        if not all(path.is_file() for path in markers):
            continue
        try:
            start = _date_value(
                disclosure.parent.parent.name.removeprefix("from="),
                field="native interval start",
            )
            end = _date_value(
                disclosure.parent.name.removeprefix("to="),
                field="native interval end",
            )
            rows = json.loads(disclosure.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise RuntimeError(
                f"invalid DART disclosures body: {disclosure}"
            ) from exc
        if start > end or not isinstance(rows, list):
            raise RuntimeError(f"invalid DART disclosures body: {disclosure}")
        observations.append((
            disclosure,
            (start, end),
            [row for row in rows if isinstance(row, dict)],
        ))
    return observations


def _completion_evidence(paths: set[Path], disclosure: Path) -> None:
    paths.update({
        disclosure,
        disclosure.parent / "structured_complete_v3.json",
        disclosure.parent / "documents_complete_v5.json",
    })


def _viewer_evidence_paths(
    root: Path,
    *,
    required_start: date,
    required_end: date,
) -> tuple[set[Path], set[str], set[str]]:
    """Bind only the current verified viewer manifest and its exact objects."""
    required = required_viewer_receipts(
        str(root),
        coverage_start=required_start,
        coverage_end=required_end,
    )
    manifest = root / VIEWER_MANIFEST_RELATIVE_PATH
    if not required and not manifest.is_file():
        return set(), set(), set()
    verified = verify_viewer_corrections(
        str(root),
        required_start=required_start,
        required_end=required_end,
        required_receipts=required,
    )
    paths = {Path(verified.manifest_path)}
    paths.update(root / probe.main_path for probe in verified.dependency_probes)
    probe_receipts = {
        probe.receipt_no for probe in verified.dependency_probes
    }
    receipts: set[str] = set()
    for evidence in verified.receipts:
        paths.update({
            root / evidence.main_path,
            root / evidence.viewer_path,
            root / evidence.economic_main_path,
            root / evidence.economic_viewer_path,
        })
        receipts.update({
            evidence.receipt_no,
            evidence.revision_root_receipt_no,
            evidence.economic_body_receipt_no,
            *evidence.family_receipt_nos,
            *evidence.official_family_order,
            *(value.partition(":")[0] for value in evidence.attachment_keys),
        })
    return (
        paths,
        {value for value in receipts if re.fullmatch(r"[0-9]{14}", value)},
        probe_receipts,
    )


def _native_semantic_evidence_paths(
    root: Path,
    *,
    required_start: date,
    required_end: date,
    dependency_receipts: set[str],
    probe_receipts: set[str],
) -> set[Path]:
    """Select native files that prove the bounded seed or an exact lineage.

    A completed interval wholly outside the seed window is not evidence merely
    because it happens to be present locally.  It enters the snapshot only
    when a verified family names one of its receipts.
    """
    observations = _completed_disclosure_observations(root)
    all_rows = [row for _, _, rows in observations for row in rows]
    corp_to_stock = _candidate_corp_to_stock(str(root), all_rows)
    paths: set[Path] = set()
    seed_receipts: set[str] = set()
    for disclosure, (start, end), rows in observations:
        if start <= required_end and end >= required_start:
            _completion_evidence(paths, disclosure)
        for row in rows:
            receipt = str(row.get("rcept_no") or "").strip()
            if re.fullmatch(r"[0-9]{14}", receipt) is None:
                continue
            accepted = _receipt_date(row)
            if (
                accepted is not None
                and required_start <= accepted <= required_end
                and _is_consumed_action_row(row, corp_to_stock)
            ):
                seed_receipts.add(receipt)

    semantic_receipts = seed_receipts | dependency_receipts
    observation_receipts = semantic_receipts | probe_receipts
    for disclosure, _, rows in observations:
        if any(
            str(row.get("rcept_no") or "").strip() in observation_receipts
            for row in rows
        ):
            _completion_evidence(paths, disclosure)

    action_root = root / "corporate_actions" / "dart"
    # Scan each namespace once.  Running a recursive glob once per receipt is
    # quadratic on the actual 31k-receipt snapshot and can take hours.
    receipt_name = re.compile(r"^rcept=([0-9]{14})(?:\..+)?$")
    for relative_root in (
        "disclosures", "structured", "documents", "documents_unavailable",
    ):
        namespace = action_root / relative_root
        if not namespace.is_dir():
            continue
        for path in namespace.rglob("rcept=*"):
            match = receipt_name.fullmatch(path.name)
            if (
                match is not None
                and match.group(1) in semantic_receipts
                and path.is_file()
            ):
                paths.add(path)
    return paths


def _evidence_paths(
    root: Path,
    *,
    required_start: date,
    required_end: date,
) -> list[Path]:
    action_root = root / "corporate_actions" / "dart"
    if not action_root.is_dir():
        raise RuntimeError(f"missing DART action root: {action_root}")
    viewer_paths, viewer_receipts, viewer_probe_receipts = _viewer_evidence_paths(
        root, required_start=required_start, required_end=required_end,
    )
    paths = _native_semantic_evidence_paths(
        root,
        required_start=required_start,
        required_end=required_end,
        dependency_receipts=viewer_receipts,
        probe_receipts=viewer_probe_receipts,
    )
    paths.update(viewer_paths)
    paths.add(root / financials.CORPCODE_BRONZE_PATH)
    paths.update(reviewed_external_evidence_paths(root))
    if active_reviewed_corrections(root):
        paths.add(root / REVIEWED_CORRECTIONS_RELATIVE_PATH)
    paths.update(cash_scale_external_evidence_paths(
        str(root), required_start=required_start, required_end=required_end,
    ))
    if (root / CASH_SCALE_MANIFEST_RELATIVE_PATH).is_file():
        paths.add(root / CASH_SCALE_MANIFEST_RELATIVE_PATH)
    if (root / SUPPORT_FAMILY_MANIFEST_RELATIVE_PATH).is_file():
        support_paths = {
            root / relative
            for relative in support_family_external_evidence_paths(
                root,
                required_start=required_start,
                required_end=required_end,
            )
        }
        paths.update(support_paths)
        # A support source names its exact canonical list body.  Bind the two
        # completion markers that make that list observation admissible too.
        for path in support_paths:
            if path.name == "disclosures_v3.json":
                _completion_evidence(paths, path)
    missing_external = [path for path in paths if not path.is_file()]
    if missing_external:
        raise RuntimeError(
            "DART reviewed correction evidence is missing: "
            f"{[str(path) for path in missing_external[:10]]}"
        )
    if not paths:
        raise RuntimeError(f"DART action snapshot has no bodies: {action_root}")
    return sorted(paths, key=lambda path: path.relative_to(root).as_posix())


def _body_entries(root: Path, paths: Sequence[Path]) -> list[dict]:
    cache = _read_hash_cache(root)

    def body_entry(path: Path) -> tuple[dict, dict[str, object] | None]:
        relative = path.relative_to(root).as_posix()
        stat = path.stat()
        cached = cache.get(relative)
        if (
            isinstance(cached, dict)
            and cached.get("content_length") == stat.st_size
            and cached.get("mtime_ns") == stat.st_mtime_ns
            and re.fullmatch(r"[0-9a-f]{64}", str(cached.get("sha256") or ""))
        ):
            digest = str(cached["sha256"])
            updated_cache = None
        else:
            digest = _sha256(path)
            updated_cache = {
                "content_length": stat.st_size,
                "mtime_ns": stat.st_mtime_ns,
                "sha256": digest,
            }
        return ({
            "path": relative,
            "content_length": stat.st_size,
            "sha256": digest,
        }, updated_cache)

    entries: list[dict] = []
    dirty = False
    live_paths: set[str] = set()
    with ThreadPoolExecutor(max_workers=32) as executor:
        for entry, updated_cache in executor.map(
            body_entry, paths, chunksize=256,
        ):
            relative = str(entry["path"])
            live_paths.add(relative)
            entries.append(entry)
            if updated_cache is not None:
                cache[relative] = updated_cache
                dirty = True
    stale = set(cache).difference(live_paths)
    if stale:
        for relative in stale:
            cache.pop(relative, None)
        dirty = True
    if dirty:
        _write_hash_cache(root, cache)
    return entries


def _body_digest(entries: Sequence[dict]) -> str:
    canonical = json.dumps(
        list(entries),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def _verify_required_viewer_evidence(
    root: Path,
    *,
    required_start: date,
    required_end: date,
) -> None:
    required = required_viewer_receipts(
        str(root),
        coverage_start=required_start,
        coverage_end=required_end,
    )
    if required:
        verify_viewer_corrections(
            str(root),
            required_start=required_start,
            required_end=required_end,
            required_receipts=required,
        )


def _write_reviewed_correction_manifest(root: Path) -> None:
    if not active_reviewed_corrections(root):
        return
    destination = root / REVIEWED_CORRECTIONS_RELATIVE_PATH
    destination.parent.mkdir(parents=True, exist_ok=True)
    payload = reviewed_corrections_manifest_bytes(root)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{destination.name}.", dir=destination.parent,
    )
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary_name, destination)
    finally:
        if os.path.exists(temporary_name):
            os.unlink(temporary_name)


def _verify_reviewed_correction_manifest(root: Path) -> None:
    if not active_reviewed_corrections(root):
        return
    path = root / REVIEWED_CORRECTIONS_RELATIVE_PATH
    try:
        actual = path.read_bytes()
    except OSError as exc:
        raise RuntimeError(
            f"missing reviewed dividend correction manifest: {path}"
        ) from exc
    if actual != reviewed_corrections_manifest_bytes(root):
        raise RuntimeError("reviewed dividend correction manifest changed")


def build_snapshot_manifest(
    base: str,
    *,
    coverage_end: date,
    coverage_start: date = DEFAULT_COVERAGE_START,
) -> VerifiedActionSnapshot:
    """Verify native markers and atomically write the content manifest."""
    root = Path(base).expanduser().resolve()
    listing_snapshot = _listing_snapshot(root)
    _write_reviewed_correction_manifest(root)
    intervals = _assert_continuous(
        _native_complete_intervals(
            root,
            required_start=coverage_start,
            required_end=coverage_end,
        ),
        required_start=coverage_start,
        required_end=coverage_end,
    )
    _verify_required_viewer_evidence(
        root, required_start=coverage_start, required_end=coverage_end,
    )
    _verify_reviewed_correction_manifest(root)
    scale_evidence = verify_source_evidence_manifest(
        str(root), required_start=coverage_start, required_end=coverage_end,
    )
    evidence_paths = _evidence_paths(
        root,
        required_start=coverage_start,
        required_end=coverage_end,
    )
    entries = _body_entries(root, evidence_paths)
    _assert_listing_snapshot_stable(root, listing_snapshot)
    listing_entry = next(
        (
            item for item in entries
            if item["path"] == listing_snapshot["path"]
        ),
        None,
    )
    if listing_entry != {
        "path": listing_snapshot["path"],
        "content_length": listing_snapshot["content_length"],
        "sha256": listing_snapshot["sha256"],
    }:
        raise RuntimeError(
            "DART listed-corp snapshot/body evidence mismatch"
        )
    disclosure_observation_audit = _disclosure_observation_audit(
        root,
        evidence_paths=evidence_paths,
        required_start=coverage_start,
        required_end=coverage_end,
    )
    payload = {
        "schema_version": SCHEMA_VERSION,
        "complete": True,
        "coverage_start": coverage_start.isoformat(),
        "coverage_end": coverage_end.isoformat(),
        "coverage_intervals": [
            {"from": start.isoformat(), "to": end.isoformat()}
            for start, end in intervals
        ],
        "listing_snapshot": listing_snapshot,
        "body_count": len(entries),
        "body_digest": _body_digest(entries),
        "disclosure_observation_audit": disclosure_observation_audit,
        "cash_adjustment_scale_source_evidence": scale_evidence.metadata,
        "objects": entries,
    }
    rendered = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    destination = root / MANIFEST_RELATIVE_PATH
    destination.parent.mkdir(parents=True, exist_ok=True)
    file_descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{destination.name}.", dir=destination.parent
    )
    try:
        with os.fdopen(file_descriptor, "wb") as stream:
            stream.write(rendered)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary_name, destination)
    finally:
        if os.path.exists(temporary_name):
            os.unlink(temporary_name)
    return verify_snapshot_manifest(
        str(root),
        required_start=coverage_start,
        required_end=coverage_end,
    )


def verify_snapshot_manifest(
    base: str,
    *,
    required_start: date = DEFAULT_COVERAGE_START,
    required_end: date | None = None,
) -> VerifiedActionSnapshot:
    """Fail closed unless markers, coverage and every body hash agree."""
    root = Path(base).expanduser().resolve()
    manifest_path = root / MANIFEST_RELATIVE_PATH
    try:
        raw = manifest_path.read_bytes()
        payload = json.loads(raw)
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(
            f"missing/invalid DART action snapshot manifest: {manifest_path}"
        ) from exc
    canonical = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    if raw != canonical:
        raise RuntimeError("DART action snapshot manifest is not canonical")
    if payload.get("schema_version") != SCHEMA_VERSION:
        raise RuntimeError("unsupported DART action snapshot schema")
    if payload.get("complete") is not True:
        raise RuntimeError("DART action snapshot is not complete")
    listing_snapshot = _listing_snapshot(root)
    coverage_start = _date_value(
        payload.get("coverage_start"), field="coverage_start"
    )
    coverage_end = _date_value(payload.get("coverage_end"), field="coverage_end")
    expected_end = required_end or coverage_end
    if coverage_start != required_start:
        raise RuntimeError(
            "DART coverage start mismatch: "
            f"manifest={coverage_start} required={required_start}"
        )
    if required_end is not None and coverage_end != required_end:
        raise RuntimeError(
            "DART coverage end mismatch: "
            f"manifest={coverage_end} required={required_end}"
        )
    declared_intervals = tuple(
        (
            _date_value(item.get("from"), field="interval from"),
            _date_value(item.get("to"), field="interval to"),
        )
        for item in payload.get("coverage_intervals") or []
    )
    continuous = _assert_continuous(
        declared_intervals,
        required_start=required_start,
        required_end=expected_end,
    )
    native = set(_native_complete_intervals(
        root,
        required_start=coverage_start,
        required_end=coverage_end,
    ))
    if any(interval not in native for interval in declared_intervals):
        raise RuntimeError("snapshot manifest declares an unverified native interval")
    _verify_required_viewer_evidence(
        root, required_start=coverage_start, required_end=coverage_end,
    )
    _verify_reviewed_correction_manifest(root)
    scale_evidence = verify_source_evidence_manifest(
        str(root), required_start=coverage_start, required_end=coverage_end,
    )
    if payload.get("cash_adjustment_scale_source_evidence") != (
        scale_evidence.metadata
    ):
        raise RuntimeError(
            "cash-adjustment scale source evidence metadata changed"
        )

    declared_entries = payload.get("objects")
    if not isinstance(declared_entries, list):
        raise RuntimeError("DART snapshot objects must be a list")
    actual_paths = _evidence_paths(
        root, required_start=coverage_start, required_end=coverage_end,
    )
    disclosure_observation_audit = _disclosure_observation_audit(
        root,
        evidence_paths=actual_paths,
        required_start=coverage_start,
        required_end=coverage_end,
    )
    if payload.get("disclosure_observation_audit") != (
        disclosure_observation_audit
    ):
        raise RuntimeError(
            "DART disclosure observation canonicalization metadata changed"
        )
    actual_relative = [path.relative_to(root).as_posix() for path in actual_paths]
    declared_relative = [str(item.get("path") or "") for item in declared_entries]
    if declared_relative != actual_relative:
        raise RuntimeError("DART snapshot manifest/body path set changed")
    actual_entries = _body_entries(root, actual_paths)
    _assert_listing_snapshot_stable(root, listing_snapshot)
    if payload.get("listing_snapshot") != listing_snapshot:
        raise RuntimeError("DART listed-corp snapshot metadata changed")
    listing_entry = next(
        (
            item for item in actual_entries
            if item["path"] == listing_snapshot["path"]
        ),
        None,
    )
    if listing_entry != {
        "path": listing_snapshot["path"],
        "content_length": listing_snapshot["content_length"],
        "sha256": listing_snapshot["sha256"],
    }:
        raise RuntimeError(
            "DART listed-corp snapshot/body evidence mismatch"
        )
    if declared_entries != actual_entries:
        raise RuntimeError("DART snapshot body SHA/content length mismatch")
    digest = _body_digest(actual_entries)
    if payload.get("body_digest") != digest:
        raise RuntimeError("DART snapshot aggregate body digest mismatch")
    if int(payload.get("body_count", -1)) != len(actual_entries):
        raise RuntimeError("DART snapshot body count mismatch")
    return VerifiedActionSnapshot(
        base=str(root),
        manifest_path=str(manifest_path),
        manifest_sha256=hashlib.sha256(raw).hexdigest(),
        body_digest=digest,
        body_count=len(actual_entries),
        coverage_start=coverage_start,
        coverage_end=coverage_end,
        coverage_intervals=continuous,
        listing_snapshot=listing_snapshot,
        disclosure_observation_audit=disclosure_observation_audit,
        cash_adjustment_scale_source_evidence=scale_evidence.metadata,
    )


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", required=True)
    parser.add_argument("--coverage-end", type=date.fromisoformat, required=True)
    parser.add_argument(
        "--coverage-start",
        type=date.fromisoformat,
        default=DEFAULT_COVERAGE_START,
    )
    return parser.parse_args(argv)


def main() -> None:
    args = parse_args()
    verified = build_snapshot_manifest(
        args.base,
        coverage_start=args.coverage_start,
        coverage_end=args.coverage_end,
    )
    print(json.dumps(verified.__dict__, default=str, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
