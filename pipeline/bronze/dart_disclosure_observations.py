"""Deterministic reconciliation for overlapping OpenDART list snapshots.

The OpenDART list endpoint is not immutable: later observations can add a
correction marker or update display-only issuer metadata for an already-issued
receipt.  Economic/identity fields must never drift, while the documented
display fields below are canonicalized to the observation with the latest
explicit manifest coverage end.  Every source manifest remains content-
addressed by the enclosing action snapshot.
"""
from __future__ import annotations

import hashlib
import json
import re
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


_MUTABLE_LIST_FIELDS = frozenset({"corp_cls", "corp_name", "flr_nm", "rm"})
_RECEIPT_PATTERN = re.compile(r"^[0-9]{14}$")
_INTERVAL_PATTERN = re.compile(r"^[0-9]{8}$")
_REPORT_STATUS_PREFIX = re.compile(r"^(?:\[정정명령부과\]\s*)+")


def _immutable_comparison_value(field: str, value: object) -> object:
    """Ignore only DART's retrospective correction-order display marker."""
    if field == "report_nm" and isinstance(value, str):
        return _REPORT_STATUS_PREFIX.sub("", value)
    return value


def immutable_disclosure_changes(left: dict, right: dict) -> tuple[str, ...]:
    """Return changed fields that OpenDART does not document as mutable."""
    fields = set(left) | set(right)
    return tuple(sorted(
        key for key in fields
        if (
            key not in _MUTABLE_LIST_FIELDS
            and _immutable_comparison_value(key, left.get(key))
            != _immutable_comparison_value(key, right.get(key))
        )
    ))


@dataclass(frozen=True)
class DisclosureObservation:
    path: str
    coverage_start: str
    coverage_end: str
    row: dict


def _observation(path: str | Path, row: dict) -> DisclosureObservation:
    resolved = Path(path)
    coverage_end = resolved.parent.name.removeprefix("to=")
    coverage_start = resolved.parent.parent.name.removeprefix("from=")
    if not (
        _INTERVAL_PATTERN.fullmatch(coverage_start)
        and _INTERVAL_PATTERN.fullmatch(coverage_end)
        and coverage_start <= coverage_end
    ):
        raise RuntimeError(f"invalid DART manifest interval path: {path}")
    return DisclosureObservation(
        path=str(path),
        coverage_start=coverage_start,
        coverage_end=coverage_end,
        row=row,
    )


def canonicalize_disclosures(
    rows: Iterable[tuple[str | Path, dict]],
    *,
    audit_root: str | Path | None = None,
) -> tuple[dict[str, tuple[str, dict]], dict[str, object]]:
    """Return one deterministic row per receipt plus overlap audit metadata.

    A receipt's identity/economic payload is immutable.  Only OpenDART list
    display fields known to change retrospectively may differ.  Those fields
    use the observation with the lexicographically latest explicit manifest
    interval ``(to=YYYYMMDD, from=YYYYMMDD)``.  If two observations have the
    same interval but disagree, there is no content-derived ordering and the
    snapshot fails closed.
    """
    root = Path(audit_root).resolve() if audit_root is not None else None

    def audit_path(value: str) -> str:
        if root is None:
            return value
        try:
            return Path(value).resolve().relative_to(root).as_posix()
        except ValueError:
            raise RuntimeError(
                f"DART disclosure manifest escaped snapshot root: {value}"
            ) from None

    grouped: dict[str, list[DisclosureObservation]] = {}
    observation_count = 0
    for path, row in rows:
        if not isinstance(row, dict):
            continue
        receipt = str(row.get("rcept_no") or "").strip()
        if not receipt:
            continue
        if _RECEIPT_PATTERN.fullmatch(receipt) is None:
            raise RuntimeError(f"invalid DART disclosure receipt: {receipt!r}")
        grouped.setdefault(receipt, []).append(_observation(path, row))
        observation_count += 1

    canonical: dict[str, tuple[str, dict]] = {}
    conflict_fields: Counter[str] = Counter()
    conflict_receipts: list[dict[str, object]] = []
    duplicate_receipts = 0
    for receipt, observations in sorted(grouped.items()):
        if len(observations) > 1:
            duplicate_receipts += 1
        all_fields = sorted({key for item in observations for key in item.row})
        changed = {
            key
            for key in all_fields
            if len({
                json.dumps(
                    item.row.get(key), ensure_ascii=False, sort_keys=True,
                    separators=(",", ":"), default=str,
                )
                for item in observations
            }) > 1
        }
        immutable_changes = sorted(
            key for key in changed - _MUTABLE_LIST_FIELDS
            if len({
                json.dumps(
                    _immutable_comparison_value(key, item.row.get(key)),
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                    default=str,
                )
                for item in observations
            }) > 1
        )
        if immutable_changes:
            raise RuntimeError(
                "conflicting immutable DART disclosure payloads for receipt: "
                f"{receipt} fields={immutable_changes} "
                f"paths={[audit_path(item.path) for item in observations]}"
            )

        latest_end = max(item.coverage_end for item in observations)
        latest = [
            item for item in observations if item.coverage_end == latest_end
        ]
        latest_start = max(item.coverage_start for item in latest)
        latest = [
            item for item in latest
            if item.coverage_start == latest_start
        ]
        latest_payloads = {
            json.dumps(
                item.row, ensure_ascii=False, sort_keys=True,
                separators=(",", ":"), default=str,
            )
            for item in latest
        }
        if len(latest_payloads) > 1:
            raise RuntimeError(
                "conflicting DART disclosure payloads have the same latest "
                f"interval: receipt={receipt} coverage_start={latest_start} "
                f"coverage_end={latest_end} "
                f"paths={[audit_path(item.path) for item in latest]}"
            )
        # Equivalent latest observations may come from multiple overlapping
        # windows.  Pick a stable path only after proving their payload equal.
        selected = max(
            latest,
            key=lambda item: (item.coverage_start, item.path),
        )
        canonical[receipt] = (selected.path, selected.row)
        if changed:
            conflict_fields.update(changed)
            conflict_receipts.append({
                "receipt_no": receipt,
                "changed_fields": sorted(changed),
                "selected_coverage_start": selected.coverage_start,
                "selected_coverage_end": selected.coverage_end,
                "selected_path": audit_path(selected.path),
                "observation_paths": sorted(
                    audit_path(item.path) for item in observations
                ),
            })

    conflict_payload = json.dumps(
        conflict_receipts,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    audit: dict[str, object] = {
        "contract": "latest_manifest_interval_mutable_list_fields_v3",
        "mutable_fields": sorted(_MUTABLE_LIST_FIELDS),
        "conditional_mutable_fields": {
            "report_nm": "leading_[정정명령부과]_display_marker_only",
        },
        "observation_count": observation_count,
        "unique_receipt_count": len(canonical),
        "duplicate_receipt_count": duplicate_receipts,
        "mutable_conflict_receipt_count": len(conflict_receipts),
        "mutable_conflict_field_counts": dict(sorted(conflict_fields.items())),
        "mutable_conflict_digest": hashlib.sha256(conflict_payload).hexdigest(),
        "mutable_conflict_samples": conflict_receipts[:20],
    }
    return canonical, audit
