"""품질 규칙 실행과 gate 판정."""
from __future__ import annotations

from collections import Counter

from pipeline.silver_quality.models import CandidateBundle, CheckResult, QualityGateError
from pipeline.silver_quality.registry import run_registered_rules


def evaluate(
    bundle: CandidateBundle,
    *,
    target_date=None,
    history=None,
    partition_key: str | None = None,
    changed_action_receipts: set[str] | None = None,
) -> list[CheckResult]:
    return run_registered_rules(
        bundle,
        target_date=target_date,
        history=history,
        partition_key=partition_key,
        changed_action_receipts=changed_action_receipts,
    )


def assert_publishable(results: list[CheckResult]) -> None:
    blocking = [r for r in results if r.blocks_publish]
    if blocking:
        raise QualityGateError(results)


def print_summary(results: list[CheckResult]) -> None:
    counts = Counter((r.status.value, r.severity.value) for r in results)
    rendered = ", ".join(
        f"{status}/{severity}={count}"
        for (status, severity), count in sorted(counts.items())
    )
    print(f"[silver-quality] {rendered}", flush=True)
    for item in results:
        if item.failed_count:
            print(
                f"[silver-quality] {item.severity.value} {item.rule_code} "
                f"dataset={item.dataset} failed={item.failed_count} actual={item.actual}",
                flush=True,
            )
