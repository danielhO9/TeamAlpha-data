"""품질 검사에서 공유하는 타입."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from enum import StrEnum
from typing import Any
from uuid import UUID

import pandas as pd


class Severity(StrEnum):
    CRITICAL = "CRITICAL"  # publish 차단
    ERROR = "ERROR"        # publish 차단
    WARNING = "WARNING"    # 비차단, 사람이 검토
    MODIFIED = "MODIFIED"  # 비차단, 파이프라인이 값을 변경한 곳 기록


class CheckStatus(StrEnum):
    PASS = "PASS"
    FAIL = "FAIL"


@dataclass(frozen=True)
class BatchContext:
    run_id: UUID
    mode: str
    target_date: date | None = None
    parent_run_id: UUID | None = None
    partition_key: str | None = None
    input_fingerprint: str | None = None


@dataclass
class CandidateBundle:
    assets: pd.DataFrame = field(default_factory=pd.DataFrame)
    identifiers: pd.DataFrame = field(default_factory=pd.DataFrame)
    prices: pd.DataFrame = field(default_factory=pd.DataFrame)
    fundamentals: pd.DataFrame = field(default_factory=pd.DataFrame)
    actions: pd.DataFrame = field(default_factory=pd.DataFrame)
    stats: dict[str, Any] = field(default_factory=dict)
    research_observations: list[dict] = field(default_factory=list)


@dataclass(frozen=True)
class CheckResult:
    rule_code: str
    dataset: str
    severity: Severity
    status: CheckStatus
    expected: str
    actual: str
    failed_count: int = 0
    samples: list[dict[str, Any]] = field(default_factory=list)
    partition_key: str | None = None

    @property
    def blocks_publish(self) -> bool:
        return (
            self.status == CheckStatus.FAIL
            and self.severity in {Severity.CRITICAL, Severity.ERROR}
        )


class QualityGateError(RuntimeError):
    def __init__(self, results: list[CheckResult]):
        self.results = results
        blocking = [r for r in results if r.blocks_publish]
        summary = ", ".join(
            f"{r.rule_code}({r.failed_count})" for r in blocking[:10]
        )
        super().__init__(f"Silver quality gate failed: {summary}")
