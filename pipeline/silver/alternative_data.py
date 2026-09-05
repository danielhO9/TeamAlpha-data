"""전체 재무·지분공시·승인된 투자자수급을 원자적으로 Silver에 적재한다.

각 Bronze 원문은 먼저 메모리 후보로 변환한다. 모든 자연키가 기존 Silver 자산에
정확히 매핑되고 입력 계약을 통과한 경우에만 요청 데이터셋을 한 transaction으로
upsert한다. 하나라도 실패하면 이번 run의 변경 전체를 rollback한다.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from collections.abc import Iterable

import pandas as pd

from pipeline.common import db
from pipeline.silver import (
    full_statements,
    industry_classifications,
    investor_flows,
    ownership,
    short_balances,
)
from pipeline.silver_quality import migrate, repository
from pipeline.silver_quality.models import CheckResult, CheckStatus, Severity

MODE = "alternative_research_inputs"


def _fingerprint(groups: dict[str, list[str]]) -> str:
    payload = json.dumps(
        {name: sorted(set(files)) for name, files in sorted(groups.items())},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _asset_map(
    conn,
    natural_keys: Iterable[str],
    *,
    source: str,
    identifier_type: str,
) -> dict[str, int]:
    keys = sorted({str(value) for value in natural_keys})
    if not keys:
        return {}
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT identifier, asset_id
            FROM asset_identifier
            WHERE source=%s
              AND identifier_type=%s
              AND valid_to IS NULL
              AND identifier = ANY(%s)
            """,
            (source, identifier_type, keys),
        )
        rows = cur.fetchall()
    mapping = {str(identifier): int(asset_id) for identifier, asset_id in rows}
    missing = sorted(set(keys) - set(mapping))
    if missing:
        raise RuntimeError(
            "Silver asset mapping is incomplete: "
            f"source={source}, identifier_type={identifier_type}, "
            f"missing_count={len(missing)}, sample={missing[:10]}"
        )
    return mapping


def _result(
    *,
    code: str,
    dataset: str,
    passed: bool,
    expected: str,
    actual: str,
    samples: list[dict] | None = None,
) -> CheckResult:
    return CheckResult(
        rule_code=code,
        dataset=dataset,
        severity=Severity.ERROR,
        status=CheckStatus.PASS if passed else CheckStatus.FAIL,
        expected=expected,
        actual=actual,
        failed_count=0 if passed else 1,
        samples=samples or [],
    )


def _transform_checks(
    frames: dict[str, pd.DataFrame],
    stats: dict[str, dict],
    requested: dict[str, list[str]],
) -> list[CheckResult]:
    results: list[CheckResult] = []
    for name, files in requested.items():
        frame = frames[name]
        dataset_stats = stats[name]
        results.append(_result(
            code="ALTERNATIVE_INPUT_FILES_ACCOUNTED",
            dataset=name,
            passed=int(dataset_stats["file_count"]) == len(set(files)),
            expected=f"file_count={len(set(files))}",
            actual=f"file_count={dataset_stats['file_count']}",
        ))
        rejected = int(dataset_stats.get("rejected_rows", 0))
        results.append(_result(
            code="ALTERNATIVE_INPUT_NO_REJECTED_ROWS",
            dataset=name,
            passed=rejected == 0,
            expected="rejected_rows=0",
            actual=f"rejected_rows={rejected}",
        ))
        results.append(_result(
            code="ALTERNATIVE_INPUT_ROW_ACCOUNTING",
            dataset=name,
            passed=len(frame) <= int(dataset_stats["input_rows"]),
            expected="transformed_rows <= input_rows",
            actual=(
                f"transformed_rows={len(frame)}, "
                f"input_rows={dataset_stats['input_rows']}"
            ),
        ))
    return results


def _save_metrics(conn, run_id, frames: dict[str, pd.DataFrame]) -> None:
    with conn.cursor() as cur:
        for dataset, frame in frames.items():
            cur.execute(
                """
                INSERT INTO dq_metric(
                    run_id, dataset_name, metric_name, dimension, metric_value
                ) VALUES (%s,%s,'row_count','{}'::jsonb,%s)
                """,
                (run_id, dataset, len(frame)),
            )


def publish_files(
    *,
    full_statement_files: list[str] | None = None,
    ownership_files: list[str] | None = None,
    investor_flow_files: list[str] | None = None,
    industry_files: list[str] | None = None,
    short_balance_files: list[str] | None = None,
    conn=None,
) -> dict:
    """Transform and atomically publish the explicitly supplied Bronze files."""
    requested = {
        "fundamental_statement_line": sorted(set(full_statement_files or [])),
        "ownership_disclosure_event": sorted(set(ownership_files or [])),
        "investor_flow_daily": sorted(set(investor_flow_files or [])),
        "industry_classification_observation": sorted(set(industry_files or [])),
        "short_position_balance_observation": sorted(set(short_balance_files or [])),
    }
    requested = {name: files for name, files in requested.items() if files}
    if not requested:
        raise ValueError("at least one Bronze input file is required")

    owns_connection = conn is None
    connection = conn or db.connect()
    context = None
    results: list[CheckResult] = []
    try:
        migrate.assert_current(connection)
        repository.assert_schema(connection)
        context = repository.start_run(
            connection,
            mode=MODE,
            status="RUNNING",
            input_fingerprint=_fingerprint(requested),
        )
        frames: dict[str, pd.DataFrame] = {}
        stats: dict[str, dict] = {}
        if "fundamental_statement_line" in requested:
            frames["fundamental_statement_line"], stats["fundamental_statement_line"] = (
                full_statements.prepare(files=requested["fundamental_statement_line"])
            )
        if "ownership_disclosure_event" in requested:
            frames["ownership_disclosure_event"], stats["ownership_disclosure_event"] = (
                ownership.prepare(files=requested["ownership_disclosure_event"])
            )
        if "investor_flow_daily" in requested:
            frames["investor_flow_daily"], stats["investor_flow_daily"] = (
                investor_flows.prepare(files=requested["investor_flow_daily"])
            )
        if "industry_classification_observation" in requested:
            (
                frames["industry_classification_observation"],
                stats["industry_classification_observation"],
            ) = industry_classifications.prepare(
                files=requested["industry_classification_observation"],
            )
        if "short_position_balance_observation" in requested:
            (
                frames["short_position_balance_observation"],
                stats["short_position_balance_observation"],
            ) = short_balances.prepare(
                files=requested["short_position_balance_observation"],
            )
        results = _transform_checks(frames, stats, requested)
        blocking = [result for result in results if result.blocks_publish]
        if blocking:
            raise RuntimeError(
                "alternative input quality gate failed: "
                + ", ".join(result.rule_code for result in blocking)
            )

        ticker_keys: set[str] = set()
        for name in (
            "fundamental_statement_line",
            "investor_flow_daily",
            "short_position_balance_observation",
        ):
            frame = frames.get(name)
            if frame is not None and not frame.empty:
                ticker_keys.update(frame["natural_key"].astype(str))
        ticker_map = _asset_map(
            connection, ticker_keys, source="KRX", identifier_type="ticker",
        )
        ownership_frame = frames.get("ownership_disclosure_event")
        industry_frame = frames.get("industry_classification_observation")
        corp_keys: set[str] = set()
        for frame in (ownership_frame, industry_frame):
            if frame is not None and not frame.empty:
                corp_keys.update(frame["natural_key"].astype(str))
        corp_map = _asset_map(
            connection,
            corp_keys,
            source="DART",
            identifier_type="corp_code",
        )
        connection.commit()

        published = {
            "fundamental_statement_line": 0,
            "ownership_disclosure_event": 0,
            "investor_flow_daily": 0,
            "industry_classification_observation": 0,
            "short_position_balance_observation": 0,
        }
        with connection.transaction():
            if "fundamental_statement_line" in frames:
                published["fundamental_statement_line"] = full_statements.publish(
                    connection, frames["fundamental_statement_line"], ticker_map,
                    context.run_id,
                )
            if "ownership_disclosure_event" in frames:
                published["ownership_disclosure_event"] = ownership.publish(
                    connection, frames["ownership_disclosure_event"], corp_map,
                    context.run_id,
                )
            if "investor_flow_daily" in frames:
                published["investor_flow_daily"] = investor_flows.publish(
                    connection, frames["investor_flow_daily"], ticker_map,
                    context.run_id,
                )
            if "industry_classification_observation" in frames:
                published["industry_classification_observation"] = (
                    industry_classifications.publish(
                        connection,
                        frames["industry_classification_observation"],
                        corp_map,
                        context.run_id,
                    )
                )
            if "short_position_balance_observation" in frames:
                published["short_position_balance_observation"] = (
                    short_balances.publish(
                        connection,
                        frames["short_position_balance_observation"],
                        ticker_map,
                        context.run_id,
                    )
                )
            _save_metrics(connection, context.run_id, frames)
            repository.finish_run(
                connection, context, "CERTIFIED", results, commit=False,
            )
        summary = {
            "run_id": str(context.run_id),
            "status": "CERTIFIED",
            "published": published,
            "stats": stats,
        }
        print(
            "[alternative-data] "
            + json.dumps(summary, ensure_ascii=False, sort_keys=True),
            flush=True,
        )
        return summary
    except Exception as exc:
        connection.rollback()
        if context is not None:
            failure = _result(
                code="ALTERNATIVE_INPUT_ATOMIC_PUBLISH",
                dataset="alternative_research_inputs",
                passed=False,
                expected="all requested datasets publish atomically",
                actual=f"{type(exc).__name__}: {exc}",
            )
            repository.finish_run(
                connection,
                context,
                "FAILED",
                results + [failure],
                error_message=str(exc),
            )
        raise
    finally:
        if owns_connection:
            connection.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--full-statement-file", action="append")
    parser.add_argument("--ownership-file", action="append")
    parser.add_argument("--investor-flow-file", action="append")
    parser.add_argument("--industry-file", action="append")
    parser.add_argument("--short-balance-file", action="append")
    args = parser.parse_args()
    publish_files(
        full_statement_files=args.full_statement_file,
        ownership_files=args.ownership_file,
        investor_flow_files=args.investor_flow_file,
        industry_files=args.industry_file,
        short_balance_files=args.short_balance_file,
    )


if __name__ == "__main__":
    main()
