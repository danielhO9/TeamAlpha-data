"""Replay a bounded sequence of missed daily partitions in one closed epoch."""
from __future__ import annotations

import argparse
import os
from datetime import datetime, timedelta

from pipeline import daily_full, dart_silver_backfill_ecs
from pipeline.silver_quality import freshness


def _weekdays(from_day: str, to_day: str) -> list[str]:
    start = datetime.strptime(from_day, "%Y%m%d").date()
    end = datetime.strptime(to_day, "%Y%m%d").date()
    if end < start:
        raise ValueError("gap replay end must not precede start")
    days: list[str] = []
    cursor = start
    while cursor <= end:
        if cursor.weekday() < 5:
            days.append(cursor.strftime("%Y%m%d"))
        cursor += timedelta(days=1)
    if not days:
        raise ValueError("gap replay range contains no weekdays")
    return days


def run(
    from_day: str,
    to_day: str,
    *,
    resume_building: bool = False,
) -> None:
    """Replay weekdays sequentially and certify total return on the last day.

    The contract must be healthy at entry.  Once the first raw partition
    invalidates it, the same PostgreSQL session lock fences every subsequent
    partition until the final rebuild, audit, and freshness check complete.
    """
    days = _weekdays(from_day, to_day)
    today_kst = datetime.now(daily_full.KST).date()
    if datetime.strptime(days[-1], "%Y%m%d").date() >= today_kst:
        raise ValueError("gap replay may only target completed calendar days")

    lock = dart_silver_backfill_ecs.acquire_daily_certification_lock()
    previous_override = os.environ.get("PIPELINE_DATE")
    try:
        if not dart_silver_backfill_ecs.total_return_contract_ready(conn=lock):
            if not resume_building:
                raise RuntimeError(
                    "gap replay requires a CERTIFIED total-return contract at "
                    "entry; repair existing BUILDING coverage first"
                )
            report = freshness.total_return_contract_report(lock)
            last_price_day = (
                dart_silver_backfill_ecs.certified_krx_price_coverage_end(
                    conn=lock,
                )
            )
            first_replay_day = datetime.strptime(days[0], "%Y%m%d").date()
            if (
                report.get("status") != "BUILDING"
                or report.get("coverage_end") != last_price_day.isoformat()
                or first_replay_day <= last_price_day
            ):
                raise RuntimeError(
                    "gap replay cannot safely resume the existing BUILDING "
                    f"contract: report={report} first={first_replay_day}"
                )
            print(
                "[gap-replay] resuming fenced BUILDING contract "
                f"coverage_end={last_price_day.isoformat()}",
                flush=True,
            )
        for index, day in enumerate(days):
            final = index == len(days) - 1
            os.environ["PIPELINE_DATE"] = day
            print(
                f"[gap-replay] start day={day} "
                f"position={index + 1}/{len(days)} final={final}",
                flush=True,
            )
            daily_full._main_locked(
                lock,
                allow_deferred_total_return=True,
                # Intermediate runs use the collector's exact bounded action
                # evidence for price DQ and publication. The final run alone
                # expands to the complete certified history and rebuilds TR.
                prepare_total_return=final,
                preview_total_return=final,
                close_total_return=final,
                assert_final_freshness=final,
                collect_financials=True,
                full_year_financial_snapshot=False,
                bounded_action_scope=not final,
                collect_alternative=final,
            )
            print(f"[gap-replay] complete day={day}", flush=True)
    finally:
        if previous_override is None:
            os.environ.pop("PIPELINE_DATE", None)
        else:
            os.environ["PIPELINE_DATE"] = previous_override
        dart_silver_backfill_ecs.release_daily_certification_lock(lock)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--from", dest="from_day", required=True)
    parser.add_argument("--to", dest="to_day", required=True)
    parser.add_argument("--resume-building", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run(
        args.from_day,
        args.to_day,
        resume_building=args.resume_building,
    )


if __name__ == "__main__":
    main()
