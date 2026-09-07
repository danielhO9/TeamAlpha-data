"""ECS에서 연구용 대체 입력의 Bronze 수집과 Silver 적재를 수행한다.

OpenDART 전체 재무제표·지분공시·현재 업종은 공식 API로 수집한다. KRX
투자자수급과 공매도 잔고는 이 명령이 웹에서 수집하지 않으며, 별도로 승인된
export를 Bronze에 등록한 뒤 ``silver`` phase에서만 적재한다.
"""
from __future__ import annotations

import argparse
import json
import os
import re

import boto3

from pipeline import alternative_data_incremental, dart_silver_backfill_ecs
from pipeline.bronze import (
    dart_company_profiles,
    dart_full_statements,
    dart_ownership,
)
from pipeline.silver import alternative_data
from pipeline.silver_quality import migrate


def _list_response_uris(bucket: str, prefix: str) -> list[str]:
    client = boto3.client("s3")
    uris: list[str] = []
    for page in client.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix=prefix,
    ):
        uris.extend(
            f"s3://{bucket}/{item['Key']}"
            for item in page.get("Contents", [])
            if item["Key"].endswith("/response.json")
            or (
                prefix in {"investor_flows/krx/", "short_balances/krx/"}
                and "/source." in item["Key"]
                and not item["Key"].endswith("/manifest.json")
            )
        )
    return sorted(set(uris))


def _list_legacy_full_statement_uris(bucket: str) -> list[str]:
    """List reusable list-shaped responses written by the older collector."""
    client = boto3.client("s3")
    pattern = re.compile(
        r"financials/dart_full/year=\d{4}/corp=[0-9A-Z]{6}/"
        r"(?:11011|11012|11013|11014)-(?:CFS|OFS)\.json$"
    )
    uris: list[str] = []
    for page in client.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix="financials/dart_full/",
    ):
        uris.extend(
            f"s3://{bucket}/{item['Key']}"
            for item in page.get("Contents", [])
            if pattern.search(item["Key"])
        )
    return sorted(set(uris))


def collect_full_statements(
    from_year: int,
    to_year: int,
    *,
    refresh_existing: bool = False,
    max_scopes: int | None = None,
) -> list[str]:
    return dart_full_statements.run(
        from_year,
        to_year,
        "s3",
        refresh_existing=refresh_existing,
        max_scopes=max_scopes,
    )


def collect_ownership(
    *,
    refresh_existing: bool = False,
    max_corps: int | None = None,
) -> list[str]:
    return dart_ownership.run(
        "s3",
        refresh_existing=refresh_existing,
        max_corps=max_corps,
    )


def collect_industries(
    *,
    refresh_existing: bool = False,
    max_corps: int | None = None,
) -> list[str]:
    return dart_company_profiles.run(
        "s3",
        refresh_existing=refresh_existing,
        max_corps=max_corps,
    )


def publish_existing() -> dict:
    bucket = os.environ.get("S3_BRONZE_BUCKET")
    if not bucket:
        raise SystemExit("S3_BRONZE_BUCKET is required")
    full_files = sorted(set(
        _list_response_uris(bucket, "financials/dart_statement_lines/")
        + _list_legacy_full_statement_uris(bucket)
    ))
    ownership_files = _list_response_uris(bucket, "ownership/dart/")
    investor_files = _list_response_uris(bucket, "investor_flows/krx/")
    industry_files = _list_response_uris(bucket, "company_profiles/dart/")
    short_balance_files = _list_response_uris(bucket, "short_balances/krx/")
    if not any((
        full_files,
        ownership_files,
        investor_files,
        industry_files,
        short_balance_files,
    )):
        raise RuntimeError("no alternative-input Bronze objects found")
    migrate.run()
    summary = alternative_data.publish_files(
        full_statement_files=full_files,
        ownership_files=ownership_files,
        investor_flow_files=investor_files,
        industry_files=industry_files,
        short_balance_files=short_balance_files,
    )
    alternative_data_incremental.mark_krx_published(
        investor_files + short_balance_files,
    )
    return summary


def publish_full_statement_batch(
    from_year: int,
    to_year: int,
    *,
    max_scopes: int,
) -> dict:
    """Collect and certify one bounded, recent-first initial-load batch."""
    lock = dart_silver_backfill_ecs.acquire_daily_certification_lock()
    try:
        migrate.assert_current(lock)
        files, remaining = dart_full_statements.run_bootstrap_batch(
            from_year,
            to_year,
            "s3",
            max_scopes=max_scopes,
        )
        published = {}
        if files:
            summary = alternative_data.publish_files(
                full_statement_files=files,
                conn=lock,
            )
            published = summary["published"]
        result = {
            "selected_scopes": len(files),
            "remaining_scopes": remaining,
            "published": published,
        }
        print(
            "[alternative-full-bootstrap] "
            + json.dumps(result, sort_keys=True),
            flush=True,
        )
        return result
    finally:
        dart_silver_backfill_ecs.release_daily_certification_lock(lock)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--phase",
        choices=(
            "bronze-full",
            "bronze-ownership",
            "bronze-industry",
            "full-statement-batch",
            "silver",
            "full",
        ),
        required=True,
    )
    parser.add_argument("--from", dest="from_year", type=int, default=2015)
    parser.add_argument("--to", dest="to_year", type=int)
    parser.add_argument("--max-scopes", type=int)
    parser.add_argument("--max-corps", type=int)
    parser.add_argument("--refresh-existing", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    to_year = args.to_year or args.from_year
    if args.phase == "full-statement-batch":
        publish_full_statement_batch(
            args.from_year,
            to_year,
            max_scopes=args.max_scopes or 4000,
        )
        return
    if args.phase in {"bronze-full", "full"}:
        collect_full_statements(
            args.from_year,
            to_year,
            refresh_existing=args.refresh_existing,
            max_scopes=args.max_scopes,
        )
    if args.phase in {"bronze-ownership", "full"}:
        collect_ownership(
            refresh_existing=args.refresh_existing,
            max_corps=args.max_corps,
        )
    if args.phase in {"bronze-industry", "full"}:
        collect_industries(
            refresh_existing=args.refresh_existing,
            max_corps=args.max_corps,
        )
    if args.phase in {"silver", "full"}:
        publish_existing()


if __name__ == "__main__":
    main()
