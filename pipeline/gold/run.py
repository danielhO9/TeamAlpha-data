"""Allowlisted Gold factor runner with implementation-contract checks."""
from __future__ import annotations

import argparse
import hashlib
import json
import re
from datetime import date, datetime
from pathlib import Path

from pipeline.common import db


ROOT = Path(__file__).resolve().parents[2]
MANIFEST_PATH = Path(__file__).with_name("factors") / "manifest.json"
VALUE_COLUMNS = ("asset_id", "as_of_date", "value", "rank")
ALLOWED_SILVER_RELATIONS = frozenset({
    "public.asset",
    "public.asset_identifier",
    "public.corporate_action",
    "public.dq_run",
    "public.fundamental",
    "public.price_daily",
})


def load_manifest() -> dict:
    return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))


def implementation_hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def validate_query_sql(sql: str) -> None:
    """Keep the allowlisted implementation reusable for read-only parity."""
    body = "\n".join(
        line for line in sql.splitlines() if not line.lstrip().startswith("--")
    )
    if re.search(r"\b(insert|update|delete|merge|truncate|alter|drop|create)\b", body, re.I):
        raise ValueError("factor SQL은 값을 반환하는 read-only query여야 합니다")
    for parameter in ("%(start_date)s", "%(end_date)s"):
        if parameter not in body:
            raise ValueError(f"factor SQL parameter가 없습니다: {parameter}")
    normalized = " ".join(body.lower().split())
    if "select asset_id, as_of_date, value, rank" not in normalized:
        raise ValueError(f"factor SQL은 {VALUE_COLUMNS}를 반환해야 합니다")
    if re.search(r'\b(?:from|join)\s+"', body, re.I):
        raise ValueError("factor SQL relation은 따옴표 없는 Silver allowlist 이름이어야 합니다")
    cte_names = {
        name.lower()
        for name in re.findall(
            r"(?:\bwith|,)\s*([a-z_][a-z0-9_]*)\s+as\s*\(", body, re.I,
        )
    }
    relations = {
        name.lower()
        for name in re.findall(
            r"\b(?:from|join)\s+([a-z_][a-z0-9_]*(?:\.[a-z_][a-z0-9_]*)?)",
            body,
            re.I,
        )
    }
    invalid = sorted(
        relation
        for relation in relations
        if relation != "lateral"
        and relation not in cte_names
        and relation not in ALLOWED_SILVER_RELATIONS
    )
    if invalid:
        raise ValueError(f"factor SQL은 인증 Silver relation만 읽을 수 있습니다: {invalid}")


def build_stage_sql(query_sql: str) -> str:
    """Materialize research-parity rows before replacing a live partition."""
    validate_query_sql(query_sql)
    query = query_sql.strip().removesuffix(";")
    return f"""
CREATE TEMP TABLE _gold_factor_values ON COMMIT DROP AS
{query}
"""


def build_replace_sql(query_sql: str) -> str:
    """Render the staged exact-replacement contract for review tooling."""
    return build_stage_sql(query_sql) + """
;
DELETE FROM gold.factor_value
WHERE factor_id = %(factor_id)s
  AND as_of_date BETWEEN %(start_date)s AND %(end_date)s;
INSERT INTO gold.factor_value (
    factor_id, asset_id, as_of_date, value, rank
)
SELECT %(factor_id)s, asset_id, as_of_date, value, rank
FROM _gold_factor_values
"""


# Compatibility for older read-only contract tests and imports.
build_upsert_sql = build_replace_sql


def _parse_date(value: date | str) -> date:
    if isinstance(value, date):
        return value
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise ValueError(f"invalid Gold date: {value!r}") from exc


def _load_factor(conn, factor_key: str, version: int) -> dict:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT factor_id, factor_key, version, status,
                   implementation_uri, implementation_hash, config
            FROM gold.factor
            WHERE factor_key = %s AND version = %s
            """,
            (factor_key, version),
        )
        row = cur.fetchone()
        if row is None:
            raise ValueError(
                f"Gold factor metadata가 없습니다: {factor_key} v{version}"
            )
        columns = [column.name for column in cur.description]
    return dict(zip(columns, row, strict=True))


def validate_contract(metadata: dict, spec: dict, sql_path: Path) -> None:
    if int(metadata["version"]) != int(spec["version"]):
        raise ValueError("factor version이 구현 manifest와 다릅니다")
    if metadata["status"] != "APPROVED":
        raise ValueError("APPROVED factor만 factor_value를 계산할 수 있습니다")
    uri = str(metadata["implementation_uri"])
    if not uri.endswith(spec["sql"]):
        raise ValueError(
            f"implementation_uri 불일치: expected *{spec['sql']}, observed {uri}"
        )
    if metadata["implementation_hash"] != implementation_hash(sql_path):
        raise ValueError("Gold SQL SHA-256이 게시 메타데이터와 다릅니다")
    config = metadata["config"]
    if int(config.get("predicted_sign", 0)) != int(spec["predicted_sign"]):
        raise ValueError("predicted_sign 계약이 구현 manifest와 다릅니다")
    value_contract = config.get("value_contract")
    contract_id = (
        value_contract.get("id")
        if isinstance(value_contract, dict)
        else value_contract
    )
    if contract_id != spec["value_contract"]:
        raise ValueError("value/rank 계약이 구현 manifest와 다릅니다")
    if config.get("research_definition_hash") != spec.get("research_definition_hash"):
        raise ValueError("research_definition_hash가 구현 manifest와 다릅니다")
    if config.get("frequency") != spec.get("frequency"):
        raise ValueError("frequency 계약이 구현 manifest와 다릅니다")
    validate_query_sql(sql_path.read_text(encoding="utf-8"))


def run_factor(
    conn,
    *,
    factor_key: str,
    start_date: date | str,
    end_date: date | str | None = None,
    apply: bool,
) -> int:
    start = _parse_date(start_date)
    end = _parse_date(end_date or start)
    if end < start:
        raise ValueError("Gold end_date precedes start_date")
    manifest = load_manifest()
    if factor_key not in manifest:
        raise ValueError(f"허용되지 않은 Gold 구현입니다: {factor_key}")
    spec = manifest[factor_key]
    sql_path = ROOT / spec["sql"]
    metadata = _load_factor(conn, factor_key, int(spec["version"]))
    validate_contract(metadata, spec, sql_path)
    try:
        with conn.cursor() as cur:
            params = {
                "factor_id": metadata["factor_id"],
                "start_date": start,
                "end_date": end,
            }
            cur.execute(
                build_stage_sql(sql_path.read_text(encoding="utf-8")),
                params,
            )
            cur.execute(
                """
                SELECT count(*), count(DISTINCT (asset_id, as_of_date)),
                       count(*) FILTER (
                           WHERE as_of_date < %(start_date)s
                              OR as_of_date > %(end_date)s
                              OR value::text IN ('NaN', 'Infinity', '-Infinity')
                              OR rank <= 0
                       )
                FROM _gold_factor_values
                """,
                params,
            )
            row_count, distinct_count, invalid_count = cur.fetchone()
            if row_count != distinct_count or invalid_count:
                raise ValueError(
                    "Gold daily candidate quality failed: "
                    f"rows={row_count}, distinct={distinct_count}, "
                    f"invalid={invalid_count}"
                )
            cur.execute(
                """
                DELETE FROM gold.factor_value
                WHERE factor_id = %(factor_id)s
                  AND as_of_date BETWEEN %(start_date)s AND %(end_date)s
                """,
                params,
            )
            cur.execute(
                """
                INSERT INTO gold.factor_value (
                    factor_id, asset_id, as_of_date, value, rank
                )
                SELECT %(factor_id)s, asset_id, as_of_date, value, rank
                FROM _gold_factor_values
                """,
                params,
            )
            affected = max(cur.rowcount, 0)
        if apply:
            conn.commit()
        else:
            conn.rollback()
        return affected
    except Exception:
        conn.rollback()
        raise


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--factor", required=True, choices=sorted(load_manifest()))
    scope = parser.add_mutually_exclusive_group(required=True)
    scope.add_argument(
        "--as-of-date",
        help="한 KRX 거래일 파티션(YYYY-MM-DD)",
    )
    scope.add_argument(
        "--from-date",
        help="백필 시작 거래일(YYYY-MM-DD, --to-date 필요)",
    )
    parser.add_argument("--to-date", help="백필 종료 거래일(YYYY-MM-DD)")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="생략하면 같은 SQL을 실행한 뒤 rollback하는 검증 모드",
    )
    args = parser.parse_args()
    if bool(args.from_date) != bool(args.to_date):
        parser.error("--from-date와 --to-date는 함께 지정해야 합니다")
    start_date = args.as_of_date or args.from_date
    end_date = args.as_of_date or args.to_date
    conn = db.connect()
    try:
        affected = run_factor(
            conn,
            factor_key=args.factor,
            start_date=start_date,
            end_date=end_date,
            apply=args.apply,
        )
    finally:
        conn.close()
    mode = "APPLY" if args.apply else "DRY-RUN/ROLLBACK"
    print(
        f"{args.factor} {start_date}..{end_date}: "
        f"{affected:,} rows ({mode})"
    )


if __name__ == "__main__":
    main()
