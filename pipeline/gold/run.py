"""Allowlisted Gold factor runner with implementation-contract checks."""
from __future__ import annotations

import argparse
import hashlib
import json
import re
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
    "public.factor_price_feature_daily",
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
    for parameter in ("%(start_month)s", "%(end_month)s"):
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


def build_upsert_sql(query_sql: str) -> str:
    """Wrap the exact read-only implementation used by research parity."""
    validate_query_sql(query_sql)
    query = query_sql.strip().removesuffix(";")
    return f"""
WITH factor_values AS (
{query}
)
INSERT INTO gold.factor_value (
    factor_id, asset_id, as_of_date, value, rank
)
SELECT %(factor_id)s, asset_id, as_of_date, value, rank
FROM factor_values
ON CONFLICT (factor_id, asset_id, as_of_date)
DO UPDATE SET value = EXCLUDED.value, rank = EXCLUDED.rank
"""


def _load_factor(conn, factor_key: str) -> dict:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT factor_id, factor_key, version, status,
                   implementation_uri, implementation_hash, config
            FROM gold.factor
            WHERE factor_key = %s
            ORDER BY version DESC
            LIMIT 1
            """,
            (factor_key,),
        )
        row = cur.fetchone()
        if row is None:
            raise ValueError(f"Gold factor metadata가 없습니다: {factor_key}")
        columns = [column.name for column in cur.description]
    return dict(zip(columns, row, strict=True))


def validate_contract(metadata: dict, spec: dict, sql_path: Path) -> None:
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
    validate_query_sql(sql_path.read_text(encoding="utf-8"))


def run_factor(
    conn,
    *,
    factor_key: str,
    as_of_month: str,
    apply: bool,
) -> int:
    manifest = load_manifest()
    if factor_key not in manifest:
        raise ValueError(f"허용되지 않은 Gold 구현입니다: {factor_key}")
    spec = manifest[factor_key]
    sql_path = ROOT / spec["sql"]
    metadata = _load_factor(conn, factor_key)
    validate_contract(metadata, spec, sql_path)
    try:
        with conn.cursor() as cur:
            cur.execute(build_upsert_sql(sql_path.read_text(encoding="utf-8")), {
                "factor_id": metadata["factor_id"],
                "start_month": f"{as_of_month}-01",
                "end_month": f"{as_of_month}-01",
            })
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
    parser.add_argument("--as-of-month", required=True, help="고정 signal month (YYYY-MM)")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="생략하면 같은 SQL을 실행한 뒤 rollback하는 검증 모드",
    )
    args = parser.parse_args()
    conn = db.connect()
    try:
        affected = run_factor(
            conn,
            factor_key=args.factor,
            as_of_month=args.as_of_month,
            apply=args.apply,
        )
    finally:
        conn.close()
    mode = "APPLY" if args.apply else "DRY-RUN/ROLLBACK"
    print(f"{args.factor} {args.as_of_month}: {affected:,} rows ({mode})")


if __name__ == "__main__":
    main()
