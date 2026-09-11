"""Plan every allowlisted Gold query against PostgreSQL without executing it."""
from __future__ import annotations

import argparse
from datetime import datetime

from pipeline.common import db
from pipeline.gold.run import ROOT, load_manifest, validate_query_sql


def validate_all(conn, as_of_date: str) -> None:
    target = datetime.strptime(as_of_date, "%Y-%m-%d").date()
    for factor_key, spec in load_manifest().items():
        sql_path = ROOT / spec["sql"]
        query = sql_path.read_text(encoding="utf-8")
        validate_query_sql(query)
        with conn.cursor() as cur:
            cur.execute(
                "EXPLAIN (FORMAT JSON) " + query.strip().removesuffix(";"),
                {"start_date": target, "end_date": target},
            )
            cur.fetchone()
        conn.rollback()
        print(f"[gold-validate] planned factor={factor_key}", flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--as-of-date", required=True)
    args = parser.parse_args()
    conn = db.connect()
    try:
        validate_all(conn, args.as_of_date)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
