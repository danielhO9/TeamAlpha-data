"""silver(PostgreSQL/RDS) 접속 + 대량 upsert 헬퍼."""
from __future__ import annotations

import os

import psycopg


def connect():
    url = os.environ.get("SILVER_DB_URL")
    if not url:
        raise SystemExit("SILVER_DB_URL 환경변수가 없습니다 (.env)")
    return psycopg.connect(url)


def upsert(conn, table: str, columns: list[str], rows: list[tuple],
           conflict: list[str], update: list[str]) -> int:
    """rows → 임시테이블 COPY → INSERT ... ON CONFLICT DO UPDATE. 대량에 빠름. 반환=입력행수."""
    rows = list(rows)
    if not rows:
        return 0
    cols = ", ".join(columns)
    with conn.cursor() as cur:
        cur.execute(f"CREATE TEMP TABLE _stg (LIKE {table}) ON COMMIT DROP")
        with cur.copy(f"COPY _stg ({cols}) FROM STDIN") as cp:
            for r in rows:
                cp.write_row(r)
        if update:
            action = "DO UPDATE SET " + ", ".join(f"{c}=EXCLUDED.{c}" for c in update)
        else:
            action = "DO NOTHING"
        cur.execute(
            f"INSERT INTO {table} ({cols}) SELECT {cols} FROM _stg "
            f"ON CONFLICT ({', '.join(conflict)}) {action}"
        )
    conn.commit()
    return len(rows)
