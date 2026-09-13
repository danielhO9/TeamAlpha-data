"""Atomically approve the registered daily Gold factor definitions."""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone

from pipeline.common import db
from pipeline.gold.run import ROOT, implementation_hash, load_manifest


def promote(conn, *, approved_by: str, apply: bool) -> int:
    if not approved_by.strip():
        raise ValueError("approved_by must not be empty")
    manifest = load_manifest()
    promoted = 0
    try:
        with conn.cursor() as cur:
            for factor_key, spec in manifest.items():
                cur.execute(
                    """
                    SELECT factor_id, status, implementation_uri,
                           implementation_hash, config
                    FROM gold.factor
                    WHERE factor_key = %s AND version = %s
                    FOR UPDATE
                    """,
                    (factor_key, int(spec["version"])),
                )
                row = cur.fetchone()
                if row is None:
                    raise RuntimeError(
                        f"daily candidate is not registered: {factor_key}"
                    )
                factor_id, status, uri, observed_hash, config = row
                sql_path = ROOT / spec["sql"]
                if not str(uri).endswith(spec["sql"]):
                    raise RuntimeError(f"implementation URI drift: {factor_key}")
                if observed_hash != implementation_hash(sql_path):
                    raise RuntimeError(f"implementation hash drift: {factor_key}")
                if config.get("frequency") != "daily":
                    raise RuntimeError(f"frequency is not daily: {factor_key}")
                if status == "APPROVED":
                    continue
                if status != "CANDIDATE":
                    raise RuntimeError(
                        f"cannot promote {factor_key} from status {status}"
                    )
                superseded = json.dumps({
                    "passed": False,
                    "verdict": "SUPERSEDED",
                    "superseded_by_version": int(spec["version"]),
                    "reason": "feature-safe daily implementation replacement",
                })
                cur.execute(
                    """
                    UPDATE gold.factor
                    SET evaluation = evaluation || %s::jsonb,
                        status = 'REJECTED'
                    WHERE factor_key = %s
                      AND version < %s
                      AND status = 'CANDIDATE'
                    """,
                    (superseded, factor_key, int(spec["version"])),
                )
                cur.execute(
                    """
                    UPDATE gold.factor
                    SET status = 'RETIRED'
                    WHERE factor_key = %s
                      AND status = 'APPROVED'
                      AND factor_id <> %s
                    """,
                    (factor_key, factor_id),
                )
                evaluation = {
                    "passed": True,
                    "verdict": "APPROVED",
                    "approved_by": approved_by,
                    "approved_at": datetime.now(timezone.utc).isoformat(),
                    "approval_scope": "daily_operational_migration",
                }
                cur.execute(
                    """
                    UPDATE gold.factor
                    SET evaluation = evaluation || %s::jsonb,
                        status = 'APPROVED'
                    WHERE factor_id = %s
                    """,
                    (json.dumps(evaluation), factor_id),
                )
                promoted += 1
        if apply:
            conn.commit()
        else:
            conn.rollback()
        return promoted
    except Exception:
        conn.rollback()
        raise


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--approved-by", required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    conn = db.connect()
    try:
        promoted = promote(
            conn,
            approved_by=args.approved_by,
            apply=args.apply,
        )
    finally:
        conn.close()
    mode = "APPLY" if args.apply else "DRY-RUN/ROLLBACK"
    print(f"daily promoted={promoted} ({mode})")


if __name__ == "__main__":
    main()
