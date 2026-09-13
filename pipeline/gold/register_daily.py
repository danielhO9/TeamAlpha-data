"""Register immutable daily Gold factor metadata as research candidates."""
from __future__ import annotations

import argparse
import json

from pipeline.common import db
from pipeline.gold.run import ROOT, implementation_hash, load_manifest


DESCRIPTIONS = {
    "market_leverage": "PIT liabilities divided by daily market equity",
    "operating_return_on_capital_employed": (
        "PIT TTM operating income divided by daily capital employed"
    ),
    "paid_in_capital_ratio": "PIT legal capital divided by PIT equity",
    "trading_turnover_20d": "daily trailing-20-session turnover",
    "return_kurtosis_24m": "daily-return kurtosis over 504 sessions",
    "turnover_volatility_12m": (
        "daily log-turnover volatility over 252 sessions"
    ),
}


def candidate_rows() -> list[dict]:
    rows = []
    for factor_key, spec in load_manifest().items():
        sql_path = ROOT / spec["sql"]
        rows.append({
            "factor_key": factor_key,
            "version": int(spec["version"]),
            "description": DESCRIPTIONS[factor_key],
            "implementation_uri": f"repo://TeamAlpha-data/{spec['sql']}",
            "implementation_hash": implementation_hash(sql_path),
            "config": {
                "frequency": spec["frequency"],
                "lookback": spec["lookback"],
                "predicted_sign": spec["predicted_sign"],
                "research_definition_hash": spec[
                    "research_definition_hash"
                ],
                "value_contract": {"id": spec["value_contract"]},
            },
        })
    return rows


def register(conn, *, apply: bool) -> int:
    created = 0
    try:
        with conn.cursor() as cur:
            for row in candidate_rows():
                cur.execute(
                    """
                    SELECT description, implementation_uri,
                           implementation_hash, config
                    FROM gold.factor
                    WHERE factor_key=%(factor_key)s AND version=%(version)s
                    """,
                    row,
                )
                existing = cur.fetchone()
                expected = (
                    row["description"],
                    row["implementation_uri"],
                    row["implementation_hash"],
                    row["config"],
                )
                if existing is not None:
                    if tuple(existing) != expected:
                        raise RuntimeError(
                            "daily factor version already exists with a "
                            f"different contract: {row['factor_key']}"
                        )
                    continue
                cur.execute(
                    """
                    INSERT INTO gold.factor(
                        factor_key, version, description,
                        implementation_uri, implementation_hash,
                        config, evaluation, status
                    ) VALUES (
                        %(factor_key)s, %(version)s, %(description)s,
                        %(implementation_uri)s, %(implementation_hash)s,
                        %(config)s::jsonb, '{}'::jsonb, 'CANDIDATE'
                    )
                    """,
                    {**row, "config": json.dumps(row["config"])},
                )
                created += 1
        if apply:
            conn.commit()
        else:
            conn.rollback()
        return created
    except Exception:
        conn.rollback()
        raise


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    conn = db.connect()
    try:
        created = register(conn, apply=args.apply)
    finally:
        conn.close()
    mode = "APPLY" if args.apply else "DRY-RUN/ROLLBACK"
    print(f"daily candidates={created} ({mode})")


if __name__ == "__main__":
    main()
