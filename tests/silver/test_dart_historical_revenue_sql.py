"""Real SQL against an isolated TCP-disabled disposable PostgreSQL only."""
from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import shlex
import shutil
import subprocess
import tempfile

import psycopg
import pytest

from pipeline.silver import dart_historical_revenue as m

spec = importlib.util.spec_from_file_location("_document_revenue_sql_fixtures", Path(__file__).with_name("test_dart_historical_revenue.py"))
fixtures = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fixtures)
SCHEMA = "document_revenue_synthetic_only"


@pytest.fixture(scope="module")
def local_pg(tmp_path_factory):
    initdb, pg_ctl = shutil.which("initdb"), shutil.which("pg_ctl")
    if not initdb or not pg_ctl:
        pytest.skip("local PostgreSQL binaries unavailable; no external fallback")
    work = tmp_path_factory.mktemp("document_source_sql")
    data = work / "data"
    with tempfile.TemporaryDirectory(prefix="drevpg-", dir="/tmp") as socket:
        subprocess.run([initdb, "-D", str(data), "-A", "trust", "--no-locale", "-E", "UTF8"],
                       check=True, capture_output=True, text=True)
        options = shlex.join(["-h", "", "-k", socket, "-p", "6547", "-F"])
        subprocess.run([pg_ctl, "-D", str(data), "-l", str(work / "postgres.log"), "-o", options, "-w", "start"],
                       check=True, capture_output=True, text=True)
        try:
            with psycopg.connect(host=socket, port=6547, dbname="postgres", autocommit=True) as conn:
                conn.execute(f"CREATE SCHEMA {SCHEMA}")
                conn.execute(f"SET search_path TO {SCHEMA},pg_catalog")
                conn.execute("""CREATE TABLE dq_run (
                    run_id UUID PRIMARY KEY, mode TEXT, partition_key TEXT, input_fingerprint TEXT,
                    ruleset_version TEXT, status TEXT, finished_at TIMESTAMPTZ,
                    total_rule_count INTEGER, failed_rule_count INTEGER,error_message TEXT);
                    CREATE TABLE asset(asset_id BIGINT PRIMARY KEY);
                    INSERT INTO asset VALUES(2325);
                    CREATE TABLE dq_result (
                    run_id UUID, partition_key TEXT,dataset_name TEXT,rule_code TEXT,severity TEXT,
                    status TEXT,expected_value TEXT,actual_value TEXT,failed_count INTEGER,sample_records JSONB);""")
                migration = Path("pipeline/silver_quality/migrations/020_dart_historical_revenue.sql").read_text()
                assert "public." not in migration
                conn.execute(migration)
                assert conn.execute("SELECT count(*) FROM pg_tables WHERE schemaname='public'").fetchone()[0] == 0
                conn.autocommit = False
                yield conn
        finally:
            subprocess.run([pg_ctl, "-D", str(data), "-m", "fast", "-w", "stop"],
                           check=True, capture_output=True, text=True)


def _args(version):
    args = fixtures.publish_args(fixtures.Fixture())
    args["checked_manifest"]["dataset_version"] = version
    return args


def test_migration_insert_certify_bounded_query_and_idempotency(local_pg):
    args = _args("sql-happy-v1")
    with local_pg.transaction():
        result = m.publish(local_pg, **args)
        assert result["inserted"] == 2 and result["committed"] is False
        assert local_pg.execute("SELECT status FROM dq_run WHERE run_id=%s", (result["quality_run_id"],)).fetchone()[0] == "CERTIFIED"
        query = m.ASOF_SQL.replace("public.", SCHEMA + ".")
        assert len(local_pg.execute(query, ("sql-happy-v1", "2015-05-16", "2015-05-16")).fetchall()) == 2
        assert local_pg.execute(query, ("sql-happy-v1", "2023-08-01", "2023-08-01")).fetchall() == []
    with local_pg.transaction():
        reused = m.publish(local_pg, **args)
        assert reused["inserted"] == 0 and reused["quality_run_id"] == result["quality_run_id"]


def test_mid_publication_failure_rolls_back_entire_new_source_version(local_pg):
    fixture = fixtures.Fixture()
    fixture.mapping["asset_id"] = 99999
    fixture.fy["rows"][0]["asset_id"] = 99999
    fixture.listing["rows"][0]["asset_id"] = 99999
    payload = fixture.listing_proof["payload"]
    payload["periods"][0]["asset_id"] = 99999
    body = {k: v for k, v in payload.items() if k != "snapshot_id"}
    payload["snapshot_id"] = m.sha256(json.dumps(body, sort_keys=True, separators=(",", ":")).encode())
    fixture.listing_proof["snapshot_id"] = payload["snapshot_id"]
    fixture.listing["identity_evidence_sha256"] = fixture.add(fixture.listing_proof)
    args = fixtures.publish_args(fixture)
    args["checked_manifest"]["dataset_version"] = "sql-rollback-v1"
    with local_pg.transaction():
        baseline = local_pg.execute("SELECT count(*) FROM dq_run").fetchone()[0]
    with pytest.raises(psycopg.errors.ForeignKeyViolation):
        with local_pg.transaction():
            # Source evidence is internally valid, but the synthetic target DB
            # lacks asset 99999; the FK fails after its DQ/dataset insert.
            m.publish(local_pg, **args)
    with local_pg.transaction():
        assert local_pg.execute("SELECT count(*) FROM fundamental_document_dataset WHERE dataset_version='sql-rollback-v1'").fetchone()[0] == 0
        assert local_pg.execute("SELECT count(*) FROM dq_run").fetchone()[0] == baseline


@pytest.mark.parametrize("action", ["UPDATE fundamental_document_revenue SET value=value+1 WHERE dataset_version='sql-happy-v1'",
                                    "DELETE FROM fundamental_document_revenue WHERE dataset_version='sql-happy-v1'",
                                    "TRUNCATE fundamental_document_revenue",
                                    "UPDATE fundamental_document_dataset SET bundle_sha256=repeat('0',64) WHERE dataset_version='sql-happy-v1'",
                                    "INSERT INTO fundamental_document_revenue SELECT * FROM fundamental_document_revenue WHERE dataset_version='sql-happy-v1'"])
def test_certified_version_immutable_at_database_boundary(local_pg, action):
    with pytest.raises(psycopg.errors.RaiseException, match="append-only|RUNNING"):
        with local_pg.transaction():
            local_pg.execute(action)


def test_same_version_different_manifest_cannot_overwrite(local_pg):
    args = _args("sql-happy-v1")
    args["checked_manifest"]["review"]["reviewed_at"] = "2026-10-02T13:00:00+09:00"
    with pytest.raises(m.EvidenceError, match="immutable dataset/version conflict"):
        with local_pg.transaction():
            m.publish(local_pg, **args)
