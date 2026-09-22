import hashlib
import json
import shutil
import subprocess
import tempfile
from datetime import date, datetime, timezone
from pathlib import Path
from uuid import uuid4
from types import SimpleNamespace

import pandas as pd
import psycopg
import pytest

from pipeline.silver import fmp, full_statements, research_backfill
from pipeline.silver import research_observations as research


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    body = json.dumps(value).encode()
    path.write_bytes(body)
    return hashlib.sha256(body).hexdigest()


def test_fmp_preserves_unmapped_accounts_and_rejects_undated_rows(tmp_path):
    path = tmp_path / "financials/fmp/income/year=2025/response.json"
    raw = dict(symbol="AAA", date="2025-12-31", period="FY",
               acceptedDate="2026-03-01 12:00:00", reportedCurrency="USD",
               researchAndDevelopmentExpenses=321, opaqueProviderField="retained")
    write(path, [raw, {**raw, "acceptedDate": None}])
    identifiers = pd.DataFrame([dict(identifier_type="ticker", identifier="AAA", natural_key="FMP:AAA")])
    frame, _ = fmp.prepare_fundamentals(str(tmp_path), identifiers)
    # No allowlisted values: raw preservation must still work.
    assert frame.empty
    records = frame.attrs["research_observations"]
    assert len(records) == 1
    assert records[0]["raw_row"] == raw
    assert records[0]["available_at"].tzinfo is not None


def test_profile_replay_requires_observed_timestamp_and_verifies_hash(tmp_path):
    path = tmp_path / "stock/fmp/universe/profile-bulk/snapshot_date=2026-09-01/part=0/response.json"
    checksum = write(path, [dict(symbol="AAA", sector="Technology", industry="Software")])
    entry = dict(path=str(path), sha256=checksum, dataset="FMP_PROFILE")
    with pytest.raises(ValueError, match="received_at"):
        research_backfill.prepare(entry)
    write(path.with_name("manifest.json"), {"received_at": "2026-09-02T01:00:00+00:00"})
    records, excluded = research_backfill.prepare(entry)
    assert not excluded
    assert records[0]["available_at"] == datetime(2026, 9, 2, 1, tzinfo=timezone.utc)
    with pytest.raises(ValueError, match="checksum"):
        research_backfill.prepare({**entry, "sha256": "bad"})


@pytest.mark.parametrize("singleton", [False, True])
def test_event_replay_uses_official_acceptance_not_receipt_prefix(tmp_path, singleton):
    path = tmp_path / "corporate_actions/dart/structured/event=paid_increase/year=2026/corp=005930/rcept=20260831000001.json"
    receipt = "20260831000001"
    checksum = write(path, dict(rcept_no=receipt, fdpp_op="1,000", ic_mthn="제3자배정증자"))
    disclosures = tmp_path / "disclosures.json"
    disclosure_row = dict(rcept_no=receipt, rcept_dt="20260901", report_nm="유상증자결정")
    disclosure_sha = write(disclosures, disclosure_row if singleton else {"list": [disclosure_row]})
    entry = dict(path=str(path), sha256=checksum, dataset="DART_EVENT",
                 disclosure_file=str(disclosures), disclosure_sha256=disclosure_sha)
    records, excluded = research_backfill.prepare(entry)
    assert not excluded
    assert records[0]["available_at"].date() == date(2026, 9, 2)
    assert records[0]["raw_row"]["fdpp_op"] == "1,000"
    assert disclosure_sha in research_backfill.checkpoint_version(entry)


def test_observation_identity_is_immutable_and_vintage_sensitive():
    kwargs = dict(identifier="A", source="FMP", dataset="FMP_PROFILE", raw_row={"x": 1}, source_file="x")
    first = datetime(2026, 9, 1, tzinfo=timezone.utc)
    second = datetime(2026, 9, 2, tzinfo=timezone.utc)
    one = research.observation(**kwargs, available_at=first, observed_at=first)
    same = research.observation(**kwargs, available_at=first, observed_at=first)
    two = research.observation(**kwargs, available_at=second, observed_at=second)
    assert one["observation_key"] == same["observation_key"]
    assert one["observation_key"] != two["observation_key"]
    with pytest.raises(ValueError):
        research.observation(**kwargs, available_at=first, observed_at=second)


def test_universe_filter_precedes_expensive_statement_parsing(tmp_path, monkeypatch):
    path = tmp_path / "financials/fmp/income/year=2025/response.json"
    checksum = write(path, [dict(symbol="OUTSIDE", date="2025-12-31", period="FY")])
    monkeypatch.setattr(fmp, "_parse_date", lambda _: pytest.fail("excluded issuer parsed"))
    rows, excluded = research_backfill.prepare(
        dict(path=str(path), sha256=checksum, dataset="FMP_STATEMENT"),
        eligible_identifiers={"ADMITTED"},
    )
    assert not rows and excluded == 1


def test_daily_profiles_are_hashed_only_after_universe_admission(tmp_path, monkeypatch):
    path = tmp_path / "stock/fmp/universe/company-screener/snapshot_date=2026-09-01/response.json"
    write(path, [dict(symbol="AAA", companyName="AAA Inc", exchange="NYSE", isEtf=False, isFund=False),
                 dict(symbol="OUTSIDE", companyName="Outside", exchange="LSE")])
    write(path.with_name("manifest.json"), {"received_at": "2026-09-01T00:00:00+00:00"})
    symbols = []
    original = research.observation
    def capture(**kwargs):
        symbols.append(kwargs["identifier"])
        return original(**kwargs)
    monkeypatch.setattr(research, "observation", capture)
    assets, _, _ = fmp.prepare_universe(str(tmp_path))
    assert symbols == ["AAA"]
    assert len(assets.attrs["research_observations"]) == 1


@pytest.fixture(scope="module")
def pg(tmp_path_factory):
    if any(shutil.which(x) is None for x in ("initdb", "pg_ctl")):
        pytest.skip("PostgreSQL binaries unavailable")
    root = tmp_path_factory.mktemp("research-pg")
    with tempfile.TemporaryDirectory(prefix="research-pg-", dir="/tmp") as socket:
        def command(*args):
            subprocess.run(args, check=True, capture_output=True, text=True)
        command("initdb", "-D", str(root / "db"), "-A", "trust", "-U", "postgres", "--no-locale", "--encoding=UTF8")
        command("pg_ctl", "-D", str(root / "db"), "-l", str(root / "log"),
                "-o", f"-F -k {socket} -h '' -p 55438", "-w", "start")
        try:
            with psycopg.connect(host=socket, port=55438, user="postgres") as conn:
                conn.execute("CREATE TABLE asset(asset_id bigint PRIMARY KEY)")
                conn.execute("CREATE TABLE asset_identifier(identifier text, asset_id bigint, source text, identifier_type text, valid_from date, valid_to date)")
                conn.execute("INSERT INTO asset_identifier VALUES ('AAA',1,'FMP','ticker','1900-01-01',NULL)")
                conn.execute("CREATE TABLE dq_run(run_id uuid PRIMARY KEY,status text)")
                migrations = Path("pipeline/silver_quality/migrations")
                for name in ("013_alternative_research_inputs.sql", "014_industry_short_balance.sql", "018_research_expansion.sql", "019_dart_legacy_account_namespace.sql"):
                    conn.execute((migrations / name).read_text())
                conn.execute("INSERT INTO asset VALUES (1)")
                conn.commit()
                yield conn
        finally:
            command("pg_ctl", "-D", str(root / "db"), "-m", "immediate", "-w", "stop")


def test_postgres_migration_idempotent_publish_and_certified_views(pg):
    run = uuid4()
    pg.execute("INSERT INTO dq_run VALUES (%s,'RUNNING')", (run,))
    pg.commit()
    at = datetime(2026, 9, 2, tzinfo=timezone.utc)
    records = [research.observation(
        identifier="A", source="FMP", dataset="FMP_STATEMENT",
        raw_row={"symbol": "A", "period": "FY", "reportedCurrency": "USD",
                 "researchAndDevelopmentExpenses": 321, "bad": "NaN"},
        source_file="x", available_at=at,
        metadata={"period_end": "2025-12-31", "statement_type": "IS"},
    )]
    with pg.transaction():
        assert research.publish(pg, records, {"A": 1}, run) == 1
    assert pg.execute("SELECT count(*) FROM fmp_statement_line").fetchone()[0] == 0
    pg.execute("UPDATE dq_run SET status='CERTIFIED' WHERE run_id=%s", (run,))
    pg.commit()
    with pg.transaction():
        assert research.publish(pg, records, {"A": 1}, run) == 0
    assert pg.execute("SELECT source_metric,value FROM fmp_statement_line").fetchall() == [("researchAndDevelopmentExpenses", 321)]
    assert pg.execute("SELECT research_numeric('1,234'),research_numeric('-'),research_numeric('Infinity')").fetchone() == (1234, None, None)
    pg.commit()


def test_postgres_dart_mapping_preserves_periods_excludes_dimensions_and_names(pg, tmp_path):
    run = uuid4()
    pg.execute("INSERT INTO dq_run VALUES (%s,'CERTIFIED')", (run,))
    pg.commit()
    path = tmp_path / ("financials/dart_statement_lines/year=2025/corp=005930/"
                       "report=11011/fs_type=CFS/sha256=" + "a" * 64 + "/response.json")
    base = dict(rcept_no="20260331000001", reprt_code="11011", bsns_year="2025",
                fs_div="CFS", sj_div="BS", thstrm_dt="2025.12.31", currency="KRW",
                thstrm_amount="100", thstrm_add_amount="200", account_nm="재고자산")
    write(path, {"status": "000", "list": [
        {**base, "account_id": "ifrs-full_Inventories", "ord": "1"},
        {**base, "account_id": "ifrs-full_Inventories", "ord": "2", "account_detail": "segment"},
        {**base, "account_id": "entity_Inventories", "ord": "3"},
    ]})
    frame, _ = full_statements.prepare(files=[str(path)])
    with pg.transaction():
        full_statements.publish(pg, frame, {"005930": 1}, run)
    assert pg.execute("SELECT metric,current_amount,current_cumulative_amount,metric_candidate_count FROM dart_standardized_statement_line").fetchall() == [("inventories", 100, 200, 1)]
    assert pg.execute("SELECT count(*) FROM fundamental_statement_line").fetchone()[0] == 3
    assert pg.execute("SELECT metric FROM dart_account_metric_map WHERE account_id='ifrs_Inventories' AND statement_type='BS'").fetchone() == ("inventories",)
    pg.commit()


def test_postgres_profile_and_event_views(pg):
    run = uuid4()
    pg.execute("INSERT INTO dq_run VALUES (%s,'CERTIFIED')", (run,))
    pg.commit()
    at = datetime(2026, 9, 3, tzinfo=timezone.utc)
    records = [research.observation(
        identifier="A", source="FMP", dataset="FMP_PROFILE",
        raw_row={"sector": "Technology", "industry": "Software", "fullTimeEmployees": "1,000"},
        source_file="p", available_at=at, observed_at=at,
    ), research.observation(
        identifier="A", source="DART", dataset="DART_EVENT",
        raw_row={"fdpp_op": "2,000", "fdpp_etc": "-", "ic_mthn": "제3자배정증자"},
        source_file="e", available_at=at,
        metadata={"event_type": "paid_increase", "action_scope": "ISSUER"},
    )]
    with pg.transaction():
        assert research.publish(pg, records, {"A": 1}, run) == 2
    assert pg.execute("SELECT sector,employee_count FROM company_profile_observation").fetchone() == ("Technology", 1000)
    assert pg.execute("SELECT operating_funds,other_funds FROM dart_corporate_event_detail").fetchone() == (2000, None)
    pg.commit()


def test_postgres_replay_checkpoint_skips_body_and_failed_publish_is_atomic(pg, tmp_path, monkeypatch):
    def start(conn, **kwargs):
        run = uuid4()
        conn.execute("INSERT INTO dq_run VALUES (%s,'RUNNING')", (run,))
        conn.commit()
        return SimpleNamespace(run_id=run)

    def finish(conn, context, status, results, *, commit=True, **kwargs):
        conn.execute("UPDATE dq_run SET status=%s WHERE run_id=%s", (status, context.run_id))
        if commit:
            conn.commit()

    monkeypatch.setattr(research_backfill.repository, "start_run", start)
    monkeypatch.setattr(research_backfill.repository, "finish_run", finish)
    path = tmp_path / "financials/fmp/income/year=2024/response.json"
    checksum = write(path, [dict(symbol="AAA", date="2024-12-31", period="FY",
                                acceptedDate="2025-03-01 12:00:00", rareAccount=900)])
    entry = dict(path=str(path), sha256=checksum, dataset="FMP_STATEMENT")
    assert research_backfill.replay(pg, entry)["inserted"] == 1
    assert research_backfill.replay(pg, entry, recheck=True)["inserted"] == 0
    prepare = research_backfill.prepare
    monkeypatch.setattr(research_backfill, "prepare", lambda _: pytest.fail("completed input reread"))
    assert research_backfill.replay(pg, entry)["status"] == "SKIPPED"
    monkeypatch.setattr(research_backfill, "prepare", prepare)
    checksum = write(path, [dict(symbol="AAA", date="2024-12-31", period="FY",
                                acceptedDate="2025-04-01 12:00:00", rareAccount=901)])
    entry = {**entry, "sha256": checksum}
    publish = research.publish
    def failing_publish(*args, **kwargs):
        publish(*args, **kwargs)
        raise RuntimeError("injected publication failure")
    monkeypatch.setattr(research, "publish", failing_publish)
    with pytest.raises(RuntimeError, match="injected"):
        research_backfill.replay(pg, entry)
    assert pg.execute("SELECT count(*) FROM research_input_checkpoint WHERE content_sha256=%s", (checksum,)).fetchone()[0] == 0
    assert pg.execute("SELECT count(*) FROM research_observation WHERE raw_row->>'rareAccount'='901'").fetchone()[0] == 0
    pg.commit()
