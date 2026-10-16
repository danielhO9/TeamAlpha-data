-- Isolated original-document source. Never rewrite fundamental/Gold or existing
-- certified snapshots. A version is immutable; corrections need a new version.
CREATE TABLE IF NOT EXISTS fundamental_document_dataset (
    dataset_version TEXT PRIMARY KEY,
    manifest_sha256 TEXT NOT NULL CHECK (manifest_sha256 ~ '^[a-f0-9]{64}$'),
    bundle_sha256 TEXT NOT NULL CHECK (bundle_sha256 ~ '^[a-f0-9]{64}$'),
    quality_run_id UUID NOT NULL REFERENCES dq_run(run_id),
    evidence JSONB NOT NULL,
    blocking_windows JSONB NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(dataset_version, quality_run_id)
);

CREATE TABLE IF NOT EXISTS fundamental_document_revenue (
    dataset_version TEXT NOT NULL REFERENCES fundamental_document_dataset(dataset_version),
    observation_key TEXT NOT NULL CHECK (observation_key ~ '^[a-f0-9]{64}$'),
    payload_sha256 TEXT NOT NULL CHECK (payload_sha256 ~ '^[a-f0-9]{64}$'),
    asset_id BIGINT NOT NULL REFERENCES asset(asset_id),
    stock_code TEXT NOT NULL CHECK (stock_code ~ '^[0-9]{6}$'),
    corp_code TEXT NOT NULL CHECK (corp_code ~ '^[0-9]{8}$'),
    source TEXT NOT NULL DEFAULT 'DART_DOCUMENT' CHECK (source='DART_DOCUMENT'),
    statement_type TEXT NOT NULL DEFAULT 'IS' CHECK (statement_type='IS'),
    data_basis TEXT NOT NULL DEFAULT 'STANDARDIZED' CHECK (data_basis='STANDARDIZED'),
    metric TEXT NOT NULL DEFAULT 'revenue' CHECK (metric='revenue'),
    quantity_semantics TEXT NOT NULL DEFAULT 'FLOW_CURRENT_QUARTER' CHECK (quantity_semantics='FLOW_CURRENT_QUARTER'),
    unit_type TEXT NOT NULL DEFAULT 'currency' CHECK (unit_type='currency'),
    currency TEXT NOT NULL DEFAULT 'KRW' CHECK (currency='KRW'),
    period_start DATE NOT NULL,
    period_end DATE NOT NULL,
    fiscal_period TEXT NOT NULL CHECK (fiscal_period IN ('Q1','Q2','Q3')),
    fs_type TEXT NOT NULL CHECK (fs_type IN ('CFS','OFS')),
    value NUMERIC(38,6) NOT NULL CHECK (value >= 0 AND value::text NOT IN ('NaN','Infinity','-Infinity')),
    filing_id TEXT NOT NULL CHECK (filing_id ~ '^[0-9]{14}$'),
    revision_key TEXT NOT NULL CHECK (revision_key=filing_id),
    filed DATE NOT NULL,
    available_date DATE NOT NULL CHECK (available_date=filed+1),
    available_at TIMESTAMPTZ NOT NULL,
    valid_until_exclusive DATE NOT NULL CHECK (valid_until_exclusive>available_date),
    evidence JSONB NOT NULL,
    quality_run_id UUID NOT NULL REFERENCES dq_run(run_id),
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY(dataset_version,observation_key),
    UNIQUE(dataset_version,asset_id,period_end,fiscal_period,fs_type,revision_key,metric),
    FOREIGN KEY(dataset_version,quality_run_id) REFERENCES fundamental_document_dataset(dataset_version,quality_run_id),
    CHECK (period_start<=period_end AND period_end<=filed),
    CHECK (available_at = available_date::timestamp AT TIME ZONE 'Asia/Seoul')
);
CREATE INDEX IF NOT EXISTS idx_document_revenue_asof
    ON fundamental_document_revenue(dataset_version,asset_id,available_date,valid_until_exclusive);

CREATE OR REPLACE FUNCTION require_document_revenue_building() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM dq_run q WHERE q.run_id=NEW.quality_run_id
                   AND q.status='RUNNING' AND q.mode='dart_historical_document_revenue') THEN
        RAISE EXCEPTION 'Original-document source INSERT requires its new RUNNING source DQ transaction';
    END IF;
    RETURN NEW;
END $$;
DROP TRIGGER IF EXISTS trg_document_dataset_building ON fundamental_document_dataset;
CREATE TRIGGER trg_document_dataset_building BEFORE INSERT
    ON fundamental_document_dataset FOR EACH ROW EXECUTE FUNCTION require_document_revenue_building();
DROP TRIGGER IF EXISTS trg_document_revenue_building ON fundamental_document_revenue;
CREATE TRIGGER trg_document_revenue_building BEFORE INSERT
    ON fundamental_document_revenue FOR EACH ROW EXECUTE FUNCTION require_document_revenue_building();

CREATE OR REPLACE FUNCTION reject_document_revenue_mutation() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'Original-document revenue versions are append-only: create a new dataset_version';
END $$;
DROP TRIGGER IF EXISTS trg_document_dataset_immutable ON fundamental_document_dataset;
CREATE TRIGGER trg_document_dataset_immutable BEFORE UPDATE OR DELETE OR TRUNCATE
    ON fundamental_document_dataset FOR EACH STATEMENT EXECUTE FUNCTION reject_document_revenue_mutation();
DROP TRIGGER IF EXISTS trg_document_revenue_immutable ON fundamental_document_revenue;
CREATE TRIGGER trg_document_revenue_immutable BEFORE UPDATE OR DELETE OR TRUNCATE
    ON fundamental_document_revenue FOR EACH STATEMENT EXECUTE FUNCTION reject_document_revenue_mutation();

COMMENT ON TABLE fundamental_document_revenue IS
'Bounded source-only PIT: exact dataset version and available_date <= asof < valid_until_exclusive required. No unbounded panel forward-fill.';
