-- Current DART industry observations and authorized KRX short-balance vintages.
-- Neither source supplies a trustworthy historical effective timestamp for a
-- file downloaded today, so available_at is the first observed ingestion time.

CREATE TABLE IF NOT EXISTS industry_classification_observation (
    asset_id BIGINT NOT NULL REFERENCES asset(asset_id) ON DELETE CASCADE,
    source TEXT NOT NULL,
    taxonomy TEXT NOT NULL,
    industry_code TEXT NOT NULL,
    industry_name TEXT,
    observed_at TIMESTAMPTZ NOT NULL,
    available_at TIMESTAMPTZ NOT NULL,
    effective_from DATE,
    effective_to DATE,
    observation_key TEXT NOT NULL,
    source_file_sha256 TEXT NOT NULL,
    raw_row JSONB NOT NULL,
    quality_run_id UUID NOT NULL REFERENCES dq_run(run_id),
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (asset_id, source, taxonomy, observation_key),
    CHECK (btrim(source) <> ''),
    CHECK (btrim(taxonomy) <> ''),
    CHECK (btrim(industry_code) <> ''),
    CHECK (available_at = observed_at),
    CHECK (effective_to IS NULL OR effective_from IS NOT NULL),
    CHECK (effective_to IS NULL OR effective_to >= effective_from)
);
CREATE INDEX IF NOT EXISTS ix_industry_classification_observation_pit
    ON industry_classification_observation (
        asset_id, taxonomy, available_at DESC
    );

CREATE TABLE IF NOT EXISTS short_position_balance_observation (
    asset_id BIGINT NOT NULL REFERENCES asset(asset_id) ON DELETE CASCADE,
    source TEXT NOT NULL,
    position_date DATE NOT NULL,
    market TEXT NOT NULL,
    short_balance_quantity NUMERIC(30,0),
    listed_shares NUMERIC(30,0),
    short_balance_value NUMERIC(30,4),
    market_cap NUMERIC(30,4),
    short_balance_ratio NUMERIC(18,8),
    observed_at TIMESTAMPTZ NOT NULL,
    available_at TIMESTAMPTZ NOT NULL,
    observation_key TEXT NOT NULL,
    source_file_sha256 TEXT NOT NULL,
    quality_run_id UUID NOT NULL REFERENCES dq_run(run_id),
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (asset_id, source, position_date, market, observation_key),
    CHECK (btrim(source) <> ''),
    CHECK (btrim(market) <> ''),
    CHECK (available_at = observed_at),
    CHECK (short_balance_quantity IS NULL OR short_balance_quantity >= 0),
    CHECK (listed_shares IS NULL OR listed_shares > 0),
    CHECK (short_balance_value IS NULL OR short_balance_value >= 0),
    CHECK (market_cap IS NULL OR market_cap >= 0),
    CHECK (
        short_balance_ratio IS NULL
        OR (short_balance_ratio >= 0 AND short_balance_ratio <= 100)
    ),
    CHECK (
        short_balance_quantity IS NOT NULL
        OR short_balance_value IS NOT NULL
        OR short_balance_ratio IS NOT NULL
    )
);
CREATE INDEX IF NOT EXISTS ix_short_position_balance_observation_pit
    ON short_position_balance_observation (
        asset_id, position_date, available_at DESC
    );

COMMENT ON TABLE industry_classification_observation IS
    'Observed DART current-industry vintages. Never backfill an observation '
    'before available_at; DART company overview does not provide historical '
    'classification effective dates.';
COMMENT ON TABLE short_position_balance_observation IS
    'Authorized KRX short-position balance vintages. Historical files first '
    'seen today are available today, not retroactively on position_date.';
