-- Research inputs that are independent of the existing price/return contract.
-- Every row is tied to a certified dq_run and retains the source filing/export
-- identity needed for point-in-time queries and idempotent replay.

CREATE TABLE IF NOT EXISTS fundamental_statement_line (
    asset_id BIGINT NOT NULL REFERENCES asset(asset_id) ON DELETE CASCADE,
    source TEXT NOT NULL,
    filing_id TEXT NOT NULL,
    report_code TEXT NOT NULL,
    business_year INTEGER NOT NULL,
    period_end DATE NOT NULL,
    fiscal_period TEXT NOT NULL CHECK (
        fiscal_period IN ('FY', 'Q1', 'Q2', 'Q3', 'Q4')
    ),
    fs_type TEXT NOT NULL CHECK (fs_type IN ('CFS', 'OFS')),
    statement_type TEXT NOT NULL CHECK (
        statement_type IN ('BS', 'IS', 'CIS', 'CF', 'SCE')
    ),
    account_id TEXT NOT NULL,
    account_name TEXT,
    account_detail TEXT NOT NULL DEFAULT '',
    line_order INTEGER,
    current_period_label TEXT,
    current_amount NUMERIC(38,6),
    current_cumulative_amount NUMERIC(38,6),
    prior_period_label TEXT,
    prior_amount NUMERIC(38,6),
    prior_quarter_label TEXT,
    prior_quarter_amount NUMERIC(38,6),
    prior_cumulative_amount NUMERIC(38,6),
    prior_year_label TEXT,
    prior_year_amount NUMERIC(38,6),
    currency TEXT,
    filed DATE NOT NULL,
    accepted_at TIMESTAMPTZ,
    available_date DATE NOT NULL,
    available_at TIMESTAMPTZ NOT NULL,
    revision_key TEXT NOT NULL,
    line_key TEXT NOT NULL,
    raw_line JSONB NOT NULL,
    quality_run_id UUID NOT NULL REFERENCES dq_run(run_id),
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (asset_id, source, filing_id, fs_type, line_key),
    CHECK (available_date = filed + 1),
    CHECK (available_date > period_end),
    CHECK (
        current_amount IS NOT NULL
        OR current_cumulative_amount IS NOT NULL
        OR prior_amount IS NOT NULL
        OR prior_quarter_amount IS NOT NULL
        OR prior_cumulative_amount IS NOT NULL
        OR prior_year_amount IS NOT NULL
    )
);
CREATE INDEX IF NOT EXISTS ix_fundamental_statement_line_pit
    ON fundamental_statement_line (
        asset_id, account_id, available_date, period_end
    );
CREATE INDEX IF NOT EXISTS ix_fundamental_statement_line_filing
    ON fundamental_statement_line (filing_id, fs_type, statement_type);

CREATE OR REPLACE VIEW fundamental_statement_line_current AS
SELECT asset_id, source, filing_id, report_code, business_year, period_end,
       fiscal_period, fs_type, statement_type, account_id, account_name,
       account_detail, line_order, current_period_label, current_amount,
       current_cumulative_amount, prior_period_label, prior_amount,
       prior_quarter_label, prior_quarter_amount, prior_cumulative_amount,
       prior_year_label, prior_year_amount, currency, filed, accepted_at,
       available_date, available_at, revision_key, line_key, raw_line,
       quality_run_id, loaded_at
FROM (
    SELECT f.*, row_number() OVER (
        PARTITION BY asset_id, source, period_end, fiscal_period, fs_type,
                     statement_type, account_id, account_detail, line_order
        ORDER BY available_at DESC, revision_key DESC, filing_id DESC,
                 line_key DESC
    ) AS rn
    FROM fundamental_statement_line f
) ranked
WHERE rn = 1;

CREATE TABLE IF NOT EXISTS ownership_disclosure_event (
    asset_id BIGINT NOT NULL REFERENCES asset(asset_id) ON DELETE CASCADE,
    source TEXT NOT NULL,
    disclosure_type TEXT NOT NULL CHECK (
        disclosure_type IN ('EXECUTIVE_MAJOR_SHAREHOLDER', 'FIVE_PERCENT')
    ),
    filing_id TEXT NOT NULL,
    filed DATE NOT NULL,
    available_date DATE NOT NULL,
    available_at TIMESTAMPTZ NOT NULL,
    reporter TEXT NOT NULL,
    officer_registered TEXT,
    officer_position TEXT,
    major_shareholder TEXT,
    report_type TEXT,
    report_reason TEXT,
    shares NUMERIC(30,0),
    shares_change NUMERIC(30,0),
    ownership_pct NUMERIC(18,8),
    ownership_pct_change NUMERIC(18,8),
    control_shares NUMERIC(30,0),
    control_pct NUMERIC(18,8),
    event_key TEXT NOT NULL,
    raw_row JSONB NOT NULL,
    quality_run_id UUID NOT NULL REFERENCES dq_run(run_id),
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (asset_id, source, event_key),
    CHECK (available_date = filed + 1)
);
CREATE INDEX IF NOT EXISTS ix_ownership_disclosure_event_pit
    ON ownership_disclosure_event (
        asset_id, disclosure_type, available_date, reporter
    );
CREATE UNIQUE INDEX IF NOT EXISTS uq_ownership_disclosure_filing_reporter
    ON ownership_disclosure_event (
        asset_id, source, disclosure_type, filing_id, reporter
    );

CREATE TABLE IF NOT EXISTS investor_flow_daily (
    asset_id BIGINT NOT NULL REFERENCES asset(asset_id) ON DELETE CASCADE,
    source TEXT NOT NULL,
    trade_date DATE NOT NULL,
    market TEXT NOT NULL,
    investor_type TEXT NOT NULL,
    sell_volume NUMERIC(30,0),
    buy_volume NUMERIC(30,0),
    net_volume NUMERIC(30,0),
    sell_value NUMERIC(30,4),
    buy_value NUMERIC(30,4),
    net_value NUMERIC(30,4),
    currency TEXT NOT NULL DEFAULT 'KRW',
    available_at TIMESTAMPTZ NOT NULL,
    source_file_sha256 TEXT NOT NULL,
    quality_run_id UUID NOT NULL REFERENCES dq_run(run_id),
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (asset_id, source, trade_date, market, investor_type),
    CHECK (
        sell_volume IS NULL OR buy_volume IS NULL OR net_volume IS NULL
        OR net_volume = buy_volume - sell_volume
    ),
    CHECK (
        sell_value IS NULL OR buy_value IS NULL OR net_value IS NULL
        OR net_value = buy_value - sell_value
    )
);
CREATE INDEX IF NOT EXISTS ix_investor_flow_daily_type_date
    ON investor_flow_daily (investor_type, trade_date, asset_id);

COMMENT ON TABLE fundamental_statement_line IS
    'Full OpenDART numeric statement lines. Query with available_date <= as_of; '
    'never replace this PIT filter with the current view in a backtest.';
COMMENT ON TABLE ownership_disclosure_event IS
    'OpenDART filing events, available from the day after receipt. This is a '
    'disclosure series, not an exchange execution tape.';
COMMENT ON TABLE investor_flow_daily IS
    'Licensed KRX investor-category flow exports. Same-day close signals must '
    'not use rows whose available_at is after the signal timestamp.';
