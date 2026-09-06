-- TeamAlpha silver 스키마 (PostgreSQL/RDS) — schema_tables.md 와 1:1
-- asset_id 를 중심으로 가격·재무·기업행사를 연결. source 컬럼·asset_identifier 로 소스 추가에 열려 있음.

-- 품질 실행 이력. Silver 행은 통과한 quality_run_id와 연결된다.
CREATE TABLE IF NOT EXISTS dq_run (
    run_id UUID PRIMARY KEY,
    parent_run_id UUID REFERENCES dq_run(run_id),
    mode TEXT NOT NULL,
    target_date DATE,
    partition_key TEXT,
    input_fingerprint TEXT,
    ruleset_version TEXT NOT NULL,
    status TEXT NOT NULL CHECK (
        status IN ('RUNNING','BUILDING','VALIDATING','CERTIFIED','FAILED','SKIPPED')
    ),
    started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,
    total_rule_count INTEGER NOT NULL DEFAULT 0,
    failed_rule_count INTEGER NOT NULL DEFAULT 0,
    error_message TEXT
);

CREATE TABLE IF NOT EXISTS dq_result (
    result_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES dq_run(run_id) ON DELETE CASCADE,
    partition_key TEXT,
    dataset_name TEXT NOT NULL,
    rule_code TEXT NOT NULL,
    severity TEXT NOT NULL CHECK (severity IN ('CRITICAL', 'ERROR', 'WARNING', 'MODIFIED', 'INFO')),
    status TEXT NOT NULL CHECK (status IN ('PASS', 'FAIL')),
    expected_value TEXT,
    actual_value TEXT,
    failed_count BIGINT NOT NULL DEFAULT 0,
    sample_records JSONB NOT NULL DEFAULT '[]'::jsonb,
    checked_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dq_metric (
    metric_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES dq_run(run_id) ON DELETE CASCADE,
    dataset_name TEXT NOT NULL,
    metric_name TEXT NOT NULL,
    dimension JSONB NOT NULL DEFAULT '{}'::jsonb,
    metric_value DOUBLE PRECISION,
    measured_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 일별 증분 warning의 현재 상태. 전체 관측 이력은 dq_result에 보존한다.
CREATE TABLE IF NOT EXISTS dq_warning_state (
    warning_state_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    mode TEXT NOT NULL,
    scope_key TEXT NOT NULL,
    target_date DATE,
    partition_key TEXT,
    dataset_name TEXT NOT NULL,
    rule_code TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('OPEN', 'RESOLVED')),
    first_seen_run_id UUID NOT NULL REFERENCES dq_run(run_id),
    last_failed_run_id UUID NOT NULL REFERENCES dq_run(run_id),
    last_evaluated_run_id UUID NOT NULL REFERENCES dq_run(run_id),
    resolved_run_id UUID REFERENCES dq_run(run_id),
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_failed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_evaluated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at TIMESTAMPTZ,
    observation_count BIGINT NOT NULL DEFAULT 1,
    reopen_count BIGINT NOT NULL DEFAULT 0,
    latest_failed_count BIGINT NOT NULL DEFAULT 0,
    expected_value TEXT,
    actual_value TEXT,
    sample_records JSONB NOT NULL DEFAULT '[]'::jsonb,
    UNIQUE (mode, scope_key, dataset_name, rule_code)
);
CREATE INDEX IF NOT EXISTS ix_dq_warning_state_open
    ON dq_warning_state(mode, last_failed_at DESC)
    WHERE status = 'OPEN';
CREATE INDEX IF NOT EXISTS ix_dq_warning_state_rule
    ON dq_warning_state(rule_code, status, last_failed_at DESC);

CREATE OR REPLACE VIEW dq_open_warning AS
SELECT warning_state_id, mode, scope_key, target_date, partition_key,
       dataset_name, rule_code, first_seen_run_id, last_failed_run_id,
       last_evaluated_run_id, first_seen_at, last_failed_at,
       last_evaluated_at, observation_count, reopen_count,
       latest_failed_count, expected_value, actual_value, sample_records
FROM dq_warning_state
WHERE status = 'OPEN';

-- 1. asset — 종목 마스터 (소스 독립 정체성)
CREATE TABLE IF NOT EXISTS asset (
    asset_id   BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name       TEXT NOT NULL,
    asset_type TEXT NOT NULL CHECK (asset_type IN ('stock', 'index', 'fx', 'commodity')),
    instrument_type TEXT NOT NULL DEFAULT 'unknown',
    exchange   TEXT NOT NULL,          -- 예: 'KRX'
    currency   TEXT NOT NULL,          -- 예: 'KRW'
    country_code TEXT,
    base_currency TEXT,
    price_unit TEXT,
    listed_from DATE,
    listed_to DATE,
    quality_run_id UUID REFERENCES dq_run(run_id),
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE asset ADD COLUMN IF NOT EXISTS price_unit TEXT;

-- 2. asset_identifier — 소스별 종목코드 매핑 (소스 추가 확장점)
CREATE TABLE IF NOT EXISTS asset_identifier (
    asset_id   BIGINT NOT NULL REFERENCES asset(asset_id) ON DELETE CASCADE,
    source     TEXT NOT NULL,          -- 'KRX' | 'DART' | 'FMP'
    identifier TEXT NOT NULL,          -- ticker/corp code/CIK/CUSIP/ISIN/FX pair/원자재 심볼
    identifier_type TEXT NOT NULL DEFAULT 'ticker',
    valid_from DATE NOT NULL DEFAULT DATE '0001-01-01',
    valid_to DATE,
    quality_run_id UUID REFERENCES dq_run(run_id),
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (asset_id, source, identifier_type, identifier, valid_from)
);
CREATE INDEX IF NOT EXISTS ix_asset_identifier_lookup
    ON asset_identifier(source, identifier_type, identifier, valid_from, valid_to);
CREATE UNIQUE INDEX IF NOT EXISTS uq_asset_identifier_current
    ON asset_identifier(source, identifier_type, identifier)
    WHERE valid_to IS NULL AND identifier_type <> 'cik';

-- 3. price_daily — 주식·지수·FX·원자재 일봉. shares/market_cap 흡수.
CREATE TABLE IF NOT EXISTS price_daily (
    asset_id      BIGINT NOT NULL REFERENCES asset(asset_id) ON DELETE CASCADE,
    source        TEXT NOT NULL,       -- 가격 출처 (예: 'KRX')
    trade_date    DATE NOT NULL,
    open          NUMERIC(28,8),
    high          NUMERIC(28,8),
    low           NUMERIC(28,8),
    close         NUMERIC(28,8),
    adj_close     NUMERIC(28,8),       -- 분할 등 가격 조정 종가
    total_return_close NUMERIC(28,8),  -- 배당까지 반영한 총수익 지수형 종가
    currency      TEXT,
    vwap          NUMERIC(28,8),
    available_at  TIMESTAMPTZ,
    volume        BIGINT,
    trading_value NUMERIC(30,4),
    shares        BIGINT,              -- 상장주식수 (index는 NULL)
    market_cap    NUMERIC(30,4),       -- 시가총액. FMP는 원천에 없으면 NULL
    market        TEXT,                -- 주식시장 또는 'FX'; 지수·원자재는 NULL. 날짜별 값 — 아래 참고
    quality_run_id UUID REFERENCES dq_run(run_id),
    total_return_quality_run_id UUID REFERENCES dq_run(run_id),
    total_return_loaded_at TIMESTAMPTZ,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (asset_id, source, trade_date)
);
-- market 은 asset 이 아니라 여기 있다: 종목이 시장을 옮기기 때문(KONEX→KOSDAQ 71건, KOSDAQ→KOSPI 16건).
-- 종목당 하나만 저장하면 승격 전 이력이 승격 후 시장으로 잘못 분류돼 유니버스가 오염된다.
-- 날짜별로 두면 `WHERE market IN ('KOSPI','KOSDAQ')` 이 자동으로 시점 정확(PIT)해진다.

CREATE OR REPLACE VIEW factor_price_feature_daily AS
SELECT asset_id,source,trade_date,open,high,low,close,adj_close,currency,
       vwap,available_at,volume,trading_value,shares,market_cap,market,
       quality_run_id,loaded_at
FROM price_daily;
COMMENT ON VIEW factor_price_feature_daily IS
    'Feature-safe price-only projection. Excludes ex-post total_return_close '
    'and total-return lineage; use total_return_close only as a forward label.';

-- 4. fundamental — 재무 (long, DART). 한 행 = 종목×회계기간×공시×지표.
CREATE TABLE IF NOT EXISTS fundamental (
    asset_id       BIGINT NOT NULL REFERENCES asset(asset_id) ON DELETE CASCADE,
    source         TEXT NOT NULL,      -- 'DART' …
    period_end     DATE NOT NULL,      -- 회계기간 종료일
    fiscal_period  TEXT NOT NULL CHECK (fiscal_period IN ('FY', 'Q1', 'Q2', 'Q3', 'Q4')),
    fs_type        TEXT NOT NULL CHECK (fs_type IN ('CFS', 'OFS', 'UNKNOWN')),
    statement_type TEXT NOT NULL DEFAULT 'UNKNOWN', -- BS | IS | CF
    data_basis     TEXT NOT NULL DEFAULT 'STANDARDIZED',
    filing_id      TEXT,               -- 접수번호(rcept_no)
    filed          DATE,               -- 접수일
    available_date DATE,               -- PIT 사용가능일 (filed+1 or 법정기한+1)
    accepted_at    TIMESTAMPTZ,
    available_at   TIMESTAMPTZ,
    metric         TEXT NOT NULL,      -- 표준지표: revenue, net_income, total_equity…
    value          NUMERIC(30,6),
    currency       TEXT,
    unit_type      TEXT NOT NULL DEFAULT 'currency',
    revision_key   TEXT NOT NULL,
    quality_run_id UUID REFERENCES dq_run(run_id),
    loaded_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (
        asset_id, source, statement_type, data_basis, period_end,
        fiscal_period, fs_type, revision_key, metric
    )
);
-- PIT 조회용 (available_date <= 기준일 필터)
CREATE INDEX IF NOT EXISTS ix_fundamental_pit ON fundamental (asset_id, metric, available_date);

-- 4b. OpenDART 전체 재무제표 숫자 원계정. 표준 metric으로 억지 매핑하지 않고
-- account_id/공시 revision을 보존해 새로운 재무 팩터의 인증 입력으로 사용한다.
CREATE TABLE IF NOT EXISTS fundamental_statement_line (
    asset_id BIGINT NOT NULL REFERENCES asset(asset_id) ON DELETE CASCADE,
    source TEXT NOT NULL,
    filing_id TEXT NOT NULL,
    report_code TEXT NOT NULL,
    business_year INTEGER NOT NULL,
    period_end DATE NOT NULL,
    fiscal_period TEXT NOT NULL CHECK (fiscal_period IN ('FY','Q1','Q2','Q3','Q4')),
    fs_type TEXT NOT NULL CHECK (fs_type IN ('CFS','OFS')),
    statement_type TEXT NOT NULL CHECK (statement_type IN ('BS','IS','CIS','CF','SCE')),
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
    CHECK (available_date=filed+1),
    CHECK (available_date>period_end),
    CHECK (current_amount IS NOT NULL OR current_cumulative_amount IS NOT NULL
           OR prior_amount IS NOT NULL OR prior_quarter_amount IS NOT NULL
           OR prior_cumulative_amount IS NOT NULL OR prior_year_amount IS NOT NULL)
);
CREATE INDEX IF NOT EXISTS ix_fundamental_statement_line_pit
    ON fundamental_statement_line(asset_id,account_id,available_date,period_end);
CREATE INDEX IF NOT EXISTS ix_fundamental_statement_line_filing
    ON fundamental_statement_line(filing_id,fs_type,statement_type);

CREATE OR REPLACE VIEW fundamental_statement_line_current AS
SELECT asset_id,source,filing_id,report_code,business_year,period_end,
       fiscal_period,fs_type,statement_type,account_id,account_name,
       account_detail,line_order,current_period_label,current_amount,
       current_cumulative_amount,prior_period_label,prior_amount,
       prior_quarter_label,prior_quarter_amount,prior_cumulative_amount,
       prior_year_label,prior_year_amount,currency,filed,accepted_at,
       available_date,available_at,revision_key,line_key,raw_line,
       quality_run_id,loaded_at
FROM (
    SELECT f.*,row_number() OVER (
        PARTITION BY asset_id,source,period_end,fiscal_period,fs_type,
                     statement_type,account_id,account_detail,line_order
        ORDER BY available_at DESC,revision_key DESC,filing_id DESC,line_key DESC
    ) AS rn
    FROM fundamental_statement_line f
) ranked WHERE rn=1;

-- 4c. OpenDART 임원·주요주주 및 5% 보유 공시 이벤트.
CREATE TABLE IF NOT EXISTS ownership_disclosure_event (
    asset_id BIGINT NOT NULL REFERENCES asset(asset_id) ON DELETE CASCADE,
    source TEXT NOT NULL,
    disclosure_type TEXT NOT NULL CHECK (
        disclosure_type IN ('EXECUTIVE_MAJOR_SHAREHOLDER','FIVE_PERCENT')),
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
    PRIMARY KEY(asset_id,source,event_key),
    CHECK (available_date=filed+1)
);
CREATE INDEX IF NOT EXISTS ix_ownership_disclosure_event_pit
    ON ownership_disclosure_event(asset_id,disclosure_type,available_date,reporter);
CREATE UNIQUE INDEX IF NOT EXISTS uq_ownership_disclosure_filing_reporter
    ON ownership_disclosure_event(asset_id,source,disclosure_type,filing_id,reporter);

-- 4d. 정식 KRX 데이터 상품/승인 파일에서 적재한 투자자별 종목 수급.
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
    PRIMARY KEY(asset_id,source,trade_date,market,investor_type),
    CHECK (sell_volume IS NULL OR buy_volume IS NULL OR net_volume IS NULL
           OR net_volume=buy_volume-sell_volume),
    CHECK (sell_value IS NULL OR buy_value IS NULL OR net_value IS NULL
           OR net_value=buy_value-sell_value)
);
CREATE INDEX IF NOT EXISTS ix_investor_flow_daily_type_date
    ON investor_flow_daily(investor_type,trade_date,asset_id);

-- 4e. DART 기업개황에서 실제 관측한 업종코드 vintage. 과거 효력일을 추정하지 않는다.
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
    PRIMARY KEY(asset_id,source,taxonomy,observation_key),
    CHECK (btrim(source)<>'' AND btrim(taxonomy)<>'' AND btrim(industry_code)<>''),
    CHECK (available_at=observed_at),
    CHECK (effective_to IS NULL OR effective_from IS NOT NULL),
    CHECK (effective_to IS NULL OR effective_to>=effective_from)
);
CREATE INDEX IF NOT EXISTS ix_industry_classification_observation_pit
    ON industry_classification_observation(asset_id,taxonomy,available_at DESC);

-- 4f. 정식 KRX 데이터 상품/승인 파일에서 관측한 종목별 공매도 순보유잔고.
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
    PRIMARY KEY(asset_id,source,position_date,market,observation_key),
    CHECK (btrim(source)<>'' AND btrim(market)<>''),
    CHECK (available_at=observed_at),
    CHECK (short_balance_quantity IS NULL OR short_balance_quantity>=0),
    CHECK (listed_shares IS NULL OR listed_shares>0),
    CHECK (short_balance_value IS NULL OR short_balance_value>=0),
    CHECK (market_cap IS NULL OR market_cap>=0),
    CHECK (short_balance_ratio IS NULL
           OR (short_balance_ratio>=0 AND short_balance_ratio<=100)),
    CHECK (short_balance_quantity IS NOT NULL OR short_balance_value IS NOT NULL
           OR short_balance_ratio IS NOT NULL)
);
CREATE INDEX IF NOT EXISTS ix_short_position_balance_observation_pit
    ON short_position_balance_observation(asset_id,position_date,available_at DESC);

CREATE OR REPLACE VIEW fundamental_current AS
SELECT asset_id, source, statement_type, data_basis, period_end, fiscal_period,
       fs_type, filing_id, filed, accepted_at, available_date, available_at,
       metric, value, currency, unit_type, revision_key, quality_run_id, loaded_at
FROM (
    SELECT f.*, row_number() OVER (
        PARTITION BY asset_id, source, statement_type, data_basis,
                     period_end, fiscal_period, fs_type, metric
        ORDER BY available_at DESC NULLS LAST, revision_key DESC
    ) AS rn
    FROM fundamental f
) ranked
WHERE rn=1;

-- 5. corporate_action — 가격·주식수 변화를 설명하는 원천 기업행사.
CREATE TABLE IF NOT EXISTS corporate_action (
    asset_id BIGINT NOT NULL REFERENCES asset(asset_id) ON DELETE CASCADE,
    source TEXT NOT NULL,
    action_key TEXT NOT NULL,
    action_type TEXT NOT NULL,
    announcement_date DATE,
    ex_date DATE,
    record_date DATE,
    payment_date DATE,
    cash_amount NUMERIC(28,8),
    adjusted_cash_amount NUMERIC(28,8),
    currency TEXT,
    frequency TEXT,
    ratio_numerator NUMERIC(28,12),
    ratio_denominator NUMERIC(28,12),
    expected_price_factor NUMERIC(28,12),
    share_count_factor NUMERIC(28,12),
    status TEXT NOT NULL DEFAULT 'confirmed',
    confidence TEXT,
    filing_id TEXT,
    report_name TEXT,
    dart_rm TEXT,
    corp_cls TEXT CHECK (corp_cls IS NULL OR corp_cls IN ('Y', 'K', 'N', 'E')),
    action_scope TEXT NOT NULL DEFAULT 'UNKNOWN',
    cash_amount_status TEXT,
    source_evidence_status TEXT,
    correction_of_action_key TEXT,
    revision_root_action_key TEXT,
    revision_kind TEXT,
    viewer_evidence_sha256 TEXT,
    economic_evidence_sha256 TEXT,
    reviewed_correction_id TEXT,
    payment_date_quality_status TEXT,
    source_body_sha256 TEXT,
    quality_run_id UUID REFERENCES dq_run(run_id),
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY(asset_id, source, action_key)
);
-- 기존 DB에 schema.sql을 재적용해도 신규 배당 컬럼이 보강되도록 한다.
ALTER TABLE corporate_action
    ADD COLUMN IF NOT EXISTS adjusted_cash_amount NUMERIC(28,8);
ALTER TABLE corporate_action
    ADD COLUMN IF NOT EXISTS frequency TEXT;
ALTER TABLE corporate_action
    ADD COLUMN IF NOT EXISTS report_name TEXT;
ALTER TABLE corporate_action
    ADD COLUMN IF NOT EXISTS dart_rm TEXT;
ALTER TABLE corporate_action
    ADD COLUMN IF NOT EXISTS corp_cls TEXT;
ALTER TABLE corporate_action
    ADD COLUMN IF NOT EXISTS action_scope TEXT DEFAULT 'UNKNOWN';
ALTER TABLE corporate_action
    ADD COLUMN IF NOT EXISTS cash_amount_status TEXT;
ALTER TABLE corporate_action
    ADD COLUMN IF NOT EXISTS source_evidence_status TEXT;
ALTER TABLE corporate_action
    ADD COLUMN IF NOT EXISTS correction_of_action_key TEXT;
ALTER TABLE corporate_action
    ADD COLUMN IF NOT EXISTS revision_root_action_key TEXT;
ALTER TABLE corporate_action
    ADD COLUMN IF NOT EXISTS revision_kind TEXT;
ALTER TABLE corporate_action
    ADD COLUMN IF NOT EXISTS viewer_evidence_sha256 TEXT;
ALTER TABLE corporate_action
    ADD COLUMN IF NOT EXISTS economic_evidence_sha256 TEXT;
ALTER TABLE corporate_action
    ADD COLUMN IF NOT EXISTS reviewed_correction_id TEXT;
ALTER TABLE corporate_action
    ADD COLUMN IF NOT EXISTS payment_date_quality_status TEXT;
ALTER TABLE corporate_action
    ADD COLUMN IF NOT EXISTS source_body_sha256 TEXT;
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid='corporate_action'::regclass
          AND conname='corporate_action_corp_cls_check'
    ) THEN
        ALTER TABLE corporate_action
            ADD CONSTRAINT corporate_action_corp_cls_check
            CHECK (corp_cls IS NULL OR corp_cls IN ('Y', 'K', 'N', 'E'));
    END IF;
END $$;
CREATE INDEX IF NOT EXISTS ix_corporate_action_event
    ON corporate_action(asset_id, ex_date, action_type);
CREATE INDEX IF NOT EXISTS ix_corporate_action_dividend
    ON corporate_action(asset_id, ex_date DESC)
    WHERE action_type = 'cash_dividend';

-- 배당 연구용 최소 조회 인터페이스. 원천 행은 corporate_action에만 보관한다.
CREATE OR REPLACE VIEW dividend_history AS
SELECT asset_id, source, action_key, announcement_date, ex_date, record_date,
       payment_date, cash_amount, adjusted_cash_amount, currency, frequency,
       status, confidence, filing_id, quality_run_id, loaded_at,
       report_name, action_scope, dart_rm, corp_cls,
       cash_amount_status, source_evidence_status,
       correction_of_action_key, revision_root_action_key, revision_kind,
       viewer_evidence_sha256, economic_evidence_sha256,
       reviewed_correction_id, payment_date_quality_status,
       source_body_sha256
FROM corporate_action
WHERE action_type = 'cash_dividend';

-- KRX gross total-return derivation audit and certification contract.
CREATE TABLE IF NOT EXISTS dividend_event_resolution (
    quality_run_id UUID NOT NULL REFERENCES dq_run(run_id),
    asset_id BIGINT NOT NULL REFERENCES asset(asset_id) ON DELETE CASCADE,
    source TEXT NOT NULL,
    action_key TEXT NOT NULL,
    resolution_version TEXT NOT NULL,
    is_canonical BOOLEAN NOT NULL,
    excluded_reason TEXT,
    resolved_ex_date DATE,
    ex_date_basis TEXT,
    applied_trade_date DATE,
    raw_cash_amount NUMERIC(28,8),
    adjusted_cash_amount NUMERIC(28,8),
    source_announcement_date DATE,
    revision_group_key TEXT,
    source_evidence_status TEXT,
    cash_amount_status TEXT,
    correction_of_action_key TEXT,
    revision_root_action_key TEXT,
    revision_kind TEXT,
    viewer_evidence_sha256 TEXT,
    economic_evidence_sha256 TEXT,
    reviewed_correction_id TEXT,
    payment_date_quality_status TEXT,
    previous_trade_date DATE,
    previous_close NUMERIC(28,8),
    previous_adj_close NUMERIC(28,8),
    applied_close NUMERIC(28,8),
    applied_adj_close NUMERIC(28,8),
    previous_price_scale NUMERIC(28,12),
    applied_price_scale NUMERIC(28,12),
    selected_cash_scale NUMERIC(28,12),
    cash_adjustment_scale_basis TEXT,
    scale_change_detected BOOLEAN,
    scale_evidence_action_snapshot_run_id UUID,
    scale_evidence_key TEXT,
    scale_price_factor_observed NUMERIC(28,12),
    scale_price_factor_reference NUMERIC(28,12),
    scale_price_factor_parity BOOLEAN,
    resolved_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY(
        quality_run_id, asset_id, source, action_key, resolution_version
    ),
    CHECK (
        (is_canonical AND excluded_reason IS NULL)
        OR (NOT is_canonical AND excluded_reason IS NOT NULL)
    ),
    CHECK (
        ex_date_basis IS NULL
        OR ex_date_basis IN ('KRX_NOTICE', 'KRX_T2_INFERRED')
    )
);
CREATE INDEX IF NOT EXISTS ix_dividend_resolution_run_applied
    ON dividend_event_resolution(
        quality_run_id, asset_id, applied_trade_date
    ) WHERE is_canonical;

CREATE TABLE IF NOT EXISTS dividend_source_receipt (
    quality_run_id UUID NOT NULL REFERENCES dq_run(run_id),
    receipt_no TEXT NOT NULL,
    asset_id BIGINT REFERENCES asset(asset_id) ON DELETE CASCADE,
    ticker TEXT NOT NULL,
    corp_cls TEXT,
    report_name TEXT NOT NULL,
    dart_rm TEXT,
    announcement_date DATE NOT NULL,
    revision_kind TEXT,
    revision_root_receipt_no TEXT,
    previous_receipt_no TEXT,
    terminal_receipt_no TEXT NOT NULL,
    terminal_announcement_date DATE NOT NULL,
    is_terminal_economic_revision BOOLEAN NOT NULL,
    source_evidence_status TEXT NOT NULL,
    cash_amount_status TEXT NOT NULL,
    record_date DATE,
    payment_date DATE,
    cash_amount NUMERIC(28,8),
    viewer_evidence_sha256 TEXT,
    economic_evidence_sha256 TEXT,
    reviewed_correction_id TEXT,
    payment_date_quality_status TEXT,
    pit_event_date DATE NOT NULL,
    mapping_status TEXT NOT NULL,
    excluded_reason TEXT,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY(quality_run_id, receipt_no),
    CHECK (receipt_no ~ '^[0-9]{14}$'),
    CONSTRAINT dividend_source_receipt_ticker_check
        CHECK (ticker ~ '^[0-9A-Z]{6}$'),
    CHECK (revision_root_receipt_no ~ '^[0-9]{14}$'),
    CHECK (terminal_receipt_no ~ '^[0-9]{14}$'),
    CHECK (
        previous_receipt_no IS NULL
        OR previous_receipt_no ~ '^[0-9]{14}$'
    ),
    CHECK (
        viewer_evidence_sha256 IS NULL
        OR viewer_evidence_sha256 ~ '^[0-9a-f]{64}$'
    ),
    CHECK (
        economic_evidence_sha256 IS NULL
        OR economic_evidence_sha256 ~ '^[0-9a-f]{64}$'
    ),
    CHECK (mapping_status IN ('INCLUDED','EXCLUDED')),
    CHECK (
        is_terminal_economic_revision = (receipt_no=terminal_receipt_no)
    ),
    CHECK (
        (mapping_status='INCLUDED' AND asset_id IS NOT NULL
         AND excluded_reason IS NULL)
        OR
        (mapping_status='EXCLUDED' AND excluded_reason IS NOT NULL)
    )
);

-- Idempotently replace the numeric-only pre-release constraint.  New KRX
-- short codes such as 0008Z0 are valid listed-company ticker identifiers.
ALTER TABLE dividend_source_receipt
    DROP CONSTRAINT IF EXISTS dividend_source_receipt_ticker_check;
ALTER TABLE dividend_source_receipt
    ADD CONSTRAINT dividend_source_receipt_ticker_check
    CHECK (ticker ~ '^[0-9A-Z]{6}$');

CREATE TABLE IF NOT EXISTS dart_action_snapshot_contract (
    quality_run_id UUID PRIMARY KEY REFERENCES dq_run(run_id),
    schema_version TEXT NOT NULL,
    manifest_sha256 TEXT NOT NULL CHECK (manifest_sha256 ~ '^[0-9a-f]{64}$'),
    body_digest TEXT NOT NULL CHECK (body_digest ~ '^[0-9a-f]{64}$'),
    body_count BIGINT NOT NULL CHECK (body_count > 0),
    coverage_start DATE NOT NULL,
    coverage_end DATE NOT NULL,
    action_count BIGINT NOT NULL CHECK (action_count > 0),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (coverage_start = DATE '2015-01-01'),
    CHECK (coverage_end >= coverage_start)
);

CREATE TABLE IF NOT EXISTS cash_adjustment_scale_source_evidence (
    action_snapshot_run_id UUID NOT NULL
        REFERENCES dart_action_snapshot_contract(quality_run_id),
    evidence_key TEXT NOT NULL,
    asset_id BIGINT NOT NULL REFERENCES asset(asset_id) ON DELETE CASCADE,
    ticker TEXT NOT NULL CHECK (ticker ~ '^[0-9A-Z]{6}$'),
    cash_receipt_no TEXT NOT NULL CHECK (cash_receipt_no ~ '^[0-9]{14}$'),
    cash_source_evidence_status TEXT NOT NULL,
    cash_action_body_path TEXT NOT NULL,
    cash_action_body_sha256 TEXT NOT NULL
        CHECK (cash_action_body_sha256 ~ '^[0-9a-f]{64}$'),
    cash_economic_body_path TEXT NOT NULL,
    cash_economic_body_schema TEXT NOT NULL,
    cash_economic_sha256 TEXT NOT NULL
        CHECK (cash_economic_sha256 ~ '^[0-9a-f]{64}$'),
    support_action_count INTEGER NOT NULL CHECK (support_action_count>0),
    support_action_digest TEXT NOT NULL
        CHECK (support_action_digest ~ '^[0-9a-f]{64}$'),
    support_semantic_group_count INTEGER NOT NULL
        CHECK (support_semantic_group_count>0),
    price_source TEXT NOT NULL CHECK (price_source='KRX'),
    previous_price_source_object_key TEXT NOT NULL,
    previous_price_source_content_sha256 TEXT NOT NULL
        CHECK (previous_price_source_content_sha256 ~ '^[0-9a-f]{64}$'),
    previous_price_source_etag TEXT NOT NULL
        CHECK (previous_price_source_etag ~ '^[0-9a-f]{32}(-[0-9]+)?$'),
    previous_price_source_schema TEXT NOT NULL,
    adjustment_price_source_object_key TEXT NOT NULL,
    adjustment_price_source_content_sha256 TEXT NOT NULL
        CHECK (adjustment_price_source_content_sha256 ~ '^[0-9a-f]{64}$'),
    adjustment_price_source_etag TEXT NOT NULL
        CHECK (adjustment_price_source_etag ~ '^[0-9a-f]{32}(-[0-9]+)?$'),
    adjustment_price_source_schema TEXT NOT NULL,
    previous_trade_date DATE NOT NULL,
    adjustment_trade_date DATE NOT NULL,
    raw_previous_close NUMERIC(28,8) NOT NULL CHECK (raw_previous_close>0),
    raw_applied_close NUMERIC(28,8) NOT NULL CHECK (raw_applied_close>0),
    raw_reference_price NUMERIC(28,8) NOT NULL CHECK (raw_reference_price>0),
    expected_price_factor NUMERIC(28,12) NOT NULL
        CHECK (expected_price_factor>0),
    cash_scale_basis TEXT NOT NULL
        CHECK (cash_scale_basis='PRE_EVENT_PRICE_SCALE'),
    manifest_row_sha256 TEXT NOT NULL
        CHECK (manifest_row_sha256 ~ '^[0-9a-f]{64}$'),
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY(action_snapshot_run_id,evidence_key),
    UNIQUE(
        action_snapshot_run_id,asset_id,cash_receipt_no,
        adjustment_trade_date
    ),
    UNIQUE(
        action_snapshot_run_id,evidence_key,cash_receipt_no,
        adjustment_trade_date
    ),
    FOREIGN KEY(action_snapshot_run_id,cash_receipt_no)
        REFERENCES dividend_source_receipt(quality_run_id,receipt_no),
    CHECK (length(btrim(evidence_key)) BETWEEN 1 AND 300),
    CHECK (length(btrim(cash_action_body_path))>0),
    CHECK (length(btrim(cash_economic_body_path))>0),
    CHECK (length(btrim(previous_price_source_object_key))>0),
    CHECK (length(btrim(adjustment_price_source_object_key))>0),
    CHECK (support_semantic_group_count<=support_action_count),
    CHECK (cash_source_evidence_status IN (
        'VERIFIED_OPENDART_DOCUMENT',
        'VERIFIED_DART_VIEWER_BODY',
        'VERIFIED_REVIEWED_SOURCE_ERRATUM'
    )),
    CHECK (cash_economic_body_schema IN (
        'OPENDART_DOCUMENT_ZIP_V1',
        'DART_VIEWER_HTML_V1',
        'REVIEWED_PERIODIC_JSON_V1'
    )),
    CHECK (previous_price_source_schema IN (
        'marcap_parquet_v1','krxapi_stock_parquet_v1'
    )),
    CHECK (adjustment_price_source_schema IN (
        'marcap_parquet_v1','krxapi_stock_parquet_v1'
    )),
    CHECK (previous_trade_date<adjustment_trade_date)
);

CREATE TABLE IF NOT EXISTS cash_adjustment_scale_support_action (
    action_snapshot_run_id UUID NOT NULL,
    evidence_key TEXT NOT NULL,
    support_action_source TEXT NOT NULL,
    support_action_key TEXT NOT NULL,
    support_action_type TEXT NOT NULL,
    target_cash_receipt_no TEXT NOT NULL CHECK (
        target_cash_receipt_no ~ '^[0-9]{14}$'
    ),
    target_adjustment_date DATE NOT NULL,
    support_action_body_path TEXT NOT NULL,
    support_action_body_sha256 TEXT NOT NULL
        CHECK (support_action_body_sha256 ~ '^[0-9a-f]{64}$'),
    support_action_quality_run_id UUID NOT NULL REFERENCES dq_run(run_id),
    support_announcement_date DATE,
    support_ex_date DATE,
    support_record_date DATE,
    support_ratio_numerator NUMERIC(28,12),
    support_ratio_denominator NUMERIC(28,12),
    support_entitlement_security_class TEXT,
    support_distributed_security_class TEXT,
    support_expected_price_factor NUMERIC(28,12),
    support_reference_price NUMERIC(28,8),
    support_reason TEXT,
    support_report_name TEXT NOT NULL,
    support_action_scope TEXT NOT NULL CHECK (support_action_scope='ISSUER'),
    support_semantic_group_keys TEXT NOT NULL,
    support_semantic_role TEXT NOT NULL CHECK (
        support_semantic_role IN ('ADJUSTMENT_COMPONENT','CORROBORATION')
    ),
    manifest_support_row_sha256 TEXT NOT NULL CHECK (
        manifest_support_row_sha256 ~ '^[0-9a-f]{64}$'
    ),
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY(
        action_snapshot_run_id,evidence_key,
        support_action_source,support_action_key,support_action_type
    ),
    FOREIGN KEY(action_snapshot_run_id,evidence_key)
        REFERENCES cash_adjustment_scale_source_evidence(
            action_snapshot_run_id,evidence_key
        ),
    CONSTRAINT cash_scale_support_parent_identity_fk FOREIGN KEY(
        action_snapshot_run_id,evidence_key,
        target_cash_receipt_no,target_adjustment_date
    ) REFERENCES cash_adjustment_scale_source_evidence(
        action_snapshot_run_id,evidence_key,
        cash_receipt_no,adjustment_trade_date
    ),
    CHECK (action_snapshot_run_id=support_action_quality_run_id),
    CHECK (length(btrim(support_action_source))>0),
    CHECK (length(btrim(support_action_key))>0),
    CHECK (length(btrim(support_action_type))>0),
    CHECK (length(btrim(support_action_body_path))>0),
    CHECK (length(btrim(support_report_name))>0),
    CHECK (
        (support_ratio_numerator IS NULL)=
        (support_ratio_denominator IS NULL)
    ),
    CHECK (support_ratio_numerator IS NULL OR support_ratio_numerator>0),
    CHECK (support_ratio_denominator IS NULL OR support_ratio_denominator>0),
    CHECK (
        support_entitlement_security_class IS NULL
        OR support_entitlement_security_class IN (
            'COMMON','PREFERRED','COMMON_AND_PREFERRED'
        )
    ),
    CHECK (
        support_distributed_security_class IS NULL
        OR support_distributed_security_class IN (
            'COMMON','PREFERRED','NEW_PREFERRED'
        )
    ),
    CHECK (
        support_semantic_role<>'ADJUSTMENT_COMPONENT'
        OR (
            support_entitlement_security_class IS NOT NULL
            AND support_distributed_security_class IS NOT NULL
        )
    ),
    CHECK (
        support_expected_price_factor IS NULL
        OR support_expected_price_factor>0
    ),
    CHECK (support_reference_price IS NULL OR support_reference_price>0),
    CHECK (
        support_action_source<>'DART_VIEWER'
        OR (
            support_action_key ~ '^[0-9]{14}$'
            AND support_action_body_path ~
                '^corporate_actions/dart/support_action_families/objects/'
                'sha256=[0-9a-f]{64}\.html$'
            AND support_action_body_path=
                'corporate_actions/dart/support_action_families/objects/'
                'sha256=' || support_action_body_sha256 || '.html'
            AND (
                (support_action_type='bonus_issue'
                 AND support_ex_date IS NOT NULL
                 AND support_record_date IS NULL
                 AND support_expected_price_factor IS NOT NULL)
                OR
                (support_action_type='stock_dividend'
                 AND support_ex_date IS NULL
                 AND support_record_date IS NOT NULL
                 AND support_expected_price_factor IS NULL
                 AND support_ratio_numerator IS NOT NULL
                 AND support_entitlement_security_class='COMMON'
                 AND support_distributed_security_class='COMMON')
            )
        )
    ),
    CONSTRAINT cash_scale_support_source_type_check CHECK (
        (support_action_source='DART_STRUCTURED'
         AND support_action_type='bonus_issue')
        OR (support_action_source='DART_VIEWER'
            AND support_action_type IN ('bonus_issue','stock_dividend'))
        OR (support_action_source='DART_DISCLOSURE'
            AND support_action_type IN (
                'stock_dividend','ex_dividend','rights_detachment',
                'combined_detachment'
            ))
        OR (support_action_source='KRX_KIND'
            AND support_action_type IN (
                'stock_dividend','ex_dividend','rights_detachment',
                'combined_detachment'
            ))
        OR (support_action_source='KRX_KIND'
            AND support_action_type='paid_increase'
            AND support_action_key='20180201000086'
            AND support_action_body_sha256=
                'cf15168b7b9f16f7808252be7dc2a81a06dc23b30d0d14e41cebf8674ebf35c9')
    ),
    CONSTRAINT cash_scale_support_role_semantics_check CHECK (
        (
            support_semantic_role='ADJUSTMENT_COMPONENT'
            AND (
                (support_action_source IN (
                    'DART_STRUCTURED','DART_VIEWER'
                 )
                 AND support_action_type='bonus_issue'
                 AND support_ratio_numerator IS NOT NULL
                 AND support_entitlement_security_class='COMMON'
                 AND support_distributed_security_class='COMMON'
                 AND support_expected_price_factor IS NOT NULL
                 AND support_expected_price_factor=round(
                     1::numeric / (
                         1::numeric + support_ratio_numerator /
                         support_ratio_denominator
                     ), 12
                 ))
                OR (
                    support_action_source IN (
                        'DART_DISCLOSURE','DART_VIEWER','KRX_KIND'
                    )
                    AND support_action_type='stock_dividend'
                    AND support_ratio_numerator IS NOT NULL
                    AND (
                        (support_entitlement_security_class='COMMON'
                         AND support_distributed_security_class='COMMON')
                        OR
                        (support_entitlement_security_class=
                            'COMMON_AND_PREFERRED'
                         AND support_distributed_security_class=
                            'NEW_PREFERRED')
                    )
                )
                OR (
                    support_action_source='KRX_KIND'
                    AND support_action_type='paid_increase'
                    AND support_action_key='20180201000086'
                    AND support_action_body_sha256=
                        'cf15168b7b9f16f7808252be7dc2a81a06dc23b30d0d14e41cebf8674ebf35c9'
                    AND support_ratio_numerator=0.1456981704
                    AND support_ratio_denominator=1
                    AND support_entitlement_security_class='COMMON'
                    AND support_distributed_security_class='COMMON'
                    AND support_expected_price_factor IS NULL
                    AND support_record_date=DATE '2017-12-31'
                )
            )
        ) OR (
            support_semantic_role='CORROBORATION'
            AND support_action_source IN ('DART_DISCLOSURE','KRX_KIND')
            AND support_action_type IN (
                'ex_dividend','rights_detachment','combined_detachment'
            )
            AND (
                support_action_source<>'KRX_KIND'
                OR (
                    support_entitlement_security_class IN (
                        'COMMON','PREFERRED'
                    )
                    AND support_distributed_security_class IS NULL
                    AND support_ratio_numerator IS NULL
                    AND support_ratio_denominator IS NULL
                    AND support_reference_price IS NOT NULL
                )
            )
        )
    ),
    CHECK (
        jsonb_typeof(support_semantic_group_keys::jsonb)='array'
        AND jsonb_array_length(support_semantic_group_keys::jsonb)>0
    )
);

COMMENT ON COLUMN cash_adjustment_scale_support_action.support_action_source IS
    'DART_VIEWER is an official content-addressed viewer-body bonus or stock-dividend component; it is distinct from an OpenDART structured API/disclosure row.';

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid='dividend_event_resolution'::regclass
          AND conname='dividend_resolution_scale_evidence_fk'
    ) THEN
        ALTER TABLE dividend_event_resolution
            ADD CONSTRAINT dividend_resolution_scale_evidence_fk
            FOREIGN KEY(
                scale_evidence_action_snapshot_run_id,scale_evidence_key
            ) REFERENCES cash_adjustment_scale_source_evidence(
                action_snapshot_run_id,evidence_key
            );
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid='dividend_event_resolution'::regclass
          AND conname='dividend_resolution_v2_scale_contract_check'
    ) THEN
        ALTER TABLE dividend_event_resolution
            ADD CONSTRAINT dividend_resolution_v2_scale_contract_check CHECK (
                resolution_version <> 'krx_dividend_resolution_v2'
                OR (
                    (
                        is_canonical
                        AND excluded_reason IS NULL
                        AND applied_trade_date IS NOT NULL
                        AND previous_trade_date IS NOT NULL
                        AND previous_close > 0
                        AND previous_adj_close > 0
                        AND applied_close > 0
                        AND applied_adj_close > 0
                        AND previous_price_scale > 0
                        AND applied_price_scale > 0
                        AND selected_cash_scale > 0
                        AND scale_change_detected IS NOT NULL
                        AND scale_price_factor_observed > 0
                        AND scale_price_factor_reference > 0
                        AND scale_price_factor_parity
                        AND (
                            (
                                NOT scale_change_detected
                                AND cash_adjustment_scale_basis=
                                    'STABLE_PRICE_SCALE'
                                AND scale_evidence_action_snapshot_run_id
                                    IS NULL
                                AND scale_evidence_key IS NULL
                            ) OR (
                                scale_change_detected
                                AND cash_adjustment_scale_basis=
                                    'PRE_EVENT_PRICE_SCALE'
                                AND scale_evidence_action_snapshot_run_id
                                    IS NOT NULL
                                AND scale_evidence_key IS NOT NULL
                            )
                        )
                    ) OR (
                        NOT is_canonical
                        AND excluded_reason IS NOT NULL
                        AND applied_trade_date IS NULL
                        AND previous_trade_date IS NULL
                        AND previous_close IS NULL
                        AND previous_adj_close IS NULL
                        AND applied_close IS NULL
                        AND applied_adj_close IS NULL
                        AND previous_price_scale IS NULL
                        AND applied_price_scale IS NULL
                        AND selected_cash_scale IS NULL
                        AND cash_adjustment_scale_basis IS NULL
                        AND scale_change_detected IS NULL
                        AND scale_evidence_action_snapshot_run_id IS NULL
                        AND scale_evidence_key IS NULL
                        AND scale_price_factor_observed IS NULL
                        AND scale_price_factor_reference IS NULL
                        AND scale_price_factor_parity IS NULL
                    )
                )
            );
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS price_return_contract (
    source TEXT NOT NULL,
    asset_type TEXT NOT NULL,
    field_name TEXT NOT NULL,
    methodology_version TEXT NOT NULL,
    dividend_treatment TEXT NOT NULL,
    status TEXT NOT NULL,
    coverage_start DATE,
    coverage_end DATE,
    quality_run_id UUID REFERENCES dq_run(run_id),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    certified_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY(source, asset_type, field_name),
    CHECK (status IN ('BUILDING', 'CERTIFIED', 'FAILED')),
    CHECK (
        (status = 'CERTIFIED' AND certified_at IS NOT NULL)
        OR status <> 'CERTIFIED'
    ),
    CHECK (
        status <> 'CERTIFIED'
        OR (
            coverage_start >= DATE '2015-01-01'
            AND coverage_end >= coverage_start
        )
    )
);

-- Deterministic Critical/Error invariants are also enforced by RDS. The
-- canonical deployed expressions live in migration 006_database_quality_guards.sql.
DO $$ BEGIN
IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='asset'::regclass AND conname='asset_critical_error_guard') THEN
ALTER TABLE asset ADD CONSTRAINT asset_critical_error_guard CHECK (
        quality_run_id IS NOT NULL
        AND btrim(name) <> '' AND btrim(asset_type) <> ''
        AND btrim(instrument_type) <> '' AND btrim(exchange) <> ''
        AND btrim(currency) <> '' AND base_currency IS NOT NULL
        AND btrim(base_currency) <> ''
        AND currency ~ '^[A-Z]{3}$' AND base_currency ~ '^[A-Z]{3}$'
        AND (
            (asset_type='stock' AND exchange IN ('KRX','NASDAQ','NYSE','AMEX')
             AND instrument_type IN ('common_stock','preferred_stock','adr','reit')
             AND price_unit IS NULL)
            OR (asset_type='index' AND exchange='KRX' AND instrument_type='index'
                AND price_unit IS NULL)
            OR (asset_type='fx' AND exchange='FX' AND instrument_type='fx'
                AND price_unit IS NULL)
            OR (asset_type='commodity' AND exchange='COMMODITY'
                AND instrument_type='commodity_future_continuous'
                AND currency='USD' AND base_currency='USD'
                AND price_unit IS NOT NULL AND btrim(price_unit)<>'')
        )
    );
END IF; END $$;
DO $$ BEGIN
IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='asset_identifier'::regclass AND conname='asset_identifier_critical_error_guard') THEN
ALTER TABLE asset_identifier ADD CONSTRAINT asset_identifier_critical_error_guard CHECK (
        quality_run_id IS NOT NULL AND btrim(source) <> ''
        AND btrim(identifier) <> '' AND btrim(identifier_type) <> ''
    );
END IF; END $$;
DO $$ BEGIN
IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='price_daily'::regclass AND conname='price_daily_critical_error_guard') THEN
ALTER TABLE price_daily ADD CONSTRAINT price_daily_critical_error_guard CHECK (
        quality_run_id IS NOT NULL AND btrim(source) <> ''
        AND close IS NOT NULL
        AND close::text NOT IN ('NaN','Infinity','-Infinity')
        AND adj_close IS NOT NULL
        AND adj_close::text NOT IN ('NaN','Infinity','-Infinity')
        AND (source='FMP_COMMODITY' OR (close>0 AND adj_close>0))
        AND (
            (open IS NULL AND high IS NULL AND low IS NULL
             AND source NOT IN ('FMP','FMP_FX','FMP_COMMODITY'))
            OR (open IS NOT NULL AND high IS NOT NULL AND low IS NOT NULL
                AND open::text NOT IN ('NaN','Infinity','-Infinity')
                AND high::text NOT IN ('NaN','Infinity','-Infinity')
                AND low::text NOT IN ('NaN','Infinity','-Infinity')
                AND (source='FMP_COMMODITY'
                     OR (open>0 AND high>0 AND low>0))
                AND high >= GREATEST(open,close)
                AND low <= LEAST(open,close))
        )
        AND (volume IS NULL OR volume >= 0)
        AND (trading_value IS NULL OR trading_value >= 0)
        AND (shares IS NULL OR shares > 0)
        AND (market_cap IS NULL OR market_cap >= 0)
        AND (total_return_close IS NULL OR total_return_close::text NOT IN ('NaN','Infinity','-Infinity'))
        AND (vwap IS NULL OR vwap::text NOT IN ('NaN','Infinity','-Infinity'))
        AND (currency IS NULL OR currency ~ '^[A-Z]{3}$')
        AND (source <> 'KRX' OR market IS NULL OR (market_cap IS NOT NULL AND market_cap > 0))
        AND (shares IS NULL OR market_cap IS NULL
             OR abs(market_cap-close*shares) <= abs(close*shares)*0.01)
    );
END IF; END $$;
DO $$ BEGIN
IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='fundamental'::regclass AND conname='fundamental_critical_error_guard') THEN
ALTER TABLE fundamental ADD CONSTRAINT fundamental_critical_error_guard CHECK (
        quality_run_id IS NOT NULL AND btrim(source) <> ''
        AND btrim(statement_type) <> '' AND btrim(data_basis) <> ''
        AND btrim(fiscal_period) <> '' AND btrim(fs_type) <> ''
        AND btrim(revision_key) <> '' AND btrim(metric) <> ''
        AND btrim(unit_type) <> ''
        AND statement_type IN ('BS','IS','CF','DIVIDEND')
        AND value IS NOT NULL AND value::text NOT IN ('NaN','Infinity','-Infinity')
        AND available_date IS NOT NULL AND available_at IS NOT NULL
        AND ((unit_type='shares' AND (currency IS NULL OR currency ~ '^[A-Z]{3}$'))
             OR (unit_type<>'shares' AND currency IS NOT NULL
                 AND currency ~ '^[A-Z]{3}$'))
        AND (source<>'DART' OR (
            available_date>period_end
            AND (filed IS NULL OR (filed>=period_end AND available_date=filed+1))
            AND (fs_type IN ('CFS','OFS')
                 OR (statement_type='DIVIDEND' AND fs_type='UNKNOWN'))
        ))
        AND (statement_type<>'DIVIDEND'
             OR (metric='total_cash_dividend' AND unit_type='currency')
             OR (metric IN ('payout_ratio','dividend_yield') AND unit_type='percent')
             OR (metric='cash_dividend_per_share' AND unit_type='per_share')
             OR (metric='stock_dividend_per_share' AND unit_type='shares'))
    );
END IF; END $$;
DO $$ BEGIN
IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='corporate_action'::regclass AND conname='corporate_action_critical_error_guard') THEN
ALTER TABLE corporate_action ADD CONSTRAINT corporate_action_critical_error_guard CHECK (
        quality_run_id IS NOT NULL AND btrim(source) <> ''
        AND btrim(action_key) <> '' AND btrim(action_type) <> ''
        AND btrim(status) <> ''
        AND (source NOT LIKE 'FMP_%' OR ex_date IS NOT NULL)
    );
END IF; END $$;
