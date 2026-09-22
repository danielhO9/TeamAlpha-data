-- Additive research interfaces. No rewriting fundamental/price/return history.
CREATE TABLE dart_account_metric_map (
    account_id TEXT NOT NULL,
    statement_type TEXT NOT NULL,
    metric TEXT NOT NULL,
    mapping_version TEXT NOT NULL DEFAULT 'dart-exact-id-v1',
    PRIMARY KEY (account_id, statement_type)
);
INSERT INTO dart_account_metric_map(account_id, statement_type, metric) VALUES
 ('ifrs-full_Assets','BS','total_assets'),
 ('ifrs-full_CurrentAssets','BS','current_assets'),
 ('ifrs-full_NoncurrentAssets','BS','noncurrent_assets'),
 ('ifrs-full_Liabilities','BS','total_liabilities'),
 ('ifrs-full_CurrentLiabilities','BS','current_liabilities'),
 ('ifrs-full_NoncurrentLiabilities','BS','noncurrent_liabilities'),
 ('ifrs-full_Equity','BS','total_equity'),
 ('ifrs-full_CashAndCashEquivalents','BS','cash_and_equivalents'),
 ('ifrs-full_Inventories','BS','inventories'),
 ('ifrs-full_TradeAndOtherCurrentReceivables','BS','trade_and_other_current_receivables'),
 ('ifrs-full_PropertyPlantAndEquipment','BS','property_plant_and_equipment'),
 ('ifrs-full_Goodwill','BS','goodwill'),
 ('ifrs-full_IntangibleAssetsOtherThanGoodwill','BS','intangibles_excluding_goodwill'),
 ('ifrs-full_RetainedEarnings','BS','retained_earnings'),
 ('ifrs-full_IssuedCapital','BS','capital_stock'),
 ('ifrs-full_Revenue','IS','revenue'),
 ('ifrs-full_Revenue','CIS','revenue'),
 ('ifrs-full_CostOfSales','IS','cost_of_revenue'),
 ('ifrs-full_CostOfSales','CIS','cost_of_revenue'),
 ('ifrs-full_GrossProfit','IS','gross_profit'),
 ('ifrs-full_GrossProfit','CIS','gross_profit'),
 ('ifrs-full_ProfitLoss','IS','net_income'),
 ('ifrs-full_ProfitLoss','CIS','net_income'),
 ('ifrs-full_ProfitLossBeforeTax','IS','pretax_income'),
 ('ifrs-full_ProfitLossBeforeTax','CIS','pretax_income'),
 ('ifrs-full_IncomeTaxExpenseContinuingOperations','IS','income_tax_expense'),
 ('ifrs-full_IncomeTaxExpenseContinuingOperations','CIS','income_tax_expense'),
 ('dart_OperatingIncomeLoss','IS','operating_income'),
 ('dart_OperatingIncomeLoss','CIS','operating_income'),
 ('ifrs-full_CashFlowsFromUsedInOperatingActivities','CF','operating_cash_flow'),
 ('ifrs-full_CashFlowsFromUsedInInvestingActivities','CF','investing_cash_flow'),
 ('ifrs-full_CashFlowsFromUsedInFinancingActivities','CF','financing_cash_flow'),
 ('ifrs-full_PurchaseOfPropertyPlantAndEquipmentClassifiedAsInvestingActivities','CF','purchase_of_ppe'),
 ('ifrs-full_PurchaseOfIntangibleAssetsClassifiedAsInvestingActivities','CF','purchase_of_intangibles');

-- All filing vintages survive. Consumers must filter available_at before
-- selecting the latest filing; never use a latest-only view for a backtest.
-- No fuzzy name matching, currency conversion, sign flips, or YTD/Q mixing.
CREATE VIEW dart_standardized_statement_line AS
 SELECT f.*, m.metric, m.mapping_version,
        count(*) OVER (
          PARTITION BY f.asset_id,f.source,f.filing_id,f.fs_type,
                       f.statement_type,m.metric
        ) AS metric_candidate_count
 FROM fundamental_statement_line f
 JOIN dart_account_metric_map m
   ON replace(f.account_id, ':', '_') = m.account_id
  AND f.statement_type = m.statement_type
 JOIN dq_run q ON q.run_id=f.quality_run_id AND q.status='CERTIFIED'
 WHERE f.source='DART'
   AND coalesce(btrim(f.account_detail),'') IN ('','-');

CREATE TABLE research_observation (
    asset_id BIGINT NOT NULL REFERENCES asset(asset_id),
    source TEXT NOT NULL,
    dataset TEXT NOT NULL CHECK (dataset IN ('FMP_STATEMENT','FMP_PROFILE','DART_EVENT')),
    observation_key TEXT NOT NULL,
    available_at TIMESTAMPTZ NOT NULL,
    observed_at TIMESTAMPTZ,
    source_file TEXT NOT NULL,
    raw_row JSONB NOT NULL CHECK (jsonb_typeof(raw_row)='object'),
    metadata JSONB NOT NULL CHECK (jsonb_typeof(metadata)='object'),
    quality_run_id UUID NOT NULL REFERENCES dq_run(run_id),
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY(asset_id, source, dataset, observation_key),
    CHECK (dataset <> 'FMP_PROFILE' OR
           (observed_at IS NOT NULL AND available_at=observed_at))
);
CREATE INDEX ix_research_observation_pit
 ON research_observation(dataset,asset_id,available_at);

CREATE FUNCTION research_numeric(value TEXT) RETURNS NUMERIC
 LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE AS $$
 SELECT CASE WHEN replace(btrim(value),',','') ~ '^[+-]?([0-9]+([.][0-9]*)?|[.][0-9]+)([eE][+-]?[0-9]{1,3})?$'
             AND length(value) < 100
   THEN replace(btrim(value),',','')::numeric END
$$;

CREATE VIEW fmp_statement_line AS
 SELECT r.asset_id,r.observation_key,r.available_at,r.observed_at,
        r.metadata->>'statement_type' AS statement_type,
        (r.metadata->>'period_end')::date AS period_end,
        r.raw_row->>'period' AS fiscal_period,
        r.raw_row->>'reportedCurrency' AS reported_currency,
        e.key AS source_metric,research_numeric(e.value) AS value,
        r.source_file,r.quality_run_id
 FROM research_observation r
 JOIN dq_run q ON q.run_id=r.quality_run_id AND q.status='CERTIFIED'
 CROSS JOIN LATERAL jsonb_each_text(r.raw_row) e
 WHERE r.dataset='FMP_STATEMENT'
   AND e.key NOT IN ('symbol','cik','calendarYear','fiscalYear','year')
   AND research_numeric(e.value) IS NOT NULL;

CREATE VIEW company_profile_observation AS
 SELECT i.asset_id,i.source,i.observation_key,i.observed_at,i.available_at,
        i.industry_code,NULL::text AS sector,NULL::text AS industry_name,
        i.raw_row->>'ceo_nm' AS ceo_name,i.raw_row->>'est_dt' AS establishment_date_raw,
        i.raw_row->>'acc_mt' AS fiscal_year_end_month_raw,
        NULL::numeric AS employee_count,i.raw_row,i.quality_run_id
 FROM industry_classification_observation i
 JOIN dq_run q ON q.run_id=i.quality_run_id AND q.status='CERTIFIED'
 WHERE i.source='DART'
 UNION ALL
 SELECT r.asset_id,r.source,r.observation_key,r.observed_at,r.available_at,
        NULL,r.raw_row->>'sector',r.raw_row->>'industry',
        r.raw_row->>'ceo',NULL,NULL,
        research_numeric(r.raw_row->>'fullTimeEmployees'),r.raw_row,r.quality_run_id
 FROM research_observation r
 JOIN dq_run q ON q.run_id=r.quality_run_id AND q.status='CERTIFIED'
 WHERE r.dataset='FMP_PROFILE';

CREATE VIEW dart_corporate_event_detail AS
 SELECT r.asset_id,r.observation_key,r.available_at,
        r.raw_row->>'rcept_no' AS filing_id,r.metadata->>'event_type' AS event_type,
        r.metadata->>'action_scope' AS action_scope,
        r.raw_row->>'ic_mthn' AS issuance_method,
        research_numeric(r.raw_row->>'fdpp_fclt') AS facility_funds,
        research_numeric(r.raw_row->>'fdpp_bsninh') AS business_acquisition_funds,
        research_numeric(r.raw_row->>'fdpp_op') AS operating_funds,
        research_numeric(r.raw_row->>'fdpp_dtrp') AS debt_repayment_funds,
        research_numeric(r.raw_row->>'fdpp_ocsa') AS equity_acquisition_funds,
        research_numeric(r.raw_row->>'fdpp_etc') AS other_funds,
        research_numeric(r.raw_row->>'nstk_ostk_cnt') AS new_common_shares,
        r.raw_row->>'mg_pp' AS merger_purpose,
        r.raw_row->>'mgptncmp_cmpnm' AS counterparty_name,
        r.raw_row->>'mgptncmp_rl_cmpn' AS counterparty_relationship,
        r.raw_row->>'mg_rt_bs' AS merger_valuation_basis,
        r.raw_row,r.metadata,r.source_file,r.quality_run_id
 FROM research_observation r
 JOIN dq_run q ON q.run_id=r.quality_run_id AND q.status='CERTIFIED'
 WHERE r.dataset='DART_EVENT';

-- Explicit-file replay checkpoints, versioned separately from provider ingestion.
CREATE TABLE research_input_checkpoint (
 source_file TEXT NOT NULL,
 content_sha256 TEXT NOT NULL,
 transform_version TEXT NOT NULL,
 quality_run_id UUID NOT NULL REFERENCES dq_run(run_id),
 row_count BIGINT NOT NULL,
 excluded_row_count BIGINT NOT NULL DEFAULT 0,
 PRIMARY KEY(source_file,content_sha256,transform_version)
);
