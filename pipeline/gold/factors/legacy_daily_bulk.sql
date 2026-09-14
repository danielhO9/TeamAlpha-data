-- Shared daily implementation for every legacy non-REJECTED Gold factor.
-- Month windows are translated to 21 KRX sessions per month.  Price history,
-- PIT financial state, and rolling primitives are built once per range.
-- gold-statement
CREATE TEMP TABLE _gold_price_base ON COMMIT PRESERVE ROWS AS
WITH history_start AS (
    SELECT min(trade_date) AS trade_date
    FROM (
        SELECT DISTINCT p.trade_date
        FROM public.factor_price_feature_daily p
        JOIN public.dq_run q
          ON q.run_id = p.quality_run_id AND q.status = 'CERTIFIED'
        WHERE p.source = 'KRX' AND p.market IN ('KOSPI','KOSDAQ')
          AND p.trade_date <= %(start_date)s::date
        ORDER BY p.trade_date DESC LIMIT 758
    ) dates
), certified AS (
    SELECT
        p.asset_id, p.trade_date, p.open::double precision AS open,
        p.high::double precision AS high, p.low::double precision AS low,
        p.close::double precision AS close,
        p.adj_close::double precision AS adj_close,
        p.volume::double precision AS volume,
        p.trading_value::double precision AS trading_value,
        p.shares::double precision AS shares,
        p.market_cap::double precision AS market_cap, p.market,
        a.name, a.instrument_type,
        row_number() OVER (PARTITION BY p.asset_id ORDER BY p.trade_date) AS age_days,
        lag(p.close::double precision) OVER asset_days AS previous_close,
        lag(p.adj_close::double precision) OVER asset_days AS previous_adj_close,
        lag(p.market_cap::double precision) OVER asset_days AS previous_market_cap,
        lag(p.market) OVER asset_days AS previous_market
    FROM public.factor_price_feature_daily p
    CROSS JOIN history_start h
    JOIN public.asset a
      ON a.asset_id = p.asset_id
     AND a.exchange = 'KRX' AND a.asset_type = 'stock'
    JOIN public.dq_run q
      ON q.run_id = p.quality_run_id AND q.status = 'CERTIFIED'
    WHERE p.source = 'KRX'
      AND p.market IN ('KOSPI', 'KOSDAQ')
      AND p.trade_date >= h.trade_date
      AND p.trade_date <= %(end_date)s::date
    WINDOW asset_days AS (PARTITION BY p.asset_id ORDER BY p.trade_date)
)
SELECT
    c.*,
    CASE WHEN c.previous_adj_close > 0
         THEN c.adj_close / c.previous_adj_close - 1.0 END AS daily_return,
    CASE WHEN c.high > c.low
         THEN (c.close - c.low) / (c.high - c.low) END AS close_position,
    CASE WHEN c.open > 0 THEN c.close / c.open - 1.0 END AS open_close_return,
    CASE WHEN c.previous_close > 0
         THEN c.open / c.previous_close - 1.0 END AS overnight_return,
    CASE WHEN c.market_cap > 0 AND c.adj_close > 0
         THEN c.market_cap / c.adj_close END AS share_base,
    CASE WHEN c.shares > 0 THEN c.volume / c.shares END AS share_turnover
FROM certified c;

-- gold-statement
CREATE INDEX ON _gold_price_base(asset_id, trade_date);
-- gold-statement
ANALYZE _gold_price_base;

-- gold-statement
CREATE TEMP TABLE _gold_price_roll_1 ON COMMIT PRESERVE ROWS AS
SELECT
    p.*,
    avg(trading_value) OVER w20 AS adv20,
    count(trading_value) OVER w20 AS adv20_n,
    max(daily_return) OVER w21 AS max_daily_return_21d,
    count(daily_return) OVER w21 AS max_return_n_21,
    stddev_samp(daily_return) OVER w252 AS daily_volatility_252d,
    avg(close_position) OVER w252 AS close_position_mean_252d,
    count(close_position) OVER w252 AS close_position_n_252,
    avg(open_close_return) OVER w252 AS open_close_drift_252d,
    count(open_close_return) OVER w252 AS open_close_n_252,
    avg(overnight_return) OVER w126 AS overnight_gap_mean_126d,
    count(overnight_return) OVER w126 AS overnight_n_126,
    stddev_samp(overnight_return) OVER w252 AS overnight_gap_volatility_252d,
    count(overnight_return) OVER w252 AS overnight_n_252,
    stddev_samp(CASE WHEN market_cap>0 THEN ln(market_cap) END) OVER w504 AS market_cap_instability_504d,
    count(CASE WHEN market_cap>0 THEN 1 END) OVER w504 AS market_cap_n_504,
    max(CASE WHEN adj_close>0 THEN adj_close END) OVER w252
      / nullif(min(CASE WHEN adj_close>0 THEN adj_close END) OVER w252, 0) - 1.0 AS price_range_252d,
    count(CASE WHEN adj_close>0 THEN 1 END) OVER w252 AS price_n_252,
    max(adj_close) OVER w252 AS high_252d,
    avg(share_turnover) OVER w21 AS share_turnover_21d,
    count(share_turnover) OVER w21 AS share_turnover_n_21,
    count(daily_return) OVER w252 AS return_n_252,
    sum(daily_return) OVER w252 AS return_s1_252,
    sum(power(daily_return, 2)) OVER w252 AS return_s2_252,
    sum(power(daily_return, 3)) OVER w252 AS return_s3_252,
    count(daily_return) OVER w756 AS return_n_756,
    sum(daily_return) OVER w756 AS return_s1_756,
    sum(power(daily_return, 2)) OVER w756 AS return_s2_756,
    sum(power(daily_return, 3)) OVER w756 AS return_s3_756,
    CASE WHEN previous_market_cap > 0 AND daily_return IS NOT NULL
         THEN daily_return * previous_market_cap END AS weighted_return,
    CASE WHEN previous_market_cap > 0 AND daily_return IS NOT NULL
         THEN previous_market_cap END AS return_weight
FROM _gold_price_base p
WINDOW
    w20 AS (PARTITION BY asset_id ORDER BY trade_date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),
    w21 AS (PARTITION BY asset_id ORDER BY trade_date ROWS BETWEEN 20 PRECEDING AND CURRENT ROW),
    w126 AS (PARTITION BY asset_id ORDER BY trade_date ROWS BETWEEN 125 PRECEDING AND CURRENT ROW),
    w252 AS (PARTITION BY asset_id ORDER BY trade_date ROWS BETWEEN 251 PRECEDING AND CURRENT ROW),
    w504 AS (PARTITION BY asset_id ORDER BY trade_date ROWS BETWEEN 503 PRECEDING AND CURRENT ROW),
    w756 AS (PARTITION BY asset_id ORDER BY trade_date ROWS BETWEEN 755 PRECEDING AND CURRENT ROW);

-- gold-statement
CREATE INDEX ON _gold_price_roll_1(asset_id, trade_date);
-- gold-statement
ANALYZE _gold_price_roll_1;

-- gold-statement
CREATE TEMP TABLE _gold_market_returns ON COMMIT PRESERVE ROWS AS
SELECT trade_date, previous_market AS market,
       sum(weighted_return) / nullif(sum(return_weight), 0) AS market_return
FROM _gold_price_roll_1
WHERE previous_market IS NOT NULL
GROUP BY trade_date, previous_market;

-- gold-statement
CREATE TEMP TABLE _gold_price_roll_2 ON COMMIT PRESERVE ROWS AS
WITH enriched AS (
    SELECT p.*,
           m.market_return,
           p.daily_return * p.daily_return AS r2,
           p.daily_return * m.market_return AS rm,
           m.market_return * m.market_return AS m2,
           p.adj_close / nullif(p.high_252d, 0) - 1.0 AS price_high_gap
    FROM _gold_price_roll_1 p
    LEFT JOIN _gold_market_returns m
      ON m.trade_date = p.trade_date AND m.market = p.previous_market
), rolled AS (
    SELECT e.*,
        lag(adj_close, 21) OVER days AS close_lag_21,
        lag(adj_close, 252) OVER days AS close_lag_252,
        lag(share_base, 252) OVER days AS share_base_lag_252,
        lag(share_base, 756) OVER days AS share_base_lag_756,
        lag(share_turnover_21d, 126) OVER days AS share_turnover_lag_126,
        lag(share_turnover_21d, 252) OVER days AS share_turnover_lag_252,
        lag(daily_volatility_252d, 504) OVER days AS volatility_lag_504,
        avg(max_daily_return_21d) OVER w126 AS max_return_mean_126d,
        count(max_daily_return_21d) OVER w126 AS max_return_n_126,
        stddev_samp(max_daily_return_21d) OVER w378 AS max_return_instability_378d,
        count(max_daily_return_21d) OVER w378 AS max_return_n_378,
        stddev_samp(daily_volatility_252d) OVER w126 AS volatility_instability_126d,
        count(daily_volatility_252d) OVER w126 AS volatility_n_126,
        stddev_samp(price_high_gap) OVER w504 AS price_high_gap_volatility_504d,
        count(price_high_gap) OVER w504 AS price_high_gap_n_504,
        count(daily_return) OVER w504 AS paired_n,
        sum(daily_return) OVER w504 AS r1_sum,
        sum(market_return) OVER w504 AS m1_sum,
        sum(r2) OVER w504 AS r2_sum,
        sum(rm) OVER w504 AS rm_sum,
        sum(m2) OVER w504 AS m2_sum
    FROM enriched e
    WINDOW
        days AS (PARTITION BY asset_id ORDER BY trade_date),
        w126 AS (PARTITION BY asset_id ORDER BY trade_date ROWS BETWEEN 125 PRECEDING AND CURRENT ROW),
        w378 AS (PARTITION BY asset_id ORDER BY trade_date ROWS BETWEEN 377 PRECEDING AND CURRENT ROW),
        w504 AS (PARTITION BY asset_id ORDER BY trade_date ROWS BETWEEN 503 PRECEDING AND CURRENT ROW)
)
SELECT r.*,
    CASE WHEN return_n_252 >= 189 THEN
      sqrt(return_n_252 * (return_n_252 - 1.0)) / (return_n_252 - 2.0)
      * ((return_s3_252 - 3.0 * return_s1_252 * return_s2_252 / return_n_252
          + 2.0 * power(return_s1_252, 3) / power(return_n_252, 2)) / return_n_252)
      / nullif(power((return_s2_252 - power(return_s1_252, 2) / return_n_252)
          / return_n_252, 1.5), 0) END AS return_skewness_252d,
    CASE WHEN return_n_756 >= 567 THEN
      sqrt(return_n_756 * (return_n_756 - 1.0)) / (return_n_756 - 2.0)
      * ((return_s3_756 - 3.0 * return_s1_756 * return_s2_756 / return_n_756
          + 2.0 * power(return_s1_756, 3) / power(return_n_756, 2)) / return_n_756)
      / nullif(power((return_s2_756 - power(return_s1_756, 2) / return_n_756)
          / return_n_756, 1.5), 0) END AS return_skewness_756d,
    CASE WHEN paired_n >= 378 THEN sqrt(greatest(0.0,
      (r2_sum - r1_sum * r1_sum / paired_n) / (paired_n - 1.0)
      - power((rm_sum - r1_sum * m1_sum / paired_n) / (paired_n - 1.0), 2)
        / nullif((m2_sum - m1_sum * m1_sum / paired_n) / (paired_n - 1.0), 0)
    )) END AS idiosyncratic_volatility_504d
FROM rolled r;

-- gold-statement
CREATE INDEX ON _gold_price_roll_2(asset_id, trade_date);
-- gold-statement
ANALYZE _gold_price_roll_2;

-- gold-statement
CREATE TEMP TABLE _gold_financial_states ON COMMIT PRESERVE ROWS AS
WITH target_assets AS (
    SELECT DISTINCT asset_id FROM _gold_price_base
), bounds AS (
    SELECT min(trade_date) AS history_start FROM _gold_price_base
), relevant_events AS (
    SELECT DISTINCT f.asset_id, f.available_date AS state_date
    FROM public.fundamental f
    JOIN target_assets a USING (asset_id)
    CROSS JOIN bounds b
    JOIN public.dq_run q ON q.run_id = f.quality_run_id AND q.status = 'CERTIFIED'
    WHERE f.source = 'DART' AND f.data_basis = 'STANDARDIZED'
      AND f.unit_type = 'currency' AND f.value IS NOT NULL
      AND f.available_date >= b.history_start
      AND f.available_date <= %(end_date)s::date
      AND f.metric IN (
        'total_equity','total_assets','capital_stock','current_assets',
        'current_liabilities','total_liabilities','noncurrent_assets',
        'retained_earnings','revenue','operating_income','net_income','pretax_income'
      )
), prior_events AS (
    SELECT f.asset_id, max(f.available_date) AS state_date
    FROM public.fundamental f
    JOIN target_assets a USING (asset_id)
    CROSS JOIN bounds b
    JOIN public.dq_run q ON q.run_id = f.quality_run_id AND q.status = 'CERTIFIED'
    WHERE f.source = 'DART' AND f.data_basis = 'STANDARDIZED'
      AND f.unit_type = 'currency' AND f.value IS NOT NULL
      AND f.available_date < b.history_start
      AND f.metric IN (
        'total_equity','total_assets','capital_stock','current_assets',
        'current_liabilities','total_liabilities','noncurrent_assets',
        'retained_earnings','revenue','operating_income','net_income','pretax_income'
      )
    GROUP BY f.asset_id
), events AS (
    SELECT * FROM relevant_events
    UNION SELECT * FROM prior_events
), intervals AS (
    SELECT asset_id, state_date,
           lead(state_date) OVER (PARTITION BY asset_id ORDER BY state_date) AS next_state_date
    FROM events
)
SELECT i.*, stocks.*, flows.*
FROM intervals i
LEFT JOIN LATERAL (
    SELECT
      max(value) FILTER (WHERE metric='total_equity') AS total_equity,
      max(value) FILTER (WHERE metric='total_assets') AS total_assets,
      max(value) FILTER (WHERE metric='capital_stock') AS capital_stock,
      max(value) FILTER (WHERE metric='current_assets') AS current_assets,
      max(value) FILTER (WHERE metric='current_liabilities') AS current_liabilities,
      max(value) FILTER (WHERE metric='total_liabilities') AS total_liabilities,
      max(value) FILTER (WHERE metric='noncurrent_assets') AS noncurrent_assets,
      max(value) FILTER (WHERE metric='retained_earnings') AS retained_earnings
    FROM (
      SELECT DISTINCT ON (f.metric) f.metric, f.value::double precision AS value
      FROM public.fundamental f
      JOIN public.dq_run q ON q.run_id=f.quality_run_id AND q.status='CERTIFIED'
      WHERE f.asset_id=i.asset_id AND f.available_date<=i.state_date
        AND f.source='DART' AND f.data_basis='STANDARDIZED'
        AND f.unit_type='currency' AND f.value IS NOT NULL
        AND f.metric IN ('total_equity','total_assets','capital_stock','current_assets',
          'current_liabilities','total_liabilities','noncurrent_assets','retained_earnings')
      ORDER BY f.metric, f.period_end DESC,
        CASE f.fiscal_period WHEN 'Q4' THEN 5 WHEN 'FY' THEN 4 WHEN 'Q3' THEN 3
          WHEN 'Q2' THEN 2 WHEN 'Q1' THEN 1 ELSE 0 END DESC,
        (f.fs_type='CFS') DESC, f.available_date DESC, f.revision_key DESC
    ) latest
) stocks ON true
LEFT JOIN LATERAL (
  WITH selected AS (
    SELECT DISTINCT ON (f.metric,f.period_end,f.fiscal_period)
      f.metric,f.period_end,f.fiscal_period,f.value::double precision AS value
    FROM public.fundamental f
    JOIN public.dq_run q ON q.run_id=f.quality_run_id AND q.status='CERTIFIED'
    WHERE f.asset_id=i.asset_id AND f.available_date<=i.state_date
      AND f.source='DART' AND f.data_basis='STANDARDIZED'
      AND f.unit_type='currency' AND f.value IS NOT NULL
      AND f.metric IN ('revenue','operating_income','net_income','pretax_income')
    ORDER BY f.metric,f.period_end,f.fiscal_period,
      (f.fs_type='CFS') DESC,f.available_date DESC,f.revision_key DESC
  ), direct_quarters AS (
    SELECT metric,period_end,fiscal_period,value,1 AS priority
    FROM selected WHERE fiscal_period IN ('Q1','Q2','Q3','Q4')
  ), fiscal_years AS (
    SELECT metric,period_end AS fy_end,value AS fy_value,
      lag(period_end) OVER (PARTITION BY metric ORDER BY period_end) AS previous_fy_end
    FROM selected WHERE fiscal_period='FY'
  ), fy_quarters AS (
    SELECT fy.metric,fy.fy_end,fy.fy_value,q.fiscal_period,q.value,
      row_number() OVER (PARTITION BY fy.metric,fy.fy_end,q.fiscal_period ORDER BY q.period_end DESC) AS qrank
    FROM fiscal_years fy JOIN selected q ON q.metric=fy.metric
      AND q.fiscal_period IN ('Q1','Q2','Q3') AND q.period_end<fy.fy_end
      AND q.period_end>coalesce(fy.previous_fy_end,fy.fy_end-interval '370 days')
  ), derived_q4 AS (
    SELECT metric,fy_end AS period_end,'Q4'::text AS fiscal_period,
      max(fy_value)-sum(value) AS value,0 AS priority
    FROM fy_quarters fq WHERE qrank=1 AND NOT EXISTS (
      SELECT 1 FROM direct_quarters d
      WHERE d.metric=fq.metric AND d.period_end=fq.fy_end AND d.fiscal_period='Q4')
    GROUP BY metric,fy_end HAVING count(DISTINCT fiscal_period)=3
  ), quarters AS (
    SELECT * FROM direct_quarters UNION ALL SELECT * FROM derived_q4
  ), unique_periods AS (
    SELECT *,row_number() OVER (PARTITION BY metric,period_end ORDER BY priority DESC) AS period_rank
    FROM quarters
  ), recent AS (
    SELECT *,row_number() OVER (PARTITION BY metric ORDER BY period_end DESC) AS recent_rank
    FROM unique_periods WHERE period_rank=1
  ), ttm AS (
    SELECT metric,sum(value) AS value FROM recent WHERE recent_rank<=4
    GROUP BY metric HAVING count(*)=4 AND max(period_end)-min(period_end)<=370
  )
  SELECT
    max(value) FILTER (WHERE metric='revenue') AS revenue_ttm,
    max(value) FILTER (WHERE metric='operating_income') AS operating_income_ttm,
    max(value) FILTER (WHERE metric='net_income') AS net_income_ttm,
    max(value) FILTER (WHERE metric='pretax_income') AS pretax_income_ttm
  FROM ttm
) flows ON true;

-- gold-statement
CREATE INDEX ON _gold_financial_states(asset_id, state_date, next_state_date);
-- gold-statement
ANALYZE _gold_financial_states;

-- gold-statement
CREATE TEMP TABLE _gold_daily_panel_1 ON COMMIT PRESERVE ROWS AS
SELECT p.*,
  f.total_equity,f.total_assets,f.capital_stock,f.current_assets,
  f.current_liabilities,f.total_liabilities,f.noncurrent_assets,
  f.retained_earnings,f.revenue_ttm,f.operating_income_ttm,
  f.net_income_ttm,f.pretax_income_ttm,
  f.total_equity/nullif(p.market_cap,0) AS book_to_market,
  f.revenue_ttm/nullif(p.market_cap+f.total_liabilities,0) AS enterprise_sales_yield,
  f.pretax_income_ttm/nullif(p.market_cap,0) AS pretax_yield,
  f.net_income_ttm/nullif(f.revenue_ttm,0) AS net_margin,
  f.retained_earnings/nullif(f.total_assets,0) AS retained_assets_ratio
FROM _gold_price_roll_2 p
LEFT JOIN _gold_financial_states f ON f.asset_id=p.asset_id
 AND p.trade_date>=f.state_date
 AND (f.next_state_date IS NULL OR p.trade_date<f.next_state_date);

-- gold-statement
CREATE INDEX ON _gold_daily_panel_1(asset_id, trade_date);
-- gold-statement
ANALYZE _gold_daily_panel_1;

-- gold-statement
CREATE TEMP TABLE _gold_daily_panel ON COMMIT PRESERVE ROWS AS
SELECT p.*,
  lag(book_to_market,126) OVER days AS book_to_market_lag_126,
  lag(book_to_market,252) OVER days AS book_to_market_lag_252,
  lag(capital_stock,378) OVER days AS capital_stock_lag_378,
  lag(enterprise_sales_yield,126) OVER days AS enterprise_sales_yield_lag_126,
  lag(pretax_yield,126) OVER days AS pretax_yield_lag_126,
  stddev_samp(net_margin) OVER w252 AS net_margin_volatility_252d,
  count(net_margin) OVER w252 AS net_margin_n_252,
  stddev_samp(retained_assets_ratio) OVER w252 AS retained_assets_volatility_252d,
  count(retained_assets_ratio) OVER w252 AS retained_assets_n_252
FROM _gold_daily_panel_1 p
WINDOW
  days AS (PARTITION BY asset_id ORDER BY trade_date),
  w252 AS (PARTITION BY asset_id ORDER BY trade_date ROWS BETWEEN 251 PRECEDING AND CURRENT ROW);

-- gold-statement
CREATE TEMP TABLE _gold_bulk_values ON COMMIT DROP AS
WITH eligible AS (
  SELECT * FROM _gold_daily_panel e
  WHERE trade_date BETWEEN %(start_date)s::date AND %(end_date)s::date
    AND instrument_type='common_stock' AND name !~* '(스팩|SPAC)'
    AND position('리츠' in name)=0 AND age_days>=250
    AND market_cap>0 AND adj_close>0
    AND EXISTS (
      SELECT 1 FROM public.asset_identifier ai
      WHERE ai.asset_id=e.asset_id
        AND ai.source='KRX' AND ai.identifier_type='ticker'
        AND ai.valid_from<=e.trade_date
        AND (ai.valid_to IS NULL OR ai.valid_to>=e.trade_date)
    )
), raw AS (
  SELECT factor_key,asset_id,trade_date AS as_of_date,value
  FROM eligible e
  CROSS JOIN LATERAL (VALUES
    ('adv20_to_book_equity', CASE WHEN e.adv20_n=20 AND e.total_equity>0 THEN e.adv20/e.total_equity END),
    ('asset_to_market', e.total_assets/nullif(e.market_cap,0)),
    ('book_to_market_change_12m', e.book_to_market-e.book_to_market_lag_252),
    ('book_to_market_change_6m', e.book_to_market-e.book_to_market_lag_126),
    ('capital_stock_growth_18m', e.capital_stock/nullif(e.capital_stock_lag_378,0)-1.0),
    ('capital_stock_to_assets', CASE WHEN e.total_assets>0 THEN e.capital_stock/e.total_assets END),
    ('close_position_mean_12m', CASE WHEN e.close_position_n_252>=189 THEN e.close_position_mean_252d END),
    ('current_asset_turnover', CASE WHEN e.current_assets>0 THEN e.revenue_ttm/e.current_assets END),
    ('current_liabilities_to_sales', CASE WHEN e.revenue_ttm>0 THEN e.current_liabilities/e.revenue_ttm END),
    ('enterprise_sales_yield_change_6m', e.enterprise_sales_yield-e.enterprise_sales_yield_lag_126),
    ('idiosyncratic_volatility_24m', e.idiosyncratic_volatility_504d),
    ('market_cap_instability_24m', CASE WHEN e.market_cap_n_504>=378 THEN e.market_cap_instability_504d END),
    ('max_daily_return_1m', CASE WHEN e.max_return_n_21>=10 THEN e.max_daily_return_21d END),
    ('max_daily_return_instability_18m', CASE WHEN e.max_return_n_378>=273 THEN e.max_return_instability_378d END),
    ('max_daily_return_mean_6m', CASE WHEN e.max_return_n_126>=84 THEN e.max_return_mean_126d END),
    ('momentum_12_1', e.close_lag_21/nullif(e.close_lag_252,0)-1.0),
    ('net_equity_issuance_price_adjusted_12m', e.share_base/nullif(e.share_base_lag_252,0)-1.0),
    ('net_equity_issuance_price_adjusted_36m', e.share_base/nullif(e.share_base_lag_756,0)-1.0),
    ('net_income_to_liabilities', CASE WHEN e.total_liabilities>0 THEN e.net_income_ttm/e.total_liabilities END),
    ('net_margin_volatility_12m', CASE WHEN e.net_margin_n_252>=189 THEN e.net_margin_volatility_252d END),
    ('net_working_capital_yield', (e.current_assets-e.current_liabilities)/nullif(e.market_cap,0)),
    ('nonoperating_burden_margin', (e.operating_income_ttm-e.net_income_ttm)/nullif(e.revenue_ttm,0)),
    ('open_close_drift_12m', CASE WHEN e.open_close_n_252>=189 THEN e.open_close_drift_252d END),
    ('operating_earnings_yield', e.operating_income_ttm/nullif(e.market_cap,0)),
    ('operating_income_to_current_liabilities', CASE WHEN e.current_liabilities>0 THEN e.operating_income_ttm/e.current_liabilities END),
    ('operating_income_to_liabilities', CASE WHEN e.total_liabilities>0 THEN e.operating_income_ttm/e.total_liabilities END),
    ('overnight_gap_mean_6m', CASE WHEN e.overnight_n_126>=84 THEN e.overnight_gap_mean_126d END),
    ('overnight_gap_volatility_12m', CASE WHEN e.overnight_n_252>=189 THEN e.overnight_gap_volatility_252d END),
    ('pretax_yield_change_6m', e.pretax_yield-e.pretax_yield_lag_126),
    ('price_high_gap_volatility_24m', CASE WHEN e.price_high_gap_n_504>=378 THEN e.price_high_gap_volatility_504d END),
    ('price_range_12m', CASE WHEN e.price_n_252=252 THEN e.price_range_252d END),
    ('realized_daily_volatility_change_24m', e.daily_volatility_252d/nullif(e.volatility_lag_504,0)-1.0),
    ('realized_daily_volatility_instability_6m', CASE WHEN e.volatility_n_126>=84 THEN e.volatility_instability_126d END),
    ('realized_volatility_252d', CASE WHEN e.return_n_252>=126 THEN e.daily_volatility_252d END),
    ('retained_earnings_to_assets_volatility_12m', CASE WHEN e.retained_assets_n_252>=189 THEN e.retained_assets_volatility_252d END),
    ('retained_earnings_to_equity', CASE WHEN e.total_equity>0 THEN e.retained_earnings/e.total_equity END),
    ('return_skewness_12m', e.return_skewness_252d),
    ('return_skewness_36m', e.return_skewness_756d),
    ('revenue_scale', e.revenue_ttm),
    ('revenue_to_noncurrent_assets', CASE WHEN e.noncurrent_assets>0 THEN e.revenue_ttm/e.noncurrent_assets END),
    ('shares_to_capital_stock', e.shares/nullif(e.capital_stock,0)),
    ('share_turnover_change_12m', e.share_turnover_21d/nullif(e.share_turnover_lag_252,0)-1.0),
    ('share_turnover_change_6m', e.share_turnover_21d/nullif(e.share_turnover_lag_126,0)-1.0)
  ) v(factor_key,value)
  WHERE value IS NOT NULL AND value NOT IN ('Infinity'::float8,'-Infinity'::float8)
), ranked AS (
  SELECT r.*, rank() OVER (
    PARTITION BY r.factor_key,r.as_of_date
    ORDER BY r.value * m.predicted_sign DESC
  ) AS rank
  FROM raw r JOIN _gold_factor_ids m USING (factor_key)
)
SELECT m.factor_id,r.asset_id,r.as_of_date,r.value,r.rank
FROM ranked r JOIN _gold_factor_ids m USING (factor_key);
