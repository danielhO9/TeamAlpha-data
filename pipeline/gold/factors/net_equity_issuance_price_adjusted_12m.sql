-- net_equity_issuance_price_adjusted_12m Gold implementation.
-- value = (시가총액 / 분할조정 가격)의 정확한 12개월 증가율
-- predicted_sign = -1, 따라서 rank 1은 raw value가 가장 낮은 종목이다.
WITH certified AS (
    SELECT
        p.asset_id, a.name, a.instrument_type, p.trade_date,
        p.adj_close, p.market_cap, p.market,
        row_number() OVER (
            PARTITION BY p.asset_id ORDER BY p.trade_date
        ) AS age_days,
        min(p.trade_date) OVER (PARTITION BY p.asset_id) AS first_seen
    FROM public.factor_price_feature_daily p
    JOIN public.asset a
      ON a.asset_id = p.asset_id
     AND a.exchange = 'KRX'
     AND a.asset_type = 'stock'
    JOIN public.dq_run q
      ON q.run_id = p.quality_run_id
     AND q.status = 'CERTIFIED'
    JOIN LATERAL (
        SELECT 1
        FROM public.asset_identifier ai
        WHERE ai.asset_id = p.asset_id
          AND ai.source = 'KRX'
          AND ai.identifier_type = 'ticker'
          AND ai.valid_from <= p.trade_date
          AND (ai.valid_to IS NULL OR ai.valid_to >= p.trade_date)
        ORDER BY ai.valid_from DESC
        LIMIT 1
    ) identifier ON true
    WHERE p.source = 'KRX'
      AND p.market IN ('KOSPI', 'KOSDAQ')
), monthly AS (
    SELECT
        certified.*,
        min(trade_date) OVER () AS dataset_start,
        date_trunc('month', trade_date) AS signal_month,
        row_number() OVER (
            PARTITION BY asset_id, date_trunc('month', trade_date)
            ORDER BY trade_date DESC
        ) AS month_rank
    FROM certified
), monthly_features AS (
    SELECT
        monthly.*,
        CASE
            WHEN instrument_type = 'common_stock'
             AND trade_date >= DATE '2015-01-01'
             AND market_cap > 0
             AND adj_close > 0
            THEN market_cap::double precision / adj_close::double precision
        END AS adjusted_share_base
    FROM monthly
    WHERE month_rank = 1
), lagged AS (
    SELECT
        monthly_features.*,
        lag(adjusted_share_base, 12) OVER (
            PARTITION BY asset_id ORDER BY signal_month
        ) AS prior_adjusted_share_base,
        lag(signal_month, 12) OVER (
            PARTITION BY asset_id ORDER BY signal_month
        ) AS prior_signal_month
    FROM monthly_features
), raw_values AS (
    SELECT
        asset_id,
        trade_date AS as_of_date,
        adjusted_share_base / prior_adjusted_share_base - 1 AS value,
        signal_month
    FROM lagged
    WHERE signal_month
          BETWEEN %(start_month)s::date AND %(end_month)s::date
      AND trade_date >= DATE '2015-01-01'
      AND instrument_type = 'common_stock'
      AND name !~* '(스팩|SPAC)'
      AND position('리츠' in name) = 0
      AND (age_days >= 250 OR first_seen = dataset_start)
      AND market_cap > 0
      AND adj_close > 0
      AND adjusted_share_base IS NOT NULL
      AND prior_adjusted_share_base > 0
      AND signal_month = prior_signal_month + INTERVAL '12 months'
), ranked AS (
    SELECT
        asset_id, as_of_date, value,
        rank() OVER (
            PARTITION BY signal_month ORDER BY value ASC
        ) AS rank
    FROM raw_values
)
SELECT asset_id, as_of_date, value, rank
FROM ranked;
