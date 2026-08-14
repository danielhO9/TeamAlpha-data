-- amihud_illiquidity_1m Gold implementation.
-- value = 월중 평균 |일별 분할조정 가격수익률| / 거래대금 (유효 관측 10개 이상)
-- predicted_sign = +1, 따라서 rank 1은 raw value가 가장 높은 종목이다.
WITH certified AS (
    SELECT
        p.asset_id, a.name, a.instrument_type, p.trade_date,
        p.adj_close, p.trading_value, p.market_cap, p.market,
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
), feature_inputs AS (
    SELECT
        certified.*,
        CASE
            WHEN instrument_type = 'common_stock'
             AND trade_date >= DATE '2015-01-01'
            THEN adj_close
        END AS certified_feature_price
    FROM certified
), daily_returns AS (
    SELECT
        feature_inputs.*,
        CASE
            WHEN lag(certified_feature_price) OVER (
                     PARTITION BY asset_id ORDER BY trade_date
                 ) > 0
             AND certified_feature_price > 0
            THEN certified_feature_price / lag(certified_feature_price) OVER (
                     PARTITION BY asset_id ORDER BY trade_date
                 ) - 1
        END AS daily_price_return
    FROM feature_inputs
), daily_features AS (
    SELECT
        daily_returns.*,
        avg(abs(daily_price_return) / trading_value) FILTER (
            WHERE daily_price_return IS NOT NULL
              AND trading_value > 0
        ) OVER (
            PARTITION BY asset_id, date_trunc('month', trade_date)
        ) AS amihud_illiquidity_1m,
        count(daily_price_return) FILTER (
            WHERE trading_value > 0
        ) OVER (
            PARTITION BY asset_id, date_trunc('month', trade_date)
        ) AS amihud_observations_1m
    FROM daily_returns
), monthly AS (
    SELECT
        daily_features.*,
        min(trade_date) OVER () AS dataset_start,
        row_number() OVER (
            PARTITION BY asset_id, date_trunc('month', trade_date)
            ORDER BY trade_date DESC
        ) AS month_rank
    FROM daily_features
), raw_values AS (
    SELECT
        asset_id,
        trade_date AS as_of_date,
        amihud_illiquidity_1m::double precision AS value,
        date_trunc('month', trade_date) AS signal_month
    FROM monthly
    WHERE month_rank = 1
      AND date_trunc('month', trade_date)
          BETWEEN %(start_month)s::date AND %(end_month)s::date
      AND trade_date >= DATE '2015-01-01'
      AND instrument_type = 'common_stock'
      AND name !~* '(스팩|SPAC)'
      AND position('리츠' in name) = 0
      AND (age_days >= 250 OR first_seen = dataset_start)
      AND market_cap > 0
      AND adj_close > 0
      AND amihud_observations_1m >= 10
      AND amihud_illiquidity_1m IS NOT NULL
), ranked AS (
    SELECT
        asset_id, as_of_date, value,
        rank() OVER (
            PARTITION BY signal_month ORDER BY value DESC
        ) AS rank
    FROM raw_values
)
SELECT asset_id, as_of_date, value, rank
FROM ranked;
