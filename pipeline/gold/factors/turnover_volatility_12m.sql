-- turnover_volatility_12m daily Gold implementation.
-- value = 최근 252 KRX 거래일 log(ADV20 / market_cap)의 표본표준편차.
-- 최소 189개(75 percent) 유효 관측치를 요구한다.
-- Seed each target asset once with 270 rows (19 ADV20 seed + 251 prior
-- volatility observations), then calculate the range with set-based windows.
-- predicted_sign = -1, 따라서 rank 1은 raw value가 가장 낮은 종목이다.
WITH targets AS (
    SELECT
        p.asset_id, p.trade_date AS as_of_date,
        p.trade_date AS signal_date
    FROM public.factor_price_feature_daily p
    JOIN public.asset a
      ON a.asset_id = p.asset_id
     AND a.exchange = 'KRX'
     AND a.asset_type = 'stock'
    JOIN public.dq_run q
      ON q.run_id = p.quality_run_id
     AND q.status = 'CERTIFIED'
    WHERE p.source = 'KRX'
      AND p.market IN ('KOSPI', 'KOSDAQ')
      AND p.trade_date BETWEEN %(start_date)s::date AND %(end_date)s::date
      AND a.instrument_type = 'common_stock'
      AND a.name !~* '(스팩|SPAC)'
      AND position('리츠' in a.name) = 0
      AND p.market_cap > 0
      AND p.adj_close > 0
      AND EXISTS (
          SELECT 1
          FROM public.asset_identifier ai
          WHERE ai.asset_id = p.asset_id
            AND ai.source = 'KRX'
            AND ai.identifier_type = 'ticker'
            AND ai.valid_from <= p.trade_date
            AND (ai.valid_to IS NULL OR ai.valid_to >= p.trade_date)
      )
), target_assets AS (
    SELECT DISTINCT asset_id
    FROM targets
), history_seed AS (
    SELECT
        seed.asset_id, seed.trade_date,
        seed.trading_value, seed.market_cap
    FROM target_assets target_asset
    CROSS JOIN LATERAL (
        SELECT
            p.asset_id, p.trade_date,
            p.trading_value, p.market_cap
        FROM public.factor_price_feature_daily p
        JOIN public.dq_run q
          ON q.run_id = p.quality_run_id
         AND q.status = 'CERTIFIED'
        WHERE p.asset_id = target_asset.asset_id
          AND p.source = 'KRX'
          AND p.market IN ('KOSPI', 'KOSDAQ')
          AND p.trade_date < %(start_date)s::date
          AND EXISTS (
              SELECT 1
              FROM public.asset_identifier ai
              WHERE ai.asset_id = p.asset_id
                AND ai.source = 'KRX'
                AND ai.identifier_type = 'ticker'
                AND ai.valid_from <= p.trade_date
                AND (ai.valid_to IS NULL OR ai.valid_to >= p.trade_date)
          )
        ORDER BY p.trade_date DESC
        LIMIT 270
    ) seed
), target_observations AS (
    SELECT
        p.asset_id, p.trade_date,
        p.trading_value, p.market_cap
    FROM public.factor_price_feature_daily p
    JOIN target_assets target_asset ON target_asset.asset_id = p.asset_id
    JOIN public.dq_run q
      ON q.run_id = p.quality_run_id
     AND q.status = 'CERTIFIED'
    WHERE p.source = 'KRX'
      AND p.market IN ('KOSPI', 'KOSDAQ')
      AND p.trade_date BETWEEN %(start_date)s::date AND %(end_date)s::date
      AND EXISTS (
          SELECT 1
          FROM public.asset_identifier ai
          WHERE ai.asset_id = p.asset_id
            AND ai.source = 'KRX'
            AND ai.identifier_type = 'ticker'
            AND ai.valid_from <= p.trade_date
            AND (ai.valid_to IS NULL OR ai.valid_to >= p.trade_date)
      )
), observations AS (
    SELECT asset_id, trade_date, trading_value, market_cap FROM history_seed
    UNION ALL
    SELECT asset_id, trade_date, trading_value, market_cap FROM target_observations
), daily_turnover AS (
    SELECT
        asset_id,
        trade_date,
        market_cap,
        avg(trading_value) OVER (
            PARTITION BY asset_id ORDER BY trade_date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS adv20
    FROM observations
), log_turnover AS (
    SELECT
        asset_id,
        trade_date,
        CASE
            WHEN adv20 > 0 AND market_cap > 0
            THEN ln(adv20::double precision / market_cap::double precision)
        END AS value
    FROM daily_turnover
), rolling_volatility AS (
    SELECT
        asset_id,
        trade_date,
        count(*) OVER recent_252 AS window_rows,
        count(value) OVER recent_252 AS valid_observations,
        stddev_samp(value) OVER recent_252 AS value
    FROM log_turnover
    WINDOW recent_252 AS (
        PARTITION BY asset_id ORDER BY trade_date
        ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
    )
), raw_values AS (
    SELECT
        t.asset_id, t.as_of_date, t.signal_date,
        rolling.value
    FROM targets t
    JOIN rolling_volatility rolling
      ON rolling.asset_id = t.asset_id
     AND rolling.trade_date = t.as_of_date
    WHERE rolling.window_rows = 252
      AND rolling.valid_observations >= 189
      AND rolling.value IS NOT NULL
), ranked AS (
    SELECT
        asset_id, as_of_date, value,
        rank() OVER (PARTITION BY signal_date ORDER BY value ASC) AS rank
    FROM raw_values
)
SELECT asset_id, as_of_date, value, rank
FROM ranked;
