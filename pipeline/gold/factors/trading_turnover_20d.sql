-- trading_turnover_20d daily Gold implementation.
-- value = current row 포함 최근 20 KRX 거래행 평균 거래대금 / 시가총액
-- predicted_sign = -1, 따라서 rank 1은 raw value가 가장 낮은 종목이다.
-- Read each target asset's seed history once, then calculate every target date
-- with set-based windows. The old query repeated a 250-row probe per output row.
WITH targets AS (
    SELECT
        p.asset_id, p.trade_date AS as_of_date,
        p.trade_date AS signal_date, p.market_cap
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
    SELECT seed.asset_id, seed.trade_date, seed.trading_value
    FROM target_assets target_asset
    CROSS JOIN LATERAL (
        SELECT p.asset_id, p.trade_date, p.trading_value
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
        LIMIT 249
    ) seed
), target_observations AS (
    SELECT p.asset_id, p.trade_date, p.trading_value
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
    SELECT asset_id, trade_date, trading_value FROM history_seed
    UNION ALL
    SELECT asset_id, trade_date, trading_value FROM target_observations
), rolling_values AS (
    SELECT
        asset_id,
        trade_date,
        count(*) OVER lifetime AS age_rows,
        avg(trading_value) OVER recent_20 AS adv20
    FROM observations
    WINDOW
        lifetime AS (
            PARTITION BY asset_id ORDER BY trade_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ),
        recent_20 AS (
            PARTITION BY asset_id ORDER BY trade_date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        )
), raw_values AS (
    SELECT
        t.asset_id, t.as_of_date, t.signal_date,
        rolling.adv20::double precision
            / t.market_cap::double precision AS value
    FROM targets t
    JOIN rolling_values rolling
      ON rolling.asset_id = t.asset_id
     AND rolling.trade_date = t.as_of_date
    WHERE rolling.age_rows >= 250
      AND rolling.adv20 IS NOT NULL
), ranked AS (
    SELECT
        asset_id, as_of_date, value,
        rank() OVER (
            PARTITION BY signal_date ORDER BY value ASC
        ) AS rank
    FROM raw_values
)
SELECT asset_id, as_of_date, value, rank
FROM ranked;
