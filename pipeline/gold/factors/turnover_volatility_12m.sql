-- turnover_volatility_12m daily Gold implementation.
-- value = 최근 252 KRX 거래일 log(ADV20 / market_cap)의 표본표준편차.
-- 최소 189개(75 percent) 유효 관측치를 요구한다.
-- predicted_sign = -1, 따라서 rank 1은 raw value가 가장 낮은 종목이다.
WITH certified AS (
    SELECT
        p.asset_id, a.name, a.instrument_type, p.trade_date,
        p.adj_close, p.trading_value, p.market_cap, p.market,
        avg(p.trading_value) OVER (
            PARTITION BY p.asset_id ORDER BY p.trade_date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS adv20,
        row_number() OVER (
            PARTITION BY p.asset_id ORDER BY p.trade_date
        ) AS age_days
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
      AND p.trade_date <= %(end_date)s::date
), daily_values AS (
    SELECT
        certified.*,
        CASE
            WHEN adv20 > 0 AND market_cap > 0
            THEN ln(adv20::double precision / market_cap::double precision)
        END AS log_turnover
    FROM certified
), rolling_values AS (
    SELECT
        daily_values.*,
        count(*) OVER (
            PARTITION BY asset_id ORDER BY trade_date
            ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
        ) AS window_rows,
        count(log_turnover) OVER (
            PARTITION BY asset_id ORDER BY trade_date
            ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
        ) AS valid_observations,
        stddev_samp(log_turnover) OVER (
            PARTITION BY asset_id ORDER BY trade_date
            ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
        ) AS value
    FROM daily_values
), raw_values AS (
    SELECT asset_id, trade_date AS as_of_date, trade_date AS signal_date, value
    FROM rolling_values
    WHERE trade_date BETWEEN %(start_date)s::date AND %(end_date)s::date
      AND instrument_type = 'common_stock'
      AND name !~* '(스팩|SPAC)'
      AND position('리츠' in name) = 0
      AND age_days >= 250
      AND market_cap > 0
      AND adj_close > 0
      AND window_rows = 252
      AND valid_observations >= 189
      AND value IS NOT NULL
), ranked AS (
    SELECT
        asset_id, as_of_date, value,
        rank() OVER (PARTITION BY signal_date ORDER BY value ASC) AS rank
    FROM raw_values
)
SELECT asset_id, as_of_date, value, rank
FROM ranked;
