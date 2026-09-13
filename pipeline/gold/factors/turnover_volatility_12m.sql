-- turnover_volatility_12m daily Gold implementation.
-- value = 최근 252 KRX 거래일 log(ADV20 / market_cap)의 표본표준편차.
-- 최소 189개(75 percent) 유효 관측치를 요구한다.
-- The indexed 271-row probe is exactly 19 ADV20 seed rows plus 252 values.
-- predicted_sign = -1, 따라서 rank 1은 raw value가 가장 낮은 종목이다.
WITH targets AS (
    SELECT
        p.asset_id, a.name, a.instrument_type,
        p.trade_date AS as_of_date, p.trade_date AS signal_date,
        p.adj_close, p.market_cap
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
      AND p.trade_date BETWEEN %(start_date)s::date AND %(end_date)s::date
      AND a.instrument_type = 'common_stock'
      AND a.name !~* '(스팩|SPAC)'
      AND position('리츠' in a.name) = 0
      AND p.market_cap > 0
      AND p.adj_close > 0
), raw_values AS (
    SELECT
        t.asset_id, t.as_of_date, t.signal_date, history.value
    FROM targets t
    JOIN LATERAL (
        SELECT
            count(*) AS window_rows,
            count(log_turnover) AS valid_observations,
            stddev_samp(log_turnover) AS value
        FROM (
            SELECT log_turnover
            FROM (
                SELECT
                    trade_date,
                    CASE
                        WHEN adv20 > 0 AND market_cap > 0
                        THEN ln(
                            adv20::double precision
                            / market_cap::double precision
                        )
                    END AS log_turnover
                FROM (
                    SELECT
                        observations.trade_date,
                        observations.market_cap,
                        avg(observations.trading_value) OVER (
                            ORDER BY observations.trade_date
                            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                        ) AS adv20
                    FROM (
                        SELECT
                            p.trade_date, p.trading_value, p.market_cap
                        FROM public.factor_price_feature_daily p
                        JOIN public.dq_run q
                          ON q.run_id = p.quality_run_id
                         AND q.status = 'CERTIFIED'
                        WHERE p.asset_id = t.asset_id
                          AND p.source = 'KRX'
                          AND p.market IN ('KOSPI', 'KOSDAQ')
                          AND p.trade_date <= t.as_of_date
                          AND EXISTS (
                              SELECT 1
                              FROM public.asset_identifier ai
                              WHERE ai.asset_id = p.asset_id
                                AND ai.source = 'KRX'
                                AND ai.identifier_type = 'ticker'
                                AND ai.valid_from <= p.trade_date
                                AND (
                                    ai.valid_to IS NULL
                                    OR ai.valid_to >= p.trade_date
                                )
                          )
                        ORDER BY p.trade_date DESC
                        LIMIT 271
                    ) observations
                ) rolling_adv20
            ) daily_values
            ORDER BY trade_date DESC
            LIMIT 252
        ) recent_values
    ) history ON history.window_rows = 252
             AND history.valid_observations >= 189
             AND history.value IS NOT NULL
), ranked AS (
    SELECT
        asset_id, as_of_date, value,
        rank() OVER (PARTITION BY signal_date ORDER BY value ASC) AS rank
    FROM raw_values
)
SELECT asset_id, as_of_date, value, rank
FROM ranked;
