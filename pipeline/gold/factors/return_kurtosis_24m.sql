-- return_kurtosis_24m daily Gold implementation.
-- value = 최근 504 KRX 거래일 total_return_close 일수익률의
-- pandas-compatible unbiased Fisher excess kurtosis.
-- 최소 378개(75 percent) 유효 수익률을 요구한다.
-- predicted_sign = -1, 따라서 rank 1은 raw value가 가장 낮은 종목이다.
WITH certified AS (
    SELECT
        p.asset_id, a.name, a.instrument_type, p.trade_date,
        p.total_return_close, p.market_cap, p.market,
        row_number() OVER (
            PARTITION BY p.asset_id ORDER BY p.trade_date
        ) AS age_days
    FROM public.price_daily p
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
), daily_returns AS (
    SELECT
        certified.*,
        total_return_close::double precision
            / lag(total_return_close::double precision) OVER (
                PARTITION BY asset_id ORDER BY trade_date
              ) - 1.0 AS daily_return
    FROM certified
), targets AS (
    SELECT *
    FROM daily_returns
    WHERE trade_date BETWEEN %(start_date)s::date AND %(end_date)s::date
      AND instrument_type = 'common_stock'
      AND name !~* '(스팩|SPAC)'
      AND position('리츠' in name) = 0
      AND age_days >= 504
      AND market_cap > 0
      AND total_return_close > 0
), moments AS (
    SELECT
        t.asset_id, t.trade_date AS as_of_date,
        t.trade_date AS signal_date,
        window.window_rows, window.n, window.sample_variance,
        window.fourth_sum
    FROM targets t
    JOIN LATERAL (
        SELECT
            count(*) AS window_rows,
            count(sample.daily_return) AS n,
            max(sample.sample_variance) AS sample_variance,
            sum(
                power(sample.daily_return - sample.mean_return, 4)
            ) AS fourth_sum
        FROM (
            SELECT
                observations.daily_return,
                avg(observations.daily_return) OVER () AS mean_return,
                var_samp(observations.daily_return) OVER () AS sample_variance
            FROM (
                SELECT r.daily_return
                FROM daily_returns r
                WHERE r.asset_id = t.asset_id
                  AND r.trade_date <= t.trade_date
                ORDER BY r.trade_date DESC
                LIMIT 504
            ) observations
        ) sample
    ) window ON true
), raw_values AS (
    SELECT
        asset_id, as_of_date, signal_date,
        CASE WHEN sample_variance = 0 THEN -3.0 ELSE (
            n::double precision * (n + 1)::double precision * fourth_sum
            / (
                (n - 1)::double precision
                * (n - 2)::double precision
                * (n - 3)::double precision
                * power(sample_variance, 2)
              )
            - 3.0 * power((n - 1)::double precision, 2)
              / ((n - 2)::double precision * (n - 3)::double precision)
        ) END AS value
    FROM moments
    WHERE window_rows = 504
      AND n >= 378
      AND sample_variance >= 0
), ranked AS (
    SELECT
        asset_id, as_of_date, value,
        rank() OVER (PARTITION BY signal_date ORDER BY value ASC) AS rank
    FROM raw_values
    WHERE value IS NOT NULL
)
SELECT asset_id, as_of_date, value, rank
FROM ranked;
