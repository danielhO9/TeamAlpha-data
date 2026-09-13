-- return_kurtosis_24m daily Gold implementation.
-- value = 최근 504 KRX 거래일 feature-safe adj_close 일수익률의
-- pandas-compatible unbiased Fisher excess kurtosis.
-- 최소 378개(75 percent) 유효 수익률을 요구한다.
-- predicted_sign = -1, 따라서 rank 1은 raw value가 가장 낮은 종목이다.
WITH certified AS (
    SELECT
        p.asset_id, a.name, a.instrument_type, p.trade_date,
        p.adj_close, p.market_cap, p.market,
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
), daily_returns AS (
    SELECT
        certified.*,
        adj_close::double precision
            / lag(adj_close::double precision) OVER (
                PARTITION BY asset_id ORDER BY trade_date
              ) - 1.0 AS daily_return
    FROM certified
), rolling_moments AS (
    SELECT
        daily_returns.*,
        count(*) OVER recent AS window_rows,
        count(daily_return) OVER recent AS n,
        sum(daily_return) OVER recent AS sum_1,
        sum(power(daily_return, 2)) OVER recent AS sum_2,
        sum(power(daily_return, 3)) OVER recent AS sum_3,
        sum(power(daily_return, 4)) OVER recent AS sum_4,
        var_samp(daily_return) OVER recent AS sample_variance
    FROM daily_returns
    WINDOW recent AS (
        PARTITION BY asset_id ORDER BY trade_date
        ROWS BETWEEN 503 PRECEDING AND CURRENT ROW
    )
), moments AS (
    SELECT
        asset_id, trade_date AS as_of_date, trade_date AS signal_date,
        window_rows, n, sample_variance,
        (
            sum_4
            - 4.0 * (sum_1 / n) * sum_3
            + 6.0 * power(sum_1 / n, 2) * sum_2
            - 3.0 * n * power(sum_1 / n, 4)
        ) AS fourth_sum
    FROM rolling_moments
    WHERE trade_date BETWEEN %(start_date)s::date AND %(end_date)s::date
      AND instrument_type = 'common_stock'
      AND name !~* '(스팩|SPAC)'
      AND position('리츠' in name) = 0
      AND age_days >= 504
      AND market_cap > 0
      AND adj_close > 0
      AND n >= 378
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
