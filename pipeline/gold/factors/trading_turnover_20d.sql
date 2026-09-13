-- trading_turnover_20d daily Gold implementation.
-- value = current row 포함 최근 20 KRX 거래행 평균 거래대금 / 시가총액
-- predicted_sign = -1, 따라서 rank 1은 raw value가 가장 낮은 종목이다.
-- Each target uses an indexed, bounded 250-row history probe. This proves
-- listing age without rescanning the complete price table every day.
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
        t.asset_id, t.as_of_date, t.signal_date,
        history.adv20::double precision
            / t.market_cap::double precision AS value
    FROM targets t
    JOIN LATERAL (
        SELECT
            count(*) AS age_rows,
            avg(trading_value) FILTER (
                WHERE recent_rank <= 20
            ) AS adv20
        FROM (
            SELECT
                observations.trading_value,
                row_number() OVER (
                    ORDER BY observations.trade_date DESC
                ) AS recent_rank
            FROM (
                SELECT p.trade_date, p.trading_value
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
                LIMIT 250
            ) observations
        ) numbered
    ) history ON history.age_rows = 250
    WHERE history.adv20 IS NOT NULL
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
