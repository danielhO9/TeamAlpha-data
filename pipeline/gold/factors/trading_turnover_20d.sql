-- trading_turnover_20d Gold implementation.
-- value = current row 포함 최근 20 KRX 거래행 평균 거래대금 / 시가총액
-- predicted_sign = -1, 따라서 rank 1은 raw value가 가장 낮은 종목이다.
-- %(start_date)s와 %(end_date)s는 닫힌 KRX 거래일 범위다.
WITH certified AS (
    SELECT
        p.asset_id, a.name, a.instrument_type, p.trade_date,
        p.total_return_close, p.trading_value, p.market_cap, p.market,
        avg(p.trading_value) OVER (
            PARTITION BY p.asset_id ORDER BY p.trade_date
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS adv20,
        row_number() OVER (
            PARTITION BY p.asset_id ORDER BY p.trade_date
        ) AS age_days,
        min(p.trade_date) OVER (PARTITION BY p.asset_id) AS first_seen
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
), raw_values AS (
    SELECT
        asset_id,
        trade_date AS as_of_date,
        adv20::double precision / market_cap::double precision AS value,
        trade_date AS signal_date
    FROM certified
    WHERE trade_date BETWEEN %(start_date)s::date AND %(end_date)s::date
      AND instrument_type = 'common_stock'
      AND name !~* '(스팩|SPAC)'
      AND position('리츠' in name) = 0
      AND age_days >= 250
      AND market_cap > 0
      AND total_return_close > 0
      AND adv20 IS NOT NULL
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
