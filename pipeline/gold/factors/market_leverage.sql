-- market_leverage Gold implementation.
-- value = PIT total non-common-equity liabilities / daily market equity.
-- predicted_sign = +1, therefore rank 1 is the highest raw value.
-- The query replays certified DART revisions available at each closed signal
-- date and never reads fundamental_current or a Gold relation.
WITH certified_prices AS (
    SELECT
        p.asset_id, a.name, a.instrument_type, p.trade_date,
        p.total_return_close, p.market_cap, p.market,
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
), universe AS (
    SELECT
        asset_id,
        trade_date AS as_of_date,
        trade_date AS signal_date,
        market_cap::double precision AS market_cap
    FROM certified_prices
    WHERE trade_date BETWEEN %(start_date)s::date AND %(end_date)s::date
      AND instrument_type = 'common_stock'
      AND name !~* '(스팩|SPAC)'
      AND position('리츠' in name) = 0
      AND age_days >= 250
      AND market_cap > 0
      AND total_return_close > 0
), revisions AS (
    SELECT
        u.asset_id, u.as_of_date, u.signal_date, u.market_cap,
        f.period_end, f.fiscal_period, f.value,
        f.fs_type, f.available_date, f.revision_key,
        row_number() OVER (
            PARTITION BY
                u.asset_id, u.as_of_date, f.period_end, f.fiscal_period
            ORDER BY
                (f.fs_type = 'CFS') DESC,
                f.available_date DESC,
                f.revision_key DESC
        ) AS revision_rank
    FROM universe u
    JOIN public.fundamental f
      ON f.asset_id = u.asset_id
     AND f.available_date <= u.as_of_date
     AND f.metric = 'total_liabilities'
     AND f.source = 'DART'
     AND f.data_basis = 'STANDARDIZED'
     AND f.unit_type = 'currency'
     AND f.value IS NOT NULL
    JOIN public.dq_run q
      ON q.run_id = f.quality_run_id
     AND q.status = 'CERTIFIED'
), latest_metric AS (
    SELECT
        revisions.*,
        row_number() OVER (
            PARTITION BY asset_id, as_of_date
            ORDER BY
                period_end DESC,
                CASE fiscal_period
                    WHEN 'Q4' THEN 5 WHEN 'FY' THEN 4 WHEN 'Q3' THEN 3
                    WHEN 'Q2' THEN 2 WHEN 'Q1' THEN 1 ELSE 0
                END DESC,
                (fs_type = 'CFS') DESC,
                available_date DESC,
                revision_key DESC
        ) AS metric_rank
    FROM revisions
    WHERE revision_rank = 1
), raw_values AS (
    SELECT
        asset_id, as_of_date, signal_date,
        value::double precision / market_cap AS value
    FROM latest_metric
    WHERE metric_rank = 1
      AND value >= 0
), ranked AS (
    SELECT
        asset_id, as_of_date, value,
        rank() OVER (PARTITION BY signal_date ORDER BY value DESC) AS rank
    FROM raw_values
)
SELECT asset_id, as_of_date, value, rank
FROM ranked;
