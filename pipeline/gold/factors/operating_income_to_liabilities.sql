-- operating_income_to_liabilities Gold implementation.
-- value = 신호일에 알려진 최근 4개 독립 분기 영업이익 합 / 최신 양의 총부채
-- predicted_sign = +1, 따라서 rank 1은 raw value가 가장 높은 종목이다.
WITH certified_prices AS (
    SELECT
        p.asset_id, a.name, a.instrument_type, p.trade_date,
        p.adj_close, p.market_cap, p.market,
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
), monthly AS (
    SELECT
        certified_prices.*,
        min(trade_date) OVER () AS dataset_start,
        row_number() OVER (
            PARTITION BY asset_id, date_trunc('month', trade_date)
            ORDER BY trade_date DESC
        ) AS month_rank
    FROM certified_prices
), universe AS (
    SELECT
        asset_id,
        trade_date AS as_of_date,
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
), revisions AS (
    SELECT
        u.asset_id, u.as_of_date, u.signal_month,
        f.period_end, f.fiscal_period, f.metric,
        f.value::double precision AS value,
        row_number() OVER (
            PARTITION BY
                u.asset_id, u.as_of_date, f.period_end,
                f.fiscal_period, f.metric
            ORDER BY
                (f.fs_type = 'CFS') DESC,
                f.available_date DESC,
                f.revision_key DESC
        ) AS revision_rank
    FROM universe u
    JOIN public.fundamental f
      ON f.asset_id = u.asset_id
     AND f.available_date <= u.as_of_date
     AND f.metric IN ('operating_income', 'total_liabilities')
     AND f.source = 'DART'
     AND f.data_basis = 'STANDARDIZED'
     AND f.unit_type = 'currency'
     AND f.value IS NOT NULL
    JOIN public.dq_run q
      ON q.run_id = f.quality_run_id
     AND q.status = 'CERTIFIED'
), selected AS (
    SELECT *
    FROM revisions
    WHERE revision_rank = 1
), liability_candidates AS (
    SELECT
        selected.*,
        row_number() OVER (
            PARTITION BY asset_id, as_of_date
            ORDER BY
                period_end DESC,
                CASE fiscal_period
                    WHEN 'Q4' THEN 5
                    WHEN 'FY' THEN 4
                    WHEN 'Q3' THEN 3
                    WHEN 'Q2' THEN 2
                    WHEN 'Q1' THEN 1
                    ELSE 0
                END DESC
        ) AS liability_rank
    FROM selected
    WHERE metric = 'total_liabilities'
), latest_liabilities AS (
    SELECT asset_id, as_of_date, value AS total_liabilities
    FROM liability_candidates
    WHERE liability_rank = 1
), fiscal_years AS (
    SELECT
        asset_id, as_of_date, signal_month,
        period_end AS fy_end, value AS fy_value,
        lag(period_end) OVER (
            PARTITION BY asset_id, as_of_date ORDER BY period_end
        ) AS previous_fy_end
    FROM selected
    WHERE metric = 'operating_income'
      AND fiscal_period = 'FY'
), fy_quarter_candidates AS (
    SELECT
        fy.asset_id, fy.as_of_date, fy.signal_month,
        fy.fy_end, fy.fy_value, q.period_end,
        q.fiscal_period, q.value,
        row_number() OVER (
            PARTITION BY
                fy.asset_id, fy.as_of_date, fy.fy_end, q.fiscal_period
            ORDER BY q.period_end DESC
        ) AS quarter_rank
    FROM fiscal_years fy
    JOIN selected q
      ON q.asset_id = fy.asset_id
     AND q.as_of_date = fy.as_of_date
     AND q.metric = 'operating_income'
     AND q.fiscal_period IN ('Q1', 'Q2', 'Q3')
     AND q.period_end > coalesce(
            fy.previous_fy_end, fy.fy_end - INTERVAL '370 days'
         )
     AND q.period_end < fy.fy_end
    WHERE NOT EXISTS (
        SELECT 1
        FROM selected explicit_q4
        WHERE explicit_q4.asset_id = fy.asset_id
          AND explicit_q4.as_of_date = fy.as_of_date
          AND explicit_q4.metric = 'operating_income'
          AND explicit_q4.fiscal_period = 'Q4'
          AND explicit_q4.period_end = fy.fy_end
    )
), derived_q4 AS (
    SELECT
        asset_id, as_of_date, signal_month,
        fy_end AS period_end,
        'Q4'::text AS fiscal_period,
        max(fy_value) - sum(value) AS value
    FROM fy_quarter_candidates
    WHERE quarter_rank = 1
    GROUP BY asset_id, as_of_date, signal_month, fy_end
    HAVING count(DISTINCT fiscal_period) = 3
), standalone_candidates AS (
    SELECT
        asset_id, as_of_date, signal_month,
        period_end, fiscal_period, value
    FROM selected
    WHERE metric = 'operating_income'
      AND fiscal_period IN ('Q1', 'Q2', 'Q3', 'Q4')
    UNION ALL
    SELECT
        asset_id, as_of_date, signal_month,
        period_end, fiscal_period, value
    FROM derived_q4
), standalone_ranked AS (
    SELECT
        standalone_candidates.*,
        row_number() OVER (
            PARTITION BY asset_id, as_of_date, period_end
            ORDER BY
                CASE fiscal_period
                    WHEN 'Q4' THEN 4
                    WHEN 'Q3' THEN 3
                    WHEN 'Q2' THEN 2
                    WHEN 'Q1' THEN 1
                    ELSE 0
                END DESC
        ) AS period_rank
    FROM standalone_candidates
), operating_sequence AS (
    SELECT
        standalone_ranked.*,
        row_number() OVER (
            PARTITION BY asset_id, as_of_date ORDER BY period_end DESC
        ) AS recent_rank
    FROM standalone_ranked
    WHERE period_rank = 1
), operating_ttm AS (
    SELECT
        asset_id, as_of_date, max(signal_month) AS signal_month,
        sum(value) AS operating_income_ttm
    FROM operating_sequence
    WHERE recent_rank <= 4
    GROUP BY asset_id, as_of_date
    HAVING count(*) = 4
       AND max(period_end) - min(period_end) <= 370
), raw_values AS (
    SELECT
        operating_ttm.asset_id,
        operating_ttm.as_of_date,
        operating_ttm.operating_income_ttm
            / latest_liabilities.total_liabilities AS value,
        operating_ttm.signal_month
    FROM operating_ttm
    JOIN latest_liabilities
      ON latest_liabilities.asset_id = operating_ttm.asset_id
     AND latest_liabilities.as_of_date = operating_ttm.as_of_date
    WHERE latest_liabilities.total_liabilities > 0
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
