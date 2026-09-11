-- operating_return_on_capital_employed Gold implementation.
-- value = PIT operating_income_ttm / (PIT total_assets - PIT current_liabilities)
-- where capital employed is positive. The query replays certified DART
-- revisions and reconstructs standalone Q4 from FY - Q1 - Q2 - Q3 exactly as
-- the research implementation does.
-- predicted_sign = +1, therefore rank 1 is the highest raw value.
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
        trade_date AS signal_date
    FROM certified_prices
    WHERE trade_date BETWEEN %(start_date)s::date AND %(end_date)s::date
      AND instrument_type = 'common_stock'
      AND name !~* '(스팩|SPAC)'
      AND position('리츠' in name) = 0
      AND age_days >= 250
      AND market_cap > 0
      AND total_return_close > 0
), universe_state AS (
    -- A factor value changes only when one of its three source metrics gets a
    -- newly available filing.  Collapse repeated month-ends onto that PIT
    -- state before replaying the comparatively expensive financial history.
    SELECT u.*, state.available_date AS state_date
    FROM universe u
    JOIN LATERAL (
        SELECT max(f.available_date) AS available_date
        FROM public.fundamental f
        JOIN public.dq_run q
          ON q.run_id = f.quality_run_id
         AND q.status = 'CERTIFIED'
        WHERE f.asset_id = u.asset_id
          AND f.metric IN (
              'operating_income', 'total_assets', 'current_liabilities'
          )
          AND f.available_date <= u.as_of_date
          AND f.source = 'DART'
          AND f.data_basis = 'STANDARDIZED'
          AND f.unit_type = 'currency'
          AND f.value IS NOT NULL
    ) state ON state.available_date IS NOT NULL
), states AS (
    SELECT DISTINCT asset_id, state_date
    FROM universe_state
), state_values AS (
    SELECT
        s.asset_id, s.state_date,
        oi.operating_income_ttm
            / (ta.total_assets - cl.current_liabilities) AS value
    FROM states s
    JOIN LATERAL (
        SELECT f.value::double precision AS total_assets
        FROM public.fundamental f
        JOIN public.dq_run q
          ON q.run_id = f.quality_run_id
         AND q.status = 'CERTIFIED'
        WHERE f.asset_id = s.asset_id
          AND f.metric = 'total_assets'
          AND f.available_date <= s.state_date
          AND f.source = 'DART'
          AND f.data_basis = 'STANDARDIZED'
          AND f.unit_type = 'currency'
          AND f.value IS NOT NULL
        ORDER BY
            f.period_end DESC,
            CASE f.fiscal_period
                WHEN 'Q4' THEN 5 WHEN 'FY' THEN 4 WHEN 'Q3' THEN 3
                WHEN 'Q2' THEN 2 WHEN 'Q1' THEN 1 ELSE 0
            END DESC,
            (f.fs_type = 'CFS') DESC,
            f.available_date DESC,
            f.revision_key DESC
        LIMIT 1
    ) ta ON true
    JOIN LATERAL (
        SELECT f.value::double precision AS current_liabilities
        FROM public.fundamental f
        JOIN public.dq_run q
          ON q.run_id = f.quality_run_id
         AND q.status = 'CERTIFIED'
        WHERE f.asset_id = s.asset_id
          AND f.metric = 'current_liabilities'
          AND f.available_date <= s.state_date
          AND f.source = 'DART'
          AND f.data_basis = 'STANDARDIZED'
          AND f.unit_type = 'currency'
          AND f.value IS NOT NULL
        ORDER BY
            f.period_end DESC,
            CASE f.fiscal_period
                WHEN 'Q4' THEN 5 WHEN 'FY' THEN 4 WHEN 'Q3' THEN 3
                WHEN 'Q2' THEN 2 WHEN 'Q1' THEN 1 ELSE 0
            END DESC,
            (f.fs_type = 'CFS') DESC,
            f.available_date DESC,
            f.revision_key DESC
        LIMIT 1
    ) cl ON true
    JOIN LATERAL (
        WITH candidates AS (
            SELECT
                f.period_end, f.fiscal_period, f.value::double precision AS value,
                row_number() OVER (
                    PARTITION BY f.period_end, f.fiscal_period
                    ORDER BY
                        (f.fs_type = 'CFS') DESC,
                        f.available_date DESC,
                        f.revision_key DESC
                ) AS revision_rank
            FROM public.fundamental f
            JOIN public.dq_run q
              ON q.run_id = f.quality_run_id
             AND q.status = 'CERTIFIED'
            WHERE f.asset_id = s.asset_id
              AND f.metric = 'operating_income'
              AND f.available_date <= s.state_date
              AND f.source = 'DART'
              AND f.data_basis = 'STANDARDIZED'
              AND f.unit_type = 'currency'
              AND f.value IS NOT NULL
        ), state AS (
            SELECT period_end, fiscal_period, value
            FROM candidates
            WHERE revision_rank = 1
        ), direct_quarters AS (
            SELECT
                period_end, fiscal_period, value, 1 AS explicit_priority
            FROM state
            WHERE fiscal_period IN ('Q1', 'Q2', 'Q3', 'Q4')
        ), fy_base AS (
            SELECT
                period_end AS fy_end, value AS fy_value,
                lag(period_end) OVER (ORDER BY period_end) AS previous_fy_end
            FROM state
            WHERE fiscal_period = 'FY'
        ), fy_quarter_ranked AS (
            SELECT
                fy.fy_end, fy.fy_value, q.fiscal_period,
                q.value AS quarter_value,
                row_number() OVER (
                    PARTITION BY fy.fy_end, q.fiscal_period
                    ORDER BY q.period_end DESC
                ) AS quarter_rank
            FROM fy_base fy
            JOIN state q
              ON q.fiscal_period IN ('Q1', 'Q2', 'Q3')
             AND q.period_end < fy.fy_end
             AND q.period_end > coalesce(
                 fy.previous_fy_end, fy.fy_end - interval '370 days'
             )
        ), derived_q4 AS (
            SELECT
                q.fy_end AS period_end, 'Q4'::text AS fiscal_period,
                (max(q.fy_value) - sum(q.quarter_value))::double precision AS value,
                0 AS explicit_priority
            FROM fy_quarter_ranked q
            WHERE q.quarter_rank = 1
              AND NOT EXISTS (
                  SELECT 1
                  FROM direct_quarters d
                  WHERE d.period_end = q.fy_end
                    AND d.fiscal_period = 'Q4'
              )
            GROUP BY q.fy_end
            HAVING count(DISTINCT q.fiscal_period) = 3
        ), standalone_candidates AS (
            SELECT * FROM direct_quarters
            UNION ALL
            SELECT * FROM derived_q4
        ), standalone_ranked AS (
            SELECT
                standalone_candidates.*,
                row_number() OVER (
                    PARTITION BY period_end
                    ORDER BY
                        CASE fiscal_period
                            WHEN 'Q4' THEN 4 WHEN 'Q3' THEN 3
                            WHEN 'Q2' THEN 2 WHEN 'Q1' THEN 1 ELSE 0
                        END DESC,
                        explicit_priority DESC
                ) AS period_rank
            FROM standalone_candidates
        ), recent AS (
            SELECT *, row_number() OVER (ORDER BY period_end DESC) AS recent_rank
            FROM standalone_ranked
            WHERE period_rank = 1
        )
        SELECT sum(value)::double precision AS operating_income_ttm
        FROM recent
        WHERE recent_rank <= 4
        HAVING count(*) = 4
           AND max(period_end) - min(period_end) <= 370
    ) oi ON true
    WHERE ta.total_assets - cl.current_liabilities > 0
), raw_values AS (
    SELECT
        u.asset_id, u.as_of_date, u.signal_date, s.value
    FROM universe_state u
    JOIN state_values s
      ON s.asset_id = u.asset_id
     AND s.state_date = u.state_date
), ranked AS (
    SELECT
        asset_id, as_of_date, value,
        rank() OVER (PARTITION BY signal_date ORDER BY value DESC) AS rank
    FROM raw_values
    WHERE value IS NOT NULL
)
SELECT asset_id, as_of_date, value, rank
FROM ranked;
