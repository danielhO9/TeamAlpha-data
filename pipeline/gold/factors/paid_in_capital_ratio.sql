-- paid_in_capital_ratio Gold implementation.
-- value = PIT 법정 자본금 / PIT 자기자본 (자기자본 > 0)
-- predicted_sign = -1, 따라서 rank 1은 raw value가 가장 낮은 종목이다.
-- fundamental_current는 사용하지 않고 닫힌 signal month 범위의 종목별 월말 거래일로
-- DART revision을 재생한다. 두 지표의 최신 회계기간은 독립적으로 선택한다.
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
      AND instrument_type = 'common_stock'
      AND name !~* '(스팩|SPAC)'
      AND position('리츠' in name) = 0
      AND (age_days >= 250 OR first_seen = dataset_start)
      AND market_cap > 0
      AND adj_close > 0
), revisions AS (
    SELECT
        u.asset_id, u.as_of_date, u.signal_month,
        f.period_end, f.fiscal_period, f.metric, f.value,
        f.fs_type, f.available_date, f.revision_key,
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
     AND f.metric IN ('capital_stock', 'total_equity')
     AND f.source = 'DART'
     AND f.data_basis = 'STANDARDIZED'
     AND f.unit_type = 'currency'
    JOIN public.dq_run q
      ON q.run_id = f.quality_run_id
     AND q.status = 'CERTIFIED'
), latest_metric AS (
    SELECT
        revisions.*,
        row_number() OVER (
            PARTITION BY asset_id, as_of_date, metric
            ORDER BY
                period_end DESC,
                fiscal_period DESC,
                (fs_type = 'CFS') DESC,
                available_date DESC,
                revision_key DESC
        ) AS metric_rank
    FROM revisions
    WHERE revision_rank = 1
), pivoted AS (
    SELECT
        asset_id, as_of_date, signal_month,
        max(value) FILTER (
            WHERE metric = 'capital_stock' AND metric_rank = 1
        )::double precision AS capital_stock,
        max(value) FILTER (
            WHERE metric = 'total_equity' AND metric_rank = 1
        )::double precision AS total_equity
    FROM latest_metric
    GROUP BY asset_id, as_of_date, signal_month
), raw_values AS (
    SELECT
        asset_id, as_of_date, signal_month,
        capital_stock / total_equity AS value
    FROM pivoted
    WHERE capital_stock IS NOT NULL
      AND total_equity > 0
), ranked AS (
    SELECT
        asset_id, as_of_date, value,
        rank() OVER (
            PARTITION BY signal_month ORDER BY value ASC
        ) AS rank
    FROM raw_values
)
SELECT asset_id, as_of_date, value, rank
FROM ranked;
