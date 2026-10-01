-- Auditable financial-unit repair and explicitly quoted KRW FX input.
CREATE TABLE IF NOT EXISTS public.financial_unit_repair (
    repair_id text PRIMARY KEY,
    run_id uuid NOT NULL REFERENCES public.dq_run(run_id),
    original_row jsonb NOT NULL,
    corrected_value numeric NOT NULL,
    evidence jsonb NOT NULL,
    repaired_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS public.fx_rate_daily (
    currency text NOT NULL CHECK (currency IN ('USD','CNY','JPY','HKD','GBP')),
    rate_date date NOT NULL,
    krw_per_unit numeric NOT NULL CHECK (krw_per_unit > 0 AND krw_per_unit < 'Infinity'::numeric),
    source text NOT NULL,
    evidence_sha256 text NOT NULL,
    quality_run_id uuid NOT NULL REFERENCES public.dq_run(run_id),
    PRIMARY KEY (currency, rate_date)
);
-- A global FX daily close labelled D is not available at KRX close on D.
-- Use the previous dated close only; allow weekends/holidays, not stale quotes.
CREATE OR REPLACE FUNCTION public.factor_fx_rate(ccy text, signal_date date)
RETURNS numeric LANGUAGE sql STABLE AS $$
    SELECT CASE WHEN ccy='KRW' THEN 1::numeric ELSE (
        SELECT r.krw_per_unit FROM public.fx_rate_daily r
        JOIN public.dq_run q ON q.run_id=r.quality_run_id AND q.status='CERTIFIED'
        WHERE r.currency=ccy AND r.rate_date < signal_date
          AND r.rate_date >= signal_date-7
        ORDER BY r.rate_date DESC LIMIT 1
    ) END
$$;
