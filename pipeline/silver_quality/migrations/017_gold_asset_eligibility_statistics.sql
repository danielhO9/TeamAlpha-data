-- Gold eligibility combines correlated asset fields and a REIT-name
-- expression. Without expression statistics PostgreSQL estimated one target
-- and nested the entire rolling-window panel under each actual target asset.
-- Preserve the factor SQL/contracts; improve cardinality estimates instead.
CREATE STATISTICS IF NOT EXISTS public.gold_asset_eligibility_stats
ON exchange, asset_type, instrument_type, (position('리츠' in name))
FROM public.asset;

ANALYZE public.asset;
