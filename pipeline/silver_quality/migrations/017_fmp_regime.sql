-- Regime inputs are observations, outside the equity universe/return contract.
CREATE TABLE fmp_regime_observation (
    series TEXT NOT NULL CHECK (series IN ('^VIX','DX-Y.NYB','^SOX','^GSPC','HYG','IEF','LQD','TLT','US_TREASURY')),
    observation_date DATE NOT NULL,
    metric TEXT NOT NULL,
    value DOUBLE PRECISION NOT NULL CHECK (value::text NOT IN ('NaN','Infinity','-Infinity')),
    unit TEXT NOT NULL CHECK (unit IN ('percent','USD','index_points')),
    payload JSONB NOT NULL CHECK (jsonb_typeof(payload)='object'),
    revision TEXT NOT NULL CHECK (revision ~ '^[0-9a-f]{64}$'),
    observed_at TIMESTAMPTZ NOT NULL,
    source_uri TEXT NOT NULL CHECK (btrim(source_uri)<>''),
    quality_run_id UUID NOT NULL REFERENCES dq_run(run_id),
    PRIMARY KEY (series,observation_date,metric,revision,observed_at),
    CHECK ((series='US_TREASURY' AND unit='percent' AND metric IN
        ('month1','month2','month3','month6','year1','year2','year3','year5','year7','year10','year20','year30'))
        OR (series<>'US_TREASURY' AND metric='close' AND value>0 AND
            unit=CASE WHEN series IN ('HYG','IEF','LQD','TLT') THEN 'USD' ELSE 'index_points' END))
);
CREATE INDEX ON fmp_regime_observation (series,observation_date);
CREATE FUNCTION fmp_regime_require_certified() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NOT EXISTS (SELECT 1 FROM dq_run WHERE run_id=NEW.quality_run_id AND status='CERTIFIED') THEN
  RAISE EXCEPTION 'FMP regime observation requires certified quality run';
 END IF;
 RETURN NEW;
END $$;
CREATE CONSTRAINT TRIGGER fmp_regime_certified_guard AFTER INSERT OR UPDATE ON fmp_regime_observation
DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION fmp_regime_require_certified();
-- Ex-post view: backfilled revisions are NOT historical PIT features.
CREATE VIEW fmp_regime_latest AS
SELECT DISTINCT ON (series,observation_date,metric) r.*
FROM fmp_regime_observation r JOIN dq_run q ON q.run_id=r.quality_run_id
WHERE q.status='CERTIFIED'
ORDER BY series,observation_date,metric,observed_at DESC,revision DESC;
CREATE FUNCTION fmp_regime_asof(cutoff TIMESTAMPTZ)
RETURNS SETOF fmp_regime_observation LANGUAGE sql STABLE AS $$
 SELECT DISTINCT ON (series,observation_date,metric) r.*
 FROM fmp_regime_observation r JOIN dq_run q ON q.run_id=r.quality_run_id
 WHERE q.status='CERTIFIED' AND r.observed_at<=cutoff
 ORDER BY series,observation_date,metric,observed_at DESC,revision DESC
$$;
