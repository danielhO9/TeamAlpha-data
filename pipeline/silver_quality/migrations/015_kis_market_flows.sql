-- Additive only. Applied separately after operational approval.
CREATE TABLE kis_market_observation (
    asset_id BIGINT NOT NULL REFERENCES asset(asset_id),
    ticker TEXT NOT NULL CHECK (ticker ~ '^[0-9A-Z]{6}$'),
    trade_date DATE NOT NULL,
    venue TEXT NOT NULL CHECK (venue IN ('J','NX','UN')),
    kind TEXT NOT NULL CHECK (kind IN ('investor','short')),
    revision TEXT NOT NULL CHECK (revision ~ '^[0-9a-f]{64}$'),
    payload JSONB NOT NULL CHECK (jsonb_typeof(payload)='object'),
    first_observed_at TIMESTAMPTZ NOT NULL,
    provider_available_at TIMESTAMPTZ,
    research_available_at TIMESTAMPTZ NOT NULL,
    availability_basis TEXT NOT NULL CHECK (availability_basis='POLICY_ASSUMPTION'),
    policy_version TEXT NOT NULL CHECK (btrim(policy_version)<>''),
    source_uris TEXT[] NOT NULL CHECK (cardinality(source_uris)>0),
    historical_revision_risk BOOLEAN NOT NULL DEFAULT TRUE,
    quality_run_id UUID NOT NULL REFERENCES dq_run(run_id),
    PRIMARY KEY (asset_id,trade_date,venue,kind,revision,first_observed_at),
    CHECK (kind<>'short' OR venue='J')
);
CREATE INDEX ON kis_market_observation(trade_date,venue,kind);
CREATE FUNCTION kis_require_certified() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NOT EXISTS (SELECT 1 FROM dq_run WHERE run_id=NEW.quality_run_id AND status='CERTIFIED') THEN
   RAISE EXCEPTION 'KIS observation requires certified quality run';
 END IF;
 RETURN NEW;
END $$;
CREATE CONSTRAINT TRIGGER kis_certified_guard AFTER INSERT OR UPDATE ON kis_market_observation
DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION kis_require_certified();
-- Ex-post view: NOT a point-in-time historical feature view.
CREATE VIEW kis_market_latest AS
SELECT DISTINCT ON (asset_id,trade_date,venue,kind) k.*
FROM kis_market_observation k JOIN dq_run d ON d.run_id=k.quality_run_id
WHERE d.status='CERTIFIED'
ORDER BY asset_id,trade_date,venue,kind,first_observed_at DESC,revision DESC;
-- Strict as-of access never returns a revision before it was actually observed.
CREATE FUNCTION kis_market_asof(cutoff TIMESTAMPTZ)
RETURNS SETOF kis_market_observation LANGUAGE sql STABLE AS $$
 SELECT DISTINCT ON (asset_id,trade_date,venue,kind) k.*
 FROM kis_market_observation k JOIN dq_run d ON d.run_id=k.quality_run_id
 WHERE d.status='CERTIFIED' AND k.first_observed_at<=cutoff AND k.research_available_at<=cutoff
 ORDER BY asset_id,trade_date,venue,kind,first_observed_at DESC,revision DESC
$$;
