-- Versioned listing metadata. Does not rewrite existing asset/PIT identities.
CREATE TABLE asset_listing_snapshot (
    snapshot_id TEXT PRIMARY KEY CHECK (snapshot_id ~ '^[0-9a-f]{64}$'),
    payload JSONB NOT NULL CHECK (jsonb_typeof(payload)='object'),
    source_uri TEXT NOT NULL CHECK (btrim(source_uri)<>''),
    observed_at TIMESTAMPTZ NOT NULL,
    quality_run_id UUID NOT NULL REFERENCES dq_run(run_id),
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (payload ?& ARRAY['snapshot_id','schema','periods','asset_ids',
        'coverage_start','verified_through','observed_at','evidence_uri','evidence_sha256','audit_sha256']),
    CHECK (payload->>'snapshot_id'=snapshot_id),
    CHECK (payload->>'schema'='asset-listing-snapshot-v1'),
    CHECK (jsonb_typeof(payload->'periods')='array')
);
CREATE FUNCTION listing_snapshot_require_certified() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NOT EXISTS (SELECT 1 FROM dq_run WHERE run_id=NEW.quality_run_id AND status='CERTIFIED') THEN
   RAISE EXCEPTION 'listing snapshot requires certified quality run';
 END IF;
 RETURN NEW;
END $$;
CREATE CONSTRAINT TRIGGER listing_snapshot_certified_guard
AFTER INSERT OR UPDATE ON asset_listing_snapshot DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION listing_snapshot_require_certified();
CREATE FUNCTION listing_snapshot_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 RAISE EXCEPTION 'listing snapshots are immutable; publish a new version';
END $$;
CREATE TRIGGER listing_snapshot_immutable_guard BEFORE UPDATE OR DELETE ON asset_listing_snapshot
FOR EACH ROW EXECUTE FUNCTION listing_snapshot_immutable();
