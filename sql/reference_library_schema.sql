-- Isolated, append-only measurement library. NOT gold.factor alpha approval.
-- Installed transactionally by factor-research/scripts/reference_library.py.
CREATE SCHEMA IF NOT EXISTS gold;
CREATE TABLE gold.reference_schema_version (
    version text PRIMARY KEY, ddl_sha256 text NOT NULL CHECK (length(ddl_sha256)=64)
);
CREATE TABLE gold.reference_definition (
    generation_id text NOT NULL,
    reference_id text NOT NULL,
    definition_sha256 text NOT NULL CHECK (length(definition_sha256)=64),
    specification jsonb NOT NULL CHECK (jsonb_typeof(specification)='object'),
    role text NOT NULL CHECK (role='MEASUREMENT_AND_CONTROL_NOT_ALPHA_APPROVAL'),
    PRIMARY KEY (generation_id, reference_id, definition_sha256)
);
CREATE TABLE gold.reference_dataset (
    dataset_id text PRIMARY KEY CHECK (length(dataset_id)=64),
    generation_id text NOT NULL,
    source_version text NOT NULL,
    input_sha256 text NOT NULL CHECK (length(input_sha256)=64),
    values_sha256 text NOT NULL CHECK (length(values_sha256)=64),
    row_count bigint NOT NULL CHECK (row_count>0),
    metadata jsonb NOT NULL CHECK (jsonb_typeof(metadata)='object'),
    write_txid bigint NOT NULL DEFAULT txid_current(),
    created_at timestamptz NOT NULL DEFAULT current_timestamp,
    UNIQUE (dataset_id, generation_id)
);
CREATE TABLE gold.reference_value (
    dataset_id text NOT NULL,
    generation_id text NOT NULL,
    reference_id text NOT NULL,
    definition_sha256 text NOT NULL,
    asset_id bigint NOT NULL REFERENCES public.asset(asset_id),
    stock_code text NOT NULL CHECK (stock_code ~ '^[0-9A-Z]{6}$'),
    as_of_date date NOT NULL,
    known_at timestamptz NOT NULL,
    value double precision NOT NULL CHECK (value > '-Infinity'::float8 AND value < 'Infinity'::float8),
    PRIMARY KEY (dataset_id, reference_id, asset_id, as_of_date),
    FOREIGN KEY (dataset_id, generation_id) REFERENCES gold.reference_dataset(dataset_id,generation_id),
    FOREIGN KEY (generation_id, reference_id, definition_sha256)
        REFERENCES gold.reference_definition(generation_id,reference_id,definition_sha256),
    CHECK (known_at < ((as_of_date + 1)::timestamp AT TIME ZONE 'Asia/Seoul'))
);
CREATE INDEX reference_value_lookup ON gold.reference_value(generation_id, reference_id, as_of_date);
CREATE FUNCTION gold.reject_reference_mutation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'Reference artifacts are immutable; publish a new dataset/version'; END;
$$;
CREATE FUNCTION gold.require_reference_build_transaction() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM gold.reference_dataset d
        WHERE d.dataset_id=NEW.dataset_id AND d.write_txid=txid_current()) THEN
        RAISE EXCEPTION 'Reference values may only be inserted in the dataset creation transaction';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER reference_value_creation_transaction BEFORE INSERT ON gold.reference_value
    FOR EACH ROW EXECUTE FUNCTION gold.require_reference_build_transaction();
CREATE TRIGGER reference_definition_immutable BEFORE UPDATE OR DELETE ON gold.reference_definition
    FOR EACH ROW EXECUTE FUNCTION gold.reject_reference_mutation();
CREATE TRIGGER reference_dataset_immutable BEFORE UPDATE OR DELETE ON gold.reference_dataset
    FOR EACH ROW EXECUTE FUNCTION gold.reject_reference_mutation();
CREATE TRIGGER reference_value_immutable BEFORE UPDATE OR DELETE ON gold.reference_value
    FOR EACH ROW EXECUTE FUNCTION gold.reject_reference_mutation();
CREATE TRIGGER reference_schema_immutable BEFORE UPDATE OR DELETE ON gold.reference_schema_version
    FOR EACH ROW EXECUTE FUNCTION gold.reject_reference_mutation();
CREATE TRIGGER reference_definition_no_truncate BEFORE TRUNCATE ON gold.reference_definition
    FOR EACH STATEMENT EXECUTE FUNCTION gold.reject_reference_mutation();
CREATE TRIGGER reference_dataset_no_truncate BEFORE TRUNCATE ON gold.reference_dataset
    FOR EACH STATEMENT EXECUTE FUNCTION gold.reject_reference_mutation();
CREATE TRIGGER reference_value_no_truncate BEFORE TRUNCATE ON gold.reference_value
    FOR EACH STATEMENT EXECUTE FUNCTION gold.reject_reference_mutation();
CREATE TRIGGER reference_schema_no_truncate BEFORE TRUNCATE ON gold.reference_schema_version
    FOR EACH STATEMENT EXECUTE FUNCTION gold.reject_reference_mutation();
