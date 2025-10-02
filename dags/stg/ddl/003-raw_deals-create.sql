CREATE TABLE IF NOT EXISTS stg.raw_deals (
    id SERIAL PRIMARY KEY,
    transaction_number VARCHAR NOT NULL UNIQUE,
    payload JSONB NOT NULL,
    source_loaded_at TIMESTAMP NOT NULL DEFAULT NOW(),
    ingestion_id UUID DEFAULT gen_random_uuid()
);
