{{ config(materialized='view') }}

select
    id,
    transaction_number,
    payload::jsonb as payload,
    source_loaded_at,
    ingestion_id
from stg.raw_deals
