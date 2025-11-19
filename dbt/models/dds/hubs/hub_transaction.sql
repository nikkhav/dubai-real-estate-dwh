{{ config(materialized='incremental', unique_key='transaction_hk') }}

with src as (
    select
        transaction_number,
        ingestion_id,
        load_ts
    from {{ ref('stg_deals') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['transaction_number']) }} as transaction_hk,
    transaction_number,
    ingestion_id,
    load_ts as load_dts
from src