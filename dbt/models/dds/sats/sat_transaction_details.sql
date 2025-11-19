{{ config(materialized='incremental', unique_key='transaction_hk') }}

with src as (

    select
        {{ dbt_utils.generate_surrogate_key(['transaction_number']) }} as transaction_hk,

        (payload ->> 'INSTANCE_DATE')::timestamp  as transaction_date,
        payload ->> 'PROCEDURE_EN'               as procedure_type,
        (payload ->> 'TRANS_VALUE')::float        as transaction_value_aed,

        load_ts,

        {{ dbt_utils.generate_surrogate_key([
            "payload ->> 'INSTANCE_DATE'",
            "payload ->> 'PROCEDURE_EN'",
            "payload ->> 'TRANS_VALUE'"
        ]) }} as hashdiff
    from {{ ref('stg_deals') }}
)

select * from src