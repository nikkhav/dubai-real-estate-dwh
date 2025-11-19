{{ config(materialized='incremental', unique_key='property_hk') }}

with src as (
    select
        {{ dbt_utils.generate_surrogate_key([
            "payload ->> 'PROJECT_EN'",
            "payload ->> 'AREA_EN'",
            "payload ->> 'PROP_TYPE_EN'",
            "payload ->> 'PROP_SB_TYPE_EN'",
            "payload ->> 'ROOMS_EN'",
            "payload ->> 'ACTUAL_AREA'"
        ]) }} as property_hk,
        load_ts as load_dts
    from {{ ref('stg_deals') }}
)

select * from src