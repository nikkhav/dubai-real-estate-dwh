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

        payload ->> 'PROJECT_EN'        as project,
        payload ->> 'AREA_EN'           as area,
        payload ->> 'PROP_TYPE_EN'      as property_type,
        payload ->> 'PROP_SB_TYPE_EN'   as property_subtype,
        payload ->> 'USAGE_EN'          as usage_type,
        payload ->> 'ROOMS_EN'          as rooms,
        payload ->> 'PARKING'           as parking,
        payload ->> 'IS_FREE_HOLD_EN'   as freehold,
        payload ->> 'IS_OFFPLAN_EN'     as offplan,

        (payload ->> 'ACTUAL_AREA')::float as area_sqm,

        load_ts,

        {{ dbt_utils.generate_surrogate_key([
            "payload ->> 'PROJECT_EN'",
            "payload ->> 'AREA_EN'",
            "payload ->> 'PROP_TYPE_EN'",
            "payload ->> 'PROP_SB_TYPE_EN'",
            "payload ->> 'USAGE_EN'",
            "payload ->> 'ROOMS_EN'",
            "payload ->> 'PARKING'",
            "payload ->> 'IS_FREE_HOLD_EN'",
            "payload ->> 'IS_OFFPLAN_EN'",
            "payload ->> 'ACTUAL_AREA'"
        ]) }} as hashdiff

    from {{ ref('stg_deals') }}
)

select * from src