{{ config(materialized='incremental', unique_key='transaction_number') }}

select
    r.transaction_number,
    (r.payload ->> 'INSTANCE_DATE')::timestamp as instance_date,
    (r.payload ->> 'GROUP_EN') as group_en,
    (r.payload ->> 'PROCEDURE_EN') as procedure_en,
    (r.payload ->> 'USAGE_EN') as usage_en,
    (r.payload ->> 'PROP_TYPE_EN') as prop_type_en,
    (r.payload ->> 'PROP_SB_TYPE_EN') as prop_sb_type_en,
    (r.payload ->> 'ROOMS_EN') as rooms_en,
    (r.payload ->> 'PARKING') as parking,
    (r.payload ->> 'TRANS_VALUE')::numeric as trans_value,
    (r.payload ->> 'ACTUAL_AREA')::numeric as actual_area,
    (r.payload ->> 'PROCEDURE_AREA')::numeric as procedure_area,
    (r.payload ->> 'IS_OFFPLAN_EN') as is_offplan_en,
    (r.payload ->> 'IS_FREE_HOLD_EN') as is_free_hold_en,
    (r.payload ->> 'TOTAL_BUYER')::int as total_buyer,
    (r.payload ->> 'TOTAL_SELLER')::int as total_seller,
    (r.payload ->> 'PROJECT_EN') as project_en,
    (r.payload ->> 'MASTER_PROJECT_EN') as master_project_en,
    (r.payload ->> 'AREA_EN') as area_en,
    (r.payload ->> 'NEAREST_METRO_EN') as nearest_metro_en,
    (r.payload ->> 'NEAREST_MALL_EN') as nearest_mall_en,
    (r.payload ->> 'NEAREST_LANDMARK_EN') as nearest_landmark_en,
    r.source_loaded_at,
    r.ingestion_id
from {{ ref('stg_raw_deals') }} r

{% if is_incremental() %}
where r.source_loaded_at > (select max(source_loaded_at) from {{ this }})
{% endif %}
