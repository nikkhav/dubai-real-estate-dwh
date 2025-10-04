{{ config(materialized='incremental', unique_key='prop_type_en') }}

select distinct
    (r.payload ->> 'PROP_TYPE_EN') as prop_type_en,
    (r.payload ->> 'PROP_SB_TYPE_EN') as prop_sb_type_en
from {{ ref('stg_raw_deals') }} r
where r.payload ->> 'PROP_TYPE_EN' is not null
