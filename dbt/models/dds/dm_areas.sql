{{ config(materialized='incremental', unique_key='area_en') }}

select distinct
    (r.payload ->> 'AREA_EN') as area_en,
    (r.payload ->> 'NEAREST_METRO_EN') as nearest_metro_en,
    (r.payload ->> 'NEAREST_MALL_EN') as nearest_mall_en,
    (r.payload ->> 'NEAREST_LANDMARK_EN') as nearest_landmark_en
from {{ ref('stg_raw_deals') }} r
where r.payload ->> 'AREA_EN' is not null
