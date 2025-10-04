{{ config(materialized='incremental', unique_key='project_en') }}

select distinct
    (r.payload ->> 'PROJECT_EN') as project_en,
    (r.payload ->> 'MASTER_PROJECT_EN') as master_project_en,
    (r.payload ->> 'AREA_EN') as area_en
from {{ ref('stg_raw_deals') }} r
where r.payload ->> 'PROJECT_EN' is not null
