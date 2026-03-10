{{ config(materialized='table') }}

select
  category_id as category_key,
  category_name
from {{ ref('stg_category') }}