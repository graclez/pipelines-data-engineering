{{ config(materialized='table') }}

select
  film_id as film_key,
  category_id as category_key
from {{ ref('stg_film_category') }}