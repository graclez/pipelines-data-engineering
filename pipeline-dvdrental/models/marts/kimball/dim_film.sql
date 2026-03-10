{{ config(materialized='table') }}

select
  film_id as film_key,
  title,
  rating,
  language_name,
  rental_duration,
  rental_rate,
  length,
  replacement_cost,
  release_year
from {{ ref('stg_film') }}