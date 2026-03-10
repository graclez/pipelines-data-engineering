{{ config(materialized='table') }}

select
  film_id as film_key,
  actor_id as actor_key
from {{ ref('stg_film_actor') }}