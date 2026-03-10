{{ config(materialized='table') }}

select
  actor_id as actor_key,
  first_name,
  last_name
from {{ ref('stg_actor') }}