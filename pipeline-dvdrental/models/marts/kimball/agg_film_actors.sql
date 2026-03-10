{{ config(materialized='table') }}

select
  b.film_key,
  string_agg(a.first_name || ' ' || a.last_name, ', ' order by a.last_name, a.first_name) as actor_list,
  count(*) as actor_count
from {{ ref('bridge_film_actor') }} b
join {{ ref('dim_actor') }} a
  on b.actor_key = a.actor_key
group by 1