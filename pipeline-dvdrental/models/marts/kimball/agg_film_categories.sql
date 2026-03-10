{{ config(materialized='table') }}

select
  b.film_key,
  string_agg(c.category_name, ', ' order by c.category_name) as category_list,
  count(*) as category_count
from {{ ref('bridge_film_category') }} b
join {{ ref('dim_category') }} c
  on b.category_key = c.category_key
group by 1