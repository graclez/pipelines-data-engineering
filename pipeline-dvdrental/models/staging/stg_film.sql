with film as (
  select * from {{ source('airbyte_pfinal', 'film') }}
),
lang as (
  select * from {{ source('airbyte_pfinal', 'language') }}
)
select
  f.film_id,
  f.title,
  f.description,
  f.release_year,
  f.rental_duration,
  f.rental_rate,
  f.length,
  f.replacement_cost,
  f.rating,
  f.language_id,
  l.name as language_name,
  f.last_update
from film f
left join lang l on f.language_id = l.language_id