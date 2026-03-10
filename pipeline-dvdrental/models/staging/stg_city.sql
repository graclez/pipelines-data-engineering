with src as (
  select * from {{ source('airbyte_pfinal', 'city') }}
)
select
  city_id,
  city,
  country_id,
  last_update
from src