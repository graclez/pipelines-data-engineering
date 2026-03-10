with src as (
  select * from {{ source('airbyte_pfinal', 'country') }}
)
select
  country_id,
  country,
  last_update
from src