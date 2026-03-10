with src as (
  select * from {{ source('airbyte_pfinal', 'address') }}
)
select
  address_id,
  address,
  address2,
  district,
  city_id,
  postal_code,
  phone,
  last_update
from src