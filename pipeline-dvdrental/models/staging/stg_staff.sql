with src as (
  select * from {{ source('airbyte_pfinal', 'staff') }}
)
select
  staff_id,
  first_name,
  last_name,
  address_id,
  email,
  store_id,
  active as is_active,
  username,
  last_update
from src