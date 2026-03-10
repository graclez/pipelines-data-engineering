with src as (
  select * from {{ source('airbyte_pfinal', 'store') }}
)
select
  store_id,
  manager_staff_id,
  address_id,
  last_update
from src