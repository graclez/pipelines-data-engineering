with src as (
  select * from {{ source('airbyte_pfinal', 'customer') }}
)
select
  customer_id,
  store_id,
  first_name,
  last_name,
  email,
  activebool as is_active,
  create_date::date as created_date,
  last_update
from src