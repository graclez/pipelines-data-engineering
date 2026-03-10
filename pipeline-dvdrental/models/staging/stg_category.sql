select
  category_id,
  name as category_name,
  last_update
from {{ source('airbyte_pfinal', 'category') }}