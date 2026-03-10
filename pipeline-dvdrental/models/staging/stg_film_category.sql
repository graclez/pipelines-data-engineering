select
  film_id,
  category_id,
  last_update
from {{ source('airbyte_pfinal', 'film_category') }}