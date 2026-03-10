select
  film_id,
  actor_id,
  last_update
from {{ source('airbyte_pfinal', 'film_actor') }}