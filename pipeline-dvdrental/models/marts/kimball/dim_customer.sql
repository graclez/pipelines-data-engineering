{{ config(materialized='table') }}

select
  customer_id as customer_key,
  store_id as store_key,
  first_name,
  last_name,
  email,
  is_active,
  created_date
from {{ ref('stg_customer') }}