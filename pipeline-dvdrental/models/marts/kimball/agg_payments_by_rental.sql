{{ config(materialized='table') }}

select
  rental_key,
  count(*) as payment_count,
  sum(amount) as total_paid,
  min(payment_date) as first_payment_ts,
  max(payment_date) as last_payment_ts
from {{ ref('fct_payment') }}
group by 1