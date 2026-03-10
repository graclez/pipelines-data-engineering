{{ config(materialized='table') }}

select
  payment_id as payment_key,
  customer_id as customer_key,
  rental_id as rental_key,
  staff_id as staff_key,
  amount,
  payment_date,
  cast(payment_date as date) as payment_date_key
from {{ ref('stg_payment') }}