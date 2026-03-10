{{ config(materialized='table') }}

with staff as (
  select * from {{ ref('stg_staff') }}
),
addr as (
  select * from {{ ref('stg_address') }}
),
city as (
  select * from {{ ref('stg_city') }}
),
country as (
  select * from {{ ref('stg_country') }}
)

select
  s.staff_id as staff_key,
  s.first_name,
  s.last_name,
  s.email,
  s.is_active,
  s.username,
  s.store_id as store_key,

  a.address,
  a.address2,
  a.district,
  a.postal_code,
  a.phone,
  c.city,
  co.country

from staff s
left join addr a on s.address_id = a.address_id
left join city c on a.city_id = c.city_id
left join country co on c.country_id = co.country_id