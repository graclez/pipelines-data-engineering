{{ config(materialized='table') }}

with store as (
  select * from {{ ref('stg_store') }}
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
  s.store_id as store_key,
  s.manager_staff_id as manager_staff_key,

  a.address_id,
  a.address,
  a.address2,
  a.district,
  a.postal_code,
  a.phone,

  c.city_id,
  c.city,

  co.country_id,
  co.country

from store s
left join addr a on s.address_id = a.address_id
left join city c on a.city_id = c.city_id
left join country co on c.country_id = co.country_id