{{ config(materialized='table') }}

with rentals as (
    select * from {{ ref('fct_rental') }}
),
film_cat as (
    select * from {{ ref('bridge_film_category') }}
),
categories as (
    select * from {{ ref('dim_category') }}
)

select
    r.customer_key,
    c.category_key,
    c.category_name,
    count(r.rental_key) as rentals_per_category,
    sum(r.total_paid) as revenue_per_category
from rentals r
join film_cat fc on r.film_key = fc.film_key
join categories c on fc.category_key = c.category_key
group by
    r.customer_key,
    c.category_key,
    c.category_name