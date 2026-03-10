{{ config(materialized='table') }}

with fct as (
    select * from {{ ref('fct_rental') }}
),
cust as (
    select * from {{ ref('dim_customer') }}
),
film as (
    select * from {{ ref('dim_film') }}
),
store as (
    select * from {{ ref('dim_store') }}
),
staff as (
    select * from {{ ref('dim_staff') }}
),
date_dim as (
    select * from {{ ref('dim_date') }}
),
film_cat as (
    select * from {{ ref('agg_film_categories') }}
),
film_act as (
    select * from {{ ref('agg_film_actors') }}
)

select
    -- Grain
    fct.rental_key,

    -- Rental info
    fct.rental_date,
    fct.return_date,
    fct.rental_days,
    fct.is_returned,

    -- Payment metrics
    fct.payment_count,
    fct.total_paid,
    fct.first_payment_ts,
    fct.last_payment_ts,

    -- Customer
    cust.customer_key,
    cust.first_name as customer_first_name,
    cust.last_name as customer_last_name,
    cust.email,
    cust.is_active as customer_active,

    -- Film
    film.film_key,
    film.title as film_title,
    film.rating,
    film.language_name,
    film.rental_rate,
    film.length,
    film.replacement_cost,

    film_cat.category_list,
    film_cat.category_count,

    film_act.actor_list,
    film_act.actor_count,

    -- Store
    store.store_key,
    store.city as store_city,
    store.country as store_country,

    -- Staff
    staff.staff_key,
    staff.first_name as staff_first_name,
    staff.last_name as staff_last_name,

    -- Date attributes
    date_dim.year,
    date_dim.month,
    date_dim.month_name,
    date_dim.quarter,
    date_dim.day_name,
    date_dim.is_weekend,
	fct.rental_date_key,
	fct.return_date_key,
	fct.is_paid,
	date_dim.date_key,
	date_dim.day,
	date_dim.day_of_week,
	date_dim.year || '-' || lpad(cast(date_dim.month as varchar), 2, '0') as year_month,

from fct
join cust on fct.customer_key = cust.customer_key
join film on fct.film_key = film.film_key
join store on fct.store_key = store.store_key
join staff on fct.staff_key = staff.staff_key
left join date_dim on fct.rental_date_key = date_dim.date_key
left join film_cat on fct.film_key = film_cat.film_key
left join film_act on fct.film_key = film_act.film_key