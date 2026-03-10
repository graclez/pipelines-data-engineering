{{ config(materialized='table') }}

with ranked as (
    select
        *,
        row_number() over (
            partition by customer_key
            order by revenue_per_category desc
        ) as rn
    from {{ ref('agg_customer_category') }}
)

select
    customer_key,
    category_key as favorite_category_key,
    category_name as favorite_category_name,
    rentals_per_category as favorite_category_rentals,
    revenue_per_category as favorite_category_revenue
from ranked
where rn = 1