-- Falla si hay diferencias entre total_paid en fct_rental y la suma de payments por rental

with p as (
    select
        rental_key,
        sum(amount) as payment_sum
    from {{ ref('fct_payment') }}
    group by 1
),
r as (
    select
        rental_key,
        total_paid
    from {{ ref('fct_rental') }}
)

select
    r.rental_key,
    r.total_paid,
    coalesce(p.payment_sum, 0) as payment_sum,
    r.total_paid - coalesce(p.payment_sum, 0) as diff
from r
left join p on r.rental_key = p.rental_key
where abs(r.total_paid - coalesce(p.payment_sum, 0)) > 0.00001