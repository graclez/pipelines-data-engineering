-- Falla si obt_rentals tiene más de 1 fila por rental_key

select
    rental_key,
    count(*) as cnt
from {{ ref('obt_rentals') }}
group by 1
having count(*) > 1