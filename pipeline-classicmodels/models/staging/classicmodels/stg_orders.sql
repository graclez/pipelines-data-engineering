select
    ordernumber as order_number,
    orderdate as order_date,
    requireddate as required_date,
    shippeddate as shipped_date,
    status,
    comments,
    customernumber as customer_number
from {{ source('classicmodels','orders') }}