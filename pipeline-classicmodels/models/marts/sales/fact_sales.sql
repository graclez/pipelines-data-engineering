select
    o.order_number,
    o.order_date,
    o.customer_number,
    od.product_code,
    od.order_line_number,
    od.quantity_ordered,
    od.price_each,
    od.quantity_ordered * od.price_each as sales_amount
from {{ ref('stg_orders') }} o
join {{ ref('stg_orderdetails') }} od
    on cast(o.order_number as varchar) = cast(od.order_number as varchar)