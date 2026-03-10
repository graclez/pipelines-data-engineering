select
    customer_number,
    customer_name,
    contact_last_name,
    contact_first_name,
    phone,
    city,
    state,
    postal_code,
    country,
    credit_limit
from {{ ref('stg_customers') }}
where customer_number is not null