select
    customernumber as customer_number,
    customername as customer_name,
    contactlastname as contact_last_name,
    contactfirstname as contact_first_name,
    phone,
    city,
    state,
    postalcode as postal_code,
    country,
    creditlimit as credit_limit
from {{ source('classicmodels','customers') }}