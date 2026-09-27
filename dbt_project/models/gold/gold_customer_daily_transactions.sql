{{ config(
    materialized='table',
    file_format='iceberg',
    schema='gold'
) }}

with daily_transactions as (

    select
        customer_id,
        booking_date,
        currency,
        count(*) as transaction_count,
        sum(amount) as total_amount
    from {{ ref('silver_transactions') }}
    where customer_id is not null
      and booking_date is not null
    group by
        customer_id,
        booking_date,
        currency

)

select
    d.customer_id,
    concat_ws(' ', c.first_name, c.last_name) as customer_name,
    d.booking_date,
    d.currency,
    d.transaction_count,
    d.total_amount
from daily_transactions as d
left join {{ ref('silver_customers') }} as c
    on d.customer_id = c.customer_id