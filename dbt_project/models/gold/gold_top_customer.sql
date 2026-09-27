{{ config(
    materialized='table',
    file_format='iceberg',
    schema='gold'
) }}

with customer_totals as (

    select
        customer_id,
        customer_name,
        currency,
        sum(transaction_count) as transaction_count,
        sum(total_amount) as total_transaction_amount
    from {{ ref('gold_customer_daily_transactions') }}
    where currency = 'EUR'
    group by
        customer_id,
        customer_name,
        currency

),

ranked as (

    select
        *,
        row_number() over (
            order by total_transaction_amount desc, customer_id
        ) as customer_rank
    from customer_totals

)

select
    customer_id,
    customer_name,
    currency,
    transaction_count,
    total_transaction_amount,
    customer_rank
from ranked
where customer_rank <= 20