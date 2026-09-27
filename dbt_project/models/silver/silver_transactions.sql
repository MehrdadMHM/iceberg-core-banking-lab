{{ config(
    materialized='table',
    file_format='iceberg'
) }}

select
    transaction_id,
    customer_id,
    account_id,
    gl_account_code,
    transaction_type,
    amount,
    currency,
    status,
    booking_date,
    created_at,
    _source_system,
    _ingested_at
from {{ source('bronze', 'transactions') }}
where transaction_id is not null