{{ config(
    materialized='incremental',
    incremental_strategy='append',
    file_format='iceberg',
    schema='raw_vault'
) }}

with transactions as (
    select
        trim(transaction_id) as transaction_id,
        _source_system as record_source,
        min(_ingested_at) as load_dts
    from {{ source('bronze', 'transactions') }}
    where transaction_id is not null
    group by trim(transaction_id), _source_system
),

prepared as (
    select
        sha2(concat(record_source, '|', transaction_id), 256) as transaction_hk,
        transaction_id,
        load_dts,
        record_source
    from transactions
)

select transaction_hk, transaction_id, load_dts, record_source
from prepared

{% if is_incremental() %}
where transaction_hk not in (
    select transaction_hk from {{ this }}
)
{% endif %}