{{ config(
    materialized='incremental',
    incremental_strategy='append',
    file_format='iceberg',
    schema='raw_vault'
) }}

with relationships as (
    select
        trim(transaction_id) as transaction_id,
        trim(account_id) as account_id,
        _source_system as record_source,
        min(_ingested_at) as load_dts
    from {{ source('bronze', 'transactions') }}
    where transaction_id is not null
      and account_id is not null
    group by trim(transaction_id), trim(account_id), _source_system
),

prepared as (
    select
        sha2(concat(record_source, '|', transaction_id), 256) as transaction_hk,
        sha2(concat(record_source, '|', account_id), 256) as account_hk,
        load_dts,
        record_source
    from relationships
),

links as (
    select
        sha2(concat(transaction_hk, '|', account_hk), 256) as transaction_source_account_hk,
        transaction_hk,
        account_hk,
        load_dts,
        record_source
    from prepared
)

select *
from links

{% if is_incremental() %}
where transaction_source_account_hk not in (
    select transaction_source_account_hk from {{ this }}
)
{% endif %}