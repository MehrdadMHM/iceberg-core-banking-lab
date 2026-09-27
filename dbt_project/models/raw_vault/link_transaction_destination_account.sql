{{ config(
    materialized='incremental',
    incremental_strategy='append',
    file_format='iceberg',
    schema='raw_vault'
) }}

with relationships as (
    select
        trim(transaction_id) as transaction_id,
        trim(destination_account_id) as destination_account_id,
        _source_system as record_source,
        min(_ingested_at) as load_dts
    from {{ source('bronze', 'transactions') }}
    where transaction_id is not null
      and destination_account_id is not null
      and trim(destination_account_id) <> ''
    group by
        trim(transaction_id),
        trim(destination_account_id),
        _source_system
),

prepared as (
    select
        sha2(concat(record_source, '|', transaction_id), 256) as transaction_hk,
        sha2(concat(record_source, '|', destination_account_id), 256)
            as destination_account_hk,
        load_dts,
        record_source
    from relationships
),

links as (
    select
        sha2(concat(transaction_hk, '|', destination_account_hk), 256)
            as transaction_destination_account_hk,
        transaction_hk,
        destination_account_hk,
        load_dts,
        record_source
    from prepared
)

select *
from links

{% if is_incremental() %}
where transaction_destination_account_hk not in (
    select transaction_destination_account_hk from {{ this }}
)
{% endif %}