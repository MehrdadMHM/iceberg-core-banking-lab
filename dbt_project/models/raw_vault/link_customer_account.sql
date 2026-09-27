{{ config(
    materialized='incremental',
    incremental_strategy='append',
    file_format='iceberg',
    schema='raw_vault'
) }}

with relationships as (
    select
        trim(customer_id) as customer_id,
        trim(account_id) as account_id,
        _source_system as record_source,
        min(_snapshot_at) as load_dts
    from {{ source('bronze_history', 'accounts') }}
    where customer_id is not null
      and account_id is not null
    group by trim(customer_id), trim(account_id), _source_system
),

prepared as (
    select
        sha2(concat(record_source, '|', customer_id), 256) as customer_hk,
        sha2(concat(record_source, '|', account_id), 256) as account_hk,
        load_dts,
        record_source
    from relationships
),

links as (
    select
        sha2(concat(customer_hk, '|', account_hk), 256) as customer_account_hk,
        customer_hk,
        account_hk,
        load_dts,
        record_source
    from prepared
)

select *
from links

{% if is_incremental() %}
where customer_account_hk not in (
    select customer_account_hk from {{ this }}
)
{% endif %}