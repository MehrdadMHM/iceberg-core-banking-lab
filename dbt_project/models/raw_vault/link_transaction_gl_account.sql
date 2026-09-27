{{ config(
    materialized='incremental',
    incremental_strategy='append',
    file_format='iceberg',
    schema='raw_vault'
) }}

with relationships as (
    select
        trim(transaction_id) as transaction_id,
        trim(gl_account_code) as gl_account_code,
        _source_system as record_source,
        min(_ingested_at) as load_dts
    from {{ source('bronze', 'transactions') }}
    where transaction_id is not null
      and gl_account_code is not null
    group by trim(transaction_id), trim(gl_account_code), _source_system
),

prepared as (
    select
        sha2(concat(record_source, '|', transaction_id), 256) as transaction_hk,
        sha2(concat(record_source, '|', gl_account_code), 256) as gl_account_hk,
        load_dts,
        record_source
    from relationships
),

links as (
    select
        sha2(concat(transaction_hk, '|', gl_account_hk), 256)
            as transaction_gl_account_hk,
        transaction_hk,
        gl_account_hk,
        load_dts,
        record_source
    from prepared
)

select *
from links

{% if is_incremental() %}
where transaction_gl_account_hk not in (
    select transaction_gl_account_hk from {{ this }}
)
{% endif %}