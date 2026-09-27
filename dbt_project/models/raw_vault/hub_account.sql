{{ config(
    materialized='incremental',
    incremental_strategy='append',
    file_format='iceberg',
    schema='raw_vault'
) }}

with accounts as (
    select
        trim(account_id) as account_id,
        _source_system as record_source,
        min(_ingested_at) as load_dts
    from {{ source('bronze', 'accounts') }}
    where account_id is not null
    group by trim(account_id), _source_system
),

prepared as (
    select
        sha2(concat(record_source, '|', account_id), 256) as account_hk,
        account_id,
        load_dts,
        record_source
    from accounts
)

select account_hk, account_id, load_dts, record_source
from prepared

{% if is_incremental() %}
where account_hk not in (
    select account_hk from {{ this }}
)
{% endif %}