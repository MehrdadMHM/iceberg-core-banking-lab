{{ config(
    materialized='incremental',
    incremental_strategy='append',
    file_format='iceberg',
    schema='raw_vault'
) }}

with gl_accounts as (
    select
        trim(gl_account_code) as gl_account_code,
        _source_system as record_source,
        min(_ingested_at) as load_dts
    from {{ source('bronze', 'gl_accounts') }}
    where gl_account_code is not null
    group by trim(gl_account_code), _source_system
),

prepared as (
    select
        sha2(concat(record_source, '|', gl_account_code), 256) as gl_account_hk,
        gl_account_code,
        load_dts,
        record_source
    from gl_accounts
)

select gl_account_hk, gl_account_code, load_dts, record_source
from prepared

{% if is_incremental() %}
where gl_account_hk not in (
    select gl_account_hk from {{ this }}
)
{% endif %}