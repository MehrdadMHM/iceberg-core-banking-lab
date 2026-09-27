{{ config(
    materialized='incremental',
    incremental_strategy='append',
    file_format='iceberg',
    schema='raw_vault'
) }}

with snapshots as (
    select
        sha2(concat(_source_system, '|', trim(gl_account_code)), 256)
            as gl_account_hk,
        trim(gl_account_code) as gl_account_code,
        account_name,
        account_category,
        is_active,
        sha2(
            to_json(named_struct(
                'account_name', account_name,
                'account_category', account_category,
                'is_active', is_active
            )),
            256
        ) as hashdiff,
        _snapshot_at as load_dts,
        _snapshot_id as snapshot_id,
        _source_system as record_source
    from {{ source('bronze_history', 'gl_accounts') }}
    where gl_account_code is not null
),

compared as (
    select
        *,
        lag(hashdiff) over (
            partition by gl_account_hk
            order by load_dts, snapshot_id
        ) as previous_hashdiff
    from snapshots
),

changed as (
    select
        gl_account_hk,
        gl_account_code,
        account_name,
        account_category,
        is_active,
        hashdiff,
        load_dts,
        snapshot_id,
        record_source
    from compared
    where previous_hashdiff is null
       or hashdiff <> previous_hashdiff
)

select *
from changed

{% if is_incremental() %}
where not exists (
    select 1
    from {{ this }} existing
    where existing.gl_account_hk = changed.gl_account_hk
      and existing.snapshot_id = changed.snapshot_id
)
{% endif %}