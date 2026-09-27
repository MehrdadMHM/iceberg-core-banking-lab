{{ config(
    materialized='incremental',
    incremental_strategy='append',
    file_format='iceberg',
    schema='raw_vault'
) }}

with snapshots as (
    select
        sha2(
            concat(_source_system, '|', trim(account_id)),
            256
        ) as account_hk,
        trim(account_id) as account_id,
        iban,
        currency,
        status,
        opened_at,
        sha2(
            to_json(named_struct(
                'iban', iban,
                'currency', currency,
                'status', status,
                'opened_at', cast(opened_at as string)
            )),
            256
        ) as hashdiff,
        _snapshot_at as load_dts,
        _snapshot_id as snapshot_id,
        _source_system as record_source
    from {{ source('bronze_history', 'accounts') }}
    where account_id is not null
),

compared as (
    select
        *,
        lag(hashdiff) over (
            partition by account_hk
            order by load_dts, snapshot_id
        ) as previous_hashdiff
    from snapshots
),

changed as (
    select
        account_hk,
        account_id,
        iban,
        currency,
        status,
        opened_at,
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
    from {{ this }} as existing
    where existing.account_hk = changed.account_hk
      and existing.snapshot_id = changed.snapshot_id
)
{% endif %}