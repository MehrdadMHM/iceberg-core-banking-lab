{{ config(
    materialized='incremental',
    incremental_strategy='append',
    file_format='iceberg',
    schema='raw_vault'
) }}

with snapshots as (
    select
        sha2(concat(_source_system, '|', trim(transaction_id)), 256)
            as transaction_hk,
        trim(transaction_id) as transaction_id,
        transaction_type,
        amount,
        currency,
        status,
        booking_date,
        created_at,
        sha2(
            to_json(named_struct(
                'transaction_type', transaction_type,
                'amount', cast(amount as string),
                'currency', currency,
                'status', status,
                'booking_date', cast(booking_date as string),
                'created_at', cast(created_at as string)
            )),
            256
        ) as hashdiff,
        _snapshot_at as load_dts,
        _snapshot_id as snapshot_id,
        _source_system as record_source
    from {{ source('bronze_history', 'transactions') }}
    where transaction_id is not null
),

compared as (
    select
        *,
        lag(hashdiff) over (
            partition by transaction_hk
            order by load_dts, snapshot_id
        ) as previous_hashdiff
    from snapshots
),

changed as (
    select
        transaction_hk,
        transaction_id,
        transaction_type,
        amount,
        currency,
        status,
        booking_date,
        created_at,
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
    where existing.transaction_hk = changed.transaction_hk
      and existing.snapshot_id = changed.snapshot_id
)
{% endif %}