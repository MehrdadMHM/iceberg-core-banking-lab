{{ config(
    materialized='incremental',
    incremental_strategy='append',
    file_format='iceberg',
    schema='raw_vault'
) }}

with snapshots as (
    select
        sha2(
            concat(_source_system, '|', trim(customer_id)),
            256
        ) as customer_hk,
        trim(customer_id) as customer_id,
        first_name,
        last_name,
        created_at,
        sha2(
            to_json(named_struct(
                'first_name', first_name,
                'last_name', last_name,
                'created_at', cast(created_at as string)
            )),
            256
        ) as hashdiff,
        _snapshot_at as load_dts,
        _snapshot_id as snapshot_id,
        _source_system as record_source
    from {{ source('bronze_history', 'customers') }}
    where customer_id is not null
),

compared as (
    select
        *,
        lag(hashdiff) over (
            partition by customer_hk
            order by load_dts, snapshot_id
        ) as previous_hashdiff
    from snapshots
),

changed as (
    select
        customer_hk,
        customer_id,
        first_name,
        last_name,
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
    from {{ this }} as existing
    where existing.customer_hk = changed.customer_hk
      and existing.snapshot_id = changed.snapshot_id
)
{% endif %}