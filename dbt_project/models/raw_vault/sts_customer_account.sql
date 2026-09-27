{{ config(
    materialized='incremental',
    incremental_strategy='append',
    file_format='iceberg',
    schema='raw_vault'
) }}

with batches as (
    select distinct
        _snapshot_id as snapshot_id,
        _snapshot_at as load_dts
    from {{ source('bronze_history', 'accounts') }}
),

relationships as (
    select distinct
        sha2(concat(_source_system, '|', trim(customer_id)), 256)
            as customer_hk,
        sha2(concat(_source_system, '|', trim(account_id)), 256)
            as account_hk,
        _source_system as record_source,
        _snapshot_id as snapshot_id,
        _snapshot_at as load_dts
    from {{ source('bronze_history', 'accounts') }}
    where customer_id is not null
      and account_id is not null
),

keys as (
    select
        sha2(concat(customer_hk, '|', account_hk), 256)
            as customer_account_hk,
        customer_hk,
        account_hk,
        record_source,
        snapshot_id,
        load_dts
    from relationships
),

first_seen as (
    select
        customer_account_hk,
        customer_hk,
        account_hk,
        record_source,
        min(load_dts) as first_load_dts
    from keys
    group by
        customer_account_hk,
        customer_hk,
        account_hk,
        record_source
),

observations as (
    select
        f.customer_account_hk,
        f.customer_hk,
        f.account_hk,
        b.snapshot_id,
        b.load_dts,
        f.record_source,
        case
            when k.customer_account_hk is not null then true
            else false
        end as is_active
    from first_seen f
    cross join batches b
    left join keys k
        on k.customer_account_hk = f.customer_account_hk
       and k.snapshot_id = b.snapshot_id
    where b.load_dts >= f.first_load_dts
),

compared as (
    select
        *,
        lag(is_active) over (
            partition by customer_account_hk
            order by load_dts, snapshot_id
        ) as previous_is_active
    from observations
),

changes as (
    select
        customer_account_hk,
        customer_hk,
        account_hk,
        is_active,
        load_dts,
        snapshot_id,
        record_source
    from compared
    where previous_is_active is null
       or is_active <> previous_is_active
)

select *
from changes

{% if is_incremental() %}
where not exists (
    select 1
    from {{ this }} existing
    where existing.customer_account_hk = changes.customer_account_hk
      and existing.snapshot_id = changes.snapshot_id
)
{% endif %}