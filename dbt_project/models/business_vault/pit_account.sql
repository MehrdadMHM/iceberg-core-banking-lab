{{ config(
    materialized='table',
    file_format='iceberg',
    schema='business_vault'
) }}

with snapshot_times as (
    select distinct
        _snapshot_id as snapshot_id,
        _snapshot_at as snapshot_at
    from {{ source('bronze_history', 'accounts') }}
),

as_of_versions as (
    select
        h.account_hk,
        h.account_id,
        b.snapshot_id,
        b.snapshot_at,
        max(s.load_dts) as sat_account_load_dts
    from {{ ref('hub_account') }} h
    cross join snapshot_times b
    left join {{ ref('sat_account_details') }} s
        on s.account_hk = h.account_hk
       and s.load_dts <= b.snapshot_at
    group by
        h.account_hk,
        h.account_id,
        b.snapshot_id,
        b.snapshot_at
),

valid_versions as (
    select *
    from as_of_versions
    where sat_account_load_dts is not null
),

with_previous as (
    select
        *,
        lag(sat_account_load_dts) over (
            partition by account_hk
            order by snapshot_at, snapshot_id
        ) as previous_sat_load_dts
    from valid_versions
)

select
    account_hk,
    account_id,
    snapshot_id,
    snapshot_at,
    sat_account_load_dts
from with_previous
where previous_sat_load_dts is null
   or sat_account_load_dts <> previous_sat_load_dts