{{ config(
    materialized='table',
    file_format='iceberg',
    schema='business_vault'
) }}

with snapshot_times as (
    select distinct
        _snapshot_id as snapshot_id,
        _snapshot_at as snapshot_at
    from {{ source('bronze_history', 'customers') }}
),

as_of_versions as (
    select
        h.customer_hk,
        h.customer_id,
        b.snapshot_id,
        b.snapshot_at,
        max(s.load_dts) as sat_customer_load_dts
    from {{ ref('hub_customer') }} h
    cross join snapshot_times b
    left join {{ ref('sat_customer_details') }} s
        on s.customer_hk = h.customer_hk
       and s.load_dts <= b.snapshot_at
    group by
        h.customer_hk,
        h.customer_id,
        b.snapshot_id,
        b.snapshot_at
),

valid_versions as (
    select *
    from as_of_versions
    where sat_customer_load_dts is not null
),

with_previous as (
    select
        *,
        lag(sat_customer_load_dts) over (
            partition by customer_hk
            order by snapshot_at, snapshot_id
        ) as previous_sat_load_dts
    from valid_versions
)

select
    customer_hk,
    customer_id,
    snapshot_id,
    snapshot_at,
    sat_customer_load_dts
from with_previous
where previous_sat_load_dts is null
   or sat_customer_load_dts <> previous_sat_load_dts