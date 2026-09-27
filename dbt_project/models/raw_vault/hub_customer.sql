{{ config(
    materialized='incremental',
    incremental_strategy='append',
    file_format='iceberg',
    schema='raw_vault'
) }}

with customers as (

    select
        trim(customer_id) as customer_id,
        _source_system as record_source,
        min(_ingested_at) as load_dts
    from {{ source('bronze', 'customers') }}
    where customer_id is not null
    group by trim(customer_id), _source_system

),

prepared as (

    select
        sha2(concat(record_source, '|', customer_id), 256) as customer_hk,
        customer_id,
        load_dts,
        record_source
    from customers

)

select
    customer_hk,
    customer_id,
    load_dts,
    record_source
from prepared

{% if is_incremental() %}
where customer_hk not in (
    select customer_hk from {{ this }}
)
{% endif %}