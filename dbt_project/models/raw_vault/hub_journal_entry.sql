{{ config(
    materialized='incremental',
    incremental_strategy='append',
    file_format='iceberg',
    schema='raw_vault'
) }}

with entries as (
    select
        trim(journal_entry_id) as journal_entry_id,
        _source_system as record_source,
        min(_ingested_at) as load_dts
    from {{ source('bronze', 'journal_entries') }}
    where journal_entry_id is not null
    group by trim(journal_entry_id), _source_system
),

prepared as (
    select
        sha2(concat(record_source, '|', journal_entry_id), 256)
            as journal_entry_hk,
        journal_entry_id,
        load_dts,
        record_source
    from entries
)

select *
from prepared

{% if is_incremental() %}
where journal_entry_hk not in (
    select journal_entry_hk from {{ this }}
)
{% endif %}