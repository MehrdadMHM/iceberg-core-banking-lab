{{ config(
    materialized='incremental',
    incremental_strategy='append',
    file_format='iceberg',
    schema='raw_vault'
) }}

with relationships as (
    select
        trim(journal_entry_id) as journal_entry_id,
        trim(transaction_id) as transaction_id,
        _source_system as record_source,
        min(_ingested_at) as load_dts
    from {{ source('bronze', 'journal_entries') }}
    where journal_entry_id is not null
      and transaction_id is not null
    group by
        trim(journal_entry_id),
        trim(transaction_id),
        _source_system
),

prepared as (
    select
        sha2(concat(record_source, '|', journal_entry_id), 256)
            as journal_entry_hk,
        sha2(concat(record_source, '|', transaction_id), 256)
            as transaction_hk,
        load_dts,
        record_source
    from relationships
),

links as (
    select
        sha2(concat(journal_entry_hk, '|', transaction_hk), 256)
            as journal_entry_transaction_hk,
        journal_entry_hk,
        transaction_hk,
        load_dts,
        record_source
    from prepared
)

select *
from links

{% if is_incremental() %}
where journal_entry_transaction_hk not in (
    select journal_entry_transaction_hk from {{ this }}
)
{% endif %}