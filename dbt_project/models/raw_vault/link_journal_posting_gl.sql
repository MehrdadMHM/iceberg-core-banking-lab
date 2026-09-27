{{ config(
    materialized='incremental',
    incremental_strategy='append',
    file_format='iceberg',
    schema='raw_vault'
) }}

with postings as (
    select
        trim(posting_id) as posting_id,
        trim(journal_entry_id) as journal_entry_id,
        trim(gl_account_code) as gl_account_code,
        _source_system as record_source,
        min(_ingested_at) as load_dts
    from {{ source('bronze', 'journal_postings') }}
    where posting_id is not null
      and journal_entry_id is not null
      and gl_account_code is not null
    group by
        trim(posting_id),
        trim(journal_entry_id),
        trim(gl_account_code),
        _source_system
),

prepared as (
    select
        posting_id,
        sha2(concat(record_source, '|', journal_entry_id), 256)
            as journal_entry_hk,
        sha2(concat(record_source, '|', gl_account_code), 256)
            as gl_account_hk,
        load_dts,
        record_source
    from postings
),

links as (
    select
        sha2(
            concat(journal_entry_hk, '|', posting_id, '|', gl_account_hk),
            256
        ) as journal_posting_gl_hk,
        posting_id,
        journal_entry_hk,
        gl_account_hk,
        load_dts,
        record_source
    from prepared
)

select *
from links

{% if is_incremental() %}
where journal_posting_gl_hk not in (
    select journal_posting_gl_hk from {{ this }}
)
{% endif %}