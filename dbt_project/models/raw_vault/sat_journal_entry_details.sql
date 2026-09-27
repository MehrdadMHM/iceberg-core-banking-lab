{{ config(
    materialized='incremental',
    incremental_strategy='append',
    file_format='iceberg',
    schema='raw_vault'
) }}

with snapshots as (
    select
        sha2(concat(_source_system, '|', trim(journal_entry_id)), 256)
            as journal_entry_hk,
        trim(journal_entry_id) as journal_entry_id,
        posting_date,
        status,
        sha2(
            to_json(named_struct(
                'posting_date', cast(posting_date as string),
                'status', status
            )),
            256
        ) as hashdiff,
        _snapshot_at as load_dts,
        _snapshot_id as snapshot_id,
        _source_system as record_source
    from {{ source('bronze_history', 'journal_entries') }}
    where journal_entry_id is not null
),

compared as (
    select
        *,
        lag(hashdiff) over (
            partition by journal_entry_hk
            order by load_dts, snapshot_id
        ) as previous_hashdiff
    from snapshots
),

changed as (
    select
        journal_entry_hk,
        journal_entry_id,
        posting_date,
        status,
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
    where existing.journal_entry_hk = changed.journal_entry_hk
      and existing.snapshot_id = changed.snapshot_id
)
{% endif %}