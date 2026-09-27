{{ config(
    materialized='incremental',
    incremental_strategy='append',
    file_format='iceberg',
    schema='raw_vault'
) }}

with snapshots as (
    select
        sha2(
            concat(
                sha2(
                    concat(_source_system, '|', trim(journal_entry_id)),
                    256
                ),
                '|',
                trim(posting_id),
                '|',
                sha2(
                    concat(_source_system, '|', trim(gl_account_code)),
                    256
                )
            ),
            256
        ) as journal_posting_gl_hk,
        trim(posting_id) as posting_id,
        debit_amount,
        credit_amount,
        sha2(
            to_json(named_struct(
                'debit_amount', cast(debit_amount as string),
                'credit_amount', cast(credit_amount as string)
            )),
            256
        ) as hashdiff,
        _snapshot_at as load_dts,
        _snapshot_id as snapshot_id,
        _source_system as record_source
    from {{ source('bronze_history', 'journal_postings') }}
    where posting_id is not null
      and journal_entry_id is not null
      and gl_account_code is not null
),

compared as (
    select
        *,
        lag(hashdiff) over (
            partition by journal_posting_gl_hk
            order by load_dts, snapshot_id
        ) as previous_hashdiff
    from snapshots
),

changed as (
    select
        journal_posting_gl_hk,
        posting_id,
        debit_amount,
        credit_amount,
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
    where existing.journal_posting_gl_hk = changed.journal_posting_gl_hk
      and existing.snapshot_id = changed.snapshot_id
)
{% endif %}