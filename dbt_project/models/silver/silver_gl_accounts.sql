{{ config(materialized='table', file_format='iceberg') }}

select *
from {{ source('bronze', 'gl_accounts') }}