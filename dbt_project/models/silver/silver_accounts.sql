{{ config(materialized='table', file_format='iceberg') }}

select *
from {{ source('bronze', 'accounts') }}