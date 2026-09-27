{{ config(
    materialized='table',
    file_format='iceberg',
    schema='gold'
) }}

select
    a.account_id,
    a.customer_id,
    concat_ws(' ', c.first_name, c.last_name) as customer_name,
    t.gl_account_code,
    g.account_name as gl_account_name,
    a.currency,
    count(t.transaction_id) as transaction_count,
    coalesce(
        sum(t.amount),
        cast(0 as decimal(18, 2))
    ) as total_transaction_amount
from {{ ref('silver_accounts') }} as a
left join {{ ref('silver_customers') }} as c
    on a.customer_id = c.customer_id
left join {{ ref('silver_transactions') }} as t
    on a.account_id = t.account_id
   and a.currency = t.currency
left join {{ ref('silver_gl_accounts') }} as g
    on t.gl_account_code = g.gl_account_code
group by
    a.account_id,
    a.customer_id,
    c.first_name,
    c.last_name,
    t.gl_account_code,
    g.account_name,
    a.currency