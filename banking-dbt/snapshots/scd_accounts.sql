{% snapshot scd_accounts %}

{{
    config(
        target_schema='SILVER',
        unique_key='account_id',
        strategy='timestamp',
        updated_at='updated_at',
        invalidate_hard_deletes=True
    )
}}

SELECT
    account_id,
    customer_id,
    account_type,
    account_number,
    balance,
    interest_rate,
    credit_limit,
    overdraft_limit,
    status,
    opened_branch_id,
    updated_at
FROM {{ ref('stg_accounts') }}

{% endsnapshot %}
