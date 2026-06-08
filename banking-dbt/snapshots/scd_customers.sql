{% snapshot scd_customers %}

{{
    config(
        target_schema='SILVER',
        unique_key='customer_id',
        strategy='timestamp',
        updated_at='updated_at',
        invalidate_hard_deletes=True
    )
}}

SELECT
    customer_id,
    first_name,
    last_name,
    email,
    credit_score,
    annual_income,
    employment_status,
    risk_rating,
    home_branch_id,
    is_active,
    updated_at
FROM {{ ref('stg_customers') }}

{% endsnapshot %}
