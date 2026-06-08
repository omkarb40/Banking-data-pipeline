{{
    config(
        materialized='incremental',
        unique_key='transaction_id'
    )
}}

/*
    fct_transactions — Gold Layer
    One row per transaction with FK references to dimensions
    and derived analytics columns.
*/

WITH transactions AS (
    SELECT * FROM {{ ref('stg_transactions') }}
    {% if is_incremental() %}
        WHERE loaded_at > (SELECT MAX(loaded_at) FROM {{ this }})
    {% endif %}
),

accounts AS (
    SELECT
        account_id,
        customer_id,
        account_type
    FROM {{ ref('stg_accounts') }}
)

SELECT
    t.transaction_id,
    t.account_id,
    a.customer_id,
    a.account_type,

    -- Transaction details
    t.transaction_type,
    t.amount,
    t.balance_before,
    t.balance_after,
    t.balance_after - t.balance_before              AS balance_change,
    t.currency,
    t.status,
    t.channel,
    t.description,
    t.reference_id,
    t.counterparty_account,

    -- Merchant info
    t.merchant_name,
    t.merchant_category,

    -- Fraud / compliance
    t.is_flagged,
    t.flag_reason,
    t.processed_branch_id,

    -- Time dimensions
    t.transaction_at,
    DATE(t.transaction_at)                          AS transaction_date,
    EXTRACT(HOUR FROM t.transaction_at)             AS transaction_hour,
    DAYOFWEEK(t.transaction_at)                     AS day_of_week,
    CASE
        WHEN DAYOFWEEK(t.transaction_at) IN (0, 6) THEN TRUE
        ELSE FALSE
    END                                             AS is_weekend,
    EXTRACT(MONTH FROM t.transaction_at)            AS transaction_month,
    EXTRACT(YEAR FROM t.transaction_at)             AS transaction_year,

    -- Derived flags
    CASE WHEN t.amount > 10000 THEN TRUE ELSE FALSE END AS is_large_transaction,
    CASE WHEN t.amount > 50000 THEN TRUE ELSE FALSE END AS is_high_value,
    CASE
        WHEN EXTRACT(HOUR FROM t.transaction_at) < 6
            OR EXTRACT(HOUR FROM t.transaction_at) > 22
        THEN TRUE
        ELSE FALSE
    END                                             AS is_off_hours,

    -- Flow direction
    CASE
        WHEN t.transaction_type IN ('deposit', 'interest', 'refund') THEN 'inflow'
        WHEN t.transaction_type IN ('withdrawal', 'payment', 'fee') THEN 'outflow'
        WHEN t.transaction_type = 'transfer' THEN 'transfer'
    END                                             AS flow_direction,

    t.loaded_at
FROM transactions t
LEFT JOIN accounts a ON t.account_id = a.account_id
