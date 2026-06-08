{{
    config(
        materialized='table'
    )
}}

/*
    dim_accounts — Gold Layer
    Account dimension with customer details denormalized.
*/

WITH accounts AS (
    SELECT * FROM {{ ref('stg_accounts') }}
),

customers AS (
    SELECT
        customer_id,
        first_name,
        last_name,
        first_name || ' ' || last_name AS full_name,
        credit_score,
        risk_rating
    FROM {{ ref('stg_customers') }}
),

txn_summary AS (
    SELECT
        account_id,
        COUNT(*)                                           AS total_transactions,
        SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS completed_transactions,
        SUM(CASE WHEN is_flagged THEN 1 ELSE 0 END)       AS flagged_transactions,
        MIN(transaction_at)                                AS first_transaction_at,
        MAX(transaction_at)                                AS last_transaction_at,
        SUM(CASE WHEN transaction_type = 'deposit' AND status = 'completed'
            THEN amount ELSE 0 END)                        AS total_deposits,
        SUM(CASE WHEN transaction_type = 'withdrawal' AND status = 'completed'
            THEN amount ELSE 0 END)                        AS total_withdrawals
    FROM {{ ref('stg_transactions') }}
    GROUP BY account_id
)

SELECT
    a.account_id,
    a.customer_id,
    c.full_name                                     AS customer_name,
    c.credit_score,
    c.risk_rating                                   AS customer_risk_rating,
    a.account_type,
    a.account_number,
    a.balance,
    a.currency,
    a.interest_rate,
    a.credit_limit,
    a.overdraft_limit,
    a.status,
    a.opened_branch_id,
    a.opened_at,
    a.closed_at,

    -- Transaction summary
    COALESCE(ts.total_transactions, 0)              AS total_transactions,
    COALESCE(ts.completed_transactions, 0)          AS completed_transactions,
    COALESCE(ts.flagged_transactions, 0)            AS flagged_transactions,
    COALESCE(ts.total_deposits, 0)                  AS total_deposits,
    COALESCE(ts.total_withdrawals, 0)               AS total_withdrawals,
    ts.first_transaction_at,
    ts.last_transaction_at,

    -- Derived
    CASE
        WHEN a.account_type = 'credit' AND a.credit_limit > 0
            THEN ROUND(ABS(a.balance) / a.credit_limit * 100, 2)
        ELSE NULL
    END                                             AS credit_utilization_pct,

    DATEDIFF('day', a.opened_at, CURRENT_TIMESTAMP()) AS account_age_days,

    a.updated_at
FROM accounts a
LEFT JOIN customers c ON a.customer_id = c.customer_id
LEFT JOIN txn_summary ts ON a.account_id = ts.account_id
