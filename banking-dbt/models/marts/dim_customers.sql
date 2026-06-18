{{
    config(
        materialized='table'
    )
}}

/*
    dim_customers — Gold Layer
    Customer dimension with aggregated account and risk metrics.
*/

WITH customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

account_metrics AS (
    SELECT
        customer_id,
        COUNT(*)                                    AS total_accounts,
        SUM(balance)                                AS total_balance,
        COUNT(CASE WHEN status = 'active' THEN 1 END) AS active_accounts,
        MAX(opened_at)                              AS latest_account_opened,
        SUM(CASE WHEN account_type = 'checking' THEN balance ELSE 0 END) AS checking_balance,
        SUM(CASE WHEN account_type = 'savings' THEN balance ELSE 0 END)  AS savings_balance,
        SUM(CASE WHEN account_type = 'credit' THEN balance ELSE 0 END)   AS credit_balance
    FROM {{ ref('stg_accounts') }}
    GROUP BY customer_id
),

loan_metrics AS (
    SELECT
        customer_id,
        COUNT(*)                                    AS total_loans,
        SUM(outstanding)                            AS total_outstanding,
        COUNT(CASE WHEN status = 'defaulted' THEN 1 END) AS defaulted_loans
    FROM {{ ref('stg_loans') }}
    GROUP BY customer_id
)

SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
    c.first_name || ' ' || c.last_name             AS full_name,
    c.email,
    c.phone,
    c.date_of_birth,
    DATEDIFF('year', c.date_of_birth, CURRENT_DATE()) AS age,
    c.city,
    c.state,
    c.zip_code,
    c.country,
    c.credit_score,
    c.annual_income,
    c.employment_status,
    c.risk_rating,
    c.home_branch_id,

    -- Account metrics
    COALESCE(am.total_accounts, 0)                  AS total_accounts,
    COALESCE(am.active_accounts, 0)                 AS active_accounts,
    COALESCE(am.total_balance, 0)                   AS total_balance,
    COALESCE(am.checking_balance, 0)                AS checking_balance,
    COALESCE(am.savings_balance, 0)                 AS savings_balance,
    COALESCE(am.credit_balance, 0)                  AS credit_balance,
    am.latest_account_opened,

    -- Loan metrics
    COALESCE(lm.total_loans, 0)                     AS total_loans,
    COALESCE(lm.total_outstanding, 0)               AS total_loan_outstanding,
    COALESCE(lm.defaulted_loans, 0)                 AS defaulted_loans,

    -- Derived segments
    CASE
        WHEN c.credit_score >= 750 THEN 'excellent'
        WHEN c.credit_score >= 700 THEN 'good'
        WHEN c.credit_score >= 650 THEN 'fair'
        WHEN c.credit_score >= 550 THEN 'poor'
        ELSE 'very_poor'
    END                                             AS credit_tier,

    CASE
        WHEN COALESCE(am.total_balance, 0) >= 100000 THEN 'high_value'
        WHEN COALESCE(am.total_balance, 0) >= 25000  THEN 'mid_value'
        ELSE 'standard'
    END                                             AS customer_segment,

    c.created_at                                    AS customer_since,
    c.updated_at
FROM customers c
LEFT JOIN account_metrics am ON c.customer_id = am.customer_id
LEFT JOIN loan_metrics lm ON c.customer_id = lm.customer_id
