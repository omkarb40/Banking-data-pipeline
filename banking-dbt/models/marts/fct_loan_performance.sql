{{
    config(
        materialized='table'
    )
}}

/*
    fct_loan_performance — Gold Layer
    Loan-level metrics for risk analysis and portfolio dashboards.
*/

WITH loans AS (
    SELECT * FROM {{ ref('stg_loans') }}
),

customers AS (
    SELECT
        customer_id,
        first_name || ' ' || last_name AS full_name,
        credit_score,
        annual_income,
        risk_rating
    FROM {{ ref('stg_customers') }}
)

SELECT
    l.loan_id,
    l.account_id,
    l.customer_id,
    c.full_name                                     AS customer_name,
    c.credit_score,
    c.annual_income,
    c.risk_rating                                   AS customer_risk_rating,

    -- Loan details
    l.loan_type,
    l.principal,
    l.interest_rate,
    l.term_months,
    l.monthly_payment,
    l.total_paid,
    l.outstanding,
    l.status,
    l.disbursed_at,
    l.maturity_date,
    l.next_payment_date,

    -- Derived metrics
    CASE
        WHEN l.principal > 0
        THEN ROUND((l.total_paid / l.principal) * 100, 2)
        ELSE 0
    END                                             AS repayment_pct,

    CASE
        WHEN l.principal > 0
        THEN ROUND((l.outstanding / l.principal) * 100, 2)
        ELSE 0
    END                                             AS outstanding_pct,

    CASE
        WHEN c.annual_income > 0 AND l.monthly_payment > 0
        THEN ROUND((l.monthly_payment * 12 / c.annual_income) * 100, 2)
        ELSE NULL
    END                                             AS debt_to_income_pct,

    CASE
        WHEN l.disbursed_at IS NOT NULL
        THEN DATEDIFF('month', l.disbursed_at, CURRENT_TIMESTAMP())
        ELSE 0
    END                                             AS months_since_disbursement,

    CASE
        WHEN l.maturity_date IS NOT NULL
        THEN DATEDIFF('month', CURRENT_DATE(), l.maturity_date)
        ELSE l.term_months
    END                                             AS months_remaining,

    -- Risk flags
    CASE
        WHEN l.status = 'defaulted' THEN 'defaulted'
        WHEN l.status = 'repaying'
            AND l.next_payment_date < CURRENT_DATE()
        THEN 'past_due'
        WHEN l.status IN ('repaying', 'disbursed')
            AND c.credit_score < 550
        THEN 'high_risk'
        WHEN l.status IN ('repaying', 'disbursed') THEN 'current'
        WHEN l.status = 'closed' THEN 'paid_off'
        ELSE l.status
    END                                             AS risk_category,

    -- Total cost of loan
    l.monthly_payment * l.term_months               AS total_loan_cost,
    (l.monthly_payment * l.term_months) - l.principal AS total_interest_cost,

    l.created_at,
    l.updated_at
FROM loans l
LEFT JOIN customers c ON l.customer_id = c.customer_id
