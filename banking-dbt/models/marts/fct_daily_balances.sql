{{
    config(
        materialized='table'
    )
}}

/*
    fct_daily_balances — Gold Layer
    Daily aggregation of transaction activity per account.
    Powers time-series dashboards and trend analysis.
*/

WITH daily_txns AS (
    SELECT
        account_id,
        DATE(transaction_at)                        AS txn_date,

        -- Volume
        COUNT(*)                                    AS total_transactions,
        COUNT(CASE WHEN status = 'completed' THEN 1 END) AS completed_transactions,
        COUNT(CASE WHEN status = 'failed' THEN 1 END)    AS failed_transactions,

        -- Inflows
        SUM(CASE
            WHEN transaction_type IN ('deposit', 'interest', 'refund')
                AND status = 'completed'
            THEN amount ELSE 0
        END)                                        AS total_inflow,

        -- Outflows
        SUM(CASE
            WHEN transaction_type IN ('withdrawal', 'payment', 'fee')
                AND status = 'completed'
            THEN amount ELSE 0
        END)                                        AS total_outflow,

        -- Transfers
        SUM(CASE
            WHEN transaction_type = 'transfer' AND status = 'completed'
            THEN amount ELSE 0
        END)                                        AS total_transfers,

        -- Flags
        COUNT(CASE WHEN is_flagged THEN 1 END)      AS flagged_count,

        -- Closing balance (last balance_after of the day)
        LAST_VALUE(balance_after) OVER (
            PARTITION BY account_id, DATE(transaction_at)
            ORDER BY transaction_at
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )                                           AS closing_balance

    FROM {{ ref('stg_transactions') }}
    WHERE status IN ('completed', 'failed')
    GROUP BY account_id, DATE(transaction_at), balance_after, transaction_at
),

aggregated AS (
    SELECT
        account_id,
        txn_date,
        SUM(total_transactions)                     AS total_transactions,
        SUM(completed_transactions)                 AS completed_transactions,
        SUM(failed_transactions)                    AS failed_transactions,
        SUM(total_inflow)                           AS total_inflow,
        SUM(total_outflow)                          AS total_outflow,
        SUM(total_transfers)                        AS total_transfers,
        SUM(flagged_count)                          AS flagged_count,
        MAX(closing_balance)                        AS closing_balance
    FROM daily_txns
    GROUP BY account_id, txn_date
),

accounts AS (
    SELECT
        account_id,
        customer_id,
        account_type
    FROM {{ ref('stg_accounts') }}
)

SELECT
    a.account_id,
    a.customer_id,
    a.account_type,
    ag.txn_date,
    ag.total_transactions,
    ag.completed_transactions,
    ag.failed_transactions,
    ag.total_inflow,
    ag.total_outflow,
    ag.total_transfers,
    ag.total_inflow - ag.total_outflow              AS net_flow,
    ag.flagged_count,
    ag.closing_balance
FROM aggregated ag
JOIN accounts a ON ag.account_id = a.account_id
