/*
    Assert: For completed transactions, balance_after should reflect
    the correct arithmetic from balance_before ± amount.
    
    Tolerance of $0.02 to account for floating-point rounding.
    Any rows returned indicate a balance tracking issue.
*/

SELECT
    transaction_id,
    transaction_type,
    amount,
    balance_before,
    balance_after,
    balance_after - balance_before AS actual_change,
    CASE
        WHEN transaction_type IN ('deposit', 'interest', 'refund')
            THEN amount
        ELSE -amount
    END AS expected_change
FROM {{ ref('fct_transactions') }}
WHERE status = 'completed'
  AND balance_before IS NOT NULL
  AND balance_after IS NOT NULL
  AND ABS(
      (balance_after - balance_before) -
      CASE
          WHEN transaction_type IN ('deposit', 'interest', 'refund')
              THEN amount
          ELSE -amount
      END
  ) > 0.02
