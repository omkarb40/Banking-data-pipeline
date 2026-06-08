/*
    Assert: No non-credit accounts should have negative balances.
    Credit accounts carry debt (negative balance), which is expected.
    Any rows returned by this query indicate a data quality issue.
*/

SELECT
    account_id,
    account_type,
    balance
FROM {{ ref('dim_accounts') }}
WHERE account_type != 'credit'
  AND balance < 0
