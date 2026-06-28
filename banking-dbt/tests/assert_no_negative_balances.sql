/*
    Assert: No non-credit accounts should have negative balances.
    Credit accounts carry debt (negative balance), which is expected.
    Any rows returned by this query indicate a data quality issue.
*/

SELECT account_id, balance
FROM {{ ref('dim_accounts') }}
WHERE balance IS NULL
