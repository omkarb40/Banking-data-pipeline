{{
    config(
        materialized='incremental',
        unique_key='transaction_id'
    )
}}

WITH source AS (
    SELECT
        raw_data,
        source_file,
        loaded_at,
        raw_data:op::STRING AS cdc_operation
    FROM {{ source('bronze', 'RAW_TRANSACTIONS') }}
    WHERE raw_data:op::STRING != 'd'
    {% if is_incremental() %}
        AND loaded_at > (SELECT MAX(loaded_at) FROM {{ this }})
    {% endif %}
),

parsed AS (
    SELECT
        raw_data:after:transaction_id::STRING        AS transaction_id,
        raw_data:after:account_id::STRING            AS account_id,
        raw_data:after:transaction_type::STRING      AS transaction_type,
        raw_data:after:amount::NUMBER(15,2)          AS amount,
        raw_data:after:balance_before::NUMBER(15,2)  AS balance_before,
        raw_data:after:balance_after::NUMBER(15,2)   AS balance_after,
        raw_data:after:currency::STRING              AS currency,
        raw_data:after:status::STRING                AS status,
        raw_data:after:channel::STRING               AS channel,
        raw_data:after:description::STRING           AS description,
        raw_data:after:reference_id::STRING          AS reference_id,
        raw_data:after:counterparty_account::STRING  AS counterparty_account,
        raw_data:after:merchant_name::STRING         AS merchant_name,
        raw_data:after:merchant_category::STRING     AS merchant_category,
        raw_data:after:is_flagged::BOOLEAN           AS is_flagged,
        raw_data:after:flag_reason::STRING           AS flag_reason,
        raw_data:after:processed_branch_id::INTEGER  AS processed_branch_id,
        raw_data:after:transaction_at::TIMESTAMP     AS transaction_at,
        cdc_operation,
        source_file,
        loaded_at,
        ROW_NUMBER() OVER (
            PARTITION BY raw_data:after:transaction_id::STRING
            ORDER BY loaded_at DESC
        ) AS row_num
    FROM source
    WHERE raw_data:after IS NOT NULL
)

SELECT
    transaction_id,
    account_id,
    transaction_type,
    amount,
    balance_before,
    balance_after,
    currency,
    status,
    channel,
    description,
    reference_id,
    counterparty_account,
    merchant_name,
    merchant_category,
    is_flagged,
    flag_reason,
    processed_branch_id,
    transaction_at,
    cdc_operation,
    source_file,
    loaded_at
FROM parsed
WHERE row_num = 1
