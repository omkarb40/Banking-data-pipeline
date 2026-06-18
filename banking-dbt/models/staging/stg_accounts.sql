{{
    config(
        materialized='incremental',
        unique_key='account_id'
    )
}}

/*
    stg_accounts — Silver Layer

    Debezium encoding fixes:
      - balance, credit_limit, overdraft_limit → NUMERIC(15,2) → base64, scale 2
      - interest_rate → NUMERIC(5,4) → base64, scale 4
      - opened_at, closed_at, updated_at → microseconds since epoch
*/

WITH source AS (
    SELECT
        raw_data,
        source_file,
        loaded_at,
        raw_data:op::STRING AS cdc_operation
    FROM {{ source('bronze', 'RAW_ACCOUNTS') }}
    WHERE raw_data:op::STRING != 'd'
    {% if is_incremental() %}
        AND loaded_at > (SELECT MAX(loaded_at) FROM {{ this }})
    {% endif %}
),

parsed AS (
    SELECT
        raw_data:after:account_id::STRING                       AS account_id,
        raw_data:after:customer_id::STRING                      AS customer_id,
        raw_data:after:account_type::STRING                     AS account_type,
        raw_data:after:account_number::STRING                   AS account_number,

        -- NUMERIC(15,2): balance
        BANKING_DW.SILVER.DECODE_DEBEZIUM_NUMERIC(
            raw_data:after:balance::STRING, 2
        )                                                       AS balance,

        raw_data:after:currency::STRING                         AS currency,

        -- NUMERIC(5,4): interest rate
        BANKING_DW.SILVER.DECODE_DEBEZIUM_NUMERIC(
            raw_data:after:interest_rate::STRING, 4
        )                                                       AS interest_rate,

        -- NUMERIC(15,2): credit limit (nullable)
        CASE
            WHEN raw_data:after:credit_limit IS NOT NULL
            THEN BANKING_DW.SILVER.DECODE_DEBEZIUM_NUMERIC(
                raw_data:after:credit_limit::STRING, 2
            )
            ELSE NULL
        END                                                     AS credit_limit,

        -- NUMERIC(10,2): overdraft limit
        BANKING_DW.SILVER.DECODE_DEBEZIUM_NUMERIC(
            raw_data:after:overdraft_limit::STRING, 2
        )                                                       AS overdraft_limit,

        raw_data:after:status::STRING                           AS status,
        raw_data:after:opened_branch_id::INTEGER                AS opened_branch_id,

        -- TIMESTAMP: microseconds since epoch
        TO_TIMESTAMP(raw_data:after:opened_at::NUMBER, 6)       AS opened_at,

        CASE
            WHEN raw_data:after:closed_at IS NOT NULL
            THEN TO_TIMESTAMP(raw_data:after:closed_at::NUMBER, 6)
            ELSE NULL
        END                                                     AS closed_at,

        TO_TIMESTAMP(raw_data:after:updated_at::NUMBER, 6)      AS updated_at,

        cdc_operation,
        source_file,
        loaded_at,
        ROW_NUMBER() OVER (
            PARTITION BY raw_data:after:account_id::STRING
            ORDER BY loaded_at DESC
        ) AS row_num
    FROM source
    WHERE raw_data:after IS NOT NULL
)

SELECT
    account_id,
    customer_id,
    account_type,
    account_number,
    balance,
    currency,
    interest_rate,
    credit_limit,
    overdraft_limit,
    status,
    opened_branch_id,
    opened_at,
    closed_at,
    updated_at,
    cdc_operation,
    source_file,
    loaded_at
FROM parsed
WHERE row_num = 1
