{{
    config(
        materialized='incremental',
        unique_key='loan_id'
    )
}}

/*
    stg_loans — Silver Layer

    Debezium encoding fixes:
      - principal, monthly_payment, total_paid, outstanding → NUMERIC(15,2) → base64, scale 2
      - interest_rate → NUMERIC(5,4) → base64, scale 4
      - next_payment_date, maturity_date → days since epoch
      - disbursed_at, created_at, updated_at → microseconds since epoch
*/

WITH source AS (
    SELECT
        raw_data,
        source_file,
        loaded_at,
        raw_data:op::STRING AS cdc_operation
    FROM {{ source('bronze', 'RAW_LOANS') }}
    WHERE raw_data:op::STRING != 'd'
    {% if is_incremental() %}
        AND loaded_at > (SELECT MAX(loaded_at) FROM {{ this }})
    {% endif %}
),

parsed AS (
    SELECT
        raw_data:after:loan_id::STRING                          AS loan_id,
        raw_data:after:account_id::STRING                       AS account_id,
        raw_data:after:customer_id::STRING                      AS customer_id,
        raw_data:after:loan_type::STRING                        AS loan_type,

        -- NUMERIC(15,2): principal
        BANKING_DW.SILVER.DECODE_DEBEZIUM_NUMERIC(
            raw_data:after:principal::STRING, 2
        )                                                       AS principal,

        -- NUMERIC(5,4): interest rate
        BANKING_DW.SILVER.DECODE_DEBEZIUM_NUMERIC(
            raw_data:after:interest_rate::STRING, 4
        )                                                       AS interest_rate,

        raw_data:after:term_months::INTEGER                     AS term_months,

        -- NUMERIC(15,2): monthly payment (nullable)
        CASE
            WHEN raw_data:after:monthly_payment IS NOT NULL
            THEN BANKING_DW.SILVER.DECODE_DEBEZIUM_NUMERIC(
                raw_data:after:monthly_payment::STRING, 2
            )
            ELSE NULL
        END                                                     AS monthly_payment,

        -- NUMERIC(15,2): total paid
        BANKING_DW.SILVER.DECODE_DEBEZIUM_NUMERIC(
            raw_data:after:total_paid::STRING, 2
        )                                                       AS total_paid,

        -- NUMERIC(15,2): outstanding
        BANKING_DW.SILVER.DECODE_DEBEZIUM_NUMERIC(
            raw_data:after:outstanding::STRING, 2
        )                                                       AS outstanding,

        raw_data:after:status::STRING                           AS status,

        -- DATE: days since epoch (nullable)
        CASE
            WHEN raw_data:after:next_payment_date IS NOT NULL
            THEN DATEADD('day',
                raw_data:after:next_payment_date::INTEGER,
                '1970-01-01'::DATE
            )
            ELSE NULL
        END                                                     AS next_payment_date,

        -- TIMESTAMP: microseconds since epoch (nullable)
        CASE
            WHEN raw_data:after:disbursed_at IS NOT NULL
            THEN TO_TIMESTAMP(raw_data:after:disbursed_at::NUMBER, 6)
            ELSE NULL
        END                                                     AS disbursed_at,

        -- DATE: days since epoch (nullable)
        CASE
            WHEN raw_data:after:maturity_date IS NOT NULL
            THEN DATEADD('day',
                raw_data:after:maturity_date::INTEGER,
                '1970-01-01'::DATE
            )
            ELSE NULL
        END                                                     AS maturity_date,

        -- TIMESTAMP: microseconds since epoch
        TO_TIMESTAMP(raw_data:after:created_at::NUMBER, 6)      AS created_at,
        TO_TIMESTAMP(raw_data:after:updated_at::NUMBER, 6)      AS updated_at,

        cdc_operation,
        source_file,
        loaded_at,
        ROW_NUMBER() OVER (
            PARTITION BY raw_data:after:loan_id::STRING
            ORDER BY loaded_at DESC
        ) AS row_num
    FROM source
    WHERE raw_data:after IS NOT NULL
)

SELECT
    loan_id,
    account_id,
    customer_id,
    loan_type,
    principal,
    interest_rate,
    term_months,
    monthly_payment,
    total_paid,
    outstanding,
    status,
    next_payment_date,
    disbursed_at,
    maturity_date,
    created_at,
    updated_at,
    cdc_operation,
    source_file,
    loaded_at
FROM parsed
WHERE row_num = 1
