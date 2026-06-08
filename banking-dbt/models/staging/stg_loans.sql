{{
    config(
        materialized='incremental',
        unique_key='loan_id'
    )
}}

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
        raw_data:after:loan_id::STRING              AS loan_id,
        raw_data:after:account_id::STRING           AS account_id,
        raw_data:after:customer_id::STRING          AS customer_id,
        raw_data:after:loan_type::STRING            AS loan_type,
        raw_data:after:principal::NUMBER(15,2)      AS principal,
        raw_data:after:interest_rate::NUMBER(5,4)   AS interest_rate,
        raw_data:after:term_months::INTEGER         AS term_months,
        raw_data:after:monthly_payment::NUMBER(15,2) AS monthly_payment,
        raw_data:after:total_paid::NUMBER(15,2)     AS total_paid,
        raw_data:after:outstanding::NUMBER(15,2)    AS outstanding,
        raw_data:after:status::STRING               AS status,
        raw_data:after:next_payment_date::DATE      AS next_payment_date,
        raw_data:after:disbursed_at::TIMESTAMP      AS disbursed_at,
        raw_data:after:maturity_date::DATE          AS maturity_date,
        raw_data:after:created_at::TIMESTAMP        AS created_at,
        raw_data:after:updated_at::TIMESTAMP        AS updated_at,
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
