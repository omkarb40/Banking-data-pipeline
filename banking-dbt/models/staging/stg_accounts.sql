{{
    config(
        materialized='incremental',
        unique_key='account_id'
    )
}}

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
        raw_data:after:account_id::STRING            AS account_id,
        raw_data:after:customer_id::STRING           AS customer_id,
        raw_data:after:account_type::STRING          AS account_type,
        raw_data:after:account_number::STRING        AS account_number,
        raw_data:after:balance::NUMBER(15,2)         AS balance,
        raw_data:after:currency::STRING              AS currency,
        raw_data:after:interest_rate::NUMBER(5,4)    AS interest_rate,
        raw_data:after:credit_limit::NUMBER(15,2)    AS credit_limit,
        raw_data:after:overdraft_limit::NUMBER(10,2) AS overdraft_limit,
        raw_data:after:status::STRING                AS status,
        raw_data:after:opened_branch_id::INTEGER     AS opened_branch_id,
        raw_data:after:opened_at::TIMESTAMP          AS opened_at,
        raw_data:after:closed_at::TIMESTAMP          AS closed_at,
        raw_data:after:updated_at::TIMESTAMP         AS updated_at,
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
