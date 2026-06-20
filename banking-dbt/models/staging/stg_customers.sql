{{
    config(
        materialized='incremental',
        unique_key='customer_id'
    )
}}

/*
    stg_customers — Silver Layer
    Extracts typed columns from Debezium CDC payload.

    Debezium encoding fixes:
      - NUMERIC fields → base64 encoded → decoded via UDF
      - DATE fields → days since epoch → DATEADD
      - TIMESTAMP fields → microseconds since epoch → TO_TIMESTAMP(val, 6)
*/

WITH source AS (
    SELECT
        raw_data,
        source_file,
        loaded_at,
        raw_data:op::STRING AS cdc_operation
    FROM {{ source('bronze', 'RAW_CUSTOMERS') }}
    WHERE raw_data:op::STRING != 'd'
    {% if is_incremental() %}
        AND loaded_at > (SELECT MAX(loaded_at) FROM {{ this }})
    {% endif %}
),

parsed AS (
    SELECT
        raw_data:after:customer_id::STRING                      AS customer_id,
        raw_data:after:first_name::STRING                       AS first_name,
        raw_data:after:last_name::STRING                        AS last_name,
        raw_data:after:email::STRING                            AS email,
        raw_data:after:phone::STRING                            AS phone,

        -- DATE: Debezium sends as days since 1970-01-01
        DATEADD('day',
            raw_data:after:date_of_birth::INTEGER,
            '1970-01-01'::DATE
        )                                                       AS date_of_birth,

        raw_data:after:address::STRING                          AS address,
        raw_data:after:city::STRING                             AS city,
        raw_data:after:state::STRING                            AS state,
        raw_data:after:zip_code::STRING                         AS zip_code,
        raw_data:after:country::STRING                          AS country,
        raw_data:after:credit_score::INTEGER                    AS credit_score,

        -- NUMERIC(12,2): Debezium base64-encodes PostgreSQL NUMERIC types
        BANKING_DW.SILVER.DECODE_DEBEZIUM_NUMERIC(
            raw_data:after:annual_income::STRING, 2
        )                                                       AS annual_income,

        raw_data:after:employment_status::STRING                AS employment_status,
        raw_data:after:risk_rating::STRING                      AS risk_rating,
        raw_data:after:home_branch_id::INTEGER                  AS home_branch_id,
        raw_data:after:is_active::BOOLEAN                       AS is_active,

        -- TIMESTAMP: Debezium sends as microseconds since epoch
        TO_TIMESTAMP(raw_data:after:created_at::NUMBER, 6)      AS created_at,
        TO_TIMESTAMP(raw_data:after:updated_at::NUMBER, 6)      AS updated_at,

        cdc_operation,
        source_file,
        loaded_at,
        ROW_NUMBER() OVER (
            PARTITION BY raw_data:after:customer_id::STRING
            ORDER BY loaded_at DESC
        ) AS row_num
    FROM source
    WHERE raw_data:after IS NOT NULL
)

SELECT
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    date_of_birth,
    address,
    city,
    state,
    zip_code,
    country,
    credit_score,
    annual_income,
    employment_status,
    risk_rating,
    home_branch_id,
    is_active,
    created_at,
    updated_at,
    cdc_operation,
    source_file,
    loaded_at
FROM parsed
WHERE row_num = 1
