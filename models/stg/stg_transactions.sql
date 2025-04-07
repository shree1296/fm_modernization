{{ config(
    tags=["staging", "transactions"]
) }}

WITH source AS (
    SELECT
        TRANSACTION_ID,  -- Ensure it's treated as a string
        ACCOUNT_ID,
        TRY_CAST(TRANSACTION_DATE AS DATE) AS TRANSACTION_DATE,  -- Convert date format
        AMOUNT,
        TRANSACTION_TYPE,
        MERCHANT_ID,
        STATUS,
        CREATED_AT,
        INGESTION_TIMESTAMP
    FROM {{ source('raw_src', 'src_transactions') }}
    WHERE TRY_CAST(TRANSACTION_DATE AS DATE) IS NOT NULL  -- Remove invalid dates
        AND AMOUNT >= 0  -- Filter out negative transactions
        AND INGESTION_TIMESTAMP IS NOT NULL  -- Ensure record traceability
),

final AS (
    SELECT
        TRANSACTION_ID,
        ACCOUNT_ID,
        TRANSACTION_DATE,
        AMOUNT,
        UPPER(TRIM(TRANSACTION_TYPE)) AS TRANSACTION_TYPE,  -- Standardize transaction type
        MERCHANT_ID,
        UPPER(TRIM(STATUS)) AS STATUS,  -- Standardize status
        CREATED_AT,
        INGESTION_TIMESTAMP
    FROM source
)

SELECT * FROM final