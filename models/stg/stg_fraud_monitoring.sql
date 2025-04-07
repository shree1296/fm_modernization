{{ config(
    tags=["staging", "fraud-monitoring"]
) }}

WITH source AS (
    -- Selecting necessary fields from the raw source to improve performance
    SELECT
        FRAUD_ID,
        TRANSACTION_ID,
        ACCOUNT_ID,
        TRY_CAST(DETECTED_DATE AS DATE) AS DETECTED_DATE, -- Converting string to DATE format
        UPPER(TRIM(FRAUD_TYPE)) AS FRAUD_TYPE,  -- Standardizing fraud type values
        UPPER(TRIM(STATUS)) AS STATUS,  -- Standardizing status values
        CREATED_AT,
        INGESTION_TIMESTAMP
    FROM {{ source('raw_src', 'src_fraud_monitoring') }}
),

validated AS (
    -- Handling data inconsistencies and ensuring integrity
    SELECT
        FRAUD_ID,
        TRANSACTION_ID,
        ACCOUNT_ID,
        CASE 
            WHEN DETECTED_DATE < '2000-01-01' THEN NULL  -- Remove invalid past dates
            ELSE DETECTED_DATE 
        END AS DETECTED_DATE,  -- Removed the extra comma here
        FRAUD_TYPE,
        STATUS,
        CREATED_AT,
        INGESTION_TIMESTAMP
    FROM source
)

-- Final selection of transformed fields to ensure optimized query execution
SELECT 
    FRAUD_ID,
    TRANSACTION_ID,
    ACCOUNT_ID,
    DETECTED_DATE,
    FRAUD_TYPE,
    STATUS,
    CREATED_AT,
    INGESTION_TIMESTAMP
FROM validated
