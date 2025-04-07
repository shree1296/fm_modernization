{{ config(
    materialized='table',
    tags=['fact', 'fraud']
) }}

WITH source AS (
    SELECT 
        CAST(FRAUD_ID AS STRING) AS FRAUD_ID, 
        CAST(TRANSACTION_ID AS STRING) AS TRANSACTION_ID,
        CAST(ACCOUNT_ID AS STRING) AS ACCOUNT_ID,
        TO_DATE(TO_TIMESTAMP_NTZ(DETECTED_DATE)) AS DETECTED_DATE, -- ✅ Fixed Conversion
        FRAUD_TYPE,
        STATUS,
        CREATED_AT,
        INGESTION_TIMESTAMP
    FROM aw_db.raw_src_staging.stg_fraud_monitoring
),

valid_fraud_cases AS (
    SELECT 
        s.*,
        d.CALENDAR_DATE AS DETECTED_DATE_ID
    FROM source s
    LEFT JOIN aw_db.raw_src.dim_dates d 
        ON s.DETECTED_DATE = d.CALENDAR_DATE  
),

final AS (
    SELECT 
        FRAUD_ID,  
        TRANSACTION_ID, 
        ACCOUNT_ID,
        DETECTED_DATE_ID,
        FRAUD_TYPE,
        STATUS,
        CREATED_AT,
        INGESTION_TIMESTAMP
    FROM valid_fraud_cases
)

SELECT * FROM final

