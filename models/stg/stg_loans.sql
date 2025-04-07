{{ config(
    tags=["staging", "loans"])
 }}

WITH source_data AS (
    SELECT 
        LOAN_ID,
        CUSTOMER_ID,
        LOAN_TYPE,
        COALESCE(TRY_CAST(PRINCIPAL_AMOUNT AS FLOAT), 0) AS PRINCIPAL_AMOUNT,  -- Handling NULLs
        COALESCE(TRY_CAST(INTEREST_RATE AS FLOAT), 0) AS INTEREST_RATE,  -- Handling NULLs
        TRY_CAST(START_DATE AS DATE) AS START_DATE,  -- Converting to DATE
        TRY_CAST(END_DATE AS DATE) AS END_DATE,  -- Converting to DATE
        -- Standardizing STATUS values
        CASE 
            WHEN UPPER(STATUS) = 'ACTIVE' THEN 'Active'
            WHEN UPPER(STATUS) IN ('DEFAULT', 'DEFAULTED') THEN 'Defaulted'  -- ✅ Fixed
            WHEN UPPER(STATUS) = 'CLOSED' THEN 'Closed'
            ELSE STATUS  -- Retain original value for debugging
        END AS STATUS,
        INGESTION_TIMESTAMP
    FROM  {{ source('raw_src','src_loans')}}
    WHERE INGESTION_TIMESTAMP IS NOT NULL  -- Ensuring only valid records are processed
),

validated_data AS (
    SELECT 
        *,
        CASE 
            WHEN END_DATE < START_DATE THEN 'Invalid Date Range'
            ELSE 'Valid'
        END AS DATE_VALIDATION,  -- Flagging records with invalid date ranges
        CASE 
            WHEN PRINCIPAL_AMOUNT < 0 THEN 'Negative Principal'
            ELSE 'Valid'
        END AS PRINCIPAL_VALIDATION,  -- Flagging negative principal amounts
        CASE 
            WHEN INTEREST_RATE < 0 THEN 'Negative Interest'
            ELSE 'Valid'
        END AS INTEREST_VALIDATION  -- Flagging negative interest rates
    FROM source_data
)

SELECT 
    LOAN_ID,
    CUSTOMER_ID,
    LOAN_TYPE,
    PRINCIPAL_AMOUNT,
    INTEREST_RATE,
    START_DATE,
    END_DATE,
    STATUS,
    INGESTION_TIMESTAMP,
    DATE_VALIDATION,
    PRINCIPAL_VALIDATION,
    INTEREST_VALIDATION
FROM validated_data
