{{ config(
    tags=["staging", "investment-portfolio"]
) }}

WITH source AS (
    -- Selecting only necessary columns from the raw source to optimize performance
    SELECT
        INVESTMENT_ID,
        CUSTOMER_ID,
        UPPER(TRIM(ASSET_CLASS)) AS ASSET_CLASS,  -- Standardizing asset class values
        INVESTMENT_AMOUNT,
        CURRENT_VALUE,
        INVESTMENT_DATE,
        UPDATED_AT AS CREATED_AT,  -- Renaming UPDATED_AT to CREATED_AT
        INGESTION_TIMESTAMP
    FROM {{ source('raw_src', 'src_investment_portfolio') }}
),

validated AS (
    -- Apply transformations and ensure data integrity
    SELECT
        INVESTMENT_ID,
        CUSTOMER_ID,
        ASSET_CLASS,
        INVESTMENT_AMOUNT,
        CURRENT_VALUE,
        INVESTMENT_DATE,
        CREATED_AT,  -- Now correctly referring to CREATED_AT
        INGESTION_TIMESTAMP
    FROM source
    WHERE INVESTMENT_DATE IS NOT NULL  -- Ensuring only valid investment dates are processed
)

-- Final selection ensuring optimized transformations
SELECT 
    INVESTMENT_ID,
    CUSTOMER_ID,
    ASSET_CLASS,
    INVESTMENT_AMOUNT,
    CURRENT_VALUE,
    INVESTMENT_DATE,
    CREATED_AT,  -- Using CREATED_AT instead of UPDATED_AT
    INGESTION_TIMESTAMP
FROM validated
