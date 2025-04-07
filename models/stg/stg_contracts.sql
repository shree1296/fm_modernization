{{ config(
    tags=["staging", "contracts"]
) }}

WITH source AS (
    -- Select only necessary fields from the raw source table to improve performance
    SELECT
        CONTRACT_ID,
        CUSTOMER_ID,
        UPPER(TRIM(CONTRACT_TYPE)) AS CONTRACT_TYPE,  -- Standardizing contract type for consistency
        POLICY_NO,
        START_DATE,
        END_DATE,
        COST_CENTER_CD,
        UPPER(TRIM(STATUS)) AS STATUS,  -- Standardizing status values
        CREATED_AT,
        INGESTION_TIMESTAMP
    FROM {{ source('raw_src', 'src_contracts') }}
),

validated AS (
    -- Perform transformations and apply data integrity checks efficiently
    SELECT
        CONTRACT_ID,
        CUSTOMER_ID,
        CONTRACT_TYPE,
        POLICY_NO,
        START_DATE,
        CASE 
            WHEN END_DATE >= START_DATE THEN END_DATE 
            ELSE NULL  -- Replace invalid END_DATE values with NULL to maintain data integrity
        END AS END_DATE,
        COST_CENTER_CD,
        STATUS,
        CREATED_AT,
        INGESTION_TIMESTAMP
    FROM source
)

-- Final selection of transformed fields to ensure optimized query execution
SELECT 
    CONTRACT_ID,
    CUSTOMER_ID,
    CONTRACT_TYPE,
    POLICY_NO,
    START_DATE,
    END_DATE,
    COST_CENTER_CD,
    STATUS,
    CREATED_AT,
    INGESTION_TIMESTAMP
FROM validated