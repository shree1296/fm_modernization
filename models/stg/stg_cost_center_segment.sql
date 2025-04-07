{{ config(
    tags=["staging", "cost-center"]
) }}

WITH source AS (
    -- Selecting only necessary fields from the source table to improve performance
    SELECT
        SEGMENT_ID,
        NODE,
        TRIM(DESCRIPTION) AS DESCRIPTION,  -- Removing extra spaces for consistency
        PARENT,
        CREATED_AT,
        INGESTION_TIMESTAMP
    FROM {{ source('raw_src', 'src_cost_center_segment') }}
),

validated AS (
    -- Ensuring data consistency and handling missing values
    SELECT
        SEGMENT_ID,
        NODE,
        CASE 
            WHEN DESCRIPTION IS NULL OR DESCRIPTION = '' THEN 'UNKNOWN'  -- Handling missing descriptions
            ELSE DESCRIPTION 
        END AS DESCRIPTION,
        CASE 
            WHEN PARENT IS NULL THEN 'ROOT'  -- Assigning 'ROOT' to missing parent values
            ELSE PARENT 
        END AS PARENT,
        CREATED_AT,
        INGESTION_TIMESTAMP
    FROM source
)

-- Final selection of transformed fields to ensure optimized query execution
SELECT 
    SEGMENT_ID,
    NODE,
    DESCRIPTION,
    PARENT,
    CREATED_AT,
    INGESTION_TIMESTAMP
FROM validated