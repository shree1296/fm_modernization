{{ config(
    materialized='view'
) }}

WITH source AS (
    -- Extract raw data from the source table
    SELECT * 
    FROM {{ source('raw_src', 'src_channel_segment') }}
),

transformed AS (
    SELECT 
        -- Primary Key
        SEGMENT_ID,

        -- Convert NODE to INTEGER for hierarchy analysis
        NODE::INTEGER AS NODE,

        -- Clean description and parent fields
        TRIM(DESCRIPTION) AS DESCRIPTION,
        TRIM(PARENT) AS PARENT,

        -- Convert START_DATE and END_DATE from number to date format
        CASE 
            WHEN START_DATE = 0 THEN NULL 
            ELSE TRY_TO_DATE(START_DATE::VARCHAR, 'YYYYMMDD') 
        END AS START_DATE,

        CASE 
            WHEN END_DATE = 0 THEN NULL 
            ELSE TRY_TO_DATE(END_DATE::VARCHAR, 'YYYYMMDD') 
        END AS END_DATE,

        -- Convert Numeric Columns to BOOLEAN
        ENABLED::BOOLEAN AS ENABLED,
        SUMMARY::BOOLEAN AS SUMMARY,
        ALLOW_POSTING::BOOLEAN AS ALLOW_POSTING,

        -- Additional numeric fields (keeping original types)
        UDA_CHANNEL,
        AGGREGATION_CONSOL,
        DATA_STORAGE,

        -- Add an IS_ACTIVE flag: TRUE if segment is active today
        CASE 
            WHEN (CASE 
                    WHEN END_DATE = 0 THEN NULL 
                    ELSE TRY_TO_DATE(END_DATE::VARCHAR, 'YYYYMMDD') 
                  END) IS NULL 
                  OR (CASE 
                        WHEN END_DATE = 0 THEN NULL 
                        ELSE TRY_TO_DATE(END_DATE::VARCHAR, 'YYYYMMDD') 
                      END) >= CURRENT_DATE 
            THEN TRUE 
            ELSE FALSE 
        END AS IS_ACTIVE,

        -- Convert CREATED_AT to TIMESTAMP (handle invalid formats)
        CASE 
            WHEN TRY_TO_TIMESTAMP(CREATED_AT) IS NOT NULL 
            THEN TRY_TO_TIMESTAMP(CREATED_AT) 
            ELSE NULL 
        END AS CREATED_AT,

        -- Keep ingestion timestamp unchanged
        INGESTION_TIMESTAMP

    FROM source
)

SELECT * FROM transformed
