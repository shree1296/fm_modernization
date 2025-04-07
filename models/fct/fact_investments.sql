{{ config(
    materialized='table',
    tags=['fact', 'investments']
) }}

WITH source AS (
    SELECT 
        INVESTMENT_ID,
        CUSTOMER_ID AS SRC_CUSTOMER_ID,  -- ✅ Renaming to avoid ambiguity
        ASSET_CLASS,
        INVESTMENT_AMOUNT,
        CURRENT_VALUE,
        INVESTMENT_DATE,
        CREATED_AT,
        INGESTION_TIMESTAMP
    FROM aw_db.raw_src_staging.stg_investment_portfolio
),

valid_investments AS (
    SELECT 
        s.INVESTMENT_ID,
        s.SRC_CUSTOMER_ID,  
        c.CUSTOMER_ID AS DIM_CUSTOMER_ID,  -- ✅ Explicit aliasing
        d.DATE_ID AS INVESTMENT_DATE_ID,
        s.ASSET_CLASS,
        s.INVESTMENT_AMOUNT,
        s.CURRENT_VALUE,
        s.CREATED_AT,
        s.INGESTION_TIMESTAMP
    FROM source s
    LEFT JOIN aw_db.raw_src.dim_dates d 
        ON s.INVESTMENT_DATE = d.CALENDAR_DATE  
    LEFT JOIN aw_db.raw_src.dim_customers c 
        ON s.SRC_CUSTOMER_ID = c.CUSTOMER_ID  
),

final AS (
    SELECT 
        INVESTMENT_ID,
        COALESCE(DIM_CUSTOMER_ID, SRC_CUSTOMER_ID) AS CUSTOMER_ID, -- ✅ Prioritizing DIM_CUSTOMER_ID
        INVESTMENT_DATE_ID,
        ASSET_CLASS,
        INVESTMENT_AMOUNT,
        CURRENT_VALUE,
        CREATED_AT,
        INGESTION_TIMESTAMP
    FROM valid_investments
)

SELECT * FROM final
