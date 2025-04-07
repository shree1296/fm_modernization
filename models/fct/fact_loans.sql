{{ config(
    materialized='table',
    tags=['fact', 'loans']
) }}

WITH source AS (
    SELECT 
        LOAN_ID,
        CUSTOMER_ID AS SRC_CUSTOMER_ID, -- ✅ Alias to avoid ambiguity
        LOAN_TYPE,
        PRINCIPAL_AMOUNT,
        INTEREST_RATE,
        START_DATE,
        END_DATE,
        STATUS,
        INGESTION_TIMESTAMP
    FROM aw_db.raw_src_staging.stg_loans
),

valid_loans AS (
    SELECT 
        s.LOAN_ID,
        s.SRC_CUSTOMER_ID,
        c.CUSTOMER_ID AS DIM_CUSTOMER_ID, -- ✅ Explicit aliasing
        d_start.DATE_ID AS START_DATE_ID,
        d_end.DATE_ID AS END_DATE_ID,
        s.LOAN_TYPE,
        s.PRINCIPAL_AMOUNT,
        s.INTEREST_RATE,
        s.STATUS,
        s.INGESTION_TIMESTAMP
    FROM source s
    LEFT JOIN aw_db.raw_src.dim_dates d_start 
        ON s.START_DATE = d_start.CALENDAR_DATE
    LEFT JOIN aw_db.raw_src.dim_dates d_end 
        ON s.END_DATE = d_end.CALENDAR_DATE
    LEFT JOIN aw_db.raw_src.dim_customers c 
        ON s.SRC_CUSTOMER_ID = c.CUSTOMER_ID
    WHERE s.PRINCIPAL_AMOUNT > 0
),

final AS (
    SELECT 
        LOAN_ID,
        COALESCE(DIM_CUSTOMER_ID, SRC_CUSTOMER_ID) AS CUSTOMER_ID, -- ✅ Resolving ambiguity
        LOAN_TYPE,
        PRINCIPAL_AMOUNT,
        INTEREST_RATE,
        START_DATE_ID,
        END_DATE_ID,
        STATUS,
        INGESTION_TIMESTAMP
    FROM valid_loans
)

SELECT * FROM final
