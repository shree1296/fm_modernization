{{ config(
    materialized = 'table',
    tags=['dim', 'loans']
) }}

WITH loans AS (
    SELECT
        LOAN_ID,
        CUSTOMER_ID,
        LOAN_TYPE,
        PRINCIPAL_AMOUNT,
        INTEREST_RATE,
        START_DATE,
        END_DATE,
        CASE 
            WHEN INITCAP(STATUS) = 'Active' THEN 'Active'
            WHEN INITCAP(STATUS) = 'Closed' THEN 'Closed'
            WHEN INITCAP(STATUS) = 'Defaulted' THEN 'Defaulted'
            ELSE 'Unknown'  -- Catch unexpected values
        END AS STATUS,
        INGESTION_TIMESTAMP
    FROM {{ ref('stg_loans') }}
)

SELECT 
    LOAN_ID,
    CUSTOMER_ID,
    LOAN_TYPE,
    PRINCIPAL_AMOUNT,
    INTEREST_RATE,
    START_DATE,
    END_DATE,
    DATEDIFF(YEAR, START_DATE, END_DATE) AS LOAN_DURATION_YEARS,
    STATUS,
    INGESTION_TIMESTAMP
FROM loans
