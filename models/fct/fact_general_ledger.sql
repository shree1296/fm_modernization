{{ config(
    materialized='table',
    tags=['fact', 'general_ledger']
) }}

WITH source AS (
    SELECT 
        s.LEDGER_ID,  -- Already VARCHAR, no change needed
        s.ACCOUNT_ID,  -- Already NUMBER(38,0), no change
        TRY_TO_DATE(s.TRANSACTION_DATE) AS TRANSACTION_DATE,  -- Ensures date conversion
        s.DEBIT_AMOUNT,
        s.CREDIT_AMOUNT,
        s.BALANCE,
        s.CREATED_AT,
        s.INGESTION_TIMESTAMP
    FROM {{ ref('stg_general_ledger') }} s
    WHERE s.BALANCE >= 0  -- Ensures no negative balances
),

valid_transactions AS (
    SELECT 
        s.LEDGER_ID,
        s.ACCOUNT_ID,
        s.TRANSACTION_DATE,
        s.DEBIT_AMOUNT,
        s.CREDIT_AMOUNT,
        s.BALANCE,
        s.CREATED_AT,
        s.INGESTION_TIMESTAMP,
        d.DATE_ID AS TRANSACTION_DATE_ID,  -- Ensure dim_dates has DATE_ID
        dl.NET_CHANGE  -- Include the NET_CHANGE from dim_ledger
    FROM source s
    LEFT JOIN {{ ref('dim_dates') }} d 
        ON DATE_TRUNC('DAY', s.TRANSACTION_DATE) = d.CALENDAR_DATE  -- Handle time component in TRANSACTION_DATE
    LEFT JOIN {{ ref('dim_general_ledger') }} dl 
        ON s.LEDGER_ID = dl.LEDGER_ID  -- Ensure matching LEDGER_ID between staging and dimension
),

final AS (
    SELECT 
        LEDGER_ID, 
        ACCOUNT_ID, 
        TRANSACTION_DATE_ID,
        DEBIT_AMOUNT,
        CREDIT_AMOUNT,
        BALANCE,
        CREATED_AT,
        NET_CHANGE,  -- NET_CHANGE is included here from dim_ledger
        INGESTION_TIMESTAMP
    FROM valid_transactions
)

SELECT * FROM final
