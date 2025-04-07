{{ config(
    tags=["staging", "general-ledger"]
) }}

WITH source AS (
    SELECT
        LEDGER_ID,
        CAST(ACCOUNT_ID AS STRING) AS ACCOUNT_ID,
        TO_DATE(TRANSACTION_DATE, 'DD-MM-YYYY') AS TRANSACTION_DATE,  -- Parse date format properly
        COALESCE(DEBIT_AMOUNT, 0) AS DEBIT_AMOUNT,
        COALESCE(CREDIT_AMOUNT, 0) AS CREDIT_AMOUNT,
        COALESCE(BALANCE, 0) AS BALANCE,
        CREATED_AT,
        INGESTION_TIMESTAMP
    FROM {{ source('raw_src', 'src_general_ledger') }}
),

validated AS (
    SELECT
        LEDGER_ID,
        ACCOUNT_ID,
        CASE 
            WHEN TRANSACTION_DATE IS NULL OR TRANSACTION_DATE < '2000-01-01' THEN NULL
            ELSE TRANSACTION_DATE
        END AS TRANSACTION_DATE,
        DEBIT_AMOUNT,
        CREDIT_AMOUNT,
        BALANCE,
        CREATED_AT,
        INGESTION_TIMESTAMP
    FROM source
),

account_check AS (
    SELECT *
    FROM validated
    WHERE ACCOUNT_ID IN (
        SELECT DISTINCT ACCOUNT_ID FROM {{ ref('stg_accounts') }}
    )
)

SELECT 
    LEDGER_ID,
    ACCOUNT_ID,
    TRANSACTION_DATE,
    DEBIT_AMOUNT,
    CREDIT_AMOUNT,
    BALANCE,
    CREATED_AT,
    INGESTION_TIMESTAMP
FROM account_check
