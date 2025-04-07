{{ config(
    materialized = 'table',
    tags=['dim', 'general_ledger']
) }}

WITH ledger AS (
    SELECT
        LEDGER_ID,
        ACCOUNT_ID,
        TRANSACTION_DATE,
        DEBIT_AMOUNT,
        CREDIT_AMOUNT,
        BALANCE,
        INGESTION_TIMESTAMP
    FROM {{ ref('stg_general_ledger') }}
)

SELECT 
    LEDGER_ID,
    ACCOUNT_ID,
    TRANSACTION_DATE,
    DEBIT_AMOUNT,
    CREDIT_AMOUNT,
    BALANCE,
    (CREDIT_AMOUNT - DEBIT_AMOUNT) AS NET_CHANGE,
    INGESTION_TIMESTAMP
FROM ledger
