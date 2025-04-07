{{ config(
    materialized = 'table',
    tags=['dim', 'accounts']
) }}

WITH accounts AS (
    SELECT
        CAST(ACCOUNT_ID AS VARCHAR) AS ACCOUNT_ID,  -- Ensure ACCOUNT_ID is VARCHAR
        CUSTOMER_ID,
        ACCOUNT_TYPE,
        ACCOUNT_STATUS,
        BALANCE,
        REGION,
        CREATED_AT,
        INGESTION_TIMESTAMP
    FROM {{ ref('stg_accounts') }}
),

last_transaction AS (
    SELECT 
        CAST(ACCOUNT_ID AS VARCHAR) AS ACCOUNT_ID,  
        CAST(TRANSACTION_ID AS VARCHAR) AS LAST_TRANSACTION_ID,  
        TRANSACTION_DATE AS LAST_TRANSACTION_DATE
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY ACCOUNT_ID ORDER BY TRANSACTION_DATE DESC) AS rn
        FROM {{ ref('stg_transactions') }}
    ) WHERE rn = 1
),

ledger_summary AS (
    SELECT 
        CAST(ACCOUNT_ID AS VARCHAR) AS ACCOUNT_ID,  
        COALESCE(SUM(DEBIT_AMOUNT), 0) AS TOTAL_DEBITS,  
        COALESCE(SUM(CREDIT_AMOUNT), 0) AS TOTAL_CREDITS  
    FROM {{ ref('stg_general_ledger') }}
    GROUP BY ACCOUNT_ID
)

SELECT 
    a.ACCOUNT_ID,
    a.CUSTOMER_ID,
    a.ACCOUNT_TYPE,
    a.ACCOUNT_STATUS,
    a.BALANCE,
    lt.LAST_TRANSACTION_ID,
    lt.LAST_TRANSACTION_DATE,
    l.TOTAL_DEBITS,
    l.TOTAL_CREDITS,
    a.REGION,
    a.CREATED_AT,
    a.INGESTION_TIMESTAMP
FROM accounts a
LEFT JOIN last_transaction lt ON a.ACCOUNT_ID = lt.ACCOUNT_ID
LEFT JOIN ledger_summary l ON a.ACCOUNT_ID = l.ACCOUNT_ID
