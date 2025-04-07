{{ config(
    materialized='table',
    tags=['fact', 'transactions']
) }}

WITH transactions AS (
    SELECT
        TRANSACTION_ID,
        ACCOUNT_ID,
        TRANSACTION_DATE,
        AMOUNT,
        TRANSACTION_TYPE,
        MERCHANT_ID,
        STATUS,
        CREATED_AT,
        INGESTION_TIMESTAMP
    FROM aw_db.raw_src_staging.stg_transactions
)

SELECT 
    t.TRANSACTION_ID,
    t.ACCOUNT_ID,  -- Ensure ACCOUNT_ID is the correct join key
    t.TRANSACTION_DATE,
    t.AMOUNT,
    t.TRANSACTION_TYPE,
    t.MERCHANT_ID,
    t.STATUS,
    t.CREATED_AT,
    t.INGESTION_TIMESTAMP
FROM transactions t
LEFT JOIN aw_db.raw_src.dim_accounts a
    ON t.ACCOUNT_ID = a.ACCOUNT_ID  -- Correct join on ACCOUNT_ID
