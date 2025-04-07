
{{ config(
    materialized = 'table',
    tags=['dim', 'transactions']
) }}





WITH transactions AS (
    SELECT
        t.TRANSACTION_ID,
        t.ACCOUNT_ID,
        a.CUSTOMER_ID,
        t.TRANSACTION_DATE,
        t.TRANSACTION_TYPE,
        t.AMOUNT,
        t.MERCHANT_ID,
        t.STATUS,
        t.INGESTION_TIMESTAMP
    FROM {{ ref('stg_transactions') }} t
    LEFT JOIN {{ ref('stg_accounts') }} a
        ON t.ACCOUNT_ID = a.ACCOUNT_ID
)

SELECT 
    TRANSACTION_ID,
    ACCOUNT_ID,
    CUSTOMER_ID,
    TRANSACTION_DATE,
    TRANSACTION_TYPE,
    AMOUNT,
    MERCHANT_ID,
    STATUS,
    INGESTION_TIMESTAMP
FROM transactions
