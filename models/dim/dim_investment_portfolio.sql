{{ config(
    materialized = 'table',
    tags=['dim', 'investment_portfolio']
) }}



WITH investments AS (
    SELECT
        INVESTMENT_ID,
        CUSTOMER_ID,
        ASSET_CLASS,
        INVESTMENT_AMOUNT,
        CURRENT_VALUE,
        INVESTMENT_DATE,
        INGESTION_TIMESTAMP
    FROM {{ ref('stg_investment_portfolio') }}
)

SELECT 
    INVESTMENT_ID,
    CUSTOMER_ID,
    ASSET_CLASS,
    INVESTMENT_AMOUNT,
    CURRENT_VALUE,
    (CURRENT_VALUE - INVESTMENT_AMOUNT) AS INVESTMENT_GROWTH,
    INVESTMENT_DATE,
    INGESTION_TIMESTAMP
FROM investments
