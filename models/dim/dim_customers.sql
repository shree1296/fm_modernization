{{ config(
    materialized = 'table',
    tags=['dim', 'customers']
) }}

WITH all_customers AS (
    SELECT CUSTOMER_ID FROM {{ ref('stg_accounts') }}
    UNION
    SELECT CUSTOMER_ID FROM {{ ref('stg_loans') }}
    UNION
    SELECT CUSTOMER_ID FROM {{ ref('stg_contracts') }}
    UNION
    SELECT CUSTOMER_ID FROM {{ ref('stg_investment_portfolio') }}
),

accounts AS (
    SELECT *
    FROM (
        SELECT
            CUSTOMER_ID,
            ACCOUNT_ID,
            ACCOUNT_TYPE,
            ACCOUNT_STATUS,
            BALANCE,
            REGION,
            CREATED_AT,
            INGESTION_TIMESTAMP,
            ROW_NUMBER() OVER (PARTITION BY CUSTOMER_ID ORDER BY CREATED_AT DESC) AS row_num
        FROM {{ ref('stg_accounts') }}
    )
    WHERE row_num = 1
),

contracts AS (
    SELECT *
    FROM (
        SELECT
            CUSTOMER_ID,
            CONTRACT_ID,
            CONTRACT_TYPE,
            ROW_NUMBER() OVER (PARTITION BY CUSTOMER_ID ORDER BY CONTRACT_ID) AS row_num
        FROM {{ ref('stg_contracts') }}
    )
    WHERE row_num = 1
),

loans AS (
    SELECT *
    FROM (
        SELECT
            CUSTOMER_ID,
            LOAN_ID,
            LOAN_TYPE,
            PRINCIPAL_AMOUNT,
            INTEREST_RATE,
            ROW_NUMBER() OVER (PARTITION BY CUSTOMER_ID ORDER BY LOAN_ID) AS row_num
        FROM {{ ref('stg_loans') }}
    )
    WHERE row_num = 1
),

investments AS (
    SELECT *
    FROM (
        SELECT
            CUSTOMER_ID,
            INVESTMENT_ID,
            INVESTMENT_AMOUNT,
            CURRENT_VALUE,
            ROW_NUMBER() OVER (PARTITION BY CUSTOMER_ID ORDER BY INVESTMENT_ID) AS row_num
        FROM {{ ref('stg_investment_portfolio') }}
    )
    WHERE row_num = 1
)

SELECT 
    {{ dbt_utils.generate_surrogate_key(['c.CUSTOMER_ID']) }} AS customer_key,
    c.CUSTOMER_ID,
    a.ACCOUNT_ID,
    a.ACCOUNT_TYPE,
    a.ACCOUNT_STATUS,
    a.BALANCE,
    co.CONTRACT_ID,
    co.CONTRACT_TYPE,
    l.LOAN_ID,
    l.LOAN_TYPE,
    l.PRINCIPAL_AMOUNT,
    l.INTEREST_RATE,
    i.INVESTMENT_ID,
    i.INVESTMENT_AMOUNT,
    i.CURRENT_VALUE,
    a.REGION,
    a.CREATED_AT,
    a.INGESTION_TIMESTAMP
FROM all_customers c
LEFT JOIN accounts a ON c.CUSTOMER_ID = a.CUSTOMER_ID
LEFT JOIN contracts co ON c.CUSTOMER_ID = co.CUSTOMER_ID
LEFT JOIN loans l ON c.CUSTOMER_ID = l.CUSTOMER_ID
LEFT JOIN investments i ON c.CUSTOMER_ID = i.CUSTOMER_ID
