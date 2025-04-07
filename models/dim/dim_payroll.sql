{{ config(
    materialized = 'table',
    tags=['dim', 'payroll']
) }}



WITH payroll_data AS (
    SELECT
        p.PAYROLL_ID,
        p.EMPLOYEE_ID,
        p.PAY_DATE,
        p.SALARY_AMOUNT,
        p.TAX_DEDUCTION,
        p.NET_SALARY,
        p.INGESTION_TIMESTAMP
    FROM {{ ref('stg_payroll') }} p
)

SELECT 
    PAYROLL_ID,
    EMPLOYEE_ID,
    PAY_DATE,
    SALARY_AMOUNT,
    TAX_DEDUCTION,
    NET_SALARY,
    INGESTION_TIMESTAMP
FROM payroll_data
