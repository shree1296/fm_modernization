{{ config(
    materialized='table',
    tags=['fact', 'payroll']
) }}

WITH payroll_data AS (
    SELECT 
        PAYROLL_ID,
        EMPLOYEE_ID,  -- No longer joining with dim_employees
        PAY_DATE,
        SALARY_AMOUNT,
        TAX_DEDUCTION,
        NET_SALARY,
        CREATED_AT,
        INGESTION_TIMESTAMP
    FROM {{ ref('stg_payroll') }}
)

SELECT * FROM payroll_data