{{ config(
    tags=["staging", "payroll"]
) }}

WITH source AS (
    -- Selecting and renaming columns from raw source table
    SELECT
        PAYROLL_ID,
        EMPLOYEE_ID,
        TRY_CAST(PAY_DATE AS DATE) AS PAY_DATE,  -- Ensuring valid date format
        SALARY_AMOUNT,
        TAX_DEDUCTION,
        NET_SALARY,
        CREATED_AT,
        INGESTION_TIMESTAMP
    FROM {{ source('raw_src', 'src_payroll') }}
),

validated AS (
    -- Handling data quality issues
    SELECT
        PAYROLL_ID,
        EMPLOYEE_ID,
        
        -- Ensuring PAY_DATE is within the expected range
        CASE 
            WHEN PAY_DATE IS NULL OR PAY_DATE < '2000-01-01' THEN NULL 
            ELSE PAY_DATE 
        END AS PAY_DATE,
        
        SALARY_AMOUNT,
        TAX_DEDUCTION,
        NET_SALARY,
        CREATED_AT,
        INGESTION_TIMESTAMP,
        
        -- Identifying negative salary amounts
        CASE WHEN SALARY_AMOUNT < 0 THEN NULL ELSE SALARY_AMOUNT END AS VALID_SALARY_AMOUNT,
        
        -- Identifying negative tax deductions
        CASE WHEN TAX_DEDUCTION < 0 THEN NULL ELSE TAX_DEDUCTION END AS VALID_TAX_DEDUCTION,
        
        -- Identifying negative net salary
        CASE WHEN NET_SALARY < 0 THEN NULL ELSE NET_SALARY END AS VALID_NET_SALARY
    FROM source
)

SELECT * FROM validated
