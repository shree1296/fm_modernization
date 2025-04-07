{{ config(
    tags=["staging", "accounts"]
) }}

WITH source AS (
    SELECT 
        ACCOUNT_ID,           
        CUSTOMER_ID,          
        ACCOUNT_TYPE,         
        ACCOUNT_STATUS,       
        BALANCE,              
        CREATED_DATE,           
        INGESTION_TIMESTAMP
    FROM {{ source('raw_src', 'src_accounts') }}  
),

transformed AS (
    SELECT 
        ACCOUNT_ID::STRING AS account_id,         
        CUSTOMER_ID::STRING AS customer_id,       
        UPPER(ACCOUNT_TYPE) AS account_type,      
        CASE 
            WHEN ACCOUNT_STATUS ILIKE 'active' THEN 'Active' 
            WHEN ACCOUNT_STATUS ILIKE 'closed' THEN 'Closed'
            ELSE 'Unknown'
        END AS account_status,                    
        BALANCE::FLOAT AS balance,                
        TRY_TO_TIMESTAMP(CREATED_DATE) AS created_at,  
        INGESTION_TIMESTAMP::TIMESTAMP_NTZ AS ingestion_timestamp,  
        '{{ var("selected_region", var("default_region")) }}' AS region  -- Use default if not passed
    FROM source
)

SELECT * 
FROM transformed
WHERE region = '{{ var("selected_region", var("default_region")) }}'  -- Filter dynamically
