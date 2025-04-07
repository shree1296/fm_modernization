{{ config(
    materialized = 'table',
    tags=['dim', 'fraud_monitoring']
) }}

with base as (
    select * from {{ ref('stg_fraud_monitoring') }}
),

account_join as (
    select 
        b.*,
        a.customer_id
    from base b
    inner join {{ ref('stg_accounts') }} a
        on b.account_id = a.account_id
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['fraud_id']) }} as fraud_key,
        fraud_id,
        account_id,
        customer_id,
        transaction_id,
        fraud_type,
        detected_date as detected_at,
        status as resolution_status,
        created_at as resolution_details,
        null as resolved_at,
        ingestion_timestamp
    from account_join
)

select * from final
