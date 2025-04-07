{{ config(
    materialized = 'table',
    tags=['dim', 'dates']
) }}



WITH date_series AS (
    SELECT 
        DATEADD(DAY, ROW_NUMBER() OVER (ORDER BY SEQ4()) - 1, '2000-01-01') AS CALENDAR_DATE
    FROM TABLE(GENERATOR(ROWCOUNT => 36525))  -- 100 years of dates
),

final AS (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY CALENDAR_DATE) AS DATE_ID,  -- Unique Date ID
        CALENDAR_DATE,
        YEAR(CALENDAR_DATE) AS YEAR,
        MONTH(CALENDAR_DATE) AS MONTH,
        DAY(CALENDAR_DATE) AS DAY,
        WEEKOFYEAR(CALENDAR_DATE) AS WEEK,
        QUARTER(CALENDAR_DATE) AS QUARTER
    FROM date_series
)

SELECT * FROM final