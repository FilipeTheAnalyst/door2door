with 

dim_date as 
(

{{ dbt_date.get_base_dates(start_date="2019-01-01", end_date="2023-12-31") }}

),

final as 
(
    select 
    FORMAT_TIMESTAMP('%Y%m%d', date_day) as date_id,
     FORMAT_TIMESTAMP('%Y-%m-%d', date_day) as date_day,
    EXTRACT(YEAR FROM date_day) AS year,
    FORMAT_TIMESTAMP('%Y-%m', date_day) AS year_month,
    LPAD(CAST(EXTRACT(MONTH FROM date_day) AS string), 2, '0') AS month,
    EXTRACT(DAY FROM date_day) AS day,
    FORMAT_TIMESTAMP('%B', date_day) AS month_name,
    FORMAT_TIMESTAMP('%A', date_day) AS day_of_week,
    IF(EXTRACT(DAYOFWEEK FROM date_day) IN (1, 7), TRUE, FALSE) AS is_weekend
    from dim_date
)

select * from final
order by date_id