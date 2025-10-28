with src_xkcd as (
    select 
        cast(day as int) as dates_day_nr,
        cast(month as int) as dates_month_nr,
        cast(year as int) as dates_year_nr,
        cast(year as int)*10000+cast(month as int)*100+cast(day as int) as date_id,
        'xkcd' as source_nm,
        load_ts,
        row_number() over(partition by year, month, day order by load_ts desc) as row_nr
    from {{ ref('stg_xkcd_data') }} 
    where year is not null and month is not null and day is not null
)
select {{dbt_utils.generate_surrogate_key([ 'date_id', 'source_nm' ])}} as hsh_hub_dates,
        dates_day_nr,
        dates_month_nr,
        dates_year_nr,
        load_ts
from src_xkcd
where row_nr = 1 