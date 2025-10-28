{{config(
    materialized = 'table',
    unique_key = 'dates_id',
    post_hook = ["""
            DO $$
            BEGIN
                IF NOT EXISTS (
                  SELECT 1
                  FROM pg_constraint
                  WHERE conname = 'pk_dim_dates'
                ) THEN
                    alter table {{this}} add constraint pk_dim_dates primary key (dates_id);
                END IF;
            END $$;
        """]
)}}
with cte_src as (
	select hd.dates_id,
		dates_day_nr as day_nr,
		dates_month_nr as month_nr,
		dates_year_nr as year_nr,
		make_date(dates_year_nr, dates_month_nr, dates_day_nr) as full_dt
	from {{ref('hub_dates')}} hd
	left join {{ref('sat_dates_xkcd')}} sd
		on hd.hsh_hub_dates = sd.hsh_hub_dates
)
select * from cte_src