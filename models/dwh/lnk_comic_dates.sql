{{config(
    materialized = 'incremental',
    unique_key = 'hsh_lnk_comic_dates',
    post_hook = ["
            {% if flags.FULL_REFRESH %}
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_constraint WHERE conname = 'pk_hsh_lnk_comic_dates'
                ) THEN
                    alter table {{this}} add constraint pk_hsh_lnk_comic_dates primary key (hsh_lnk_comic_dates);
                END IF;

                IF NOT EXISTS (
                    SELECT 1 FROM pg_class WHERE relname = 'idx_lnk_comic_dates'
                ) THEN
                    CREATE INDEX idx_lnk_comic_dates ON {{ this }} (hsh_hub_comic, hsh_hub_dates);
                ELSE
                    REINDEX TABLE {{ this }};
                END IF;
            END $$;
            {% endif %}
        "]
)}}
with cte_src as (
    select src.num as comic_nr,
        CAST(src.year AS INT)*10000+CAST(src.month as INT)*100+CAST(src.day AS INT) as dates_id,
        'xkcd' as source_nm,
        src.load_ts as load_ts,
        row_number() over(partition by src.num, CAST(src.year AS INT)*10000+CAST(src.month as INT)*100+CAST(src.day AS INT) order by src.load_ts desc) as row_nr
    from {{ref('stg_xkcd_data')}} as src
    where src.num is not null
        and src.year is not null and src.month is not null and src.day is not null
)
select 
    {{dbt_utils.generate_surrogate_key([ 'comic_nr', 'dates_id', 'source_nm' ])}} as hsh_lnk_comic_dates,
    {{dbt_utils.generate_surrogate_key([ 'comic_nr', 'source_nm' ])}} as hsh_hub_comic,
    {{dbt_utils.generate_surrogate_key([ 'dates_id', 'source_nm' ])}} as hsh_hub_dates,
    load_ts as load_ts
from cte_src
where 1=1
    and row_nr = 1
{% if is_incremental() %}
    and load_ts > '{{get_last_loaded_ts('dwh.lnk_comic_dates', 'load_ts')}}'
{% endif %}