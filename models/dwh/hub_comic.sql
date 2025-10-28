{{config(
    materialized = 'incremental',
    unique_key = 'hsh_hub_comic',
    post_hook = ["
            {% if flags.FULL_REFRESH %}
            DO $$
            BEGIN
                IF NOT EXISTS (
                  SELECT 1
                  FROM pg_constraint
                  WHERE conname = 'pk_hsh_hub_comic'
                ) THEN
                    alter table {{this}} add constraint pk_hsh_hub_comic primary key (hsh_hub_comic);
                END IF;
            END $$;
            {% endif %}
        "]
)}}
with cte_src_comic as (
    select src.num as comic_nr,
        'xkcd' as source_nm,
        src.load_ts as comic_load_ts,
        row_number() over(partition by src.num order by src.load_ts desc) as row_nr
    from {{ref('stg_xkcd_data')}} as src
    where src.num is not null
)
select 
    {{dbt_utils.generate_surrogate_key([ 'comic_nr', 'source_nm' ])}} as hsh_hub_comic,
    comic_nr,
    source_nm,
    comic_load_ts as load_ts
from cte_src_comic
where 1=1
    and row_nr = 1
{% if is_incremental() %}
    and comic_load_ts > '{{get_last_loaded_ts('dwh.hub_comic', 'load_ts')}}'
{% endif %}