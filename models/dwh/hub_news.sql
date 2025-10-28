{{config(
    materialized = 'incremental',
    unique_key = 'hsh_hub_news',
    post_hook = ["
            {% if flags.FULL_REFRESH %}
            DO $$
            BEGIN
                IF NOT EXISTS (
                  SELECT 1
                  FROM pg_constraint
                  WHERE conname = 'pk_hsh_hub_news'
                ) THEN
                    alter table {{this}} add constraint pk_hsh_hub_news primary key (hsh_hub_news);
                END IF;
            END $$;
            {% endif %}
        "]
)}}
with cte_src_comic as (
    select src.news as news_txt,
        'xkcd' as source_nm,
        src.load_ts as news_load_ts,
        row_number() over(partition by src.news order by src.load_ts desc) as row_nr
    from {{ref('stg_xkcd_data')}} as src
    where src.news is not null and src.news <> ''
)
select 
    {{dbt_utils.generate_surrogate_key([ 'news_txt', 'source_nm' ])}} as hsh_hub_news,
    {{dbt_utils.generate_surrogate_key([ 'news_txt' ])}} as news_id,
    source_nm,
    news_load_ts as load_ts
from cte_src_comic
where 1=1
    and row_nr = 1
{% if is_incremental() %}
    and news_load_ts > '{{get_last_loaded_ts('dwh.hub_news', 'load_ts')}}'
{% endif %}