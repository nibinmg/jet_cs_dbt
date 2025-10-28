{{config(
    materialized = 'incremental',
    unique_key = 'hsh_lnk_comic_news',
    post_hook = ["
            {% if flags.FULL_REFRESH %}
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_constraint WHERE conname = 'pk_hsh_lnk_comic_news'
                ) THEN
                    alter table {{this}} add constraint pk_hsh_lnk_comic_news primary key (hsh_lnk_comic_news);
                END IF;

                IF NOT EXISTS (
                    SELECT 1 FROM pg_class WHERE relname = 'idx_lnk_comic_news'
                ) THEN
                    CREATE INDEX idx_lnk_comic_news ON {{ this }} (hsh_hub_comic, hsh_hub_news);
                ELSE
                    REINDEX TABLE {{ this }};
                END IF;
            END $$;
            {% endif %}
        "]
)}}
with cte_src as (
    select src.num as comic_nr,
        src.news as news_txt,
        'xkcd' as source_nm,
        src.load_ts as load_ts,
        row_number() over(partition by src.num, src.news order by src.load_ts desc) as row_nr
    from {{ref('stg_xkcd_data')}} as src
    where src.num is not null
        and src.news is not null and src.news <> ''
)
select 
    {{dbt_utils.generate_surrogate_key([ 'comic_nr', 'news_txt', 'source_nm' ])}} as hsh_lnk_comic_news,
    {{dbt_utils.generate_surrogate_key([ 'comic_nr', 'source_nm' ])}} as hsh_hub_comic,
    {{dbt_utils.generate_surrogate_key([ 'news_txt', 'source_nm' ])}} as hsh_hub_news,
    load_ts as load_ts
from cte_src
where 1=1
    and row_nr = 1
{% if is_incremental() %}
    and load_ts > '{{get_last_loaded_ts('dwh.lnk_comic_news', 'load_ts')}}'
{% endif %}