{{config(
    materialized = 'incremental',
    unique_key = 'hsh_lnk_comic_links',
    post_hook = ["
            {% if flags.FULL_REFRESH %}
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_constraint WHERE conname = 'pk_hsh_lnk_comic_links'
                ) THEN
                    alter table {{this}} add constraint pk_hsh_lnk_comic_links primary key (hsh_lnk_comic_links);
                END IF;

                IF NOT EXISTS (
                    SELECT 1 FROM pg_class WHERE relname = 'idx_lnk_comic_links'
                ) THEN
                    CREATE INDEX idx_lnk_comic_links ON {{ this }} (hsh_hub_comic, hsh_hub_links);
                ELSE
                    REINDEX TABLE {{ this }};
                END IF;
            END $$;
            {% endif %}
        "]
)}}
with cte_src as (
    select src.num as comic_nr,
        src.link as links_url,
        'xkcd' as source_nm,
        src.load_ts as load_ts,
        row_number() over(partition by src.num, src.link order by src.load_ts desc) as row_nr
    from {{ref('stg_xkcd_data')}} as src
    where src.num is not null
        and src.link is not null and src.link <> ''
)
select 
    {{dbt_utils.generate_surrogate_key([ 'comic_nr', 'links_url', 'source_nm' ])}} as hsh_lnk_comic_links,
    {{dbt_utils.generate_surrogate_key([ 'comic_nr', 'source_nm' ])}} as hsh_hub_comic,
    {{dbt_utils.generate_surrogate_key([ 'links_url', 'source_nm' ])}} as hsh_hub_links,
    load_ts as load_ts
from cte_src
where 1=1
    and row_nr = 1
{% if is_incremental() %}
    and load_ts > '{{get_last_loaded_ts('dwh.lnk_comic_links', 'load_ts')}}'
{% endif %}