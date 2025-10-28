{{config(
    materialized = 'incremental',
    unique_key = 'hsh_lnk_comic_images',
    post_hook = ["
            {% if flags.FULL_REFRESH %}
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_constraint WHERE conname = 'pk_hsh_lnk_comic_images'
                ) THEN
                    alter table {{this}} add constraint pk_hsh_lnk_comic_images primary key (hsh_lnk_comic_images);
                END IF;

                IF NOT EXISTS (
                    SELECT 1 FROM pg_class WHERE relname = 'idx_lnk_comic_images'
                ) THEN
                    CREATE INDEX idx_lnk_comic_images ON {{ this }} (hsh_hub_comic, hsh_hub_images);
                ELSE
                    REINDEX TABLE {{ this }};
                END IF;
            END $$;
            {% endif %}
        "]
)}}
with cte_src as (
    select src.num as comic_nr,
        src.img as images_url,
        'xkcd' as source_nm,
        src.load_ts as load_ts,
        row_number() over(partition by src.num, src.img order by src.load_ts desc) as row_nr
    from {{ref('stg_xkcd_data')}} as src
    where src.num is not null
        and src.img is not null and src.img <> ''
)
select 
    {{dbt_utils.generate_surrogate_key([ 'comic_nr', 'images_url', 'source_nm' ])}} as hsh_lnk_comic_images,
    {{dbt_utils.generate_surrogate_key([ 'comic_nr', 'source_nm' ])}} as hsh_hub_comic,
    {{dbt_utils.generate_surrogate_key([ 'images_url', 'source_nm' ])}} as hsh_hub_images,
    load_ts as load_ts
from cte_src
where 1=1
    and row_nr = 1
{% if is_incremental() %}
    and load_ts > '{{get_last_loaded_ts('dwh.lnk_comic_images', 'load_ts')}}'
{% endif %}