{{config(
    materialized = 'incremental',
    unique_key = 'hsh_hub_images',
    post_hook = ["
            {% if flags.FULL_REFRESH %}
            DO $$
            BEGIN
                IF NOT EXISTS (
                  SELECT 1
                  FROM pg_constraint
                  WHERE conname = 'pk_hsh_hub_images'
                ) THEN
                    alter table {{this}} add constraint pk_hsh_hub_images primary key (hsh_hub_images);
                END IF;
            END $$;
            {% endif %}
        "]
)}}
with cte_source as (
    select src.img as images_url,
        'xkcd' as source_nm,
        src.load_ts as images_load_ts,
        row_number() over(partition by src.img order by src.load_ts desc) as row_nr
    from {{ref('stg_xkcd_data')}} as src
    where src.img is not null and src.img <> ''
)
select 
    {{dbt_utils.generate_surrogate_key([ 'images_url', 'source_nm' ])}} as hsh_hub_images,
    {{dbt_utils.generate_surrogate_key([ 'images_url' ])}} as images_id,
    source_nm,
    images_load_ts as load_ts
from cte_source
where 1=1
    and row_nr = 1
{% if is_incremental() %}
    and images_load_ts > '{{get_last_loaded_ts('dwh.hub_images', 'load_ts')}}'
{% endif %}