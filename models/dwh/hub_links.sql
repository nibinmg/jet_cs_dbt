{{config(
    materialized = 'incremental',
    unique_key = 'hsh_hub_links',
    post_hook = ["
            {% if flags.FULL_REFRESH %}
            DO $$
            BEGIN
                IF NOT EXISTS (
                  SELECT 1
                  FROM pg_constraint
                  WHERE conname = 'pk_hsh_hub_links'
                ) THEN
                    alter table {{this}} add constraint pk_hsh_hub_links primary key (hsh_hub_links);
                END IF;
            END $$;
            {% endif %}
        "]
)}}
with cte_src_comic as (
    select src.link as links_url,
        'xkcd' as source_nm,
        src.load_ts as links_load_ts,
        row_number() over(partition by src.link order by src.load_ts desc) as row_nr
    from {{ref('stg_xkcd_data')}} as src
    where src.link is not null and src.link <> ''
)
select 
    {{dbt_utils.generate_surrogate_key([ 'links_url', 'source_nm' ])}} as hsh_hub_links,
    {{dbt_utils.generate_surrogate_key([ 'links_url' ])}} as links_id,
    source_nm,
    links_load_ts as load_ts
from cte_src_comic
where 1=1
    and row_nr = 1
{% if is_incremental() %}
    and links_load_ts > '{{get_last_loaded_ts('dwh.hub_links', 'load_ts')}}'
{% endif %}