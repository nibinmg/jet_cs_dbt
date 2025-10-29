{{config(
    materialized = 'incremental',
    unique_key = 'links_id',
    post_hook = ["""
            DO $$
            BEGIN
                IF NOT EXISTS (
                  SELECT 1
                  FROM pg_constraint
                  WHERE conname = 'pk_dim_external_links'
                ) THEN
                    alter table {{this}} add constraint pk_dim_external_links primary key (links_id);
                END IF;
            END $$;
        """]
)}}
with cte_src as (
    select hl.links_id,
        sl.links_url,
        hl.load_ts
    from {{ref('hub_links')}} hl
    left join {{ref('sat_links_xkcd')}} sl
        on hl.hsh_hub_links = sl.hsh_hub_links
    where 1=1
    {% if is_incremental() %}
        and hl.load_ts > '{{get_last_loaded_ts('dm.dim_external_links', 'load_ts')}}'
    {% endif %}
)
select * from cte_src
