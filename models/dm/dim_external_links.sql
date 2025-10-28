{{config(
    materialized = 'table',
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
        sl.links_url
    from {{ref('hub_links')}} hl
    left join {{ref('sat_links_xkcd')}} sl
        on hl.hsh_hub_links = sl.hsh_hub_links
)
select * from cte_src
