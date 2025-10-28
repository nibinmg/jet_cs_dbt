{{config(
    materialized = 'table',
    unique_key = 'images_id',
    post_hook = ["""
            DO $$
            BEGIN
                IF NOT EXISTS (
                  SELECT 1
                  FROM pg_constraint
                  WHERE conname = 'pk_dim_image_info'
                ) THEN
                    alter table {{this}} add constraint pk_dim_image_info primary key (images_id);
                END IF;
            END $$;
        """]
)}}
with cte_src as (
    select hi.images_id,
        si.images_url
    from {{ref('hub_images')}} hi
    left join {{ref('sat_images_xkcd')}} si 
        on hi.hsh_hub_images = si.hsh_hub_images
)
select * from cte_src