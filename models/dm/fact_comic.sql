{{config(
    materialized = 'table',
    unique_key = 'comic_id',
    post_hook = ["""
            DO $$
            BEGIN
                IF NOT EXISTS (
                  SELECT 1
                  FROM pg_constraint
                  WHERE conname = 'pk_fact_comic'
                ) THEN
                    alter table {{this}} add constraint pk_fact_comic primary key (comic_id);
                END IF;

                IF NOT EXISTS (
                    SELECT 1 FROM pg_class WHERE relname = 'idx_fact_comic_dates_id'
                ) THEN
                    CREATE INDEX idx_fact_comic_dates_id ON {{ this }} (published_date_id);
                ELSE
                    REINDEX TABLE {{ this }};
                END IF;

                IF NOT EXISTS (
                    SELECT 1 FROM pg_class WHERE relname = 'idx_fact_comic_external_links'
                ) THEN
                    CREATE INDEX idx_fact_comic_external_links ON {{ this }} (external_links_id);
                ELSE
                    REINDEX TABLE {{ this }};
                END IF;

                IF NOT EXISTS (
                    SELECT 1 FROM pg_class WHERE relname = 'idx_fact_comic_images'
                ) THEN
                    CREATE INDEX idx_fact_comic_images ON {{ this }} (images_id);
                ELSE
                    REINDEX TABLE {{ this }};
                END IF;

                IF NOT EXISTS (
                    SELECT 1 FROM pg_class WHERE relname = 'idx_fact_comic_news'
                ) THEN
                    CREATE INDEX idx_fact_comic_news ON {{ this }} (news_id);
                ELSE
                    REINDEX TABLE {{ this }};
                END IF;
            END $$;
        """]
)}}
with cte_src as (
    select c.comic_nr  as comic_id, 
        d.dates_id as published_date_id,
        i.images_id, 
        l.links_id as external_links_id, 
        n.news_id,
        length(regexp_replace(hc.comic_safe_title_txt, '[^A-Za-z]', '', 'g')) * 5 as creator_cost_amt,
        floor(random()*10000) as comic_views_nr,
        round((random()*9+1)::numeric,1) as customer_review_score
    from {{ref('hub_comic')}} c
    left join {{ref('sat_comic_xkcd')}} hc
        on c.hsh_hub_comic = hc.hsh_hub_comic
    left join {{ref('lnk_comic_dates')}} cd
        on c.hsh_hub_comic = cd.hsh_hub_comic
    left join {{ref('hub_dates')}} d 
        on cd.hsh_hub_dates = d.hsh_hub_dates
    left join {{ref('lnk_comic_images')}} ci
        on c.hsh_hub_comic = ci.hsh_hub_comic
    left join {{ref('hub_images')}} i
        on ci.hsh_hub_images = i.hsh_hub_images
    left join {{ref('lnk_comic_links')}} cl
        on c.hsh_hub_comic = cl.hsh_hub_comic
    left join {{ref('hub_links')}} l
        on cl.hsh_hub_links = l.hsh_hub_links
    left join {{ref('lnk_comic_news')}} cn
        on c.hsh_hub_comic = cn.hsh_hub_comic
    left join {{ref('hub_news')}} n
        on cn.hsh_hub_news = n.hsh_hub_news
)
select comic_id,
    published_date_id,
    images_id,
    external_links_id,
    news_id,
    creator_cost_amt,
    comic_views_nr,
    customer_review_score
from cte_src