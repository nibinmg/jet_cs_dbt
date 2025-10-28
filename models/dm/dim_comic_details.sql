{{config(
    materialized = 'table',
    unique_key = 'comic_id',
    post_hook = ["""
            DO $$
            BEGIN
                IF NOT EXISTS (
                  SELECT 1
                  FROM pg_constraint
                  WHERE conname = 'pk_dim_comic_details'
                ) THEN
                    alter table {{this}} add constraint pk_dim_comic_details primary key (comic_id);
                END IF;
            END $$;
        """]
)}}
with cte_src as (
    select hc.comic_nr as comic_id,
        sc.comic_title_txt as title_txt,
        sc.comic_safe_title_txt as safe_title_txt,
        sc.comic_transcript_txt as transcript_txt,
        sc.comic_alt_txt as alt_txt
    from {{ref('hub_comic')}} hc
    left join {{ref('sat_comic_xkcd')}} sc
        on hc.hsh_hub_comic = sc.hsh_hub_comic
)
select * from cte_src