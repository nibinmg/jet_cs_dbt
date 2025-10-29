{{config(
    materialized = 'incremental',
    unique_key = 'news_id',
    post_hook = ["""
            DO $$
            BEGIN
                IF NOT EXISTS (
                  SELECT 1
                  FROM pg_constraint
                  WHERE conname = 'pk_dim_news_data'
                ) THEN
                    alter table {{this}} add constraint pk_dim_news_data primary key (news_id);
                END IF;
            END $$;
        """]
)}}
with cte_src as (
    select hn.news_id,
        sn.news_txt,
        hn.load_ts
    from {{ref('hub_news')}} hn
    left join {{ref('sat_news_xkcd')}} sn
        on hn.hsh_hub_news = sn.hsh_hub_news
    where 1=1
    {% if is_incremental() %}
        and hn.load_ts > '{{get_last_loaded_ts('dm.dim_news_data', 'load_ts')}}'
    {% endif %}
)
select * from cte_src