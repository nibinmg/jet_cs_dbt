with src_xkcd as (
    select 
        news as news_txt,
        'xkcd' as source_nm,
        load_ts,
        row_number() over(partition by news order by load_ts desc) as row_nr
    from {{ ref('stg_xkcd_data') }} 
    where news is not null and news <> ''
)
select {{dbt_utils.generate_surrogate_key([ 'news_txt', 'source_nm' ])}} as hsh_hub_news,
        news_txt,
        load_ts
from src_xkcd
where row_nr = 1 