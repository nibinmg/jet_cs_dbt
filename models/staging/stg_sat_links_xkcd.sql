with src_xkcd as (
    select 
        link as links_url,
        'xkcd' as source_nm,
        load_ts,
        row_number() over(partition by link order by load_ts desc) as row_nr
    from {{ ref('stg_xkcd_data') }} 
    where link is not null and link <> ''
)
select {{dbt_utils.generate_surrogate_key([ 'links_url', 'source_nm' ])}} as hsh_hub_links,
        links_url,
        load_ts
from src_xkcd
where row_nr = 1 