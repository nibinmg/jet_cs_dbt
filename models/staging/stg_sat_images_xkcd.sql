with src_xkcd as (
    select 
        img as images_url,
        'xkcd' as source_nm,
        load_ts,
        row_number() over(partition by img order by load_ts desc) as row_nr
    from {{ ref('stg_xkcd_data') }} 
    where img is not null and img <> ''
)
select {{dbt_utils.generate_surrogate_key([ 'images_url', 'source_nm' ])}} as hsh_hub_images,
        images_url,
        load_ts
from src_xkcd
where row_nr = 1 