with src_xkcd as (
    select 
        num as comic_nr,
        title as comic_title_txt,
        safe_title as comic_safe_title_txt,
        transcript as comic_transcript_txt,
        alt as comic_alt_txt,
        load_ts,
        'xkcd' as source_nm,
        row_number() over(partition by num order by load_ts desc) as row_nr
    from {{ ref('stg_xkcd_data') }}
    where num is not null
)
select {{dbt_utils.generate_surrogate_key([ 'comic_nr', 'source_nm' ])}} as hsh_hub_comic,
    comic_title_txt,
    comic_safe_title_txt,
    comic_transcript_txt,
    comic_alt_txt,
    load_ts
from src_xkcd
where row_nr = 1