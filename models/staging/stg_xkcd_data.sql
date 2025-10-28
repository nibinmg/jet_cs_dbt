with src_xkcd as (
    select num,
        day,
        month,
        year,
        title,
        safe_title,
        transcript,
        alt,
        link,
        news,
        img,
        load_ts
    from {{ source('jet_cs_source', 'xkcd_data') }}
)
select * from src_xkcd