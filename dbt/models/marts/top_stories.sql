/*
    Mart: top_stories
    ==================
    Top 100 Hacker News stories by score for each ingestion day.
    Uses ROW_NUMBER to rank within each day and filter to the top 100.
*/

with ranked as (
    select
        *,
        row_number() over (
            partition by ingested_date
            order by score desc
        ) as rank
    from {{ ref('stg_stories') }}
)

select
    id,
    title,
    score,
    author,
    num_comments,
    published_at,
    story_date,
    url,
    ingested_date,
    rank
from ranked
where rank <= 100
