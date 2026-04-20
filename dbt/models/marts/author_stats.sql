/*
    Mart: author_stats
    ===================
    Per-author aggregations across all ingested stories:
    - Total posts
    - Average score
    - Total comments received
    - First and last seen dates
*/

select
    author,
    count(*)                         as post_count,
    round(avg(score), 2)             as avg_score,
    sum(num_comments)                as total_comments,
    min(story_date)                  as first_seen,
    max(story_date)                  as last_seen
from {{ ref('stg_stories') }}
group by author
order by post_count desc
