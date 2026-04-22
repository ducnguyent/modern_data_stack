/*
    Mart: devto_author_stats
    ========================
    Per-author aggregations across all ingested DEV.to articles:
    - Total posts
    - Total reactions
    - Average reading time
    - Most used tags
*/

with author_base as (
    select
        author,
        count(*) as post_count,
        sum(positive_reactions_count + public_reactions_count) as total_reactions,
        round(avg(reading_time_minutes), 2) as avg_reading_time
    from {{ ref('stg_devto') }}
    group by author
),

tags_unnested as (
    select
        author,
        trim(unnest(string_split(tag_list, ','))) as tag
    from {{ ref('stg_devto') }}
    where tag_list is not null and tag_list != ''
),

author_tags as (
    select
        author,
        tag,
        count(*) as tag_count
    from tags_unnested
    group by author, tag
),

ranked_tags as (
    select
        author,
        tag,
        tag_count,
        row_number() over (partition by author order by tag_count desc) as rn
    from author_tags
),

top_tags as (
    select
        author,
        string_agg(tag, ', ' order by tag_count desc) as most_used_tags
    from ranked_tags
    where rn <= 3
    group by author
)

select
    b.author,
    b.post_count,
    b.total_reactions,
    b.avg_reading_time,
    t.most_used_tags
from author_base b
left join top_tags t on b.author = t.author
order by b.post_count desc
