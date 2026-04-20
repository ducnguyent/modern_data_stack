/*
    Staging: stg_stories
    =====================
    Clean and cast raw HN story fields from the ingestion layer.
    - Renames `by` → `author` (reserved keyword in some engines)
    - Converts epoch `time` → proper timestamp
    - Derives `story_date` for downstream partitioning
    - Coalesces nulls to sensible defaults
*/

with source as (
    select * from raw.raw_stories
),

cleaned as (
    select
        id,
        coalesce(title, 'Untitled')         as title,
        coalesce(score, 0)                   as score,
        coalesce("by", 'unknown')            as author,
        coalesce(descendants, 0)             as num_comments,
        cast(to_timestamp("time") as timestamp) as published_at,
        cast(to_timestamp("time") as date)     as story_date,
        url,
        ingested_date
    from source
)

select * from cleaned
