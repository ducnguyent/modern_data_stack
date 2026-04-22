/*
    Staging: stg_devto
    =====================
    Clean and cast raw DEV.to article fields from the ingestion layer.
    - Converts published_at to proper timestamp
    - Casts reactions and comments to int
*/

with source as (
    select * from raw.devto_articles
),

cleaned as (
    select
        id,
        coalesce(title, 'Untitled')         as title,
        coalesce(positive_reactions_count, 0) as positive_reactions_count,
        coalesce(public_reactions_count, 0)   as public_reactions_count,
        coalesce(comments_count, 0)           as comments_count,
        coalesce(author, 'unknown')           as author,
        cast(published_at as timestamp)       as published_at,
        cast(published_at as date)            as story_date,
        tag_list,
        url,
        coalesce(reading_time_minutes, 0)     as reading_time_minutes,
        ingested_date
    from source
)

select * from cleaned
