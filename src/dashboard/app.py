"""
Streamlit Dashboard Application
===============================
Provides an interactive overview of the Hacker News and DEV.to data
using data from the local DuckDB warehouse.
"""

import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
from pathlib import Path

# ---------------------------------------------------------------------------
# Configuration & Setup
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="HN & DEV.to Pipeline Dashboard",
    page_icon="📰",
    layout="wide",
    initial_sidebar_state="expanded"
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DB_PATH = PROJECT_ROOT / "data" / "warehouse" / "hn.duckdb"

# ---------------------------------------------------------------------------
# Data Loading (Cached)
# ---------------------------------------------------------------------------
@st.cache_data(ttl=3600)
def load_data() -> tuple[pd.DataFrame | None, pd.DataFrame | None, pd.DataFrame | None, pd.DataFrame | None]:
    if not DB_PATH.exists():
        return None, None, None, None
    
    try:
        with duckdb.connect(str(DB_PATH), read_only=True) as conn:
            # HN
            df_stories_hn = conn.execute("""
                SELECT 
                    id, title, score, author, num_comments, published_at, url, story_date 
                FROM main.top_stories 
                ORDER BY score DESC;
            """).df()
            
            df_authors_hn = conn.execute("""
                SELECT 
                    author, post_count, avg_score, total_comments, first_seen, last_seen 
                FROM main.author_stats 
                ORDER BY post_count DESC;
            """).df()

            # DEV.to
            df_stories_devto = conn.execute("""
                SELECT 
                    id, title, positive_reactions_count as score, author, comments_count as num_comments, published_at, url, story_date, reading_time_minutes, tag_list
                FROM main.stg_devto
                ORDER BY positive_reactions_count DESC;
            """).df()

            df_authors_devto = conn.execute("""
                SELECT 
                    author, post_count, total_reactions, avg_reading_time, most_used_tags
                FROM main.devto_author_stats 
                ORDER BY post_count DESC;
            """).df()
            
            return df_stories_hn, df_authors_hn, df_stories_devto, df_authors_devto
    except Exception as e:
        st.error(f"Error connecting to the database: {e}")
        return None, None, None, None

# ---------------------------------------------------------------------------
# App Main
# ---------------------------------------------------------------------------
def main():
    st.title("📰 Data Pipeline Dashboard")
    
    df_stories_hn, df_authors_hn, df_stories_devto, df_authors_devto = load_data()
    
    if df_stories_hn is None or df_stories_hn.empty:
        st.warning(
            "⚠️ Database not found or dbt models have not been run. "
            "Please run the ETL pipeline first:\n\n"
            "```bash\n"
            "python -m src.orchestration.pipeline\n"
            "```"
        )
        st.stop()

    st.sidebar.header("Filter Data")
    
    source = st.sidebar.radio("Data Source", ["Hacker News", "DEV.to"])
    
    if source == "Hacker News":
        df_stories = df_stories_hn
        df_authors = df_authors_hn
        label_score = "Score"
        label_item = "Stories"
    else:
        df_stories = df_stories_devto
        df_authors = df_authors_devto
        label_score = "Reactions"
        label_item = "Articles"

    if df_stories is None or df_stories.empty:
         st.warning(f"No data available for {source}.")
         st.stop()

    min_score = int(df_stories["score"].min())
    max_score = int(df_stories["score"].max())
    
    # Safe fallback if min and max are the same
    if min_score == max_score:
        max_score = min_score + 1
        
    selected_min_score = st.sidebar.slider(
        f"Minimum {label_score}",
        min_value=min_score,
        max_value=max_score,
        value=min_score
    )
    
    unique_authors = sorted(df_stories["author"].dropna().unique().tolist())
    selected_authors = st.sidebar.multiselect(
        "Filter by Author",
        options=unique_authors,
        default=[]
    )
    
    mask = df_stories["score"] >= selected_min_score
    if selected_authors:
        mask &= df_stories["author"].isin(selected_authors)
        
    filtered_stories = df_stories[mask]

    st.subheader(f"Overview: {source}")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric(label=f"Total {label_item} (Filtered)", value=f"{len(filtered_stories):,}")
        
    with col2:
        if not filtered_stories.empty:
            highest_score_idx = filtered_stories["score"].idxmax()
            top_story = filtered_stories.loc[highest_score_idx]
            st.metric(label=f"Highest {label_score}", value=f"{top_story['score']:,}", delta=top_story["author"])
        else:
            st.metric(label=f"Highest {label_score}", value="N/A")
            
    with col3:
        if not filtered_stories.empty:
            most_active = filtered_stories["author"].value_counts().idxmax()
            active_count = filtered_stories["author"].value_counts().max()
            st.metric(label="Most Active Author (Filtered)", value=most_active, delta=f"{active_count} posts")
        else:
            st.metric(label="Most Active Author (Filtered)", value="N/A")

    st.divider()

    tab1, tab2, tab3 = st.tabs(["📊 Overview", "👤 Author Analytics", "🗄️ Raw Data"])

    with tab1:
        st.markdown(f"### {label_item} Scatter over Time")
        if not filtered_stories.empty:
            fig_timeline = px.scatter(
                filtered_stories,
                x="published_at",
                y="score",
                color="score",
                size="num_comments",
                hover_data=["title", "author"],
                title=f"{label_score} Distribution (size = comments)",
                color_continuous_scale="Viridis"
            )
            st.plotly_chart(fig_timeline, width="stretch")
        else:
            st.info("No data matches the current filters.")

    with tab2:
        st.markdown(f"### Top 10 Authors by Total {label_score} (Filtered)")
        if not filtered_stories.empty:
            author_agg = filtered_stories.groupby("author")["score"].sum().reset_index()
            top_10_authors = author_agg.sort_values(by="score", ascending=False).head(10)
            
            fig_authors = px.bar(
                top_10_authors,
                x="score",
                y="author",
                orientation="h",
                title=f"Top 10 Authors by Aggregate {label_score}",
                labels={"score": f"Total {label_score}", "author": "Author"}
            )
            fig_authors.update_layout(yaxis={'categoryorder':'total ascending'})
            st.plotly_chart(fig_authors, width="stretch")
        else:
            st.info("No data matches the current filters.")
            
        st.markdown("### All-time Author Stats (From Data Warehouse)")
        
        column_config = {}
        if source == "Hacker News":
             column_config={
                 "avg_score": st.column_config.NumberColumn(format="%.2f"),
                 "first_seen": st.column_config.DateColumn(format="YYYY-MM-DD"),
                 "last_seen": st.column_config.DateColumn(format="YYYY-MM-DD"),
             }
        else:
             column_config={
                 "avg_reading_time": st.column_config.NumberColumn(format="%.2f"),
             }
             
        st.dataframe(
            df_authors,
            width="stretch",
            column_config=column_config
        )

    with tab3:
        st.markdown(f"### Filtered {label_item}")
        st.dataframe(
            filtered_stories,
            width="stretch",
            column_config={
                "url": st.column_config.LinkColumn("URL"),
                "published_at": st.column_config.DatetimeColumn(format="YYYY-MM-DD HH:mm:ss")
            },
            hide_index=True
        )

if __name__ == "__main__":
    main()
