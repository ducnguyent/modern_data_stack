"""
Streamlit Dashboard Application
===============================
Provides an interactive overview of the Hacker News top stories
and author statistics using data from the local DuckDB warehouse.
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
    page_title="Hacker News Pipeline Dashboard",
    page_icon="📰",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Use relative path from this script assuming execution from repo root 
# via `streamlit run src/dashboard/app.py`
PROJECT_ROOT = Path(__file__).resolve().parents[2]
DB_PATH = PROJECT_ROOT / "data" / "warehouse" / "hn.duckdb"

# ---------------------------------------------------------------------------
# Data Loading (Cached)
# ---------------------------------------------------------------------------
@st.cache_data(ttl=3600)
def load_data() -> tuple[pd.DataFrame | None, pd.DataFrame | None]:
    """Fetch Top Stories and Author Stats from DuckDB securely in read-only mode."""
    if not DB_PATH.exists():
        return None, None
    
    try:
        # read_only=True prevents locking conflicts with the running Prefect pipeline
        with duckdb.connect(str(DB_PATH), read_only=True) as conn:
            # Query top stories
            df_stories = conn.execute("""
                SELECT 
                    id, title, score, author, num_comments, published_at, url, story_date 
                FROM main.top_stories 
                ORDER BY score DESC;
            """).df()
            
            # Query author stats
            df_authors = conn.execute("""
                SELECT 
                    author, post_count, avg_score, total_comments, first_seen, last_seen 
                FROM main.author_stats 
                ORDER BY post_count DESC;
            """).df()
            
            return df_stories, df_authors
    except Exception as e:
        st.error(f"Error connecting to the database: {e}")
        return None, None

# ---------------------------------------------------------------------------
# App Main
# ---------------------------------------------------------------------------
def main():
    st.title("📰 Hacker News Pipeline Dashboard")
    
    # 1. Check if DB/Data exists
    df_stories, df_authors = load_data()
    
    if df_stories is None or df_authors is None or df_stories.empty:
        st.warning(
            "⚠️ Database not found or dbt models have not been run. "
            "Please run the ETL pipeline first:\n\n"
            "```bash\n"
            "python -m src.orchestration.pipeline\n"
            "```"
        )
        st.stop()

    # 2. Sidebar Filters
    st.sidebar.header("Filter Data")
    
    # Min Score Filter
    min_score = int(df_stories["score"].min())
    max_score = int(df_stories["score"].max())
    selected_min_score = st.sidebar.slider(
        "Minimum Story Score",
        min_value=min_score,
        max_value=max_score,
        value=min_score
    )
    
    # Author Filter
    unique_authors = sorted(df_stories["author"].dropna().unique().tolist())
    selected_authors = st.sidebar.multiselect(
        "Filter by Author",
        options=unique_authors,
        default=[]
    )
    
    # Apply Filters to stories
    mask = df_stories["score"] >= selected_min_score
    if selected_authors:
        mask &= df_stories["author"].isin(selected_authors)
        
    filtered_stories = df_stories[mask]

    # 3. KPI Metrics
    st.subheader("Overview")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric(label="Total Stories (Filtered)", value=f"{len(filtered_stories):,}")
        
    with col2:
        if not filtered_stories.empty:
            highest_score_idx = filtered_stories["score"].idxmax()
            top_story = filtered_stories.loc[highest_score_idx]
            st.metric(label="Highest Score", value=f"{top_story['score']:,}", delta=top_story["author"])
        else:
            st.metric(label="Highest Score", value="N/A")
            
    with col3:
        if not filtered_stories.empty:
            most_active = filtered_stories["author"].value_counts().idxmax()
            active_count = filtered_stories["author"].value_counts().max()
            st.metric(label="Most Active Author (Filtered)", value=most_active, delta=f"{active_count} posts")
        else:
            st.metric(label="Most Active Author (Filtered)", value="N/A")

    st.divider()

    # 4. Tabs
    tab1, tab2, tab3 = st.tabs(["📊 Overview", "👤 Author Analytics", "🗄️ Raw Data"])

    # --- TAB 1: Overview ---
    with tab1:
        st.markdown("### Story Scores over Time")
        if not filtered_stories.empty:
            fig_timeline = px.scatter(
                filtered_stories,
                x="published_at",
                y="score",
                color="score",
                size="num_comments",
                hover_data=["title", "author"],
                title="Score Distribution (size = comments)",
                color_continuous_scale="Viridis"
            )
            st.plotly_chart(fig_timeline, use_container_width=True)
        else:
            st.info("No data matches the current filters.")

    # --- TAB 2: Author Analytics ---
    with tab2:
        st.markdown("### Top 10 Authors by Total Score (Selected Stories)")
        if not filtered_stories.empty:
            # Aggregate based on filtered stories
            author_agg = filtered_stories.groupby("author")["score"].sum().reset_index()
            top_10_authors = author_agg.sort_values(by="score", ascending=False).head(10)
            
            fig_authors = px.bar(
                top_10_authors,
                x="score",
                y="author",
                orientation="h",
                title="Top 10 Authors by Aggregate Score",
                labels={"score": "Total Score", "author": "Author"}
            )
            fig_authors.update_layout(yaxis={'categoryorder':'total ascending'})
            st.plotly_chart(fig_authors, use_container_width=True)
        else:
            st.info("No data matches the current filters.")
            
        st.markdown("### All-time Author Stats (From Data Warehouse)")
        # Show the raw author_stats mart (unaffected by sidebar filters for broader context)
        st.dataframe(
            df_authors,
            use_container_width=True,
            column_config={
                "avg_score": st.column_config.NumberColumn(format="%.2f"),
                "first_seen": st.column_config.DateColumn(format="YYYY-MM-DD"),
                "last_seen": st.column_config.DateColumn(format="YYYY-MM-DD"),
            }
        )

    # --- TAB 3: Raw Data ---
    with tab3:
        st.markdown("### Filtered Stories")
        st.dataframe(
            filtered_stories,
            use_container_width=True,
            column_config={
                "url": st.column_config.LinkColumn("Story URL"),
                "published_at": st.column_config.DatetimeColumn(format="YYYY-MM-DD HH:mm:ss")
            },
            hide_index=True
        )

if __name__ == "__main__":
    main()
