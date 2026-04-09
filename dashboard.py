"""
Enforcement Action Monitor — Dashboard

Browse and search historical enforcement actions stored in the SQLite database.

Usage:
    streamlit run dashboard.py
"""

import os
from datetime import datetime, timedelta
from pathlib import Path

import streamlit as st
import pandas as pd

from diff import DiffEngine

# --- Config ---

DB_PATH = os.environ.get("DB_PATH", "seen_actions.db")

# Map source names to categories for filtering
SOURCE_CATEGORIES = {
    "OCC": "Federal Banking",
    "OCC Enforcement Search": "Federal Banking",
    "FDIC": "Federal Banking",
    "FDIC Orders": "Federal Banking",
    "Federal Reserve": "Federal Banking",
    "NCUA": "Federal Banking",
    "CFPB": "Federal Banking",
    "CFPB Actions": "Federal Banking",
    "SEC Litigation": "Federal Other",
    "FinCEN": "Federal Other",
    "OFAC": "Federal Other",
}

# Anything not explicitly mapped is categorized by prefix
def get_category(source: str) -> str:
    if source in SOURCE_CATEGORIES:
        return SOURCE_CATEGORIES[source]
    # State sources — infer from common patterns
    insurance_keywords = ["TDI", "OIR", "DOI", "CDI", "OCI", "OIC", "Insurance"]
    if any(kw in source for kw in insurance_keywords):
        return "State Insurance"
    return "State Banking"


# --- Page setup ---

st.set_page_config(
    page_title="Enforcement Action Monitor",
    page_icon="*",
    layout="wide",
)

os.chdir(Path(__file__).parent)


@st.cache_resource
def get_db():
    return DiffEngine(DB_PATH, check_same_thread=False)


db = get_db()


# --- Sidebar ---

st.sidebar.title("Filters")

search_text = st.sidebar.text_input("Search", placeholder="Institution name, keyword, or source...")

# Source filter
all_sources = db.get_sources()
if all_sources:
    selected_sources = st.sidebar.multiselect("Source", options=all_sources)
else:
    selected_sources = []

# Category filter
all_categories = sorted(set(get_category(s) for s in all_sources))
selected_categories = st.sidebar.multiselect("Category", options=all_categories)

# Date filter
col1, col2 = st.sidebar.columns(2)
default_from = datetime.now() - timedelta(days=365)
date_from = col1.date_input("From", value=default_from)
date_to = col2.date_input("To", value=datetime.now())

# If category filter is active, map it back to sources
if selected_categories and not selected_sources:
    selected_sources = [s for s in all_sources if get_category(s) in selected_categories]
elif selected_categories and selected_sources:
    selected_sources = [s for s in selected_sources if get_category(s) in selected_categories]


# --- Query ---

rows = db.search(
    text=search_text,
    sources=selected_sources or None,
    date_from=str(date_from) if date_from else "",
    date_to=str(date_to) if date_to else "",
    limit=2000,
)

df = pd.DataFrame(rows) if rows else pd.DataFrame(columns=["source", "title", "url", "date", "first_seen"])


# --- Header ---

st.title("Enforcement Action Monitor")

total_count = db.count()
filtered_count = len(df)

col_a, col_b, col_c, col_d = st.columns(4)
col_a.metric("Total Actions in DB", f"{total_count:,}")
col_b.metric("Matching Filters", f"{filtered_count:,}")

if not df.empty:
    sources_count = df["source"].nunique()
    col_c.metric("Sources", sources_count)

    # Count by category
    df["category"] = df["source"].apply(get_category)
    category_counts = df["category"].value_counts()
    top_cat = category_counts.index[0] if len(category_counts) > 0 else "N/A"
    col_d.metric("Top Category", top_cat, f"{category_counts.iloc[0]:,}" if len(category_counts) > 0 else "")


# --- Results table ---

if not df.empty:
    # Format for display
    display_df = df[["first_seen", "source", "title", "url", "date"]].copy()
    display_df.columns = ["First Seen", "Source", "Title", "Link", "Action Date"]

    # Truncate first_seen to date only
    display_df["First Seen"] = display_df["First Seen"].str[:10]

    st.dataframe(
        display_df,
        column_config={
            "Link": st.column_config.LinkColumn("Link", display_text="View"),
            "Title": st.column_config.TextColumn("Title", width="large"),
            "Source": st.column_config.TextColumn("Source", width="small"),
        },
        hide_index=True,
        width="stretch",
        height=600,
    )

    # --- Category breakdown ---
    st.subheader("By Category")
    cat_chart = df["category"].value_counts().reset_index()
    cat_chart.columns = ["Category", "Count"]
    st.bar_chart(cat_chart, x="Category", y="Count")

    # --- Export ---
    csv = df[["source", "title", "url", "date", "first_seen"]].to_csv(index=False)
    st.sidebar.download_button(
        label="Download CSV",
        data=csv,
        file_name=f"enforcement_actions_{datetime.now().strftime('%Y%m%d')}.csv",
        mime="text/csv",
    )
else:
    st.info("No enforcement actions found matching your filters.")


# --- Footer ---
st.sidebar.markdown("---")
st.sidebar.caption(f"Database: {DB_PATH}")
st.sidebar.caption(f"Last updated: check `first_seen` dates in results")
