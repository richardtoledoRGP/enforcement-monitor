"""
Search All Actions — browse and filter the full enforcement actions database.
"""

import os
from datetime import datetime, timedelta
from pathlib import Path

import streamlit as st
import pandas as pd

from diff import DiffEngine

# --- Config ---

DB_PATH = os.environ.get("DB_PATH", "seen_actions.db")

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


def get_category(source: str) -> str:
    if source in SOURCE_CATEGORIES:
        return SOURCE_CATEGORIES[source]
    insurance_keywords = ["TDI", "OIR", "DOI", "CDI", "OCI", "OIC", "Insurance"]
    if any(kw in source for kw in insurance_keywords):
        return "State Insurance"
    return "State Banking"


# --- Page setup ---

st.set_page_config(
    page_title="Search All Actions",
    page_icon="*",
    layout="wide",
    initial_sidebar_state="collapsed",
)

os.chdir(Path(__file__).parent.parent)

# --- Auth gate (shared session state with main page) ---

if not st.session_state.get("authenticated"):
    st.warning("Please log in on the main page first.")
    st.stop()


@st.cache_resource
def get_db():
    return DiffEngine(DB_PATH, check_same_thread=False)


db = get_db()
all_sources = db.get_sources()
total_count = db.count()


# --- Header ---

col_title, col_nav = st.columns([4, 1])
col_title.title("Search All Actions")
col_nav.page_link("dashboard.py", label="Back to Dashboard", icon=":material/arrow_back:")


# --- Filters (inline, not sidebar) ---

filter_col1, filter_col2, filter_col3, filter_col4 = st.columns([3, 2, 2, 2])

search_text = filter_col1.text_input("Search", placeholder="Institution name, keyword, or source...")

if all_sources:
    selected_sources = filter_col2.multiselect("Source", options=all_sources)
else:
    selected_sources = []

all_categories = sorted(set(get_category(s) for s in all_sources))
selected_categories = filter_col3.multiselect("Category", options=all_categories)

date_cols = filter_col4.columns(2)
default_from = datetime.now() - timedelta(days=365)
date_from = date_cols[0].date_input("From", value=default_from)
date_to = date_cols[1].date_input("To", value=datetime.now())

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


# --- Metrics ---

col_e, col_f, col_g = st.columns(3)
col_e.metric("Total in DB", f"{total_count:,}")
col_f.metric("Matching Filters", f"{len(df):,}")
if not df.empty:
    col_g.metric("Sources", df["source"].nunique())


# --- Results table ---

if not df.empty:
    df["category"] = df["source"].apply(get_category)

    display_df = df[["first_seen", "source", "title", "url", "date"]].copy()
    display_df.columns = ["Load Date", "Source", "Title", "Link", "Action Date"]
    display_df["Load Date"] = display_df["Load Date"].str[:10]

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

    # Category breakdown
    with st.expander("By Category"):
        cat_chart = df["category"].value_counts().reset_index()
        cat_chart.columns = ["Category", "Count"]
        st.bar_chart(cat_chart, x="Category", y="Count")

    # Export
    csv = df[["source", "title", "url", "date", "first_seen"]].to_csv(index=False)
    st.download_button(
        label="Download CSV",
        data=csv,
        file_name=f"enforcement_actions_{datetime.now().strftime('%Y%m%d')}.csv",
        mime="text/csv",
    )
else:
    st.info("No enforcement actions found matching your filters.")
