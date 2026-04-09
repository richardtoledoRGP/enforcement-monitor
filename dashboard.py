"""
Enforcement Action Monitor — Dashboard

Browse and search historical enforcement actions stored in the SQLite database.
New actions from the past 7 days are highlighted at the top.

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
NEW_ACTION_DAYS = 7

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


def get_category(source: str) -> str:
    if source in SOURCE_CATEGORIES:
        return SOURCE_CATEGORIES[source]
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


# --- Authentication ---

def check_password() -> bool:
    """Show a login form and return True if the password is correct."""
    if st.session_state.get("authenticated"):
        return True

    with st.container():
        st.subheader("Login")
        password = st.text_input("Password", type="password", key="password_input")
        if st.button("Sign in"):
            if password == st.secrets.get("password", ""):
                st.session_state["authenticated"] = True
                st.rerun()
            else:
                st.error("Incorrect password")
        return False


if not check_password():
    st.stop()


@st.cache_resource
def get_db():
    return DiffEngine(DB_PATH, check_same_thread=False)


db = get_db()
total_count = db.count()
all_sources = db.get_sources()


# --- New Actions (past 7 days by actual issuance date) ---

new_rows = db.get_recent_actions(days=NEW_ACTION_DAYS, limit=500)
new_df = pd.DataFrame(new_rows) if new_rows else pd.DataFrame(columns=["source", "title", "url", "date", "first_seen"])

st.title("Enforcement Action Monitor")

if not new_df.empty:
    new_df["category"] = new_df["source"].apply(get_category)

    st.header(f"New Actions (Past {NEW_ACTION_DAYS} Days)")

    col_a, col_b, col_c, col_d = st.columns(4)
    col_a.metric("New Actions", f"{len(new_df):,}")
    col_b.metric("Sources", new_df["source"].nunique())

    new_cat_counts = new_df["category"].value_counts()
    if len(new_cat_counts) > 0:
        col_c.metric("Top Category", new_cat_counts.index[0], f"{new_cat_counts.iloc[0]:,}")
    if len(new_cat_counts) > 1:
        col_d.metric("2nd Category", new_cat_counts.index[1], f"{new_cat_counts.iloc[1]:,}")

    # New actions table — use parsed_date if available
    date_col = "parsed_date" if "parsed_date" in new_df.columns else "date"
    new_display = new_df[[date_col, "source", "title", "url"]].copy()
    new_display.columns = ["Action Date", "Source", "Title", "Link"]

    st.dataframe(
        new_display,
        column_config={
            "Link": st.column_config.LinkColumn("Link", display_text="View"),
            "Title": st.column_config.TextColumn("Title", width="large"),
            "Source": st.column_config.TextColumn("Source", width="small"),
        },
        hide_index=True,
        width="stretch",
        height=min(400, 50 + len(new_df) * 35),
    )

    # Breakdown by source
    with st.expander("Breakdown by source"):
        source_chart = new_df["source"].value_counts().reset_index()
        source_chart.columns = ["Source", "Count"]
        st.bar_chart(source_chart, x="Source", y="Count")

    st.divider()
else:
    st.info(f"No new enforcement actions in the past {NEW_ACTION_DAYS} days.")
    st.divider()


# --- Sidebar filters ---

st.sidebar.title("Search All Actions")

search_text = st.sidebar.text_input("Search", placeholder="Institution name, keyword, or source...")

if all_sources:
    selected_sources = st.sidebar.multiselect("Source", options=all_sources)
else:
    selected_sources = []

all_categories = sorted(set(get_category(s) for s in all_sources))
selected_categories = st.sidebar.multiselect("Category", options=all_categories)

col1, col2 = st.sidebar.columns(2)
default_from = datetime.now() - timedelta(days=365)
date_from = col1.date_input("From", value=default_from)
date_to = col2.date_input("To", value=datetime.now())

if selected_categories and not selected_sources:
    selected_sources = [s for s in all_sources if get_category(s) in selected_categories]
elif selected_categories and selected_sources:
    selected_sources = [s for s in selected_sources if get_category(s) in selected_categories]


# --- Full search results ---

rows = db.search(
    text=search_text,
    sources=selected_sources or None,
    date_from=str(date_from) if date_from else "",
    date_to=str(date_to) if date_to else "",
    limit=2000,
)

df = pd.DataFrame(rows) if rows else pd.DataFrame(columns=["source", "title", "url", "date", "first_seen"])

st.header("All Actions")

col_e, col_f, col_g = st.columns(3)
col_e.metric("Total in DB", f"{total_count:,}")
col_f.metric("Matching Filters", f"{len(df):,}")
if not df.empty:
    col_g.metric("Sources", df["source"].nunique())

if not df.empty:
    df["category"] = df["source"].apply(get_category)

    display_df = df[["first_seen", "source", "title", "url", "date"]].copy()
    display_df.columns = ["First Seen", "Source", "Title", "Link", "Action Date"]
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

    # Category breakdown
    with st.expander("By Category"):
        cat_chart = df["category"].value_counts().reset_index()
        cat_chart.columns = ["Category", "Count"]
        st.bar_chart(cat_chart, x="Category", y="Count")

    # Export
    csv = df[["source", "title", "url", "date", "first_seen"]].to_csv(index=False)
    st.sidebar.download_button(
        label="Download CSV",
        data=csv,
        file_name=f"enforcement_actions_{datetime.now().strftime('%Y%m%d')}.csv",
        mime="text/csv",
    )
else:
    st.info("No enforcement actions found matching your filters.")

st.sidebar.markdown("---")
st.sidebar.caption(f"Database: {DB_PATH}")
