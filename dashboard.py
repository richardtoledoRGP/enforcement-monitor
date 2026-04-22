"""
Enforcement Action Monitor — Dashboard

New enforcement actions from the past 5 days are highlighted on this landing page.
Use the "Search All Actions" page to browse and filter the full database.

Usage:
    streamlit run dashboard.py
"""

import os
from datetime import datetime
from pathlib import Path

import streamlit as st
import pandas as pd

from diff import DiffEngine

# --- Config ---

DB_PATH = os.environ.get("DB_PATH", "seen_actions.db")
NEW_ACTION_DAYS = 5

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
    initial_sidebar_state="collapsed",
)

os.chdir(Path(__file__).parent)

# Hide sidebar and page navigation
st.markdown("""
<style>
    [data-testid="stSidebar"] { display: none; }
    [data-testid="stSidebarCollapsedControl"] { display: none; }
</style>
""", unsafe_allow_html=True)


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

# --- Header ---

st.title("Enforcement Action Monitor")
st.page_link("pages/Search_All_Actions.py", label="Search All Actions", icon=":material/search:")

last_updated = db.last_updated()
if last_updated:
    try:
        lu_dt = datetime.fromisoformat(last_updated)
        st.caption(f"Last updated: {lu_dt.strftime('%B %d, %Y at %I:%M %p')} UTC  |  Total actions in DB: {db.count():,}")
    except ValueError:
        st.caption(f"Last updated: {last_updated[:19]}  |  Total actions in DB: {db.count():,}")


# --- New Actions (past 5 days by actual issuance date) ---

new_rows = db.get_recent_actions(days=NEW_ACTION_DAYS, limit=500)
new_df = pd.DataFrame(new_rows) if new_rows else pd.DataFrame(columns=["source", "title", "url", "date", "first_seen"])

if not new_df.empty:
    new_df["category"] = new_df["source"].apply(get_category)

    st.header(f"New Actions (Past {NEW_ACTION_DAYS} Days)")

    new_cat_counts = new_df["category"].value_counts()
    n_cols = 2 + min(len(new_cat_counts), 3)
    cols = st.columns(n_cols)
    cols[0].metric("New Actions", f"{len(new_df):,}")
    cols[1].metric("Sources", new_df["source"].nunique())
    for i, (cat, count) in enumerate(new_cat_counts.head(3).items()):
        cols[2 + i].metric(cat, f"{count:,}")

    # New actions table
    new_df["load_date"] = new_df["first_seen"].str[:10]
    # Ensure summary/ai_overview columns exist (may be missing in older DB rows)
    if "summary" not in new_df.columns:
        new_df["summary"] = ""
    if "ai_overview" not in new_df.columns:
        new_df["ai_overview"] = ""
    new_df["summary"] = new_df["summary"].fillna("")
    new_df["ai_overview"] = new_df["ai_overview"].fillna("")

    new_display = new_df[["load_date", "source", "title", "summary", "ai_overview", "url"]].copy()
    new_display.columns = ["Load Date", "Source", "Title", "Summary", "AI Overview", "Link"]

    st.dataframe(
        new_display,
        column_config={
            "Link": st.column_config.LinkColumn("Link", display_text="View"),
            "Title": st.column_config.TextColumn("Title", width="medium"),
            "Source": st.column_config.TextColumn("Source", width="small"),
            "Summary": st.column_config.TextColumn("Summary", width="medium"),
            "AI Overview": st.column_config.TextColumn("AI Overview", width="medium"),
        },
        hide_index=True,
        width="stretch",
        height=min(600, 50 + len(new_df) * 35),
    )

    # Breakdown by source
    with st.expander("Breakdown by source"):
        source_chart = new_df["source"].value_counts().reset_index()
        source_chart.columns = ["Source", "Count"]
        st.bar_chart(source_chart, x="Source", y="Count")

else:
    st.info(f"No new enforcement actions in the past {NEW_ACTION_DAYS} days.")
