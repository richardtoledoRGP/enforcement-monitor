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

# Explicit source-to-group mappings.
# Sources can appear in multiple groups (e.g., combined banking + insurance agencies).
FEDERAL_SOURCES = [
    "OCC", "OCC Enforcement Search", "FDIC", "FDIC Orders",
    "Federal Reserve", "NCUA", "CFPB", "CFPB Actions",
    "SEC Litigation", "FinCEN", "OFAC",
]

STATE_BANKING_SOURCES = [
    "CA DFPI", "NY DFS", "NY DFS Press", "TX DOB", "IL IDFPR",
    "OH Financial Institutions", "NJ DOBI", "GA Banking",
    "NC Commissioner of Banks", "MA Division of Banks", "WA DFI",
    "MD Financial Regulation", "CT Banking", "FL OFR", "AZ DIFI",
    "OR DFR", "MI DIFS", "PA Banking", "CO Banking", "MN Commerce",
    # Combined banking + insurance agencies (also in STATE_INSURANCE_SOURCES)
    "NJ DOBI", "AZ DIFI", "MI DIFS",
]

STATE_INSURANCE_SOURCES = [
    "TX TDI", "FL OIR", "NC DOI", "MA DOI", "IL DOI", "WA OIC",
    "GA OCI", "PA Insurance", "OH DOI", "CO DOI", "CA CDI",
    # Combined banking + insurance agencies (also in STATE_BANKING_SOURCES)
    "NJ DOBI", "AZ DIFI", "MI DIFS",
    # NY DFS covers insurance too
    "NY DFS", "NY DFS Press",
]


def get_category(source: str) -> str:
    if source in FEDERAL_SOURCES:
        return "Federal"
    # Check insurance first since some appear in both
    if source in STATE_INSURANCE_SOURCES and source not in STATE_BANKING_SOURCES:
        return "State Insurance"
    if source in STATE_BANKING_SOURCES and source not in STATE_INSURANCE_SOURCES:
        return "State Banking"
    if source in STATE_BANKING_SOURCES and source in STATE_INSURANCE_SOURCES:
        return "State Banking & Insurance"
    # Fallback
    return "State Banking"


# --- Page setup ---

st.set_page_config(
    page_title="Search All Actions",
    page_icon="*",
    layout="wide",
    initial_sidebar_state="collapsed",
)

os.chdir(Path(__file__).parent.parent)

# Hide sidebar and page navigation
st.markdown("""
<style>
    [data-testid="stSidebar"] { display: none; }
    [data-testid="stSidebarCollapsedControl"] { display: none; }
</style>
""", unsafe_allow_html=True)

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

# Build per-group source lists (only sources that exist in the DB)
federal_in_db = sorted(s for s in all_sources if s in FEDERAL_SOURCES)
state_banking_in_db = sorted(s for s in all_sources if s in STATE_BANKING_SOURCES)
state_insurance_in_db = sorted(s for s in all_sources if s in STATE_INSURANCE_SOURCES)


# --- Header ---

st.title("Search All Actions")
st.page_link("dashboard.py", label="Back to Dashboard", icon=":material/arrow_back:")


# --- Filters ---

search_text = st.text_input("Search", placeholder="Institution name, keyword, or source...")

filter_col1, filter_col2, filter_col3, filter_col4 = st.columns([1, 1, 1, 1])

selected_federal = filter_col1.multiselect("Federal Sources", options=federal_in_db)
selected_state_banking = filter_col2.multiselect("State Banking Sources", options=state_banking_in_db)
selected_state_insurance = filter_col3.multiselect("State Insurance Sources", options=state_insurance_in_db)

date_cols = filter_col4.columns(2)
default_from = datetime.now() - timedelta(days=365)
date_from = date_cols[0].date_input("From", value=default_from)
date_to = date_cols[1].date_input("To", value=datetime.now())

# Combine all selected sources (deduplicated)
selected_sources = list(set(selected_federal + selected_state_banking + selected_state_insurance))


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
