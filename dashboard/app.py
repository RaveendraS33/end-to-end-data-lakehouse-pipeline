"""Streamlit dashboard for the lakehouse, served from Trino.

Reads the curated Iceberg tables and renders headline metrics plus a few
breakdowns: status mix, rejection reasons, and daily clean volume. It queries
Trino exactly the way an analyst would, which keeps the visualisation honest --
nothing is precomputed.
"""
import os

import pandas as pd
import streamlit as st
import trino

TRINO_HOST = os.getenv("TRINO_HOST", "trino")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8080"))

st.set_page_config(page_title="Lakehouse Dashboard", page_icon="📊", layout="wide")


@st.cache_resource
def get_connection():
    return trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user="dashboard",
        catalog="iceberg",
        schema="quality",
    )


@st.cache_data(ttl=30)
def run_query(sql: str) -> pd.DataFrame:
    cursor = get_connection().cursor()
    cursor.execute(sql)
    columns = [column[0] for column in cursor.description]
    return pd.DataFrame(cursor.fetchall(), columns=columns)


st.title("📊 Lakehouse Transactions Dashboard")
st.caption(f"Live from Trino at {TRINO_HOST}:{TRINO_PORT} · cached for 30s")

if st.button("Refresh"):
    st.cache_data.clear()

try:
    clean_total = int(run_query("SELECT count(*) AS c FROM iceberg.quality.transactions_clean")["c"][0])
    bad_total = int(run_query("SELECT count(*) AS c FROM iceberg.quality.transactions_bad")["c"][0])
except Exception as error:  # noqa: BLE001 - surface connection/setup issues in the UI
    st.error(f"Could not query Trino. Is the stack running and are the tables created?\n\n{error}")
    st.stop()

processed_total = clean_total + bad_total
clean_rate = (clean_total / processed_total * 100) if processed_total else 0.0

col1, col2, col3 = st.columns(3)
col1.metric("Clean transactions", f"{clean_total:,}")
col2.metric("Rejected transactions", f"{bad_total:,}")
col3.metric("Clean rate", f"{clean_rate:.1f}%")

left, right = st.columns(2)

with left:
    st.subheader("Clean transactions by status")
    status_df = run_query(
        """
        SELECT status, count(*) AS record_count
        FROM iceberg.quality.transactions_clean
        GROUP BY status
        ORDER BY record_count DESC
        """
    )
    if status_df.empty:
        st.info("No clean records yet.")
    else:
        st.bar_chart(status_df.set_index("status"))

with right:
    st.subheader("Rejections by reason")
    reason_df = run_query(
        """
        SELECT error_reason, count(*) AS bad_record_count
        FROM iceberg.quality.transactions_bad
        GROUP BY error_reason
        ORDER BY bad_record_count DESC
        """
    )
    if reason_df.empty:
        st.info("No rejected records yet.")
    else:
        st.bar_chart(reason_df.set_index("error_reason"))

st.subheader("Daily clean volume (by event date)")
daily_df = run_query(
    """
    SELECT date(event_ts) AS event_day, count(*) AS record_count
    FROM iceberg.quality.transactions_clean
    WHERE event_ts IS NOT NULL
    GROUP BY date(event_ts)
    ORDER BY event_day
    """
)
if daily_df.empty:
    st.info("No dated clean records yet.")
else:
    st.line_chart(daily_df.set_index("event_day"))
