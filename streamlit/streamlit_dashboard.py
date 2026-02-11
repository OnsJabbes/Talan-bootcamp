import os
import time
import pandas as pd
import streamlit as st
import plotly.express as px
from sqlalchemy import create_engine, text

# ----------------------------
# Config
# ----------------------------
PG_HOST = os.getenv("PG_HOST", "postgres-mart")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DB   = os.getenv("PG_DB", "martdb")
PG_USER = os.getenv("PG_USER", "mart")
PG_PASSWORD = os.getenv("PG_PASSWORD", "mart")

engine = create_engine(
    f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}",
    pool_pre_ping=True
)

st.set_page_config(page_title="Amazon OLTP Realtime", page_icon="AMZ", layout="wide")
st.title("Amazon Sales - OLTP Realtime Dashboard (Aggregated)")
st.caption("Source: `orders_live` (Kafka -> Spark Streaming Aggregation -> Postgres)")

# ----------------------------
# Sidebar controls
# ----------------------------
st.sidebar.header("Controls")
refresh = st.sidebar.slider("Auto-refresh (seconds)", 2, 60, 5)

st.sidebar.markdown("---")
st.sidebar.subheader("Filters")


@st.cache_data(ttl=5)
def load_filter_values():
    q = text("""
        SELECT
          (SELECT array_agg(DISTINCT category) FROM orders_live) AS categories,
          (SELECT array_agg(DISTINCT country) FROM orders_live) AS countries,
          (SELECT array_agg(DISTINCT paymentmethod) FROM orders_live) AS paymentmethods,
          (SELECT min(day) FROM orders_live) AS min_date,
          (SELECT max(day) FROM orders_live) AS max_date
    """)
    df = pd.read_sql(q, engine)
    if df.empty:
        return {}, None, None
    row = df.iloc[0].to_dict()

    def safe_list(x):
        return sorted([v for v in (x or []) if v is not None])

    values = {
        "category": safe_list(row.get("categories")),
        "country": safe_list(row.get("countries")),
        "paymentmethod": safe_list(row.get("paymentmethods")),
    }
    return values, row.get("min_date"), row.get("max_date")


values, min_date, max_date = load_filter_values()

if min_date is None or max_date is None:
    st.warning("No data yet in Postgres. Wait for Spark Streaming to fill `orders_live`.")
    st.info("Tip: check `select count(*) from orders_live;` inside postgres-mart.")
    time.sleep(refresh)
    st.rerun()

date_range = st.sidebar.date_input(
    "Date range",
    value=(pd.to_datetime(min_date).date(), pd.to_datetime(max_date).date()),
    min_value=pd.to_datetime(min_date).date(),
    max_value=pd.to_datetime(max_date).date(),
)

category_sel = st.sidebar.multiselect("Category", values.get("category", []), default=[])
country_sel = st.sidebar.multiselect("Country", values.get("country", []), default=[])
pay_sel = st.sidebar.multiselect("Payment method", values.get("paymentmethod", []), default=[])

st.sidebar.markdown("---")
st.sidebar.markdown("### Pipelines")
st.sidebar.markdown("- **OLTP**: CSV -> Kafka -> Spark Streaming (agg) -> Postgres -> Streamlit")
st.sidebar.markdown("- **OLAP**: CSV -> HDFS (Bronze/Silver/Gold) -> Trino")


# ----------------------------
# SQL helpers
# ----------------------------
def build_where():
    clauses = []
    params = {}

    if isinstance(date_range, tuple) and len(date_range) == 2:
        clauses.append("day BETWEEN :d1 AND :d2")
        params["d1"] = pd.to_datetime(date_range[0])
        params["d2"] = pd.to_datetime(date_range[1])

    def in_list(col, sel, key):
        if sel:
            placeholders = []
            for i, v in enumerate(sel):
                k = f"{key}{i}"
                placeholders.append(f":{k}")
                params[k] = v
            clauses.append(f"{col} IN ({', '.join(placeholders)})")

    in_list("category", category_sel, "cat")
    in_list("country", country_sel, "cty")
    in_list("paymentmethod", pay_sel, "pay")

    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    return where, params


@st.cache_data(ttl=5)
def load_kpis(where_sql, params):
    q = text(f"""
        SELECT
          COALESCE(SUM(totalorders), 0)::bigint       AS total_orders,
          COALESCE(SUM(totalrevenue), 0)::float8       AS total_revenue,
          COALESCE(SUM(totalnetrevenue), 0)::float8    AS net_revenue,
          COALESCE(SUM(totalquantity), 0)::bigint      AS total_quantity,
          CASE WHEN SUM(totalorders) > 0
               THEN SUM(totalrevenue) / SUM(totalorders)
               ELSE 0 END::float8                      AS avg_order_value,
          COALESCE(SUM(uniquecustomers), 0)::bigint    AS unique_customers
        FROM orders_live
        {where_sql}
    """)
    return pd.read_sql(q, engine, params=params)


@st.cache_data(ttl=5)
def load_daily_trend(where_sql, params):
    q = text(f"""
        SELECT
          day,
          SUM(totalorders)::bigint     AS orders,
          SUM(totalrevenue)::float8    AS revenue,
          SUM(totalnetrevenue)::float8 AS net_revenue
        FROM orders_live
        {where_sql}
        GROUP BY day
        ORDER BY day
    """)
    return pd.read_sql(q, engine, params=params)


@st.cache_data(ttl=5)
def load_by_dimension(where_sql, params, dim, metric="totalrevenue", topn=15):
    q = text(f"""
        SELECT {dim} AS k, SUM({metric})::float8 AS v
        FROM orders_live
        {where_sql}
        GROUP BY 1
        ORDER BY v DESC
        LIMIT {topn}
    """)
    return pd.read_sql(q, engine, params=params)


@st.cache_data(ttl=5)
def load_raw_agg(where_sql, params, limit=500):
    q = text(f"""
        SELECT *
        FROM orders_live
        {where_sql}
        ORDER BY processing_timestamp DESC NULLS LAST
        LIMIT {limit}
    """)
    return pd.read_sql(q, engine, params=params)


# ----------------------------
# Load data
# ----------------------------
where_sql, params = build_where()

kpi = load_kpis(where_sql, params)
kpi = kpi.iloc[0].to_dict() if not kpi.empty else {
    "total_orders": 0, "total_revenue": 0.0, "net_revenue": 0.0,
    "total_quantity": 0, "avg_order_value": 0.0, "unique_customers": 0,
}

daily = load_daily_trend(where_sql, params)

top_cat = load_by_dimension(where_sql, params, "category", "totalrevenue")
top_cty = load_by_dimension(where_sql, params, "country", "totalrevenue")
pay_counts = load_by_dimension(where_sql, params, "paymentmethod", "totalorders")

# ----------------------------
# KPI Row
# ----------------------------
st.markdown("---")
st.subheader("Key Performance Indicators")

c1, c2, c3, c4, c5, c6 = st.columns(6)

with c1:
    st.metric("Total Orders", f"{int(kpi['total_orders']):,}")
with c2:
    st.metric("Total Revenue", f"${kpi['total_revenue']:,.2f}")
with c3:
    st.metric("Net Revenue", f"${kpi['net_revenue']:,.2f}")
with c4:
    st.metric("Avg Order Value", f"${kpi['avg_order_value']:.2f}")
with c5:
    st.metric("Total Quantity", f"{int(kpi['total_quantity']):,}")
with c6:
    st.metric("Unique Customers", f"{int(kpi['unique_customers']):,}")

# ----------------------------
# Daily trend
# ----------------------------
st.markdown("---")
st.header("Daily Trend")

colA, colB = st.columns(2)

with colA:
    if daily.empty:
        st.info("No daily trend data yet.")
    else:
        fig = px.bar(
            daily, x="day", y="revenue",
            title="Revenue by Day",
            labels={"day": "Date", "revenue": "Revenue ($)"},
        )
        fig.update_layout(height=380)
        st.plotly_chart(fig, use_container_width=True)

with colB:
    if daily.empty:
        st.info("No daily trend data yet.")
    else:
        fig = px.bar(
            daily, x="day", y="orders",
            title="Orders by Day",
            labels={"day": "Date", "orders": "Orders"},
        )
        fig.update_layout(height=380)
        st.plotly_chart(fig, use_container_width=True)

# ----------------------------
# Business insights
# ----------------------------
st.markdown("---")
st.header("Business Insights")

col1, col2 = st.columns(2)

with col1:
    st.subheader("Top Categories by Revenue")
    if not top_cat.empty:
        fig = px.bar(top_cat, x="k", y="v", labels={"k": "Category", "v": "Revenue ($)"})
        fig.update_layout(height=360)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No category data yet.")

with col2:
    st.subheader("Payment Methods (share)")
    if not pay_counts.empty:
        fig = px.pie(pay_counts, names="k", values="v", title="Orders by Payment Method")
        fig.update_layout(height=360)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No payment method data yet.")

col3, col4 = st.columns(2)

with col3:
    st.subheader("Top Countries by Revenue")
    if not top_cty.empty:
        fig = px.bar(top_cty, x="k", y="v", labels={"k": "Country", "v": "Revenue ($)"})
        fig.update_layout(height=360)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No country data yet.")

with col4:
    st.subheader("Revenue vs Net Revenue by Day")
    if not daily.empty:
        fig = px.line(
            daily, x="day", y=["revenue", "net_revenue"],
            title="Revenue vs Net Revenue",
            labels={"day": "Date", "value": "Amount ($)", "variable": "Metric"},
            markers=True,
        )
        fig.update_layout(height=360)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No data yet.")

# ----------------------------
# Aggregated data table
# ----------------------------
st.markdown("---")
st.header("Aggregated Data (latest rows)")

latest = load_raw_agg(where_sql, params, limit=500)

if latest.empty:
    st.warning("No aggregated rows match your filters.")
else:
    st.dataframe(latest, use_container_width=True, height=420)

    with st.expander("Download filtered data"):
        csv = latest.to_csv(index=False).encode("utf-8")
        st.download_button(
            "Download CSV",
            data=csv,
            file_name="orders_live_aggregated.csv",
            mime="text/csv",
        )

# Auto-refresh
time.sleep(refresh)
st.rerun()
