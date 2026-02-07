import os
import time
import numpy as np
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

st.set_page_config(page_title="Amazon OLTP Realtime", page_icon="📦", layout="wide")
st.title("📦 Amazon Sales — OLTP Realtime Dashboard (Postgres)")
st.caption("Source: `orders_live` (Spark Streaming → Postgres)")

# ----------------------------
# Sidebar controls
# ----------------------------
st.sidebar.header("⚙️ Controls")
refresh = st.sidebar.slider("Auto-refresh (seconds)", 2, 60, 5)
limit = st.sidebar.slider("Latest rows", 50, 5000, 300, step=50)

st.sidebar.markdown("---")
st.sidebar.subheader("🔎 Filters")

@st.cache_data(ttl=30)
def load_filter_values():
    q = text("""
        SELECT
          (SELECT array_agg(DISTINCT category) FROM orders_live) AS categories,
          (SELECT array_agg(DISTINCT country) FROM orders_live) AS countries,
          (SELECT array_agg(DISTINCT paymentmethod) FROM orders_live) AS paymentmethods,
          (SELECT array_agg(DISTINCT orderstatus) FROM orders_live) AS orderstatuses,
          (SELECT min(orderdate) FROM orders_live) AS min_date,
          (SELECT max(orderdate) FROM orders_live) AS max_date
    """)
    df = pd.read_sql(q, engine)
    if df.empty:
        return {}, None, None
    row = df.iloc[0].to_dict()

    def safe_list(x):
        return [v for v in (x or []) if v is not None]

    values = {
        "category": safe_list(row.get("categories")),
        "country": safe_list(row.get("countries")),
        "paymentmethod": safe_list(row.get("paymentmethods")),
        "orderstatus": safe_list(row.get("orderstatuses")),
    }
    return values, row.get("min_date"), row.get("max_date")

values, min_date, max_date = load_filter_values()

if min_date is None or max_date is None:
    st.warning("⚠️ No data yet in Postgres. Wait for Spark Streaming to fill `orders_live`.")
    st.info("Tip: check `select count(*) from orders_live;` inside postgres-mart.")
    time.sleep(refresh)
    st.rerun()

date_range = st.sidebar.date_input(
    "OrderDate range",
    value=(pd.to_datetime(min_date).date(), pd.to_datetime(max_date).date()),
    min_value=pd.to_datetime(min_date).date(),
    max_value=pd.to_datetime(max_date).date()
)

category_sel = st.sidebar.multiselect("Category", values.get("category", []), default=[])
country_sel = st.sidebar.multiselect("Country", values.get("country", []), default=[])
pay_sel = st.sidebar.multiselect("Payment method", values.get("paymentmethod", []), default=[])
status_sel = st.sidebar.multiselect("Order status", values.get("orderstatus", []), default=[])

st.sidebar.markdown("---")
st.sidebar.markdown("### Pipelines")
st.sidebar.markdown("- **OLTP**: Kafka → Spark Streaming → Postgres → Streamlit")
st.sidebar.markdown("- **OLAP**: HDFS Gold → (Batch) → Postgres → Power BI")

# ----------------------------
# SQL helpers
# ----------------------------
def build_where():
    clauses = []
    params = {}

    # dates
    if isinstance(date_range, tuple) and len(date_range) == 2:
        clauses.append("orderdate BETWEEN :d1 AND :d2")
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
    in_list("orderstatus", status_sel, "sts")

    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    return where, params

@st.cache_data(ttl=5)
def load_latest_orders(n, where_sql, params):
    q = text(f"""
        SELECT *
        FROM orders_live
        {where_sql}
        ORDER BY processing_timestamp DESC NULLS LAST
        LIMIT {n}
    """)
    return pd.read_sql(q, engine, params=params)

@st.cache_data(ttl=10)
def load_kpis(where_sql, params):
    q = text(f"""
        SELECT
          COUNT(*)::bigint AS total_orders,
          COALESCE(SUM(totalamount), 0)::float8 AS total_revenue,
          COALESCE(AVG(totalamount), 0)::float8 AS avg_order_value,
          COUNT(DISTINCT customerid)::bigint AS unique_customers
        FROM orders_live
        {where_sql}
    """)
    return pd.read_sql(q, engine, params=params)

@st.cache_data(ttl=10)
def load_timeseries(where_sql, params):
    q = text(f"""
        SELECT
          date_trunc('minute', processing_timestamp) AS minute,
          COUNT(*)::bigint AS orders,
          COALESCE(SUM(totalamount), 0)::float8 AS revenue
        FROM orders_live
        {where_sql}
        GROUP BY 1
        ORDER BY 1
    """)
    return pd.read_sql(q, engine, params=params)

@st.cache_data(ttl=20)
def load_group_counts(where_sql, params, colname, metric="count"):
    if metric == "count":
        q = text(f"""
          SELECT {colname} AS k, COUNT(*)::bigint AS v
          FROM orders_live
          {where_sql}
          GROUP BY 1
          ORDER BY v DESC
          LIMIT 15
        """)
    else:  # revenue
        q = text(f"""
          SELECT {colname} AS k, COALESCE(SUM(totalamount), 0)::float8 AS v
          FROM orders_live
          {where_sql}
          GROUP BY 1
          ORDER BY v DESC
          LIMIT 15
        """)
    return pd.read_sql(q, engine, params=params)

@st.cache_data(ttl=20)
def load_scatter(where_sql, params):
    q = text(f"""
      SELECT
        quantity,
        totalamount,
        category,
        country,
        paymentmethod
      FROM orders_live
      {where_sql}
      ORDER BY processing_timestamp DESC NULLS LAST
      LIMIT 5000
    """)
    return pd.read_sql(q, engine, params=params)

# ----------------------------
# Load data
# ----------------------------
where_sql, params = build_where()

kpi = load_kpis(where_sql, params)
kpi = kpi.iloc[0].to_dict() if not kpi.empty else {
    "total_orders": 0, "total_revenue": 0.0, "avg_order_value": 0.0, "unique_customers": 0
}

ts = load_timeseries(where_sql, params)
latest = load_latest_orders(limit, where_sql, params)

top_cat = load_group_counts(where_sql, params, "category", metric="revenue")
top_cty = load_group_counts(where_sql, params, "country", metric="revenue")
status_counts = load_group_counts(where_sql, params, "orderstatus", metric="count")
pay_counts = load_group_counts(where_sql, params, "paymentmethod", metric="count")

# ----------------------------
# KPI Row
# ----------------------------
st.markdown("---")
st.subheader("📈 Key Performance Indicators")

c1, c2, c3, c4, c5, c6 = st.columns(6)

with c1:
    st.metric("Total Orders", f"{int(kpi['total_orders']):,}")
with c2:
    st.metric("Total Revenue", f"${kpi['total_revenue']:,.2f}")
with c3:
    st.metric("Avg Order Value", f"${kpi['avg_order_value']:.2f}")
with c4:
    st.metric("Unique Customers", f"{int(kpi['unique_customers']):,}")

with c5:
    top_cat_name = top_cat["k"].iloc[0] if not top_cat.empty else "-"
    st.metric("Top Category (Rev)", str(top_cat_name))
with c6:
    top_cty_name = top_cty["k"].iloc[0] if not top_cty.empty else "-"
    st.metric("Top Country (Rev)", str(top_cty_name))

# ----------------------------
# Time series
# ----------------------------
st.markdown("---")
st.header("⏱️ Realtime Trend (minute buckets)")

colA, colB = st.columns(2)

with colA:
    fig_rev = px.line(
        ts, x="minute", y="revenue",
        title="Revenue over time",
        labels={"minute": "Time", "revenue": "Revenue ($)"}
    )
    fig_rev.update_layout(height=380)
    st.plotly_chart(fig_rev, use_container_width=True)

with colB:
    fig_ord = px.line(
        ts, x="minute", y="orders",
        title="Orders over time",
        labels={"minute": "Time", "orders": "Orders"}
    )
    fig_ord.update_layout(height=380)
    st.plotly_chart(fig_ord, use_container_width=True)

# ----------------------------
# Business insights
# ----------------------------
st.markdown("---")
st.header("🧠 Business Insights")

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
        fig = px.pie(pay_counts, names="k", values="v", title="Orders by payment method")
        fig.update_layout(height=360)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No payment method data yet.")

col3, col4 = st.columns(2)

with col3:
    st.subheader("Order Status (live)")
    if not status_counts.empty:
        fig = px.bar(status_counts, x="k", y="v", labels={"k": "Status", "v": "Orders"})
        fig.update_layout(height=360)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No status data yet.")

with col4:
    st.subheader("Top Countries by Revenue")
    if not top_cty.empty:
        fig = px.bar(top_cty, x="k", y="v", labels={"k": "Country", "v": "Revenue ($)"})
        fig.update_layout(height=360)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No country data yet.")

# ----------------------------
# Scatter analysis (FIXED: clearer chart)
# ----------------------------
st.markdown("---")
st.header("📌 Correlations")

scatter_df = load_scatter(where_sql, params)

colS1, colS2 = st.columns([2, 1])

with colS1:
    st.subheader("Revenue vs Quantity (clear)")

    if not scatter_df.empty:
        sdf = scatter_df.dropna(subset=["quantity", "totalamount"]).copy()
        sdf["quantity"] = sdf["quantity"].astype(int)

        # keep legend readable: show top 8 categories, rest => Other
        if "category" in sdf.columns:
            top_cats = sdf["category"].value_counts().head(8).index.tolist()
            sdf["category_plot"] = sdf["category"].where(sdf["category"].isin(top_cats), "Other")
            color_col = "category_plot"
        else:
            color_col = None

        fig = px.box(
            sdf,
            x="quantity",
            y="totalamount",
            color=color_col,
            points="all",
            title="Revenue distribution by Quantity (box + points)",
            labels={"quantity": "Quantity", "totalamount": "Total Amount"}
        )

        # jitter + transparency to avoid clutter
        fig.update_traces(jitter=0.35, pointpos=0, marker=dict(opacity=0.20, size=4))
        fig.update_layout(height=420)

        # optional: uncomment if values span a lot
        fig.update_yaxes(type="log")

        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No data yet for this chart.")

with colS2:
    st.subheader("Quick distribution")
    if not scatter_df.empty:
        fig = px.histogram(scatter_df, x="totalamount", nbins=40, title="Order amount distribution")
        fig.update_layout(height=420)
        st.plotly_chart(fig, use_container_width=True)

# ----------------------------
# Latest orders table
# ----------------------------
st.markdown("---")
st.header("📋 Latest Orders (live)")

if latest.empty:
    st.warning("No rows match your filters.")
else:
    st.dataframe(latest, use_container_width=True, height=420)

    with st.expander("⬇️ Download filtered latest orders"):
        csv = latest.to_csv(index=False).encode("utf-8")
        st.download_button(
            "Download CSV",
            data=csv,
            file_name="orders_live_latest.csv",
            mime="text/csv"
        )

# Auto-refresh
time.sleep(refresh)
st.rerun()
