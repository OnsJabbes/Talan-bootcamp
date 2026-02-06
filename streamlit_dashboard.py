 # -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
from pyhive import hive
import time

# Page configuration
st.set_page_config(
    page_title="Amazon Sales Analytics Dashboard",
    page_icon="📊",
    layout="wide"
)

def get_hive_connection():
    """Create connection to HiveServer2"""
    max_retries = 5
    retry_delay = 3
    
    for attempt in range(max_retries):
        try:
            conn = hive.Connection(
                host='hive-server',
                port=10000,
                username='hive'
            )
            return conn
        except Exception as e:
            if attempt < max_retries - 1:
                st.warning("Waiting for Hive server (attempt {}/{})...".format(attempt + 1, max_retries))
                time.sleep(retry_delay)
            else:
                st.error("Could not connect to Hive server: {}".format(str(e)))
                return None
    return None

@st.cache_data(ttl=60)
def load_gold_data():
    """Load aggregated data from Gold layer using Hive"""
    try:
        conn = get_hive_connection()
        if conn is None:
            return pd.DataFrame()
        
        # Query Gold layer table
        query = """
        SELECT 
            window_start,
            window_end,
            total_orders,
            total_revenue,
            avg_order_value,
            total_quantity,
            total_net_revenue,
            unique_customers,
            category,
            payment_method,
            country
        FROM datalake.gold_amazon_sales_aggregated_view
        ORDER BY window_start DESC
        LIMIT 1000
        """
        
        gold_df = pd.read_sql(query, conn)
        conn.close()
        
        # Convert timestamp columns
        if not gold_df.empty:
            gold_df['window_start'] = pd.to_datetime(gold_df['window_start'])
            gold_df['window_end'] = pd.to_datetime(gold_df['window_end'])
            if 'avg_quantity' not in gold_df.columns:
                if 'total_quantity' in gold_df.columns and 'total_orders' in gold_df.columns:
                    gold_df['avg_quantity'] = gold_df['total_quantity'] / gold_df['total_orders'].replace(0, pd.NA)
        
        return gold_df
    except Exception as e:
        st.error("Error loading Gold data from Hive: {}".format(str(e)))
        return pd.DataFrame()

@st.cache_data(ttl=60)
def load_silver_sample():
    """Load sample data from Silver layer using Hive"""
    try:
        conn = get_hive_connection()
        if conn is None:
            return pd.DataFrame()
        
        # Query Silver layer table (sample data)
        query = """
        SELECT *
        FROM datalake.silver_amazon_sales
        WHERE year_part = 2020 AND month_part = 1
        LIMIT 1000
        """
        
        silver_df = pd.read_sql(query, conn)
        conn.close()
        
        return silver_df
    except Exception as e:
        st.error("Error loading Silver data from Hive: {}".format(str(e)))
        return pd.DataFrame()

def main():
    # Title
    st.title("📊 Amazon Sales Real-Time Analytics Dashboard")
    st.markdown("---")
    
    # Sidebar
    st.sidebar.header("Dashboard Controls")
    refresh_button = st.sidebar.button("🔄 Refresh Data")
    
    st.sidebar.markdown("---")
    st.sidebar.markdown("### Data Lake Architecture")
    st.sidebar.markdown("- **Bronze**: Raw JSON from Kafka")
    st.sidebar.markdown("- **Silver**: Cleaned Parquet (partitioned)")
    st.sidebar.markdown("- **Gold**: Aggregated KPIs (1-min windows)")
    
    # Load data
    with st.spinner("Loading data from HDFS Gold layer..."):
        gold_df = load_gold_data()
    
    if gold_df.empty:
        st.warning("⚠️ No data available yet. Waiting for streaming data...")
        st.info("💡 The producer is sending data to Kafka, and Spark is processing it into HDFS layers.")
        return
    
    # KPI Metrics Row
    st.header("📈 Key Performance Indicators")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_orders = gold_df['total_orders'].sum()
        st.metric("Total Orders", "{:,}".format(int(total_orders)))
    
    with col2:
        total_revenue = gold_df['total_revenue'].sum()
        st.metric("Total Revenue", "${:,.2f}".format(total_revenue))
    
    with col3:
        avg_order = gold_df['avg_order_value'].mean()
        st.metric("Avg Order Value", "${:.2f}".format(avg_order))
    
    with col4:
        unique_customers = gold_df['unique_customers'].sum()
        st.metric("Unique Customers", "{:,}".format(int(unique_customers)))
    
    st.markdown("---")
    
    # Time series charts
    st.header("📊 Time Series Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Revenue over time
        fig_revenue = px.line(
            gold_df, 
            x='window_start', 
            y='total_revenue',
            title='Revenue Over Time (1-Minute Windows)',
            labels={'window_start': 'Time', 'total_revenue': 'Revenue ($)'}
        )
        fig_revenue.update_layout(height=400)
        st.plotly_chart(fig_revenue, use_container_width=True)
    
    with col2:
        # Orders over time
        fig_orders = px.line(
            gold_df, 
            x='window_start', 
            y='total_orders',
            title='Orders Over Time (1-Minute Windows)',
            labels={'window_start': 'Time', 'total_orders': 'Number of Orders'}
        )
        fig_orders.update_layout(height=400)
        st.plotly_chart(fig_orders, use_container_width=True)
    
    st.markdown("---")
    
    # Category and Payment Analysis
    st.header("🛍️ Business Insights")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Top categories
        st.subheader("Top Product Categories")
        if 'category' in gold_df.columns:
            category_counts = gold_df['category'].value_counts().head(10)
            fig_cat = px.bar(
                x=category_counts.index,
                y=category_counts.values,
                labels={'x': 'Category', 'y': 'Frequency'},
                title='Most Popular Categories'
            )
            fig_cat.update_layout(height=350)
            st.plotly_chart(fig_cat, use_container_width=True)
    
    with col2:
        # Top payment methods
        st.subheader("Payment Methods Distribution")
        if 'payment_method' in gold_df.columns:
            payment_counts = gold_df['payment_method'].value_counts()
            fig_payment = px.pie(
                values=payment_counts.values,
                names=payment_counts.index,
                title='Payment Methods Used'
            )
            fig_payment.update_layout(height=350)
            st.plotly_chart(fig_payment, use_container_width=True)
    
    st.markdown("---")
    
    # Detailed metrics
    st.header("📋 Detailed Metrics")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        # Average quantity per order
        fig_qty = px.line(
            gold_df,
            x='window_start',
            y='avg_quantity',
            title='Average Quantity per Order',
            labels={'window_start': 'Time', 'avg_quantity': 'Avg Quantity'}
        )
        fig_qty.update_layout(height=300)
        st.plotly_chart(fig_qty, use_container_width=True)
    
    with col2:
        # Customers per window
        fig_cust = px.bar(
            gold_df,
            x='window_start',
            y='unique_customers',
            title='Unique Customers per Window',
            labels={'window_start': 'Time', 'unique_customers': 'Customers'}
        )
        fig_cust.update_layout(height=300)
        st.plotly_chart(fig_cust, use_container_width=True)
    
    with col3:
        # Revenue vs Orders scatter
        fig_scatter = px.scatter(
            gold_df,
            x='total_orders',
            y='total_revenue',
            title='Revenue vs Orders Correlation',
            labels={'total_orders': 'Number of Orders', 'total_revenue': 'Revenue ($)'},
            trendline="ols"
        )
        fig_scatter.update_layout(height=300)
        st.plotly_chart(fig_scatter, use_container_width=True)
    
    st.markdown("---")
    
    # Raw data table (expandable)
    with st.expander("📄 View Raw Gold Layer Data"):
        st.dataframe(
            gold_df.style.format({
                'total_revenue': '${:,.2f}',
                'total_net_revenue': '${:,.2f}',
                'avg_order_value': '${:.2f}',
                'avg_quantity': '{:.2f}',
                'total_quantity': '{:,.0f}',
                'total_orders': '{:,.0f}',
                'unique_customers': '{:,.0f}'
            }),
            use_container_width=True
        )
    
    # Silver layer sample
    st.markdown("---")
    st.header("🔍 Silver Layer Sample Data")
    
    with st.spinner("Loading Silver layer sample..."):
        silver_sample = load_silver_sample()
    
    if not silver_sample.empty:
        with st.expander("📊 View Sample Detailed Records (1000 rows)"):
            st.dataframe(silver_sample, use_container_width=True)
            
            # Download button
            csv = silver_sample.to_csv(index=False)
            st.download_button(
                label="📥 Download Sample as CSV",
                data=csv,
                file_name="amazon_sales_sample.csv",
                mime="text/csv"
            )
    
    # Footer
    st.markdown("---")
    st.markdown("**Data Pipeline**: Amazon.csv → Kafka → Spark Streaming → HDFS (Bronze/Silver/Gold)")
    st.markdown("**Last Update**: {}".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))

if __name__ == "__main__":
    main()
