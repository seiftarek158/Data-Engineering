import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine
import os

# Page Config
st.set_page_config(page_title="Stock Portfolio Visualization - Stage 5", layout="wide")

# -----------------------------------------------------------------------------
# Database Connection
# -----------------------------------------------------------------------------
DB_USER = os.getenv("POSTGRES_USER", "postgres")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "postgres")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5454")
DB_NAME = os.getenv("POSTGRES_DB", "Trades_Database")

# If running inside Docker, use internal port
if DB_HOST == 'pgdatabase':
    DB_PORT = "5432"

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

@st.cache_resource
def get_database_engine():
    return create_engine(DATABASE_URL)

try:
    engine = get_database_engine()
    st.sidebar.success(f"✅ Connected to DB at {DB_HOST}")
except Exception as e:
    st.sidebar.error(f"❌ Database Connection Failed: {e}")
    st.stop()

# -----------------------------------------------------------------------------
# Title
# -----------------------------------------------------------------------------
st.title("📊 Stock Portfolio Visualization Dashboard")
st.markdown("**Stage 5: Analytics & Visualization**")
st.markdown("---")

# -----------------------------------------------------------------------------
# Sidebar - Data Selection
# -----------------------------------------------------------------------------
st.sidebar.header("⚙️ Dashboard Controls")

# Table selection
available_tables = ["final_stocks", "spark_analytics", "integrated_main", "trades"]
selected_table = st.sidebar.selectbox("Select Data Source", available_tables, index=0)

# Refresh button
if st.sidebar.button("🔄 Refresh Data"):
    st.cache_data.clear()
    st.rerun()

# -----------------------------------------------------------------------------
# Load Data Function
# -----------------------------------------------------------------------------
@st.cache_data(ttl=300)
def load_data(table_name):
    """Load data from specified table"""
    try:
        query = f"SELECT * FROM {table_name} LIMIT 10000"
        df = pd.read_sql(query, engine)
        return df
    except Exception as e:
        st.error(f"Error loading {table_name}: {e}")
        return pd.DataFrame()

# -----------------------------------------------------------------------------
# Main Dashboard
# -----------------------------------------------------------------------------

# Load selected data
with st.spinner(f'Loading data from {selected_table}...'):
    df = load_data(selected_table)

if df.empty:
    st.warning(f"No data available in {selected_table}")
    st.stop()

# Display basic statistics
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Total Records", f"{len(df):,}")
with col2:
    st.metric("Total Columns", f"{len(df.columns)}")
with col3:
    if 'symbol' in df.columns:
        st.metric("Unique Symbols", f"{df['symbol'].nunique()}")
    elif 'stock_id' in df.columns:
        st.metric("Unique Stocks", f"{df['stock_id'].nunique()}")
    else:
        st.metric("Data Source", selected_table)
with col4:
    if 'trade_date' in df.columns:
        st.metric("Date Range", f"{df['trade_date'].nunique()} days")
    elif 'date_id' in df.columns:
        st.metric("Date Range", f"{df['date_id'].nunique()} days")
    else:
        st.metric("Status", "✅ Loaded")

st.markdown("---")

# -----------------------------------------------------------------------------
# Visualization Sections
# -----------------------------------------------------------------------------

# Section 1: Data Overview
st.header("📋 Data Overview")
with st.expander("View Raw Data Sample", expanded=False):
    st.dataframe(df.head(100), use_container_width=True)

with st.expander("Data Statistics", expanded=False):
    st.write(df.describe())

st.markdown("---")

# Section 2: Visualizations based on available columns
st.header("📈 Analytics Visualizations")

# Check for numeric columns for visualization
numeric_cols = df.select_dtypes(include=['float64', 'int64']).columns.tolist()

if len(numeric_cols) > 0:
    
    # Row 1: Distribution plots
    col1, col2 = st.columns(2)
    
    with col1:
        if 'price' in df.columns:
            st.subheader("Price Distribution")
            fig = px.histogram(df, x='price', nbins=50, 
                             title="Price Distribution",
                             labels={'price': 'Price', 'count': 'Frequency'})
            fig.update_layout(showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
        elif 'close_price' in df.columns:
            st.subheader("Close Price Distribution")
            fig = px.histogram(df, x='close_price', nbins=50,
                             title="Close Price Distribution",
                             labels={'close_price': 'Close Price', 'count': 'Frequency'})
            fig.update_layout(showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.subheader(f"{numeric_cols[0]} Distribution")
            fig = px.histogram(df, x=numeric_cols[0], nbins=50)
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        if 'quantity' in df.columns:
            st.subheader("Quantity Distribution")
            fig = px.histogram(df, x='quantity', nbins=50,
                             title="Quantity Distribution",
                             labels={'quantity': 'Quantity', 'count': 'Frequency'})
            fig.update_layout(showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
        elif 'volume' in df.columns:
            st.subheader("Volume Distribution")
            fig = px.histogram(df, x='volume', nbins=50,
                             title="Volume Distribution",
                             labels={'volume': 'Volume', 'count': 'Frequency'})
            fig.update_layout(showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
        elif len(numeric_cols) > 1:
            st.subheader(f"{numeric_cols[1]} Distribution")
            fig = px.histogram(df, x=numeric_cols[1], nbins=50)
            st.plotly_chart(fig, use_container_width=True)
    
    st.markdown("---")
    
    # Row 2: Time series or categorical analysis
    if 'trade_date' in df.columns and 'price' in df.columns:
        st.subheader("📅 Price Over Time")
        
        # Aggregate by date
        daily_data = df.groupby('trade_date').agg({
            'price': 'mean',
            'quantity': 'sum' if 'quantity' in df.columns else 'count'
        }).reset_index()
        
        fig = px.line(daily_data, x='trade_date', y='price',
                     title="Average Price Trend",
                     labels={'trade_date': 'Date', 'price': 'Average Price'})
        fig.update_traces(line_color='#1f77b4', line_width=2)
        st.plotly_chart(fig, use_container_width=True)
        
    elif 'symbol' in df.columns and 'price' in df.columns:
        st.subheader("💹 Top Stocks by Average Price")
        
        symbol_data = df.groupby('symbol')['price'].mean().sort_values(ascending=False).head(10)
        fig = px.bar(symbol_data, orientation='h',
                    title="Top 10 Stocks by Average Price",
                    labels={'value': 'Average Price', 'symbol': 'Stock Symbol'})
        st.plotly_chart(fig, use_container_width=True)
    
    st.markdown("---")
    
    # Row 3: Correlation matrix for numeric columns
    if len(numeric_cols) > 2:
        st.subheader("🔗 Correlation Matrix")
        
        # Select up to 10 numeric columns for correlation
        corr_cols = numeric_cols[:10]
        corr_matrix = df[corr_cols].corr()
        
        fig = px.imshow(corr_matrix,
                       labels=dict(color="Correlation"),
                       x=corr_cols,
                       y=corr_cols,
                       title="Feature Correlation Heatmap",
                       color_continuous_scale='RdBu_r',
                       aspect='auto')
        fig.update_layout(height=600)
        st.plotly_chart(fig, use_container_width=True)
    
    st.markdown("---")
    
    # Row 4: Box plots for outlier detection
    st.subheader("📦 Box Plots - Outlier Detection")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if 'price' in df.columns:
            fig = px.box(df, y='price', title="Price Box Plot")
            st.plotly_chart(fig, use_container_width=True)
        elif len(numeric_cols) > 0:
            fig = px.box(df, y=numeric_cols[0], title=f"{numeric_cols[0]} Box Plot")
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        if 'quantity' in df.columns:
            fig = px.box(df, y='quantity', title="Quantity Box Plot")
            st.plotly_chart(fig, use_container_width=True)
        elif len(numeric_cols) > 1:
            fig = px.box(df, y=numeric_cols[1], title=f"{numeric_cols[1]} Box Plot")
            st.plotly_chart(fig, use_container_width=True)

else:
    st.info("No numeric columns available for visualization in this table.")

st.markdown("---")

# Section 3: Spark Analytics (if available)
if selected_table == 'spark_analytics':
    st.header("⚡ Spark Analytics Results")
    
    if not df.empty:
        # Display all analytics results
        st.dataframe(df, use_container_width=True)
        
        # If there are metric columns, visualize them
        if 'metric_name' in df.columns and 'metric_value' in df.columns:
            st.subheader("Metrics Overview")
            fig = px.bar(df, x='metric_name', y='metric_value',
                        title="Spark Analytics Metrics",
                        labels={'metric_name': 'Metric', 'metric_value': 'Value'})
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Spark analytics results will appear here after Stage 4 execution.")

# -----------------------------------------------------------------------------
# Footer
# -----------------------------------------------------------------------------
st.markdown("---")
st.caption(f"🕒 Data loaded from `{selected_table}` | Dashboard Port: 8502 | Last refresh: {pd.Timestamp.now()}")
