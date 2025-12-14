"""
Real-Time Stock Portfolio Analytics Dashboard
==============================================

Interactive Streamlit dashboard for stock portfolio analysis.

Features:
- 6 Core visualizations (mandatory)
- 3 Advanced visualizations (bonus)
- Interactive filters and drill-down
- Real-time data refresh
- Export functionality
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from sqlalchemy import create_engine
import os
from datetime import datetime, timedelta

# ============================================================================
# PAGE CONFIGURATION
# ============================================================================

st.set_page_config(
    page_title="Stock Portfolio Analytics",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================================================
# DATABASE CONNECTION
# ============================================================================
# Environment variables are set by docker-compose.yaml (lines 430-435):
# - DB_HOST=pgdatabase
# - DB_USER=postgres
# - DB_PASSWORD=postgres
# - DB_PORT=5432
# - DB_NAME=Trades_Database
#
# These are injected into the container at runtime, so no .env file is needed.
# The container does NOT read from a .env file - it uses Docker environment variables.

@st.cache_resource
def get_database_engine():
    """Create and cache database connection"""
    db_host = os.getenv('DB_HOST', 'airflow-postgres')
    db_user = os.getenv('DB_USER', 'airflow')
    db_password = os.getenv('DB_PASSWORD', 'airflow')
    db_port = os.getenv('DB_PORT', '5432')
    db_name = os.getenv('DB_NAME', 'airflow')

    connection_string = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
    return create_engine(connection_string)


@st.cache_data(ttl=60)  # Cache for 60 seconds
def load_data(table_name):
    """Load data from PostgreSQL table with caching"""
    engine = get_database_engine()
    try:
        df = pd.read_sql_table(table_name, con=engine)
        return df
    except Exception as e:
        st.error(f"Error loading {table_name}: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=60)
def load_query(query):
    """Execute SQL query with caching"""
    engine = get_database_engine()
    try:
        df = pd.read_sql(query, con=engine)
        return df
    except Exception as e:
        st.error(f"Error executing query: {str(e)}")
        return pd.DataFrame()


# ============================================================================
# HEADER AND TITLE
# ============================================================================

st.title("📊 Real-Time Stock Portfolio Analytics")
st.markdown("---")

# ============================================================================
# SIDEBAR - FILTERS
# ============================================================================

st.sidebar.header("🔍 Filters")

# Refresh button
if st.sidebar.button("🔄 Refresh Data", use_container_width=True):
    st.cache_data.clear()
    st.rerun()

st.sidebar.markdown("---")

# Load main data for filtering
df_main = load_data('visualization_main_data')

if df_main.empty:
    st.error("❌ No data available. Please run Stage 5 DAG first.")
    st.stop()

# Convert date column
df_main['date'] = pd.to_datetime(df_main['date'])

# Date Range Filter
st.sidebar.subheader("📅 Date Range")
min_date = df_main['date'].min().date()
max_date = df_main['date'].max().date()

date_range = st.sidebar.date_input(
    "Select date range",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date
)

if len(date_range) == 2:
    start_date, end_date = date_range
    df_filtered = df_main[
        (df_main['date'].dt.date >= start_date) &
        (df_main['date'].dt.date <= end_date)
    ]
else:
    df_filtered = df_main

# Stock Ticker Filter
st.sidebar.subheader("📈 Stock Ticker")
all_tickers = ['All'] + sorted(df_main['stock_ticker'].dropna().unique().tolist())
selected_ticker = st.sidebar.selectbox("Select stock ticker", all_tickers)

if selected_ticker != 'All':
    df_filtered = df_filtered[df_filtered['stock_ticker'] == selected_ticker]

# Sector Filter
st.sidebar.subheader("🏢 Sector")
all_sectors = ['All'] + sorted(df_main['sector'].dropna().unique().tolist())
selected_sector = st.sidebar.selectbox("Select sector", all_sectors)

if selected_sector != 'All':
    df_filtered = df_filtered[df_filtered['sector'] == selected_sector]

# Customer Type Filter
st.sidebar.subheader("👥 Customer Type")
all_account_types = ['All'] + sorted(df_main['account_type'].dropna().unique().tolist())
selected_account_type = st.sidebar.selectbox("Select account type", all_account_types)

if selected_account_type != 'All':
    df_filtered = df_filtered[df_filtered['account_type'] == selected_account_type]

# Transaction Type Filter
st.sidebar.subheader("💱 Transaction Type")
transaction_types = ['All', 'BUY', 'SELL']
selected_transaction = st.sidebar.selectbox("Select transaction type", transaction_types)

if selected_transaction != 'All':
    df_filtered = df_filtered[df_filtered['transaction_type'] == selected_transaction]

st.sidebar.markdown("---")
st.sidebar.info(f"**Filtered Records:** {len(df_filtered):,} / {len(df_main):,}")

# ============================================================================
# LOAD METADATA
# ============================================================================

metadata = load_data('viz_metadata')
if not metadata.empty:
    last_updated = metadata['last_updated'].iloc[0]
    st.sidebar.markdown(f"**Last Updated:** {last_updated}")

# ============================================================================
# KPI CARDS (ADVANCED VISUALIZATION #1: Portfolio Performance Metrics)
# ============================================================================

st.header("📌 Portfolio Performance Metrics")

col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    total_transactions = len(df_filtered)
    st.metric("Total Transactions", f"{total_transactions:,}")

with col2:
    total_volume = df_filtered['quantity'].sum()
    st.metric("Total Volume", f"{total_volume:,.0f}")

with col3:
    total_portfolio_value = df_filtered['cumulative_portfolio_value'].max()
    st.metric("Portfolio Value", f"${total_portfolio_value:,.2f}")

with col4:
    avg_stock_price = df_filtered['stock_price'].mean()
    st.metric("Avg Stock Price", f"${avg_stock_price:.2f}")

with col5:
    unique_stocks = df_filtered['stock_ticker'].nunique()
    st.metric("Unique Stocks", f"{unique_stocks}")

st.markdown("---")

# ============================================================================
# CORE VISUALIZATIONS
# ============================================================================

# CORE VIZ 1: Trading Volume by Stock Ticker
st.header("1️⃣ Trading Volume by Stock Ticker")

# Use decoded Stage 4 table (viz_volume_by_ticker from spark_analytics_1)
stock_volume_df = load_data('viz_volume_by_ticker')

if not stock_volume_df.empty:
    stock_volume = stock_volume_df.sort_values('total_volume', ascending=False).head(20)

    fig1 = px.bar(
        stock_volume,
        x='stock_ticker',
        y='total_volume',
        title='Top 20 Stock Tickers by Trading Volume',
        labels={'total_volume': 'Total Volume', 'stock_ticker': 'Stock Ticker'},
        color='total_volume',
        color_continuous_scale='Blues'
    )
    fig1.update_layout(height=500, showlegend=False)
    st.plotly_chart(fig1, use_container_width=True)
else:
    # Fallback to filtering main data
    stock_volume = df_filtered.groupby('stock_ticker')['quantity'].sum().reset_index()
    stock_volume = stock_volume.sort_values('quantity', ascending=False).head(20)

    fig1 = px.bar(
        stock_volume,
        x='stock_ticker',
        y='quantity',
        title='Top 20 Stock Tickers by Trading Volume',
        labels={'quantity': 'Total Volume', 'stock_ticker': 'Stock Ticker'},
        color='quantity',
        color_continuous_scale='Blues'
    )
    fig1.update_layout(height=500, showlegend=False)
    st.plotly_chart(fig1, use_container_width=True)

# Export button for this chart
if st.button("📥 Export Chart 1 as PNG", key="export1"):
    fig1.write_image("chart1_trading_volume.png")
    st.success("✅ Chart exported as chart1_trading_volume.png")

st.markdown("---")

# CORE VIZ 2: Stock Price Trends by Sector
st.header("2️⃣ Stock Price Trends by Sector")

# Use pre-aggregated viz_sector_time table for better performance
sector_time_df = load_data('viz_sector_time')

if not sector_time_df.empty:
    sector_time_df['date'] = pd.to_datetime(sector_time_df['date'])

    # Apply filters if needed
    if len(date_range) == 2:
        sector_time_filtered = sector_time_df[
            (sector_time_df['date'].dt.date >= start_date) &
            (sector_time_df['date'].dt.date <= end_date)
        ]
    else:
        sector_time_filtered = sector_time_df

    if selected_sector != 'All':
        sector_time_filtered = sector_time_filtered[sector_time_filtered['sector'] == selected_sector]

    fig2 = px.line(
        sector_time_filtered,
        x='date',
        y='avg_stock_price',
        color='sector',
        title='Stock Price Trends by Sector Over Time',
        labels={'avg_stock_price': 'Average Stock Price ($)', 'date': 'Date', 'sector': 'Sector'}
    )
    fig2.update_layout(height=500, hovermode='x unified')
    st.plotly_chart(fig2, use_container_width=True)
else:
    # Fallback to computing from main data
    sector_price = df_filtered.groupby(['sector', 'date'])['stock_price'].mean().reset_index()
    fig2 = px.line(
        sector_price,
        x='date',
        y='stock_price',
        color='sector',
        title='Stock Price Trends by Sector Over Time',
        labels={'stock_price': 'Average Stock Price ($)', 'date': 'Date', 'sector': 'Sector'}
    )
    fig2.update_layout(height=500, hovermode='x unified')
    st.plotly_chart(fig2, use_container_width=True)

if st.button("📥 Export Chart 2 as PNG", key="export2"):
    fig2.write_image("chart2_price_trends.png")
    st.success("✅ Chart exported as chart2_price_trends.png")

st.markdown("---")

# CORE VIZ 3: Buy vs Sell Transactions
st.header("3️⃣ Buy vs Sell Transactions")

# Use decoded Stage 5 transaction summary table
trans_summary_df = load_data('viz_transaction_summary')

col_left, col_right = st.columns(2)

with col_left:
    if not trans_summary_df.empty:
        fig3a = px.pie(
            trans_summary_df,
            values='transaction_count',
            names='transaction_type',
            title='Transaction Type Distribution',
            color_discrete_sequence=px.colors.qualitative.Set2
        )
        fig3a.update_layout(height=400)
        st.plotly_chart(fig3a, use_container_width=True)
    else:
        # Fallback
        trans_type = df_filtered['transaction_type'].value_counts().reset_index()
        trans_type.columns = ['transaction_type', 'count']
        fig3a = px.pie(trans_type, values='count', names='transaction_type', title='Transaction Type Distribution')
        fig3a.update_layout(height=400)
        st.plotly_chart(fig3a, use_container_width=True)

with col_right:
    if not trans_summary_df.empty:
        fig3b = px.bar(
            trans_summary_df,
            x='transaction_type',
            y='total_volume',
            title='Trading Volume by Transaction Type',
            labels={'total_volume': 'Total Volume', 'transaction_type': 'Transaction Type'},
            color='transaction_type',
            color_discrete_sequence=px.colors.qualitative.Pastel
        )
        fig3b.update_layout(height=400, showlegend=False)
        st.plotly_chart(fig3b, use_container_width=True)
    else:
        # Fallback
        trans_volume = df_filtered.groupby('transaction_type')['quantity'].sum().reset_index()
        fig3b = px.bar(trans_volume, x='transaction_type', y='quantity', title='Trading Volume by Transaction Type')
        fig3b.update_layout(height=400, showlegend=False)
        st.plotly_chart(fig3b, use_container_width=True)

st.markdown("---")

# CORE VIZ 4: Trading Activity by Day of Week
st.header("4️⃣ Trading Activity by Day of Week")

# Use decoded Stage 4 table (viz_trade_by_day from spark_analytics_5)
trade_by_day_df = load_data('viz_trade_by_day')

if not trade_by_day_df.empty:
    # Stage 4 has: day, total_trade_amount
    # We'll enrich with transaction count from filtered data
    day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    dow_activity = df_filtered.groupby('day_name').agg({
        'transaction_id': 'count',
        'quantity': 'sum'
    }).reset_index()
    dow_activity['day_order'] = dow_activity['day_name'].map({day: i for i, day in enumerate(day_order)})
    dow_activity = dow_activity.sort_values('day_order')

    fig4 = go.Figure()
    fig4.add_trace(go.Bar(
        x=dow_activity['day_name'],
        y=dow_activity['transaction_id'],
        name='Transaction Count',
        marker_color='lightblue'
    ))
    fig4.add_trace(go.Scatter(
        x=dow_activity['day_name'],
        y=dow_activity['quantity'],
        name='Total Volume',
        yaxis='y2',
        mode='lines+markers',
        marker=dict(size=10, color='orange'),
        line=dict(width=3)
    ))

    fig4.update_layout(
        title='Trading Activity by Day of Week',
        xaxis_title='Day of Week',
        yaxis_title='Transaction Count',
        yaxis2=dict(title='Total Volume', overlaying='y', side='right'),
        height=500,
        hovermode='x unified'
    )
    st.plotly_chart(fig4, use_container_width=True)
else:
    # Fallback to computing from main data
    day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    dow_activity = df_filtered.groupby('day_name').agg({
        'transaction_id': 'count',
        'quantity': 'sum'
    }).reset_index()
    dow_activity['day_order'] = dow_activity['day_name'].map({day: i for i, day in enumerate(day_order)})
    dow_activity = dow_activity.sort_values('day_order')

    fig4 = go.Figure()
    fig4.add_trace(go.Bar(x=dow_activity['day_name'], y=dow_activity['transaction_id'], name='Transaction Count'))
    fig4.update_layout(title='Trading Activity by Day of Week', height=500)
    st.plotly_chart(fig4, use_container_width=True)

st.markdown("---")

# CORE VIZ 5: Customer Transaction Distribution
st.header("5️⃣ Customer Transaction Distribution")

# Use pre-computed customer distribution
customer_dist_df = load_data('viz_customer_distribution')

if not customer_dist_df.empty:
    fig5 = px.histogram(
        customer_dist_df,
        x='transaction_count',
        nbins=30,
        title='Distribution of Transactions per Customer',
        labels={'transaction_count': 'Number of Transactions', 'count': 'Number of Customers'},
        color_discrete_sequence=['#636EFA']
    )
    fig5.update_layout(height=500, showlegend=False)
    st.plotly_chart(fig5, use_container_width=True)

    # Statistics
    col_stat1, col_stat2, col_stat3 = st.columns(3)
    with col_stat1:
        st.metric("Total Customers", f"{len(customer_dist_df):,}")
    with col_stat2:
        st.metric("Avg Transactions/Customer", f"{customer_dist_df['transaction_count'].mean():.1f}")
    with col_stat3:
        st.metric("Max Transactions (1 Customer)", f"{customer_dist_df['transaction_count'].max():,}")
else:
    # Fallback
    customer_dist = df_filtered.groupby('customer_id')['transaction_id'].count().reset_index()
    customer_dist.columns = ['customer_id', 'transaction_count']
    fig5 = px.histogram(customer_dist, x='transaction_count', nbins=30, title='Distribution of Transactions per Customer')
    fig5.update_layout(height=500)
    st.plotly_chart(fig5, use_container_width=True)

st.markdown("---")

# CORE VIZ 6: Top 10 Customers by Trade Amount
st.header("6️⃣ Top 10 Customers by Trade Amount")

# Use pre-computed top customers
top_customers_df = load_data('viz_top_customers')

if not top_customers_df.empty:
    top_10 = top_customers_df.head(10).copy()
    top_10['customer_id'] = top_10['customer_id'].astype(str)

    fig6 = px.bar(
        top_10,
        x='portfolio_value',
        y='customer_id',
        orientation='h',
        title='Top 10 Customers by Portfolio Value',
        labels={'portfolio_value': 'Portfolio Value ($)', 'customer_id': 'Customer ID'},
        color='portfolio_value',
        color_continuous_scale='Viridis'
    )
    fig6.update_layout(height=500, showlegend=False, yaxis={'categoryorder': 'total ascending'})
    st.plotly_chart(fig6, use_container_width=True)
else:
    # Fallback
    customer_trade = df_filtered.groupby('customer_id')['cumulative_portfolio_value'].max().reset_index()
    customer_trade = customer_trade.sort_values('cumulative_portfolio_value', ascending=False).head(10)
    customer_trade['customer_id'] = customer_trade['customer_id'].astype(str)
    fig6 = px.bar(customer_trade, x='cumulative_portfolio_value', y='customer_id', orientation='h', title='Top 10 Customers')
    fig6.update_layout(height=500)
    st.plotly_chart(fig6, use_container_width=True)

st.markdown("---")

# ============================================================================
# ADVANCED VISUALIZATIONS
# ============================================================================

st.header("🚀 Advanced Analytics")

# ADVANCED VIZ 2: Sector Comparison Dashboard
st.subheader("📊 Sector Comparison Dashboard")

# Use pre-computed sector comparison
sector_comparison_df = load_data('viz_sector_comparison')

if not sector_comparison_df.empty:
    fig_adv2 = make_subplots(
        rows=2, cols=2,
        subplot_titles=('Transaction Count by Sector', 'Trading Volume by Sector',
                        'Avg Stock Price by Sector', 'Portfolio Value by Sector'),
        specs=[[{'type': 'bar'}, {'type': 'bar'}],
               [{'type': 'bar'}, {'type': 'bar'}]]
    )

    # Chart 1: Transaction count
    fig_adv2.add_trace(
        go.Bar(x=sector_comparison_df['sector'], y=sector_comparison_df['transaction_count'],
               name='Transactions', marker_color='lightblue'),
        row=1, col=1
    )

    # Chart 2: Trading volume
    fig_adv2.add_trace(
        go.Bar(x=sector_comparison_df['sector'], y=sector_comparison_df['total_volume'],
               name='Volume', marker_color='lightgreen'),
        row=1, col=2
    )

    # Chart 3: Average stock price
    fig_adv2.add_trace(
        go.Bar(x=sector_comparison_df['sector'], y=sector_comparison_df['avg_stock_price'],
               name='Avg Price', marker_color='lightsalmon'),
        row=2, col=1
    )

    # Chart 4: Portfolio value
    fig_adv2.add_trace(
        go.Bar(x=sector_comparison_df['sector'], y=sector_comparison_df['total_portfolio_value'],
               name='Portfolio Value', marker_color='plum'),
        row=2, col=2
    )

    fig_adv2.update_layout(height=700, showlegend=False, title_text="Sector Comparison Dashboard")
    st.plotly_chart(fig_adv2, use_container_width=True)
else:
    # Fallback
    sector_comparison = df_filtered.groupby('sector').agg({
        'transaction_id': 'count',
        'quantity': 'sum',
        'stock_price': 'mean'
    }).reset_index()
    st.write("Sector comparison data unavailable - run Stage 5 DAG first")
    st.dataframe(sector_comparison)

st.markdown("---")

# ADVANCED VIZ 3: Holiday vs Non-Holiday Trading Patterns
st.subheader("🎄 Holiday vs Non-Holiday Trading Patterns")

col_hol1, col_hol2 = st.columns(2)

with col_hol1:
    # Transaction count comparison
    holiday_trans = df_filtered.groupby('is_holiday')['transaction_id'].count().reset_index()
    holiday_trans['period'] = holiday_trans['is_holiday'].map({True: 'Holiday', False: 'Non-Holiday'})

    fig_hol1 = px.bar(
        holiday_trans,
        x='period',
        y='transaction_id',
        title='Transaction Count: Holiday vs Non-Holiday',
        labels={'transaction_id': 'Transaction Count', 'period': 'Period'},
        color='period',
        color_discrete_map={'Holiday': 'red', 'Non-Holiday': 'green'}
    )
    fig_hol1.update_layout(height=400, showlegend=False)
    st.plotly_chart(fig_hol1, use_container_width=True)

with col_hol2:
    # Trading volume comparison
    holiday_vol = df_filtered.groupby('is_holiday')['quantity'].sum().reset_index()
    holiday_vol['period'] = holiday_vol['is_holiday'].map({True: 'Holiday', False: 'Non-Holiday'})

    fig_hol2 = px.pie(
        holiday_vol,
        values='quantity',
        names='period',
        title='Trading Volume Distribution',
        color='period',
        color_discrete_map={'Holiday': 'red', 'Non-Holiday': 'green'}
    )
    fig_hol2.update_layout(height=400)
    st.plotly_chart(fig_hol2, use_container_width=True)

# Detailed comparison table
holiday_detail = df_filtered.groupby(['is_holiday', 'transaction_type']).agg({
    'transaction_id': 'count',
    'quantity': 'sum',
    'cumulative_portfolio_value': 'sum'
}).reset_index()
holiday_detail['period'] = holiday_detail['is_holiday'].map({True: 'Holiday', False: 'Non-Holiday'})
holiday_detail = holiday_detail[['period', 'transaction_type', 'transaction_id', 'quantity', 'cumulative_portfolio_value']]
holiday_detail.columns = ['Period', 'Transaction Type', 'Count', 'Volume', 'Portfolio Value']

st.dataframe(holiday_detail, use_container_width=True)

st.markdown("---")

# ADVANCED VIZ 4: Stock Liquidity Tier Analysis (Stacked Area Chart)
st.subheader("💧 Stock Liquidity Tier Analysis")

# Use pre-aggregated viz_liquidity_time table for better performance
liquidity_time_df = load_data('viz_liquidity_time')

if not liquidity_time_df.empty:
    liquidity_time_df['date'] = pd.to_datetime(liquidity_time_df['date'])

    # Apply filters if needed
    if len(date_range) == 2:
        liquidity_time_filtered = liquidity_time_df[
            (liquidity_time_df['date'].dt.date >= start_date) &
            (liquidity_time_df['date'].dt.date <= end_date)
        ]
    else:
        liquidity_time_filtered = liquidity_time_df

    fig_liq = px.area(
        liquidity_time_filtered,
        x='date',
        y='total_volume',
        color='liquidity_tier',
        title='Trading Volume by Liquidity Tier Over Time (Stacked Area)',
        labels={'total_volume': 'Trading Volume', 'date': 'Date', 'liquidity_tier': 'Liquidity Tier'}
    )
    fig_liq.update_layout(height=500, hovermode='x unified')
    st.plotly_chart(fig_liq, use_container_width=True)
else:
    # Fallback to computing from main data
    liquidity_time = df_filtered.groupby(['date', 'liquidity_tier'])['quantity'].sum().reset_index()
    fig_liq = px.area(
        liquidity_time,
        x='date',
        y='quantity',
        color='liquidity_tier',
        title='Trading Volume by Liquidity Tier Over Time (Stacked Area)',
        labels={'quantity': 'Trading Volume', 'date': 'Date', 'liquidity_tier': 'Liquidity Tier'}
    )
    fig_liq.update_layout(height=500, hovermode='x unified')
    st.plotly_chart(fig_liq, use_container_width=True)

# Liquidity tier summary
liquidity_summary = df_filtered.groupby(['liquidity_tier', 'transaction_type']).agg({
    'transaction_id': 'count',
    'quantity': 'sum'
}).reset_index()

fig_liq2 = px.bar(
    liquidity_summary,
    x='liquidity_tier',
    y='quantity',
    color='transaction_type',
    barmode='group',
    title='Trading Volume by Liquidity Tier and Transaction Type',
    labels={'quantity': 'Total Volume', 'liquidity_tier': 'Liquidity Tier', 'transaction_type': 'Transaction Type'}
)
fig_liq2.update_layout(height=400)
st.plotly_chart(fig_liq2, use_container_width=True)

st.markdown("---")

# ============================================================================
# DATA EXPLORER
# ============================================================================

st.header("🔬 Data Explorer")

with st.expander("📋 View Filtered Data"):
    st.dataframe(df_filtered.head(100), use_container_width=True)
    st.info(f"Showing first 100 of {len(df_filtered):,} filtered records")

    # Download filtered data as PDF
    from io import BytesIO
    from reportlab.lib.pagesizes import letter, landscape
    from reportlab.lib import colors
    from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
    from reportlab.lib.styles import getSampleStyleSheet

    def create_pdf(df):
        buffer = BytesIO()
        doc = SimpleDocTemplate(buffer, pagesize=landscape(letter))
        elements = []

        # Add title
        styles = getSampleStyleSheet()
        title = Paragraph(f"<b>Filtered Trading Data - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</b>", styles['Title'])
        elements.append(title)
        elements.append(Spacer(1, 12))

        # Prepare data for table (first 100 rows)
        data_subset = df.head(100)
        table_data = [data_subset.columns.tolist()] + data_subset.values.tolist()

        # Create table
        table = Table(table_data)
        table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 8),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('FONTSIZE', (0, 1), (-1, -1), 6),
            ('GRID', (0, 0), (-1, -1), 1, colors.black)
        ]))

        elements.append(table)
        doc.build(elements)
        buffer.seek(0)
        return buffer

    pdf_buffer = create_pdf(df_filtered)
    st.download_button(
        label="📥 Download Filtered Data as PDF",
        data=pdf_buffer,
        file_name=f"filtered_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf",
        mime="application/pdf"
    )

# ============================================================================
# FOOTER
# ============================================================================

st.markdown("---")
st.caption("📊 Stock Portfolio Analytics Dashboard | Team 55_0654 | Data Engineering Project")
