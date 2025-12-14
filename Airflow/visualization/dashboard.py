"""
Real-Time Stock Portfolio Analytics Dashboard
==============================================

Interactive Streamlit dashboard for stock portfolio analysis.

Features:
- 8 Specific visualization queries
- Interactive filters and drill-down
- Real-time data refresh
- HTML export functionality
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from sqlalchemy import create_engine
import os
from datetime import datetime, timedelta
import io

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

@st.cache_resource
def get_database_engine():
    """Create and cache database connection"""
    db_host = os.getenv('DB_HOST', 'pgdatabase')
    db_user = os.getenv('DB_USER', 'postgres')
    db_password = os.getenv('DB_PASSWORD', 'postgres')
    db_port = os.getenv('DB_PORT', '5432')
    db_name = os.getenv('DB_NAME', 'Trades_Database')

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


def create_download_button(fig, chart_name):
    """Create export button for HTML"""
    buffer = io.StringIO()
    fig.write_html(buffer)
    html_bytes = buffer.getvalue().encode()
    
    st.download_button(
        label=f"📥 Download Chart as HTML",
        data=html_bytes,
        file_name=f"{chart_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html",
        mime="text/html",
        key=f"html_{chart_name}",
        use_container_width=True
    )


# ============================================================================
# HEADER AND TITLE
# ============================================================================

st.title("📊 Stock Portfolio Analytics - 8 Key Insights")
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

# Load metadata
metadata = load_data('viz_metadata')

# Initialize filter variables
selected_ticker = 'All'
selected_sector = 'All'
selected_account_type = 'All'
selected_transaction = 'All'
start_date = None
end_date = None

# Date Range Filter (for Query 2)
st.sidebar.subheader("📅 Date Range")
st.sidebar.info("Applies to: Stock Price Trends by Sector")

# Load query 2 data to get date range
df_query2 = load_data('viz_stock_price_trends_by_sector')
if not df_query2.empty:
    df_query2['date'] = pd.to_datetime(df_query2['date'])
    min_date = df_query2['date'].min().date()
    max_date = df_query2['date'].max().date()

    date_range = st.sidebar.date_input(
        "Select date range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date
    )
    
    if len(date_range) == 2:
        start_date, end_date = date_range

# Stock Ticker Filter (for Query 1)
st.sidebar.subheader("📈 Stock Ticker")
st.sidebar.info("Applies to: Trading Volume by Stock Ticker")
df_query1 = load_data('viz_trading_volume_by_ticker')
if not df_query1.empty:
    all_tickers = ['All'] + sorted(df_query1['stock_ticker'].dropna().unique().tolist())
    selected_ticker = st.sidebar.selectbox("Select stock ticker", all_tickers)

# Sector Filter (for Queries 2, 7)
st.sidebar.subheader("🏢 Sector")
st.sidebar.info("Applies to: Stock Price Trends, Sector Comparison")
if not df_query2.empty:
    all_sectors = ['All'] + sorted(df_query2['sector'].dropna().unique().tolist())
    selected_sector = st.sidebar.selectbox("Select sector", all_sectors)

# Customer Type Filter (for Query 6)
st.sidebar.subheader("👥 Customer Type")
st.sidebar.info("Applies to: Top 10 Customers")
df_query6 = load_data('viz_top_10_customers_by_trade_amount')
if not df_query6.empty:
    all_account_types = ['All'] + sorted(df_query6['account_type'].dropna().unique().tolist())
    selected_account_type = st.sidebar.selectbox("Select account type", all_account_types)

# Transaction Type Filter (for Query 3)
st.sidebar.subheader("💱 Transaction Type")
st.sidebar.info("Applies to: Buy vs Sell Transactions")
transaction_types = ['All', 'BUY', 'SELL']
selected_transaction = st.sidebar.selectbox("Select transaction type", transaction_types)

st.sidebar.markdown("---")

# Display metadata info
if not metadata.empty:
    st.sidebar.success(f"✅ {len(metadata)} visualizations loaded")
    if 'created_at' in metadata.columns:
        last_updated = metadata['created_at'].max()
        st.sidebar.markdown(f"**Last Updated:** {last_updated}")

# ============================================================================
# QUERY 1: Trading Volume by Stock Ticker
# ============================================================================

st.header("1️⃣ Trading Volume by Stock Ticker")

df_viz1 = load_data('viz_trading_volume_by_ticker')

if not df_viz1.empty:
    # Apply ticker filter
    if selected_ticker != 'All':
        df_viz1_filtered = df_viz1[df_viz1['stock_ticker'] == selected_ticker]
    else:
        df_viz1_filtered = df_viz1.copy()
    
    df_viz1_filtered = df_viz1_filtered.sort_values('total_volume', ascending=False).head(20)

    fig1 = px.bar(
        df_viz1_filtered,
        x='stock_ticker',
        y='total_volume',
        title='Top 20 Stock Tickers by Trading Volume',
        labels={'total_volume': 'Total Volume', 'stock_ticker': 'Stock Ticker'},
        color='total_volume',
        color_continuous_scale='Blues',
        text='total_volume'
    )
    fig1.update_traces(texttemplate='%{text:,.0f}', textposition='outside')
    fig1.update_layout(height=500, showlegend=False)
    st.plotly_chart(fig1, use_container_width=True)
    
    # Export button
    create_download_button(fig1, "query1_trading_volume_by_ticker")
    
    # Show summary
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Tickers", len(df_viz1))
    with col2:
        st.metric("Total Volume", f"{df_viz1['total_volume'].sum():,.0f}")
    with col3:
        st.metric("Avg Volume per Ticker", f"{df_viz1['total_volume'].mean():,.0f}")
else:
    st.error("❌ No data available. Please run Stage 5 DAG first.")

st.markdown("---")

# ============================================================================
# QUERY 2: Stock Price Trends by Sector
# ============================================================================

st.header("2️⃣ Stock Price Trends by Sector")

if not df_query2.empty:
    # Apply filters
    df_viz2_filtered = df_query2.copy()
    
    if start_date and end_date:
        df_viz2_filtered = df_viz2_filtered[
            (df_viz2_filtered['date'].dt.date >= start_date) &
            (df_viz2_filtered['date'].dt.date <= end_date)
        ]
    
    if selected_sector != 'All':
        df_viz2_filtered = df_viz2_filtered[df_viz2_filtered['sector'] == selected_sector]

    fig2 = px.line(
        df_viz2_filtered,
        x='date',
        y='avg_stock_price',
        color='sector',
        title='Stock Price Trends by Sector Over Time',
        labels={'avg_stock_price': 'Average Stock Price ($)', 'date': 'Date', 'sector': 'Sector'},
        markers=True
    )
    fig2.update_layout(height=500, hovermode='x unified')
    st.plotly_chart(fig2, use_container_width=True)
    
    # Export button
    create_download_button(fig2, "query2_stock_price_trends_by_sector")
    
    # Show summary
    sector_summary = df_viz2_filtered.groupby('sector')['avg_stock_price'].agg(['mean', 'min', 'max']).reset_index()
    sector_summary.columns = ['Sector', 'Avg Price', 'Min Price', 'Max Price']
    st.dataframe(sector_summary, use_container_width=True)
else:
    st.error("❌ No data available. Please run Stage 5 DAG first.")

st.markdown("---")

# ============================================================================
# QUERY 3: Buy vs Sell Transactions
# ============================================================================

st.header("3️⃣ Buy vs Sell Transactions")

df_viz3 = load_data('viz_buy_vs_sell_transactions')

if not df_viz3.empty:
    # Apply transaction type filter
    if selected_transaction != 'All':
        df_viz3_filtered = df_viz3[df_viz3['transaction_type'] == selected_transaction]
    else:
        df_viz3_filtered = df_viz3.copy()

    col_left, col_right = st.columns(2)

    with col_left:
        fig3a = px.pie(
            df_viz3_filtered,
            values='transaction_count',
            names='transaction_type',
            title='Transaction Count Distribution',
            color_discrete_sequence=px.colors.qualitative.Set2,
            hole=0.4
        )
        fig3a.update_traces(textposition='inside', textinfo='percent+label')
        fig3a.update_layout(height=400)
        st.plotly_chart(fig3a, use_container_width=True)

    with col_right:
        fig3b = px.bar(
            df_viz3_filtered,
            x='transaction_type',
            y='total_volume',
            title='Trading Volume by Transaction Type',
            labels={'total_volume': 'Total Volume', 'transaction_type': 'Transaction Type'},
            color='transaction_type',
            color_discrete_sequence=px.colors.qualitative.Pastel,
            text='total_volume'
        )
        fig3b.update_traces(texttemplate='%{text:,.0f}', textposition='outside')
        fig3b.update_layout(height=400, showlegend=False)
        st.plotly_chart(fig3b, use_container_width=True)
    
    # Export buttons for both charts
    col_exp1, col_exp2 = st.columns(2)
    with col_exp1:
        create_download_button(fig3a, "query3_buy_vs_sell_pie")
    with col_exp2:
        create_download_button(fig3b, "query3_buy_vs_sell_bar")
    
    # Show detailed table
    st.dataframe(df_viz3_filtered, use_container_width=True)
else:
    st.error("❌ No data available. Please run Stage 5 DAG first.")

st.markdown("---")

# ============================================================================
# QUERY 4: Trading Activity by Day of Week
# ============================================================================

st.header("4️⃣ Trading Activity by Day of Week")

df_viz4 = load_data('viz_trading_activity_by_day')

if not df_viz4.empty:
    # Ensure proper day ordering
    day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']
    df_viz4['day_order'] = df_viz4['day_name'].map({day: i for i, day in enumerate(day_order)})
    df_viz4 = df_viz4.sort_values('day_order')

    fig4 = make_subplots(specs=[[{"secondary_y": True}]])

    fig4.add_trace(
        go.Bar(
            x=df_viz4['day_name'],
            y=df_viz4['transaction_count'],
            name='Transaction Count',
            marker_color='lightblue',
            text=df_viz4['transaction_count'],
            texttemplate='%{text:,.0f}',
            textposition='outside'
        ),
        secondary_y=False
    )

    fig4.add_trace(
        go.Scatter(
            x=df_viz4['day_name'],
            y=df_viz4['total_volume'],
            name='Total Volume',
            mode='lines+markers',
            marker=dict(size=10, color='orange'),
            line=dict(width=3)
        ),
        secondary_y=True
    )

    fig4.update_layout(
        title='Trading Activity by Day of Week',
        xaxis_title='Day of Week',
        height=500,
        hovermode='x unified'
    )
    fig4.update_yaxes(title_text="Transaction Count", secondary_y=False)
    fig4.update_yaxes(title_text="Total Volume", secondary_y=True)
    
    st.plotly_chart(fig4, use_container_width=True)
    
    # Export button
    create_download_button(fig4, "query4_trading_activity_by_day")
    
    # Show table
    display_df = df_viz4[['day_name', 'transaction_count', 'total_volume', 'avg_price']].copy()
    display_df.columns = ['Day', 'Transaction Count', 'Total Volume', 'Avg Price']
    st.dataframe(display_df, use_container_width=True)
else:
    st.error("❌ No data available. Please run Stage 5 DAG first.")

st.markdown("---")

# ============================================================================
# QUERY 5: Customer Transaction Distribution
# ============================================================================

st.header("5️⃣ Customer Transaction Distribution")

df_viz5 = load_data('viz_customer_transaction_distribution')

if not df_viz5.empty:
    fig5 = px.histogram(
        df_viz5,
        x='transaction_count',
        nbins=50,
        title='Distribution of Transactions per Customer',
        labels={'transaction_count': 'Number of Transactions', 'count': 'Number of Customers'},
        color_discrete_sequence=['#636EFA']
    )
    fig5.update_layout(height=500, showlegend=False)
    st.plotly_chart(fig5, use_container_width=True)

    # Export button
    create_download_button(fig5, "query5_customer_transaction_distribution")

    # Statistics
    col_stat1, col_stat2, col_stat3, col_stat4 = st.columns(4)
    with col_stat1:
        st.metric("Total Customers", f"{len(df_viz5):,}")
    with col_stat2:
        st.metric("Avg Transactions/Customer", f"{df_viz5['transaction_count'].mean():.1f}")
    with col_stat3:
        st.metric("Max Transactions", f"{df_viz5['transaction_count'].max():,}")
    with col_stat4:
        st.metric("Total Portfolio Value", f"${df_viz5['portfolio_value'].sum():,.2f}")
    
    # Show customers with most transactions
    st.subheader("Top 20 Most Active Customers")
    top_active = df_viz5.nlargest(20, 'transaction_count')[['customer_id', 'transaction_count', 'total_volume', 'portfolio_value']]
    top_active.columns = ['Customer ID', 'Transactions', 'Total Volume', 'Portfolio Value']
    st.dataframe(top_active, use_container_width=True)
else:
    st.error("❌ No data available. Please run Stage 5 DAG first.")

st.markdown("---")

# ============================================================================
# QUERY 6: Top 10 Customers by Trade Amount
# ============================================================================

st.header("6️⃣ Top 10 Customers by Trade Amount")

if not df_query6.empty:
    # Apply account type filter
    if selected_account_type != 'All':
        df_viz6_filtered = df_query6[df_query6['account_type'] == selected_account_type].head(10)
    else:
        df_viz6_filtered = df_query6.head(10).copy()
    
    df_viz6_filtered['customer_id'] = df_viz6_filtered['customer_id'].astype(str)

    fig6 = px.bar(
        df_viz6_filtered,
        x='total_trade_amount',
        y='customer_id',
        orientation='h',
        title=f'Top 10 Customers by Trade Amount {f"({selected_account_type})" if selected_account_type != "All" else ""}',
        labels={'total_trade_amount': 'Total Trade Amount ($)', 'customer_id': 'Customer ID'},
        color='total_trade_amount',
        color_continuous_scale='Viridis',
        text='total_trade_amount'
    )
    fig6.update_traces(texttemplate='$%{text:,.0f}', textposition='outside')
    fig6.update_layout(height=500, showlegend=False, yaxis={'categoryorder': 'total ascending'})
    st.plotly_chart(fig6, use_container_width=True)
    
    # Export button
    create_download_button(fig6, "query6_top_10_customers_by_trade_amount")
    
    # Show detailed table
    display_df = df_viz6_filtered[['customer_id', 'account_type', 'total_trade_amount', 'transaction_count', 'total_volume']]
    display_df.columns = ['Customer ID', 'Account Type', 'Total Trade Amount', 'Transactions', 'Total Volume']
    st.dataframe(display_df, use_container_width=True)
else:
    st.error("❌ No data available. Please run Stage 5 DAG first.")

st.markdown("---")

# ============================================================================
# QUERY 7: Sector Comparison Dashboard (Grouped Bar Charts)
# ============================================================================

st.header("7️⃣ Sector Comparison Dashboard")

df_viz7 = load_data('viz_sector_comparison_dashboard')

if not df_viz7.empty:
    # Apply sector filter
    if selected_sector != 'All':
        df_viz7_filtered = df_viz7[df_viz7['sector'] == selected_sector]
    else:
        df_viz7_filtered = df_viz7.copy()
    
    df_viz7_filtered = df_viz7_filtered.sort_values('total_portfolio_value', ascending=False)

    # Create 2x2 subplot dashboard
    fig7 = make_subplots(
        rows=2, cols=2,
        subplot_titles=('Transaction Count by Sector', 'Trading Volume by Sector',
                        'Avg Stock Price by Sector', 'Total Portfolio Value by Sector'),
        specs=[[{'type': 'bar'}, {'type': 'bar'}],
               [{'type': 'bar'}, {'type': 'bar'}]]
    )

    # Chart 1: Transaction count
    fig7.add_trace(
        go.Bar(
            x=df_viz7_filtered['sector'],
            y=df_viz7_filtered['transaction_count'],
            name='Transactions',
            marker_color='lightblue',
            text=df_viz7_filtered['transaction_count'],
            texttemplate='%{text:,.0f}',
            textposition='outside'
        ),
        row=1, col=1
    )

    # Chart 2: Trading volume
    fig7.add_trace(
        go.Bar(
            x=df_viz7_filtered['sector'],
            y=df_viz7_filtered['total_volume'],
            name='Volume',
            marker_color='lightgreen',
            text=df_viz7_filtered['total_volume'],
            texttemplate='%{text:,.0f}',
            textposition='outside'
        ),
        row=1, col=2
    )

    # Chart 3: Average stock price
    fig7.add_trace(
        go.Bar(
            x=df_viz7_filtered['sector'],
            y=df_viz7_filtered['avg_stock_price'],
            name='Avg Price',
            marker_color='lightsalmon',
            text=df_viz7_filtered['avg_stock_price'],
            texttemplate='$%{text:.2f}',
            textposition='outside'
        ),
        row=2, col=1
    )

    # Chart 4: Portfolio value
    fig7.add_trace(
        go.Bar(
            x=df_viz7_filtered['sector'],
            y=df_viz7_filtered['total_portfolio_value'],
            name='Portfolio Value',
            marker_color='plum',
            text=df_viz7_filtered['total_portfolio_value'],
            texttemplate='$%{text:,.0f}',
            textposition='outside'
        ),
        row=2, col=2
    )

    fig7.update_layout(height=800, showlegend=False, title_text="Comprehensive Sector Analysis")
    st.plotly_chart(fig7, use_container_width=True)
    
    # Export button
    create_download_button(fig7, "query7_sector_comparison_dashboard")
    
    # Show detailed table
    display_df = df_viz7_filtered.copy()
    display_df.columns = ['Sector', 'Transaction Count', 'Total Volume', 'Avg Stock Price', 'Total Portfolio Value']
    st.dataframe(display_df, use_container_width=True)
else:
    st.error("❌ No data available. Please run Stage 5 DAG first.")

st.markdown("---")

# ============================================================================
# QUERY 8: Holiday vs Non-Holiday Trading Patterns
# ============================================================================

st.header("8️⃣ Holiday vs Non-Holiday Trading Patterns")

df_viz8 = load_data('viz_holiday_vs_nonholiday_patterns')

if not df_viz8.empty:
    # Convert is_holiday to readable format
    df_viz8['period'] = df_viz8['is_holiday'].map({True: 'Holiday', False: 'Non-Holiday', 1: 'Holiday', 0: 'Non-Holiday'})
    
    col1, col2 = st.columns(2)

    with col1:
        fig8a = px.bar(
            df_viz8,
            x='period',
            y='transaction_count',
            title='Transaction Count: Holiday vs Non-Holiday',
            labels={'transaction_count': 'Transaction Count', 'period': 'Period'},
            color='period',
            color_discrete_map={'Holiday': 'red', 'Non-Holiday': 'green'},
            text='transaction_count'
        )
        fig8a.update_traces(texttemplate='%{text:,.0f}', textposition='outside')
        fig8a.update_layout(height=400, showlegend=False)
        st.plotly_chart(fig8a, use_container_width=True)

    with col2:
        fig8b = px.pie(
            df_viz8,
            values='total_volume',
            names='period',
            title='Trading Volume Distribution',
            color='period',
            color_discrete_map={'Holiday': 'red', 'Non-Holiday': 'green'},
            hole=0.4
        )
        fig8b.update_traces(textposition='inside', textinfo='percent+label')
        fig8b.update_layout(height=400)
        st.plotly_chart(fig8b, use_container_width=True)

    # Comparative metrics
    st.subheader("📊 Comparative Analysis")
    
    col_m1, col_m2, col_m3, col_m4 = st.columns(4)
    
    with col_m1:
        st.metric("Total Periods", len(df_viz8))
    with col_m2:
        total_trans = df_viz8['transaction_count'].sum()
        st.metric("Total Transactions", f"{total_trans:,}")
    with col_m3:
        total_vol = df_viz8['total_volume'].sum()
        st.metric("Total Volume", f"{total_vol:,.0f}")
    with col_m4:
        avg_price = df_viz8['avg_stock_price'].mean()
        st.metric("Overall Avg Price", f"${avg_price:.2f}")
    
    # Detailed comparison table
    st.subheader("📋 Detailed Comparison")
    display_df = df_viz8[['period', 'transaction_count', 'total_volume', 'avg_stock_price', 'total_portfolio_value']].copy()
    if 'transaction_percentage' in df_viz8.columns:
        display_df['transaction_percentage'] = df_viz8['transaction_percentage']
        display_df.columns = ['Period', 'Transaction Count', 'Total Volume', 'Avg Stock Price', 'Total Portfolio Value', 'Transaction %']
    else:
        display_df.columns = ['Period', 'Transaction Count', 'Total Volume', 'Avg Stock Price', 'Total Portfolio Value']
    
    st.dataframe(display_df, use_container_width=True)
    
    # Additional visualization: Grouped bar chart
    fig8c = go.Figure(data=[
        go.Bar(name='Transaction Count', x=df_viz8['period'], y=df_viz8['transaction_count'], marker_color='indianred'),
        go.Bar(name='Total Volume', x=df_viz8['period'], y=df_viz8['total_volume'], marker_color='lightsalmon')
    ])
    fig8c.update_layout(
        barmode='group',
        title='Holiday vs Non-Holiday: Grouped Comparison',
        height=400,
        xaxis_title='Period',
        yaxis_title='Count / Volume'
    )
    st.plotly_chart(fig8c, use_container_width=True)
    
    # Export buttons for all three charts
    col_exp1, col_exp2, col_exp3 = st.columns(3)
    with col_exp1:
        create_download_button(fig8a, "query8_holiday_bar")
    with col_exp2:
        create_download_button(fig8b, "query8_holiday_pie")
    with col_exp3:
        create_download_button(fig8c, "query8_holiday_grouped")
else:
    st.error("❌ No data available. Please run Stage 5 DAG first.")

st.markdown("---")

# ============================================================================
# FOOTER
# ============================================================================

st.markdown("---")
st.caption("📊 Stock Portfolio Analytics Dashboard - 8 Key Insights | Data Engineering Project")
