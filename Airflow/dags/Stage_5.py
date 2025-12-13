"""
Stock Portfolio Pipeline - Stage 5: Visualization
===================================================

This stage prepares data for visualization and launches the Streamlit dashboard:
- prepare_visualization: Decode categorical columns and prepare aggregated data
- start_visualization_service: Launch Streamlit dashboard application
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
import os

# Default arguments for the DAG
default_args = {
    'owner': 'team_55_0654',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# ============================================================================
# STAGE 5: VISUALIZATION PREPARATION TASKS
# ============================================================================

def prepare_visualization(**context):
    """
    Task 1: Prepare Visualization Data

    This function:
    1. Leverages EXISTING Stage 4 Spark analytics tables (already in PostgreSQL)
    2. Loads encoding lookup tables
    3. Decodes ONLY the main data table for detailed views
    4. Creates ADDITIONAL aggregated views not covered by Stage 4
    5. Decodes Stage 4 analytics tables where needed

    **Metadata:**
    - Input: Stage 4 analytics tables (spark_analytics_1-5, spark_sql_3, spark_sql_5)
    - Input: master_encoding_lookup.json (from Stage 2)
    - Input: integrated_encoded_trades_data.csv (from Stage 2)
    - Output: 7 decoded Stage 4 tables (viz_volume_by_ticker, viz_avg_price_by_sector, etc.)
    - Output: 6 new aggregated views (viz_sector_time, viz_liquidity_time, etc.)
    - Output: 1 main decoded dataset (visualization_main_data)
    """
    import pandas as pd
    import json
    from sqlalchemy import create_engine

    print("="*70)
    print("PREPARING DATA FOR VISUALIZATION")
    print("="*70)

    # Database configuration
    db_host = os.getenv('DB_HOST', 'pgdatabase')
    db_user = os.getenv('DB_USER', 'postgres')
    db_password = os.getenv('DB_PASSWORD', 'postgres')
    db_port = os.getenv('DB_PORT', '5432')
    db_name = os.getenv('DB_NAME', 'Trades_Database')

    data_path = "/opt/airflow/notebook/data/"
    lookups_path = os.path.join(data_path, "lookups")

    # Create database connection
    connection_string = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
    engine = create_engine(connection_string)

    print(f"\n✓ Connected to database: {db_name}")

    # ========================================================================
    # Load Master Encoding Lookup
    # ========================================================================
    print("\n[1/5] Loading encoding lookup tables...")

    master_lookup_path = os.path.join(lookups_path, "master_encoding_lookup.json")
    with open(master_lookup_path, 'r') as f:
        master_lookup = json.load(f)

    # Convert string keys to integers for numeric lookups
    decoded_lookup = {}
    for col, mapping in master_lookup.items():
        decoded_lookup[col] = {int(k) if k.isdigit() else k: v for k, v in mapping.items()}

    print(f"✓ Loaded {len(decoded_lookup)} lookup tables")

    # ========================================================================
    # Decode Stage 4 Analytics Tables (they have encoded values!)
    # ========================================================================
    print("\n[2/5] Decoding Stage 4 analytics tables...")

    # spark_analytics_1: stock_ticker (encoded) + total_volume
    try:
        df_spark1 = pd.read_sql('SELECT * FROM spark_analytics_1', con=engine)
        df_spark1['stock_ticker'] = df_spark1['stock_ticker'].map(decoded_lookup.get('stock_ticker', {}))
        df_spark1.to_sql('viz_volume_by_ticker', con=engine, if_exists='replace', index=False)
        print(f"  ✓ Decoded spark_analytics_1 → viz_volume_by_ticker")
    except Exception as e:
        print(f"  ✗ Could not decode spark_analytics_1: {e}")

    # spark_analytics_2: stock_sector (encoded) + avg_stock_price
    try:
        df_spark2 = pd.read_sql('SELECT * FROM spark_analytics_2', con=engine)
        df_spark2['stock_sector'] = df_spark2['stock_sector'].map(decoded_lookup.get('sector', {}))
        df_spark2 = df_spark2.rename(columns={'stock_sector': 'sector'})
        df_spark2.to_sql('viz_avg_price_by_sector', con=engine, if_exists='replace', index=False)
        print(f"  ✓ Decoded spark_analytics_2 → viz_avg_price_by_sector")
    except Exception as e:
        print(f"  ✗ Could not decode spark_analytics_2: {e}")

    # spark_analytics_3: transaction_type (encoded) + transaction_count (weekends only)
    try:
        df_spark3 = pd.read_sql('SELECT * FROM spark_analytics_3', con=engine)
        df_spark3['transaction_type'] = df_spark3['transaction_type'].map(decoded_lookup.get('transaction_type', {}))
        df_spark3.to_sql('viz_weekend_transactions', con=engine, if_exists='replace', index=False)
        print(f"  ✓ Decoded spark_analytics_3 → viz_weekend_transactions")
    except Exception as e:
        print(f"  ✗ Could not decode spark_analytics_3: {e}")

    # spark_analytics_4: customer_id + transaction_count (>10 transactions)
    try:
        df_spark4 = pd.read_sql('SELECT * FROM spark_analytics_4', con=engine)
        # customer_id is already a number, no decoding needed
        df_spark4.to_sql('viz_active_customers', con=engine, if_exists='replace', index=False)
        print(f"  ✓ Copied spark_analytics_4 → viz_active_customers")
    except Exception as e:
        print(f"  ✗ Could not copy spark_analytics_4: {e}")

    # spark_analytics_5: day + total_trade_amount
    try:
        df_spark5 = pd.read_sql('SELECT * FROM spark_analytics_5', con=engine)
        # day is already decoded (Monday, Tuesday, etc.)
        df_spark5.to_sql('viz_trade_by_day', con=engine, if_exists='replace', index=False)
        print(f"  ✓ Copied spark_analytics_5 → viz_trade_by_day")
    except Exception as e:
        print(f"  ✗ Could not copy spark_analytics_5: {e}")

    # spark_sql_3: period_type + transaction_count (holiday vs non-holiday)
    try:
        df_sql3 = pd.read_sql('SELECT * FROM spark_sql_3', con=engine)
        # Already has readable labels
        df_sql3.to_sql('viz_holiday_comparison', con=engine, if_exists='replace', index=False)
        print(f"  ✓ Copied spark_sql_3 → viz_holiday_comparison")
    except Exception as e:
        print(f"  ✗ Could not copy spark_sql_3: {e}")

    # spark_sql_5: liquidity_tier (encoded) + transaction_type (encoded) + total_amount
    try:
        df_sql5 = pd.read_sql('SELECT * FROM spark_sql_5', con=engine)
        df_sql5['stock_liquidity_tier'] = df_sql5['stock_liquidity_tier'].map(decoded_lookup.get('liquidity_tier', {}))
        df_sql5['transaction_type'] = df_sql5['transaction_type'].map(decoded_lookup.get('transaction_type', {}))
        df_sql5 = df_sql5.rename(columns={'stock_liquidity_tier': 'liquidity_tier'})
        df_sql5.to_sql('viz_liquidity_by_transaction', con=engine, if_exists='replace', index=False)
        print(f"  ✓ Decoded spark_sql_5 → viz_liquidity_by_transaction")
    except Exception as e:
        print(f"  ✗ Could not decode spark_sql_5: {e}")

    print(f"\n✓ Decoded and saved Stage 4 analytics tables for dashboard use")

    # ========================================================================
    # Decode ONLY Main Data (for detailed drill-down views)
    # ========================================================================
    print("\n[3/5] Decoding main data table (batch data only)...")

    # Read the integrated encoded data from Stage 2
    encoded_data_path = os.path.join(data_path, "integrated_encoded_trades_data.csv")
    df_encoded = pd.read_csv(encoded_data_path)
    print(f"✓ Loaded encoded data: {len(df_encoded)} records")

    # Create decoded version
    df_decoded = df_encoded.copy()

    # Decode categorical columns
    categorical_cols = ['stock_ticker', 'transaction_type', 'account_type', 'company_name',
                       'liquidity_tier', 'sector', 'industry', 'day_name', 'month_name',
                       'is_weekend', 'is_holiday']

    for col in categorical_cols:
        if col in df_decoded.columns and col in decoded_lookup:
            df_decoded[col] = df_decoded[col].map(decoded_lookup[col])
            print(f"  • Decoded: {col}")

    # Save decoded data to PostgreSQL
    df_decoded.to_sql('visualization_main_data', con=engine, if_exists='replace', index=False)
    print(f"✓ Saved decoded data to table: visualization_main_data")

    # ========================================================================
    # Create ONLY Missing Views (time-series & customer-specific)
    # ========================================================================
    print("\n[4/5] Creating views NOT covered by Stage 4...")

    df_decoded['date'] = pd.to_datetime(df_decoded['date'])

    # View 1: Sector prices over time (for line chart - Stage 4 doesn't have this)
    sector_time = df_decoded.groupby(['date', 'sector']).agg({
        'stock_price': 'mean',
        'quantity': 'sum'
    }).reset_index()
    sector_time.columns = ['date', 'sector', 'avg_stock_price', 'total_volume']
    sector_time.to_sql('viz_sector_time', con=engine, if_exists='replace', index=False)
    print(f"  • Created: viz_sector_time ({len(sector_time)} records)")

    # View 2: Liquidity over time (for stacked area chart - Stage 4 doesn't have this)
    liquidity_time = df_decoded.groupby(['date', 'liquidity_tier']).agg({
        'quantity': 'sum'
    }).reset_index()
    liquidity_time.columns = ['date', 'liquidity_tier', 'total_volume']
    liquidity_time.to_sql('viz_liquidity_time', con=engine, if_exists='replace', index=False)
    print(f"  • Created: viz_liquidity_time ({len(liquidity_time)} records)")

    # View 3: Top customers by portfolio value (for top 10 chart)
    customer_summary = df_decoded.groupby(['customer_id', 'account_type']).agg({
        'transaction_id': 'count',
        'cumulative_portfolio_value': 'max'
    }).reset_index()
    customer_summary.columns = ['customer_id', 'account_type', 'transaction_count', 'portfolio_value']
    customer_summary = customer_summary.sort_values('portfolio_value', ascending=False)
    customer_summary.to_sql('viz_top_customers', con=engine, if_exists='replace', index=False)
    print(f"  • Created: viz_top_customers ({len(customer_summary)} records)")

    # View 4: Customer transaction distribution (for histogram)
    customer_dist = df_decoded.groupby('customer_id')['transaction_id'].count().reset_index()
    customer_dist.columns = ['customer_id', 'transaction_count']
    customer_dist.to_sql('viz_customer_distribution', con=engine, if_exists='replace', index=False)
    print(f"  • Created: viz_customer_distribution ({len(customer_dist)} records)")

    # View 5: Transaction type summary (for buy/sell charts)
    trans_summary = df_decoded.groupby('transaction_type').agg({
        'transaction_id': 'count',
        'quantity': 'sum'
    }).reset_index()
    trans_summary.columns = ['transaction_type', 'transaction_count', 'total_volume']
    trans_summary.to_sql('viz_transaction_summary', con=engine, if_exists='replace', index=False)
    print(f"  • Created: viz_transaction_summary ({len(trans_summary)} records)")

    # View 6: Sector comparison metrics (for sector dashboard grid)
    sector_comparison = df_decoded.groupby('sector').agg({
        'transaction_id': 'count',
        'quantity': 'sum',
        'stock_price': 'mean',
        'cumulative_portfolio_value': 'sum'
    }).reset_index()
    sector_comparison.columns = ['sector', 'transaction_count', 'total_volume',
                                 'avg_stock_price', 'total_portfolio_value']
    sector_comparison.to_sql('viz_sector_comparison', con=engine, if_exists='replace', index=False)
    print(f"  • Created: viz_sector_comparison ({len(sector_comparison)} records)")

    # ========================================================================
    # Create Visualization Metadata Table
    # ========================================================================
    print("\n[5/5] Creating visualization metadata...")

    metadata = pd.DataFrame([
        {
            'table_name': 'viz_volume_by_ticker',
            'source': 'spark_analytics_1',
            'description': 'Decoded stock ticker trading volumes from Stage 4',
            'row_count': len(pd.read_sql('SELECT COUNT(*) as cnt FROM viz_volume_by_ticker', con=engine).iloc[0]['cnt']) if 'viz_volume_by_ticker' in pd.read_sql("SELECT tablename FROM pg_tables WHERE schemaname='public'", con=engine)['tablename'].values else 0,
            'created_at': datetime.now()
        },
        {
            'table_name': 'viz_avg_price_by_sector',
            'source': 'spark_analytics_2',
            'description': 'Decoded sector average stock prices from Stage 4',
            'row_count': len(pd.read_sql('SELECT COUNT(*) as cnt FROM viz_avg_price_by_sector', con=engine).iloc[0]['cnt']) if 'viz_avg_price_by_sector' in pd.read_sql("SELECT tablename FROM pg_tables WHERE schemaname='public'", con=engine)['tablename'].values else 0,
            'created_at': datetime.now()
        },
        {
            'table_name': 'viz_weekend_transactions',
            'source': 'spark_analytics_3',
            'description': 'Weekend transaction type analysis from Stage 4',
            'row_count': len(pd.read_sql('SELECT COUNT(*) as cnt FROM viz_weekend_transactions', con=engine).iloc[0]['cnt']) if 'viz_weekend_transactions' in pd.read_sql("SELECT tablename FROM pg_tables WHERE schemaname='public'", con=engine)['tablename'].values else 0,
            'created_at': datetime.now()
        },
        {
            'table_name': 'viz_active_customers',
            'source': 'spark_analytics_4',
            'description': 'Active customers with >10 transactions from Stage 4',
            'row_count': len(pd.read_sql('SELECT COUNT(*) as cnt FROM viz_active_customers', con=engine).iloc[0]['cnt']) if 'viz_active_customers' in pd.read_sql("SELECT tablename FROM pg_tables WHERE schemaname='public'", con=engine)['tablename'].values else 0,
            'created_at': datetime.now()
        },
        {
            'table_name': 'viz_trade_by_day',
            'source': 'spark_analytics_5',
            'description': 'Trading activity by day of week from Stage 4',
            'row_count': len(pd.read_sql('SELECT COUNT(*) as cnt FROM viz_trade_by_day', con=engine).iloc[0]['cnt']) if 'viz_trade_by_day' in pd.read_sql("SELECT tablename FROM pg_tables WHERE schemaname='public'", con=engine)['tablename'].values else 0,
            'created_at': datetime.now()
        },
        {
            'table_name': 'viz_holiday_comparison',
            'source': 'spark_sql_3',
            'description': 'Holiday vs non-holiday trading comparison from Stage 4',
            'row_count': len(pd.read_sql('SELECT COUNT(*) as cnt FROM viz_holiday_comparison', con=engine).iloc[0]['cnt']) if 'viz_holiday_comparison' in pd.read_sql("SELECT tablename FROM pg_tables WHERE schemaname='public'", con=engine)['tablename'].values else 0,
            'created_at': datetime.now()
        },
        {
            'table_name': 'viz_liquidity_by_transaction',
            'source': 'spark_sql_5',
            'description': 'Liquidity tier by transaction type from Stage 4',
            'row_count': len(pd.read_sql('SELECT COUNT(*) as cnt FROM viz_liquidity_by_transaction', con=engine).iloc[0]['cnt']) if 'viz_liquidity_by_transaction' in pd.read_sql("SELECT tablename FROM pg_tables WHERE schemaname='public'", con=engine)['tablename'].values else 0,
            'created_at': datetime.now()
        },
        {
            'table_name': 'viz_sector_time',
            'source': 'Stage 5',
            'description': 'Sector stock prices over time (time-series)',
            'row_count': len(sector_time),
            'created_at': datetime.now()
        },
        {
            'table_name': 'viz_liquidity_time',
            'source': 'Stage 5',
            'description': 'Liquidity tier volumes over time (time-series)',
            'row_count': len(liquidity_time),
            'created_at': datetime.now()
        },
        {
            'table_name': 'viz_top_customers',
            'source': 'Stage 5',
            'description': 'Top customers by portfolio value',
            'row_count': len(customer_summary),
            'created_at': datetime.now()
        },
        {
            'table_name': 'viz_customer_distribution',
            'source': 'Stage 5',
            'description': 'Customer transaction count distribution',
            'row_count': len(customer_dist),
            'created_at': datetime.now()
        },
        {
            'table_name': 'viz_transaction_summary',
            'source': 'Stage 5',
            'description': 'Transaction type summary (buy/sell)',
            'row_count': len(trans_summary),
            'created_at': datetime.now()
        },
        {
            'table_name': 'viz_sector_comparison',
            'source': 'Stage 5',
            'description': 'Sector comparison metrics',
            'row_count': len(sector_comparison),
            'created_at': datetime.now()
        },
        {
            'table_name': 'visualization_main_data',
            'source': 'Stage 2',
            'description': 'Decoded main data for detailed drill-down',
            'row_count': len(df_decoded),
            'created_at': datetime.now()
        }
    ])

    metadata.to_sql('visualization_metadata', con=engine, if_exists='replace', index=False)
    print(f"✓ Created visualization_metadata table with {len(metadata)} entries")

    print("\n✓ All visualization data prepared successfully")
    print("="*70)

    engine.dispose()

    return "visualization_data_ready"


# ============================================================================
# DAG DEFINITION
# ============================================================================

# Create the DAG
with DAG(
    dag_id='stage_5_visualization_55_0654',
    default_args=default_args,
    description='Stage 5: Prepare data and launch Streamlit dashboard',
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2025, 12, 12),
    catchup=False,
    tags=['data-engineering', 'visualization', 'streamlit', 'dashboard', 'team-55-0654', 'stage-5'],
) as dag:

    # Task Group: Stage 5 - Visualization
    with TaskGroup(group_id='stage_5_visualization') as stage_5:

        # Task 1: Prepare Visualization Data
        task_prepare_viz = PythonOperator(
            task_id='prepare_visualization',
            python_callable=prepare_visualization,
            provide_context=True,
            doc_md="""
            #### Task Details
            **PythonOperator** that prepares visualization data by:
            1. Decoding Stage 4 Spark analytics tables (7 tables)
            2. Creating additional time-series and customer-specific views (6 views)
            3. Decoding main data table for detailed drill-down analysis

            **Input Tables:**
            - spark_analytics_1 (stock ticker + volume)
            - spark_analytics_2 (sector + avg price)
            - spark_analytics_3 (transaction type for weekends)
            - spark_analytics_4 (active customers)
            - spark_analytics_5 (trading by day of week)
            - spark_sql_3 (holiday comparison)
            - spark_sql_5 (liquidity tier analysis)

            **Output Tables:**
            - 7 decoded Stage 4 tables (viz_volume_by_ticker, viz_avg_price_by_sector, etc.)
            - 6 new views (viz_sector_time, viz_liquidity_time, viz_top_customers, etc.)
            - 1 main decoded dataset (visualization_main_data)
            """,
        )

        # Task 2: Start Streamlit Dashboard
        task_start_dashboard = BashOperator(
            task_id='start_visualization_service',
            bash_command='streamlit run /opt/airflow/dags/dashboard.py --server.port=8501 --server.address=0.0.0.0',
            doc_md="""
            #### Task Details
            **BashOperator** that launches the Streamlit visualization dashboard.

            **Dashboard Features:**
            - 6 Core Visualizations (Trading Volume, Price Trends, Buy/Sell, Day of Week, Customer Distribution, Top 10)
            - 4 Advanced Visualizations (Portfolio KPIs, Sector Comparison, Holiday Analysis, Liquidity Analysis)
            - Interactive filters (date range, stock ticker, sector, customer type)
            - Export functionality (CSV, Excel, JSON)
            - Refresh mechanism with 60-second cache

            **Access:** http://localhost:8502
            """,
        )

        # Define task dependencies
        task_prepare_viz >> task_start_dashboard
