"""
Stage 5: Visualization Preparation Task Functions
==================================================

This module contains all task functions for Stage 5 of the pipeline:
- prepare_visualization: Decode data and create visualization-ready tables
"""

import os
from datetime import datetime


def prepare_visualization(**context):
    """Decode data and create visualization-ready tables"""
    import pandas as pd
    import json
    from sqlalchemy import create_engine

    print("="*70)
    print("PREPARING DATA FOR VISUALIZATION")
    print("="*70)

    db_host = os.getenv('DB_HOST', 'pgdatabase')
    db_user = os.getenv('DB_USER', 'postgres')
    db_password = os.getenv('DB_PASSWORD', 'postgres')
    db_port = os.getenv('DB_PORT', '5432')
    db_name = os.getenv('DB_NAME', 'Trades_Database')

    data_path = "/opt/airflow/notebook/data/"
    lookups_path = os.path.join(data_path, "lookups")

    connection_string = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
    engine = create_engine(connection_string)

    print(f"\n✓ Connected to database: {db_name}")

    print("\n[1/5] Loading encoding lookup tables...")

    master_lookup_path = os.path.join(lookups_path, "master_encoding_lookup.json")
    with open(master_lookup_path, 'r') as f:
        master_lookup = json.load(f)

    decoded_lookup = {}
    for col, mapping in master_lookup.items():
        decoded_lookup[col] = {int(k) if k.isdigit() else k: v for k, v in mapping.items()}

    print(f"✓ Loaded {len(decoded_lookup)} lookup tables")

    print("\n[2/5] Decoding Stage 4 analytics tables...")

    try:
        df_spark1 = pd.read_sql('SELECT * FROM spark_analytics_1', con=engine)
        df_spark1['stock_ticker'] = df_spark1['stock_ticker'].map(decoded_lookup.get('stock_ticker', {}))
        df_spark1.to_sql('viz_volume_by_ticker', con=engine, if_exists='replace', index=False)
        print(f"  ✓ Decoded spark_analytics_1 → viz_volume_by_ticker")
    except Exception as e:
        print(f"  ✗ Could not decode spark_analytics_1: {e}")

    try:
        df_spark2 = pd.read_sql('SELECT * FROM spark_analytics_2', con=engine)
        df_spark2['stock_sector'] = df_spark2['stock_sector'].map(decoded_lookup.get('sector', {}))
        df_spark2 = df_spark2.rename(columns={'stock_sector': 'sector'})
        df_spark2.to_sql('viz_avg_price_by_sector', con=engine, if_exists='replace', index=False)
        print(f"  ✓ Decoded spark_analytics_2 → viz_avg_price_by_sector")
    except Exception as e:
        print(f"  ✗ Could not decode spark_analytics_2: {e}")

    try:
        df_spark3 = pd.read_sql('SELECT * FROM spark_analytics_3', con=engine)
        df_spark3['transaction_type'] = df_spark3['transaction_type'].map(decoded_lookup.get('transaction_type', {}))
        df_spark3.to_sql('viz_weekend_transactions', con=engine, if_exists='replace', index=False)
        print(f"  ✓ Decoded spark_analytics_3 → viz_weekend_transactions")
    except Exception as e:
        print(f"  ✗ Could not decode spark_analytics_3: {e}")

    try:
        df_spark4 = pd.read_sql('SELECT * FROM spark_analytics_4', con=engine)
        df_spark4.to_sql('viz_active_customers', con=engine, if_exists='replace', index=False)
        print(f"  ✓ Copied spark_analytics_4 → viz_active_customers")
    except Exception as e:
        print(f"  ✗ Could not copy spark_analytics_4: {e}")

    try:
        df_spark5 = pd.read_sql('SELECT * FROM spark_analytics_5', con=engine)
        df_spark5.to_sql('viz_trade_by_day', con=engine, if_exists='replace', index=False)
        print(f"  ✓ Copied spark_analytics_5 → viz_trade_by_day")
    except Exception as e:
        print(f"  ✗ Could not copy spark_analytics_5: {e}")

    try:
        df_sql3 = pd.read_sql('SELECT * FROM spark_sql_3', con=engine)
        df_sql3.to_sql('viz_holiday_comparison', con=engine, if_exists='replace', index=False)
        print(f"  ✓ Copied spark_sql_3 → viz_holiday_comparison")
    except Exception as e:
        print(f"  ✗ Could not copy spark_sql_3: {e}")

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

    print("\n[3/5] Decoding main data table...")

    encoded_data_path = os.path.join(data_path, "integrated_encoded_trades_data.csv")
    df_encoded = pd.read_csv(encoded_data_path)
    print(f"✓ Loaded encoded data: {len(df_encoded)} records")

    df_decoded = df_encoded.copy()

    categorical_cols = ['stock_ticker', 'transaction_type', 'account_type', 'company_name',
                       'liquidity_tier', 'sector', 'industry', 'day_name', 'month_name',
                       'is_weekend', 'is_holiday']

    for col in categorical_cols:
        if col in df_decoded.columns and col in decoded_lookup:
            df_decoded[col] = df_decoded[col].map(decoded_lookup[col])
            print(f"  • Decoded: {col}")

    df_decoded.to_sql('visualization_main_data', con=engine, if_exists='replace', index=False)
    print(f"✓ Saved decoded data to table: visualization_main_data")

    print("\n[4/5] Creating additional visualization views...")

    df_decoded['date'] = pd.to_datetime(df_decoded['date'])

    sector_time = df_decoded.groupby(['date', 'sector']).agg({
        'stock_price': 'mean',
        'quantity': 'sum'
    }).reset_index()
    sector_time.columns = ['date', 'sector', 'avg_stock_price', 'total_volume']
    sector_time.to_sql('viz_sector_time', con=engine, if_exists='replace', index=False)
   
    liquidity_time = df_decoded.groupby(['date', 'liquidity_tier']).agg({
        'quantity': 'sum'
    }).reset_index()
    liquidity_time.columns = ['date', 'liquidity_tier', 'total_volume']
    liquidity_time.to_sql('viz_liquidity_time', con=engine, if_exists='replace', index=False)
   
    customer_summary = df_decoded.groupby(['customer_id', 'account_type']).agg({
        'transaction_id': 'count',
        'cumulative_portfolio_value': 'max'
    }).reset_index()
    customer_summary.columns = ['customer_id', 'account_type', 'transaction_count', 'portfolio_value']
    customer_summary = customer_summary.sort_values('portfolio_value', ascending=False)
    customer_summary.to_sql('viz_top_customers', con=engine, if_exists='replace', index=False)
   
    customer_dist = df_decoded.groupby('customer_id')['transaction_id'].count().reset_index()
    customer_dist.columns = ['customer_id', 'transaction_count']
    customer_dist.to_sql('viz_customer_distribution', con=engine, if_exists='replace', index=False)
   
    trans_summary = df_decoded.groupby('transaction_type').agg({
        'transaction_id': 'count',
        'quantity': 'sum'
    }).reset_index()
    trans_summary.columns = ['transaction_type', 'transaction_count', 'total_volume']
    trans_summary.to_sql('viz_transaction_summary', con=engine, if_exists='replace', index=False)

    sector_comparison = df_decoded.groupby('sector').agg({
        'transaction_id': 'count',
        'quantity': 'sum',
        'stock_price': 'mean',
        'cumulative_portfolio_value': 'sum'
    }).reset_index()
    sector_comparison.columns = ['sector', 'transaction_count', 'total_volume',
                                 'avg_stock_price', 'total_portfolio_value']
    sector_comparison.to_sql('viz_sector_comparison', con=engine, if_exists='replace', index=False)

    print("\n[5/5] Creating visualization metadata...")

    metadata = pd.DataFrame([
        {'table_name': 'viz_volume_by_ticker', 'source': 'spark_analytics_1',
         'description': 'Decoded stock ticker trading volumes', 'created_at': datetime.now()},
        {'table_name': 'viz_avg_price_by_sector', 'source': 'spark_analytics_2',
         'description': 'Decoded sector average stock prices', 'created_at': datetime.now()},
        {'table_name': 'viz_weekend_transactions', 'source': 'spark_analytics_3',
         'description': 'Weekend transaction type analysis', 'created_at': datetime.now()},
        {'table_name': 'viz_active_customers', 'source': 'spark_analytics_4',
         'description': 'Active customers with >10 transactions', 'created_at': datetime.now()},
        {'table_name': 'viz_trade_by_day', 'source': 'spark_analytics_5',
         'description': 'Trading activity by day of week', 'created_at': datetime.now()},
        {'table_name': 'viz_sector_time', 'source': 'Stage 5',
         'description': 'Sector stock prices over time', 'row_count': len(sector_time), 'created_at': datetime.now()},
        {'table_name': 'viz_liquidity_time', 'source': 'Stage 5',
         'description': 'Liquidity tier volumes over time', 'row_count': len(liquidity_time), 'created_at': datetime.now()},
        {'table_name': 'viz_top_customers', 'source': 'Stage 5',
         'description': 'Top customers by portfolio value', 'row_count': len(customer_summary), 'created_at': datetime.now()},
        {'table_name': 'visualization_main_data', 'source': 'Stage 2',
         'description': 'Decoded main data for detailed drill-down', 'row_count': len(df_decoded), 'created_at': datetime.now()}
    ])

    metadata.to_sql('visualization_metadata', con=engine, if_exists='replace', index=False)
    print(f"✓ Created visualization_metadata table with {len(metadata)} entries")

    print("✓ All visualization data prepared successfully")
    print("="*70)

    engine.dispose()
