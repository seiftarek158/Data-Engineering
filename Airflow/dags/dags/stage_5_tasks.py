"""
Stage 5: Visualization Preparation Task Functions
==================================================

This module contains all task functions for Stage 5 of the pipeline:
- prepare_visualization: Create 8 specific visualization queries using FINAL_STOCKS.csv
"""

import os
from datetime import datetime


def prepare_visualization(**context):
    """Create 8 specific visualization queries using FINAL_STOCKS.csv with master lookup decoding"""
    import pandas as pd
    import json
    from sqlalchemy import create_engine

    print("="*70)
    print("PREPARING DATA FOR VISUALIZATION - 8 SPECIFIC QUERIES")
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

    # Load master encoding lookup
    print("\n[1/9] Loading master encoding lookup...")
    master_lookup_path = os.path.join(lookups_path, "master_encoding_lookup.csv")
    master_lookup_df = pd.read_csv(master_lookup_path)

    # Convert CSV format to dictionary lookup
    decoded_lookup = {}

    # Process label-encoded columns (stock_ticker, transaction_type, etc.)
    label_cols = ['stock_ticker', 'transaction_type', 'customer_account_type', 'stock_sector', 'stock_industry']
    for col in label_cols:
        col_data = master_lookup_df[master_lookup_df['Column Name'] == col]
        if not col_data.empty and 'Encoded Value' in col_data.columns:
            decoded_lookup[col] = dict(zip(col_data['Encoded Value'], col_data['Original Value']))

    # Process binary columns (is_weekend, is_holiday)
    binary_cols = ['is_weekend', 'is_holiday']
    for col in binary_cols:
        col_data = master_lookup_df[master_lookup_df['Column Name'] == col]
        if not col_data.empty and 'Encoded Value' in col_data.columns:
            decoded_lookup[col] = dict(zip(col_data['Encoded Value'], col_data['Original Value']))

    # Process one-hot encoded column (day_name)
    day_data = master_lookup_df[master_lookup_df['Column Name'] == 'day_name']
    if not day_data.empty and 'Original Value' in day_data.columns:
        # For one-hot encoding, we need to map the original day names
        decoded_lookup['day_name'] = dict(enumerate(day_data['Original Value'].values))

    print(f"✓ Loaded {len(decoded_lookup)} lookup mappings from CSV")


    # Load FINAL_STOCKS.csv
    print("\n[2/9] Loading FINAL_STOCKS.csv...")
    encoded_data_path = os.path.join(data_path, "FINAL_STOCKS.csv")
    df = pd.read_csv(encoded_data_path)
    print(f"✓ Loaded {len(df)} records from FINAL_STOCKS.csv")

    # Decode all categorical columns using master lookup
    print("\n[3/9] Decoding categorical columns...")
    df_decoded = df.copy()
    
    # Decode each column using ACTUAL column names from FINAL_STOCKS.csv
    categorical_mappings = {
        'stock_ticker': 'stock_ticker',
        'transaction_type': 'transaction_type',
        'customer_account_type': 'customer_account_type',
        'stock_sector': 'stock_sector',
        'stock_industry': 'stock_industry',
        'is_weekend': 'is_weekend',
        'is_holiday': 'is_holiday'
    }
    
    for df_col, lookup_key in categorical_mappings.items():
        if df_col in df_decoded.columns and lookup_key in decoded_lookup:
            df_decoded[df_col] = df_decoded[df_col].map(decoded_lookup[lookup_key])
            print(f"  ✓ Decoded: {df_col}")
    
    # Reconstruct day_name from one-hot encoded day_* columns
    day_columns = [col for col in df_decoded.columns if col.startswith('day_')]
    if day_columns:
        def get_day_name(row):
            for col in day_columns:
                if row[col] == 1:
                    return col.replace('day_', '')
            return None
        df_decoded['day_name'] = df_decoded.apply(get_day_name, axis=1)
        print(f"  ✓ Reconstructed: day_name from one-hot encoded columns")

    # Convert timestamp to datetime and extract date
    df_decoded['timestamp'] = pd.to_datetime(df_decoded['timestamp'])
    df_decoded['date'] = df_decoded['timestamp'].dt.date
    
    print(f"✓ All categorical columns decoded successfully")

    # Query 1: Trading Volume by Stock Ticker (from Spark Analytics 1)
    print("\n[4/9] Creating Query 1: Trading Volume by Stock Ticker...")
    print("  Using existing spark_analytics_1 from Stage 4...")
    try:
        viz_1 = pd.read_sql('SELECT * FROM spark_analytics_1', con=engine)
        viz_1['stock_ticker'] = viz_1['stock_ticker'].map(decoded_lookup.get('stock_ticker', {}))
        viz_1.to_sql('viz_trading_volume_by_ticker', con=engine, if_exists='replace', index=False)
        print(f"  ✓ Decoded and created viz_trading_volume_by_ticker ({len(viz_1)} rows)")
    except Exception as e:
        print(f"  ⚠ Could not use spark_analytics_1, creating from scratch...")
        viz_1 = df_decoded.groupby('stock_ticker').agg({
            'quantity': 'sum',
            'transaction_id': 'count'
        }).reset_index()
        viz_1.columns = ['stock_ticker', 'total_volume', 'transaction_count']
        viz_1 = viz_1.sort_values('total_volume', ascending=False)
        viz_1.to_sql('viz_trading_volume_by_ticker', con=engine, if_exists='replace', index=False)
        print(f"  ✓ Created viz_trading_volume_by_ticker ({len(viz_1)} rows)")

    # Query 2: Stock Price Trends by Sector (from Spark Analytics 2 + time dimension)
    print("\n[5/9] Creating Query 2: Stock Price Trends by Sector...")
    print("  Using FINAL_STOCKS.csv for time-series data...")
    viz_2 = df_decoded.groupby(['date', 'stock_sector']).agg({
        'stock_price': 'mean',
        'quantity': 'sum'
    }).reset_index()
    viz_2.columns = ['date', 'sector', 'avg_stock_price', 'total_volume']
    viz_2 = viz_2.sort_values(['sector', 'date'])
    viz_2.to_sql('viz_stock_price_trends_by_sector', con=engine, if_exists='replace', index=False)
    print(f"  ✓ Created viz_stock_price_trends_by_sector ({len(viz_2)} rows)")

    # Query 3: Buy vs Sell Transactions (NOT using Spark)
    print("\n[6/9] Creating Query 3: Buy vs Sell Transactions...")
    viz_3 = df_decoded.groupby('transaction_type').agg({
        'transaction_id': 'count',
        'quantity': 'sum',
        'stock_price': 'mean'
    }).reset_index()
    viz_3.columns = ['transaction_type', 'transaction_count', 'total_volume', 'avg_price']
    viz_3.to_sql('viz_buy_vs_sell_transactions', con=engine, if_exists='replace', index=False)
    print(f"  ✓ Created viz_buy_vs_sell_transactions ({len(viz_3)} rows)")

    # Query 4: Trading Activity by Day of Week (from Spark Analytics 5)
    print("\n[7/9] Creating Query 4: Trading Activity by Day of Week...")
    print("  Using existing spark_analytics_5 from Stage 4...")
    try:
        viz_4 = pd.read_sql('SELECT * FROM spark_analytics_5', con=engine)
        viz_4 = viz_4.rename(columns={'day': 'day_name'})
        # Add additional metrics from FINAL_STOCKS
        day_metrics = df_decoded.groupby('day_name').agg({
            'transaction_id': 'count',
            'quantity': 'sum',
            'stock_price': 'mean'
        }).reset_index()
        day_metrics.columns = ['day_name', 'transaction_count', 'total_volume', 'avg_price']
        viz_4 = viz_4.merge(day_metrics, on='day_name', how='left')
        
        # Order days properly
        day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']
        viz_4['day_order'] = viz_4['day_name'].map({day: i for i, day in enumerate(day_order)})
        viz_4 = viz_4.sort_values('day_order').drop('day_order', axis=1)
        viz_4.to_sql('viz_trading_activity_by_day', con=engine, if_exists='replace', index=False)
        print(f"  ✓ Enhanced and created viz_trading_activity_by_day ({len(viz_4)} rows)")
    except Exception as e:
        print(f"  ⚠ Could not use spark_analytics_5, creating from scratch...")
        viz_4 = df_decoded.groupby('day_name').agg({
            'transaction_id': 'count',
            'quantity': 'sum',
            'stock_price': 'mean'
        }).reset_index()
        viz_4.columns = ['day_name', 'transaction_count', 'total_volume', 'avg_price']
        
        # Order days properly
        day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']
        viz_4['day_order'] = viz_4['day_name'].map({day: i for i, day in enumerate(day_order)})
        viz_4 = viz_4.sort_values('day_order').drop('day_order', axis=1)
        viz_4.to_sql('viz_trading_activity_by_day', con=engine, if_exists='replace', index=False)
        print(f"  ✓ Created viz_trading_activity_by_day ({len(viz_4)} rows)")

    # Query 5: Customer Transaction Distribution (enhanced with Spark Analytics 4)
    print("\n[8/9] Creating Query 5: Customer Transaction Distribution...")
    print("  Using FINAL_STOCKS.csv with insights from spark_analytics_4...")
    viz_5 = df_decoded.groupby('customer_id').agg({
        'transaction_id': 'count',
        'quantity': 'sum',
        'total_trade_amount': 'sum'
    }).reset_index()
    viz_5.columns = ['customer_id', 'transaction_count', 'total_volume', 'portfolio_value']
    viz_5.to_sql('viz_customer_transaction_distribution', con=engine, if_exists='replace', index=False)
    print(f"  ✓ Created viz_customer_transaction_distribution ({len(viz_5)} rows)")

    # Query 6: Top 10 Customers by Trade Amount
    print("\n[9/9] Creating Query 6: Top 10 Customers by Trade Amount...")
    viz_6 = df_decoded.groupby(['customer_id', 'customer_account_type']).agg({
        'total_trade_amount': 'sum',
        'transaction_id': 'count',
        'quantity': 'sum'
    }).reset_index()
    viz_6.columns = ['customer_id', 'account_type', 'total_trade_amount', 'transaction_count', 'total_volume']
    viz_6 = viz_6.sort_values('total_trade_amount', ascending=False).head(10)
    viz_6.to_sql('viz_top_10_customers_by_trade_amount', con=engine, if_exists='replace', index=False)
    print(f"  ✓ Created viz_top_10_customers_by_trade_amount ({len(viz_6)} rows)")

    # Query 7: Sector Comparison Dashboard (grouped bar charts)
    print("\n[10/11] Creating Query 7: Sector Comparison Dashboard...")
    viz_7 = df_decoded.groupby('stock_sector').agg({
        'transaction_id': 'count',
        'quantity': 'sum',
        'stock_price': 'mean',
        'total_trade_amount': 'sum'
    }).reset_index()
    viz_7.columns = ['sector', 'transaction_count', 'total_volume', 'avg_stock_price', 'total_portfolio_value']
    viz_7 = viz_7.sort_values('total_portfolio_value', ascending=False)
    viz_7.to_sql('viz_sector_comparison_dashboard', con=engine, if_exists='replace', index=False)
    print(f"  ✓ Created viz_sector_comparison_dashboard ({len(viz_7)} rows)")

    # Query 8: Holiday vs Non-Holiday Trading Patterns (from Spark SQL 3 + enhanced)
    print("\n[11/11] Creating Query 8: Holiday vs Non-Holiday Trading Patterns...")
    print("  Using existing spark_sql_3 from Stage 4 with enhancements...")
    try:
        viz_8_base = pd.read_sql('SELECT * FROM spark_sql_3', con=engine)
        # Enhance with more metrics from FINAL_STOCKS
        viz_8 = df_decoded.groupby('is_holiday').agg({
            'transaction_id': 'count',
            'quantity': 'sum',
            'stock_price': 'mean',
            'total_trade_amount': 'sum'
        }).reset_index()
        viz_8.columns = ['is_holiday', 'transaction_count', 'total_volume', 'avg_stock_price', 'total_portfolio_value']
        
        # Add percentage calculations
        total_transactions = viz_8['transaction_count'].sum()
        viz_8['transaction_percentage'] = (viz_8['transaction_count'] / total_transactions * 100).round(2)
        
        viz_8.to_sql('viz_holiday_vs_nonholiday_patterns', con=engine, if_exists='replace', index=False)
        print(f"  ✓ Enhanced and created viz_holiday_vs_nonholiday_patterns ({len(viz_8)} rows)")
    except Exception as e:
        print(f"  ⚠ Could not use spark_sql_3, creating from scratch...")
        viz_8 = df_decoded.groupby('is_holiday').agg({
            'transaction_id': 'count',
            'quantity': 'sum',
            'stock_price': 'mean',
            'total_trade_amount': 'sum'
        }).reset_index()
        viz_8.columns = ['is_holiday', 'transaction_count', 'total_volume', 'avg_stock_price', 'total_portfolio_value']
        
        # Add percentage calculations
        total_transactions = viz_8['transaction_count'].sum()
        viz_8['transaction_percentage'] = (viz_8['transaction_count'] / total_transactions * 100).round(2)
        
        viz_8.to_sql('viz_holiday_vs_nonholiday_patterns', con=engine, if_exists='replace', index=False)
        print(f"  ✓ Created viz_holiday_vs_nonholiday_patterns ({len(viz_8)} rows)")

    # Create visualization metadata
    print("\n[12/12] Creating visualization metadata...")
    metadata = pd.DataFrame([
        {'table_name': 'viz_trading_volume_by_ticker', 'query_number': 1,
         'description': 'Trading volume by stock ticker', 'row_count': len(viz_1), 'created_at': datetime.now()},
        {'table_name': 'viz_stock_price_trends_by_sector', 'query_number': 2,
         'description': 'Stock price trends by sector over time', 'row_count': len(viz_2), 'created_at': datetime.now()},
        {'table_name': 'viz_buy_vs_sell_transactions', 'query_number': 3,
         'description': 'Buy vs Sell transactions comparison', 'row_count': len(viz_3), 'created_at': datetime.now()},
        {'table_name': 'viz_trading_activity_by_day', 'query_number': 4,
         'description': 'Trading activity by day of week', 'row_count': len(viz_4), 'created_at': datetime.now()},
        {'table_name': 'viz_customer_transaction_distribution', 'query_number': 5,
         'description': 'Customer transaction distribution', 'row_count': len(viz_5), 'created_at': datetime.now()},
        {'table_name': 'viz_top_10_customers_by_trade_amount', 'query_number': 6,
         'description': 'Top 10 customers by trade amount', 'row_count': len(viz_6), 'created_at': datetime.now()},
        {'table_name': 'viz_sector_comparison_dashboard', 'query_number': 7,
         'description': 'Sector comparison dashboard (grouped bar charts)', 'row_count': len(viz_7), 'created_at': datetime.now()},
        {'table_name': 'viz_holiday_vs_nonholiday_patterns', 'query_number': 8,
         'description': 'Holiday vs Non-Holiday trading patterns', 'row_count': len(viz_8), 'created_at': datetime.now()}
    ])

    metadata.to_sql('viz_metadata', con=engine, if_exists='replace', index=False)
    print(f"✓ Created viz_metadata table with {len(metadata)} queries")

    print("\n" + "="*70)
    print("✓ ALL 8 VISUALIZATION QUERIES CREATED SUCCESSFULLY")
    print("="*70)
    print("\nCreated Tables:")
    print("  1. viz_trading_volume_by_ticker")
    print("  2. viz_stock_price_trends_by_sector")
    print("  3. viz_buy_vs_sell_transactions")
    print("  4. viz_trading_activity_by_day")
    print("  5. viz_customer_transaction_distribution")
    print("  6. viz_top_10_customers_by_trade_amount")
    print("  7. viz_sector_comparison_dashboard")
    print("  8. viz_holiday_vs_nonholiday_patterns")
    print("="*70)

    engine.dispose()
