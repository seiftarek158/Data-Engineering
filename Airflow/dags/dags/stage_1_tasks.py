"""
Stage 1: Data Cleaning & Integration Task Functions
====================================================

This module contains all task functions for Stage 1 of the pipeline:
- clean_missing_values_task: Handle missing values in daily_trade_prices
- detect_outliers_task: Identify and handle outliers (>10% threshold)
- integrate_datasets_task: Merge all datasets starting from trades.csv
- load_to_postgres_task: Load cleaned data into PostgreSQL warehouse
"""


def clean_missing_values(dtp_input_path, trades_input_path, **context):
    """Handle missing values in daily_trade_prices"""
    import pandas as pd
    
    print("="*70)
    print("TASK 1: CLEANING MISSING VALUES")
    print("="*70)
    
    dtp = pd.read_csv(dtp_input_path)
    trades = pd.read_csv(trades_input_path)
    
    print(f"Loaded {len(dtp)} records from daily_trade_prices")
    print(f"Loaded {len(trades)} records from trades")
    
    missing_count = dtp.isnull().sum().sum()
    print(f"Total missing values in daily_trade_prices: {missing_count}")
    
    dtp_copy = dtp.copy()
    trades_copy = trades.copy()
    
    trades_copy['date'] = pd.to_datetime(trades_copy['timestamp']).dt.date
    
    estimated_prices = trades_copy.groupby(['date', 'stock_ticker']).apply(
        lambda x: (x['cumulative_portfolio_value'] / x['quantity'] * x['quantity']).sum() / x['quantity'].sum()
    ).reset_index(name='price')
    
    print(f"Calculated {len(estimated_prices)} estimated prices from trades")
    
    for col in dtp_copy.columns:
        if col == 'date':
            continue
        
        stock_prices = estimated_prices[estimated_prices['stock_ticker'] == col].set_index('date')['price']
        missing_mask = dtp_copy[col].isnull()
        if missing_mask.any():
            dtp_copy.loc[missing_mask, col] = dtp_copy.loc[missing_mask, 'date'].map(stock_prices)
        
        dtp_copy[col] = dtp_copy[col].fillna(method='ffill')
    
    dtp_copy['date'] = pd.to_datetime(dtp_copy['date'])
    
    rows = []
    for _, row in dtp_copy.iterrows():
        date = row['date']
        for ticker in dtp_copy.columns:
            if ticker != 'date':
                rows.append({
                    'date': date,
                    'stock_ticker': ticker,
                    'stock_price': row[ticker]
                })
    
    dtp_cleaned = pd.DataFrame(rows)
    
    remaining_nulls = dtp_cleaned.isnull().sum().sum()
    print(f"Remaining missing values after cleaning: {remaining_nulls}")
    print(f"Cleaned data shape: {dtp_cleaned.shape}")
    
    output_path = '/opt/airflow/notebook/data/dtp_cleaned.csv'
    dtp_cleaned.to_csv(output_path, index=False)
    print(f"Saved cleaned daily_trade_prices to: {output_path}")
    print("✓ TASK 1 COMPLETED")
    
    return dtp_cleaned


def detect_outliers(trades_input_path, dtp_input_path, dim_customer_input_path, **context):
    """Identify and handle outliers (>10% threshold)"""
    import pandas as pd
    import numpy as np
    from scipy.stats import mstats
    
    print("="*70)
    print("TASK 2: DETECTING AND HANDLING OUTLIERS")
    print("="*70)
    
    trades = pd.read_csv(trades_input_path)
    print(f"Loaded {len(trades)} records from trades")
    
    numeric_columns = ['quantity', 'average_trade_size', 'cumulative_portfolio_value']
    trades_copy = trades.copy()
    
    for col in numeric_columns:
        print(f"\nAnalyzing column: {col}")
        series = trades_copy[col]
        n = len(series)
        
        q1 = series.quantile(0.25)
        q3 = series.quantile(0.75)
        iqr = q3 - q1
        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr
        iqr_outliers_mask = (series < lower) | (series > upper)
        iqr_pct = (iqr_outliers_mask.sum() / n) * 100
        
        print(f"  IQR outliers: {iqr_pct:.2f}%")
        
        if iqr_pct > 10 and col != 'average_trade_size':
            print(f"  → Applying log transformation (IQR: {iqr_pct:.2f}% > 10%)")
            if trades_copy[col].min() <= 0:
                trades_copy[col + '_log'] = np.log1p(trades_copy[col])
            else:
                trades_copy[col + '_log'] = np.log(trades_copy[col])
        elif iqr_pct > 10 and col == 'average_trade_size':
            print(f"  → Applying winsorization (IQR: {iqr_pct:.2f}% > 10%)")
            trades_copy[col + '_winsorized'] = mstats.winsorize(trades_copy[col], limits=[0.15, 0.15])
        else:
            print(f"  ✓ No transformation needed (outliers < 10%)")
    
    trades_output_path = '/opt/airflow/notebook/data/trades_outliers_handled.csv'
    trades_copy.to_csv(trades_output_path, index=False)
    print(f"\nSaved outlier-handled trades to: {trades_output_path}")
    
    dtp_imputed = pd.read_csv(dtp_input_path)
    print(f"Loaded {len(dtp_imputed)} records from dtp_cleaned")
    
    cols = ['STK001','STK002','STK003','STK004','STK005','STK006','STK007','STK008','STK009','STK010',
        'STK011','STK012','STK013','STK014','STK015','STK016','STK017','STK018','STK019','STK020']
    num_of_outlier_columns = 0
    for col in cols:
        series = dtp_imputed.loc[dtp_imputed['stock_ticker'] == col, 'stock_price']
        n_stock = len(series)
        
        if n_stock == 0:
            continue
        
        q1 = series.quantile(0.25)
        q3 = series.quantile(0.75)
        iqr = q3 - q1
        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr
        iqr_outliers_mask = (series < lower) | (series > upper)
        iqr_pct = (iqr_outliers_mask.sum() / n_stock) * 100
        print(f"\nAnalyzing dtp column: {col}")
        print(f"  IQR outliers: {iqr_pct:.2f}%")
        if iqr_pct > 10:
            num_of_outlier_columns += 1
    
    print(f"\nTotal stock columns with >10% outliers: {num_of_outlier_columns} out of {len(cols)}")
    if num_of_outlier_columns > 0:
        if dtp_imputed['stock_price'].min() <= 0:
            dtp_imputed['stock_price_log'] = np.log1p(dtp_imputed['stock_price'])
        else:
            dtp_imputed['stock_price_log'] = np.log(dtp_imputed['stock_price'])
    
    dtp_output_path = '/opt/airflow/notebook/data/dtp_cleaned_outlier_handled.csv'
    dtp_imputed.to_csv(dtp_output_path, index=False)
    print(f"\nSaved outlier-handled daily_trade_prices to: {dtp_output_path}")
    
    dc = pd.read_csv(dim_customer_input_path)
    print(f"Loaded {len(dc)} records from dim_customer")
    
    series = dc['avg_trade_size_baseline']
    mean = series.mean()
    std = series.std()
    z_scores = (series - mean) / std if std else 0
    z_outliers_mask = abs(z_scores) > 3
    z_pct = (z_outliers_mask.sum() / n) * 100
    if z_pct > 10:
        print(f"  → Applying winsorization (Z-score: {z_pct:.2f}% > 10%)")
        dc['avg_trade_size_baseline_winsorized'] = mstats.winsorize(dc['avg_trade_size_baseline'], limits=[0.15, 0.15])
    
    dim_customer_output_path = '/opt/airflow/notebook/data/dim_customer_outlier_handled.csv'
    dc.to_csv(dim_customer_output_path, index=False)
    print(f"\nSaved outlier-handled dim_customer to: {dim_customer_output_path}")
    print("✓ TASK 2 COMPLETED")
    
    return dc, dtp_imputed, trades_copy


def integrate_datasets(trades_input_path, dim_customer_input_path, dim_date_input_path,
                            dim_stock_input_path, dtp_input_path, **context):
    """Merge all datasets starting from trades.csv"""
    import pandas as pd
    
    print("="*70)
    print("TASK 3: INTEGRATING DATASETS")
    print("="*70)
    
    print("Loading datasets...")
    trades = pd.read_csv(trades_input_path)
    dim_customer = pd.read_csv(dim_customer_input_path)
    dim_date = pd.read_csv(dim_date_input_path)
    dim_stock = pd.read_csv(dim_stock_input_path)
    dtp_cleaned = pd.read_csv(dtp_input_path)
    
    print(f"  Trades: {len(trades)} records")
    print(f"  Dim Customer: {len(dim_customer)} records")
    print(f"  Dim Date: {len(dim_date)} records")
    print(f"  Dim Stock: {len(dim_stock)} records")
    print(f"  Daily Trade Prices (cleaned): {len(dtp_cleaned)} records")
    
    trades['date'] = pd.to_datetime(trades['timestamp']).dt.date
    
    print("\nMerging trades with dim_customer...")
    integrated = trades.merge(dim_customer, how='left', on='customer_id')
    print(f"  Result: {len(integrated)} records")
    
    print("Merging with dim_stock...")
    integrated = integrated.merge(dim_stock, how='left', on='stock_ticker')
    print(f"  Result: {len(integrated)} records")
    
    print("Merging with dim_date...")
    dim_date['date'] = pd.to_datetime(dim_date['date']).dt.date
    integrated = integrated.merge(
        dim_date[['date', 'day_name', 'is_weekend', 'is_holiday']], 
        how='left', 
        on='date'
    )
    print(f"  Result: {len(integrated)} records")
    
    print("Merging with daily_trade_prices...")
    dtp_cleaned['date'] = pd.to_datetime(dtp_cleaned['date']).dt.date
    integrated = integrated.merge(dtp_cleaned, how='left', on=['date', 'stock_ticker'])
    print(f"  Result: {len(integrated)} records")
    
    integrated = integrated.rename(columns={
        'account_type': 'customer_account_type',
        'liquidity_tier': 'stock_liquidity_tier',
        'sector': 'stock_sector',
        'industry': 'stock_industry'
    })
    
    integrated.drop('date', axis=1, inplace=True)
    integrated['total_trade_amount'] = integrated['stock_price'] * integrated['quantity']
    
    required_columns = [
        'transaction_id', 'timestamp', 'customer_id', 'stock_ticker', 
        'transaction_type', 'quantity', 'average_trade_size_winsorized', 
        'stock_price_log', 'total_trade_amount', 'customer_account_type', 
        'day_name', 'is_weekend', 'is_holiday', 'stock_liquidity_tier', 
        'stock_sector', 'stock_industry'
    ]
    
    integrated = integrated[required_columns]
    integrated.columns = [col.lower() for col in integrated.columns]
    
    integrated = integrated.rename(columns={
        'stock_price_log': 'stock_price',
        'average_trade_size_winsorized': 'average_trade_size'
    })
    
    print(f"\nFinal integrated dataset shape: {integrated.shape}")
    
    output_path = '/opt/airflow/notebook/data/integrated_data.csv'
    integrated.to_csv(output_path, index=False)
    print(f"Saved integrated data to: {output_path}")
    print("✓ TASK 3 COMPLETED")
    
    return integrated


def load_to_postgres(input_path, table_name, **context):
    """Load cleaned data into PostgreSQL warehouse"""
    import pandas as pd
    from sqlalchemy import create_engine
    import psycopg2
    from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
    from dotenv import load_dotenv
    import os
    
    load_dotenv()
    
    print("="*70)
    print("TASK 4: LOADING TO POSTGRESQL")
    print("="*70)
    
    DB_HOST = os.getenv('DB_HOST', 'pgdatabase')
    DB_USER = os.getenv('DB_USER', 'postgres')
    DB_PASSWORD = os.getenv('DB_PASSWORD', 'postgres')
    DB_PORT = os.getenv('DB_PORT', '5432')
    DB_NAME = os.getenv('DB_NAME', 'Trades_Database')
    
    print(f"Database: {DB_NAME} @ {DB_HOST}:{DB_PORT}")
    
    integrated_data = pd.read_csv(input_path)
    print(f"Loaded {len(integrated_data)} records")
    
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        cursor.execute(f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{DB_NAME}'")
        exists = cursor.fetchone()
        
        if not exists:
            cursor.execute(f'CREATE DATABASE {DB_NAME}')
            print(f"✓ Database '{DB_NAME}' created")
        else:
            print(f"✓ Database '{DB_NAME}' already exists")
        
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Error creating database: {e}")
        raise
    
    try:
        connection_string = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
        engine = create_engine(connection_string)
        
        print(f"\nWriting {len(integrated_data)} records to table '{table_name}'...")
        integrated_data.to_sql(table_name, con=engine, if_exists='replace', index=False)
        print(f"✓ Data successfully written to table '{table_name}'")
        
        with engine.connect() as connection:
            result = connection.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = result.fetchone()[0]
            print(f"✓ Verified {count} records in table '{table_name}'")
    except Exception as ex:
        print(f"Error loading data to PostgreSQL: {ex}")
        raise
    
    print("✓ TASK 4 COMPLETED")
