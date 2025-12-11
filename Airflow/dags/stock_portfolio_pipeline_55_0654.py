"""
Stock Portfolio Pipeline DAG - Team 55_0654
=============================================

This DAG implements the data cleaning and integration pipeline for stock portfolio data.
It converts all Milestone 1 functions into Airflow tasks organized in a task group.

Stage 1: Data Cleaning & Integration
- clean_missing_values: Handle missing values in daily_trade_prices
- detect_outliers: Identify and handle outliers (>10% threshold)
- integrate_datasets: Merge all datasets starting from trades.csv
- load_to_postgres: Load cleaned data into PostgreSQL warehouse
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
import pandas as pd
import numpy as np
from scipy.stats import mstats
from sqlalchemy import create_engine
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import sys
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add app/src to Python path to import custom modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'app', 'src'))

# Default arguments for the DAG
default_args = {
    'owner': 'team_55_0654',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Data paths


# ============================================================================
# MILESTONE 1 FUNCTIONS CONVERTED TO AIRFLOW TASKS
# ============================================================================

def clean_missing_values_task(dtp_input_path, trades_input_path, **context):
    """
    Task 1: Handle missing values in daily_trade_prices
    
    This function:
    1. Loads daily_trade_prices.csv and trades.csv
    2. Imputes missing values using trade data estimates
    3. Applies forward fill for remaining nulls
    4. Saves cleaned data to XCom for downstream tasks
    """
    print("="*70)
    print("TASK 1: CLEANING MISSING VALUES")
    print("="*70)
    
    # Load datasets
    print(f"Loading daily_trade_prices from: {dtp_input_path}")
    dtp = pd.read_csv(dtp_input_path)
    print(f"Loaded {len(dtp)} records from daily_trade_prices")
    
    print(f"Loading trades from: {trades_input_path}")
    trades = pd.read_csv(trades_input_path)
    print(f"Loaded {len(trades)} records from trades")
    
    # Check for missing values
    missing_count = dtp.isnull().sum().sum()
    print(f"Total missing values in daily_trade_prices: {missing_count}")
    
    # Impute missing data
    print("\nImputing missing values using trade data...")
    dtp_copy = dtp.copy()
    trades_copy = trades.copy()
    
    # Convert timestamp to date
    trades_copy['date'] = pd.to_datetime(trades_copy['timestamp']).dt.date
    
    # Calculate estimated prices per date per stock
    estimated_prices = trades_copy.groupby(['date', 'stock_ticker']).apply(
        lambda x: (x['cumulative_portfolio_value'] / x['quantity'] * x['quantity']).sum() / x['quantity'].sum()
    ).reset_index(name='price')
    
    print(f"Calculated {len(estimated_prices)} estimated prices from trades")
    
    # Fill missing values for each stock column
    for col in dtp_copy.columns:
        if col == 'date':
            continue
        
        # Get trade prices for this stock
        stock_prices = estimated_prices[estimated_prices['stock_ticker'] == col].set_index('date')['price']
        
        # Fill missing values from trades
        missing_mask = dtp_copy[col].isnull()
        if missing_mask.any():
            dtp_copy.loc[missing_mask, col] = dtp_copy.loc[missing_mask, 'date'].map(stock_prices)
        
        # Forward fill remaining nulls
        dtp_copy[col] = dtp_copy[col].fillna(method='ffill')
    
    # Convert date back to datetime
    dtp_copy['date'] = pd.to_datetime(dtp_copy['date'])
    
    # Transform to long format for merging
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
    
    # Verify no missing values remain
    remaining_nulls = dtp_cleaned.isnull().sum().sum()
    print(f"\nRemaining missing values after cleaning: {remaining_nulls}")
    print(f"Cleaned data shape: {dtp_cleaned.shape}")
    
    # Save to temporary file for next task
    output_path = os.path.join(OUTPUT_DIR, 'dtp_cleaned.csv')
    dtp_cleaned.to_csv(output_path, index=False)
    print(f"Saved cleaned daily_trade_prices to: {output_path}")
    
    print("="*70)
    print("✓ TASK 1 COMPLETED")
    print("="*70)
    
    return output_path


def detect_outliers_task(trades_input_path, dtp_input_path, dim_customer_input_path, **context):
    """
    Task 2: Identify and handle outliers (>10% threshold)
    
    This function:
    1. Loads trades.csv
    2. Detects outliers using IQR and Z-score methods
    3. Handles outliers with log transformation or winsorization
    4. Only transforms columns with >10% outliers
    5. Saves processed data for integration
    """
    print("="*70)
    print("TASK 2: DETECTING AND HANDLING OUTLIERS")
    print("="*70)
    
    # Load trades data
    print(f"Loading trades from: {trades_input_path}")
    trades = pd.read_csv(trades_input_path)
    print(f"Loaded {len(trades)} records")
    
    # Columns to check for outliers
    numeric_columns = ['quantity', 'average_trade_size', 'cumulative_portfolio_value']
    
    trades_copy = trades.copy()
    
    for col in numeric_columns:
        print(f"\nAnalyzing column: {col}")
        series = trades_copy[col]
        n = len(series)
        
        # IQR Method
        q1 = series.quantile(0.25)
        q3 = series.quantile(0.75)
        iqr = q3 - q1
        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr
        iqr_outliers_mask = (series < lower) | (series > upper)
        iqr_pct = (iqr_outliers_mask.sum() / n) * 100
        
        
        
        print(f"  IQR outliers: {iqr_pct:.2f}%")
        
        # Handle outliers if >10% threshold
        if iqr_pct > 10 and col != 'average_trade_size':
            print(f"  → Applying log transformation (IQR: {iqr_pct:.2f}% > 10%)")
            if trades_copy[col].min() <= 0:
                trades_copy[col + '_log'] = np.log1p(trades_copy[col])
            else:
                trades_copy[col + '_log'] = np.log(trades_copy[col])
        elif iqr_pct > 10 and col == 'average_trade_size':
            print(f"  → Applying winsorization (Z-score: {iqr_pct:.2f}% > 10%)")
            trades_copy[col + '_winsorized'] = mstats.winsorize(trades_copy[col], limits=[0.15, 0.15])
        else:
            print(f"  ✓ No transformation needed (outliers < 10%)")
    
    # Save processed data
    trades_output_path = os.path.join(OUTPUT_DIR, 'trades_outliers_handled.csv')
    trades_copy.to_csv(trades_output_path, index=False)
    print(f"\nSaved outlier-handled trades to: {trades_output_path}")
    
    print(f"Loading dtp_cleaned from: {dtp_input_path}")
    dtp_imputed = pd.read_csv(dtp_input_path)
    print(f"Loaded {len(dtp_imputed)} records")


    cols = ['STK001','STK002','STK003','STK004','STK005','STK006','STK007','STK008','STK009','STK010',
        'STK011','STK012','STK013','STK014','STK015','STK016','STK017','STK018','STK019','STK020']
    num_of_outlier_columns = 0
    for col in cols:
        series = dtp_imputed.loc[dtp_imputed['stock_ticker'] == col, 'stock_price']
        
        q1 = series.quantile(0.25)
        q3 = series.quantile(0.75)
        iqr = q3 - q1
        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr
        iqr_outliers_mask = (series < lower) | (series > upper)
        iqr_pct = (iqr_outliers_mask.sum() / n) * 100
        
        if iqr_pct > 10:
            num_of_outlier_columns += 1
    if(num_of_outlier_columns > 0):
        
        if dtp_imputed['stock_price'].min() <= 0:
            dtp_imputed['stock_price' + '_log'] = np.log1p(dtp_imputed['stock_price'])
        else:
            dtp_imputed['stock_price' + '_log'] = np.log(dtp_imputed['stock_price'])
    
    dtp_output_path = os.path.join(OUTPUT_DIR, 'dtp_cleaned_outlier_handled.csv')
    dtp_imputed.to_csv(dtp_output_path, index=False)
    print(f"\nSaved outlier-handled daily_trade_prices to: {dtp_output_path}")

    print(f"Loading dim_customer from: {dim_customer_input_path}")
    dc = pd.read_csv(dim_customer_input_path)
    print(f"Loaded {len(dc)} records")

    series = dc['avg_trade_size_baseline']
    mean = series.mean()
    std = series.std()
    z_scores = (series - mean) / std if std else 0
    z_outliers_mask = abs(z_scores) > 3
    z_pct = (z_outliers_mask.sum() / n) * 100
    if z_pct > 10:
        print(f"  → Applying winsorization (Z-score: {z_pct:.2f}% > 10%)")
        dc['avg_trade_size_baseline_winsorized'] = mstats.winsorize(dc['avg_trade_size_baseline'], limits=[0.15, 0.15])

    dim_customer_output_path = os.path.join(OUTPUT_DIR, 'dim_customer_outlier_handled.csv')
    dc.to_csv(dim_customer_output_path, index=False)
    print(f"\nSaved outlier-handled dim_customer to: {dim_customer_output_path}")

    print("="*70)
    print("✓ TASK 2 COMPLETED")
    print("="*70)
    
    return dim_customer_output_path


def integrate_datasets_task(trades_input_path, dim_customer_input_path, dim_date_input_path,
                            dim_stock_input_path, dtp_input_path, **context):
    """
    Task 3: Merge all datasets starting from trades.csv
    
    This function:
    1. Loads all cleaned datasets (trades, dim_customer, dim_date, dim_stock, dtp)
    2. Performs left joins starting from trades
    3. Calculates total_trade_amount
    4. Selects and renames required columns
    5. Saves integrated dataset for loading
    """
    print("="*70)
    print("TASK 3: INTEGRATING DATASETS")
    print("="*70)
    
    # Load all datasets
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
    
    # Convert timestamp to date for merging
    trades['date'] = pd.to_datetime(trades['timestamp']).dt.date
    
    # Merge trades with dim_customer
    print("\nMerging trades with dim_customer...")
    integrated = trades.merge(dim_customer, how='left', on='customer_id')
    print(f"  Result: {len(integrated)} records")
    
    # Merge with dim_stock
    print("Merging with dim_stock...")
    integrated = integrated.merge(dim_stock, how='left', on='stock_ticker')
    print(f"  Result: {len(integrated)} records")
    
    # Merge with dim_date
    print("Merging with dim_date...")
    dim_date['date'] = pd.to_datetime(dim_date['date']).dt.date
    integrated = integrated.merge(
        dim_date[['date', 'day_name', 'is_weekend', 'is_holiday']], 
        how='left', 
        on='date'
    )
    print(f"  Result: {len(integrated)} records")
    
    # Merge with daily_trade_prices
    print("Merging with daily_trade_prices...")
    dtp_cleaned['date'] = pd.to_datetime(dtp_cleaned['date']).dt.date
    integrated = integrated.merge(dtp_cleaned, how='left', on=['date', 'stock_ticker'])
    print(f"  Result: {len(integrated)} records")
    
    # Rename columns
    integrated = integrated.rename(columns={
        'account_type': 'customer_account_type',
        'liquidity_tier': 'stock_liquidity_tier',
        'sector': 'stock_sector',
        'industry': 'stock_industry'
    })
    
    # Drop temporary date column
    integrated.drop('date', axis=1, inplace=True)
    
    # Calculate total_trade_amount
    integrated['total_trade_amount'] = integrated['stock_price'] * integrated['quantity']
    
    # Select required columns
    required_columns = [
        'transaction_id', 'timestamp', 'customer_id', 'stock_ticker', 
        'transaction_type', 'quantity', 'average_trade_size_winsorized', 
        'stock_price_log', 'total_trade_amount', 'customer_account_type', 
        'day_name', 'is_weekend', 'is_holiday', 'stock_liquidity_tier', 
        'stock_sector', 'stock_industry'
    ]
    
    integrated = integrated[required_columns]
    
    # Rename columns to lowercase
    integrated.columns = [col.lower() for col in integrated.columns]
    
    # Rename transformed columns back to original names
    integrated = integrated.rename(columns={
        'stock_price_log': 'stock_price',
        'average_trade_size_winsorized': 'average_trade_size'
    })
    
    print(f"\nFinal integrated dataset shape: {integrated.shape}")
    print(f"Columns: {list(integrated.columns)}")
    
    # Save integrated data
    output_path = os.path.join(OUTPUT_DIR, 'integrated_data.csv')
    integrated.to_csv(output_path, index=False)
    print(f"Saved integrated data to: {output_path}")
    
    print("="*70)
    print("✓ TASK 3 COMPLETED")
    print("="*70)
    
    return output_path


def load_to_postgres_task(input_path, table_name, **context):
    """
    Task 4: Load cleaned data into PostgreSQL warehouse
    
    This function:
    1. Loads the integrated dataset
    2. Creates database if not exists
    3. Loads data into PostgreSQL table 'cleaned_trades'
    4. Verifies successful load
    """
    print("="*70)
    print("TASK 4: LOADING TO POSTGRESQL")
    print("="*70)
    
    # Get database credentials from environment variables
    DB_HOST = os.getenv('DB_HOST', 'pgdatabase')
    DB_USER = os.getenv('DB_USER', 'postgres')
    DB_PASSWORD = os.getenv('DB_PASSWORD', 'postgres')
    DB_PORT = os.getenv('DB_PORT', '5432')
    DB_NAME = os.getenv('DB_NAME', 'Trades_Database')
    
    print(f"Database Configuration:")
    print(f"  Host: {DB_HOST}")
    print(f"  Port: {DB_PORT}")
    print(f"  Database: {DB_NAME}")
    print(f"  User: {DB_USER}")
    
    # Load integrated data
    print(f"\nLoading integrated data from: {input_path}")
    integrated_data = pd.read_csv(input_path)
    print(f"Loaded {len(integrated_data)} records")
    
    # Create database if not exists
    try:
        print(f"\nConnecting to PostgreSQL server...")
        conn = psycopg2.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        # Check if database exists
        cursor.execute(f"SELECT 1 FROM pg_catalog.pg_database WHERE datname = '{DB_NAME}'")
        exists = cursor.fetchone()
        
        if not exists:
            # Create database
            cursor.execute(f'CREATE DATABASE {DB_NAME}')
            print(f"✓ Database '{DB_NAME}' created successfully")
        else:
            print(f"✓ Database '{DB_NAME}' already exists")

        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error creating database: {e}")
        raise
    
    # Save to PostgreSQL
    try:
        print(f"\nConnecting to database '{DB_NAME}'...")
        # postgresql://username:password@host:port/database_name
        connection_string = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
        engine = create_engine(connection_string)
        
        # Test connection
        with engine.connect() as connection:
            print('✓ Connected to Database')
        
        print(f"\nWriting {len(integrated_data)} records to table '{table_name}'...")
        integrated_data.to_sql(table_name, con=engine, if_exists='replace', index=False)
        print(f"✓ Data successfully written to table '{table_name}'")
        
        # Verify the data was loaded
        with engine.connect() as connection:
            result = connection.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = result.fetchone()[0]
            print(f"✓ Verified {count} records in table '{table_name}'")
        
    except ValueError as vx:
        print(f"ValueError: {vx}")
        raise
    except Exception as ex:
        print(f"Error loading data to PostgreSQL: {ex}")
        raise
    
    print("="*70)
    print("✓ TASK 4 COMPLETED - Data loaded to PostgreSQL")
    print("="*70)
    
    return table_name


# ============================================================================
# DAG DEFINITION
# ============================================================================

# Create the DAG
with DAG(
    dag_id='stock_portfolio_pipeline_55_0654',
    default_args=default_args,
    description='Stock Portfolio Data Cleaning & Integration Pipeline - Team 55_0654',
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2025, 12, 11),
    catchup=False,
    tags=['data-engineering', 'stock-portfolio', 'milestone-1', 'team-55-0654'],
) as dag:
    
    # Task Group: Stage 1 - Data Cleaning & Integration
    with TaskGroup(group_id='stage_1_data_cleaning_integration') as stage_1:
        
        # Task 1: Clean Missing Values
        clean_missing_values = PythonOperator(
            task_id='clean_missing_values',
            python_callable=clean_missing_values_task,
            op_kwargs={
                'dtp_input_path': '/opt/airflow/notebook/data/daily_trade_prices.csv',
                'trades_input_path': '/opt/airflow/notebook/data/trades.csv'
            },
            provide_context=True,
        )
        
        # Task 2: Detect and Handle Outliers
        detect_outliers = PythonOperator(
            task_id='detect_outliers',
            python_callable=detect_outliers_task,
            op_kwargs={
                'trades_input_path':'/opt/airflow/notebook/data/trades.csv',
                'dtp_input_path': '/opt/airflow/notebook/data/dtp_cleaned.csv',
                'dim_customer_input_path': '/opt/airflow/notebook/data/dim_customer.csv'
            },
            provide_context=True,
            
        )
        
        # Task 3: Integrate Datasets
        integrate_datasets = PythonOperator(
            task_id='integrate_datasets',
            python_callable=integrate_datasets_task,
            op_kwargs={
                'trades_input_path':  '/opt/airflow/notebook/data/trades_outliers_handled.csv',
                'dim_customer_input_path': '/opt/airflow/notebook/data/dim_customer_outlier_handled.csv',
                'dim_date_input_path': '/opt/airflow/notebook/data/dim_date.csv',
                'dim_stock_input_path': '/opt/airflow/notebook/data/dim_stock.csv',
                'dtp_input_path': '/opt/airflow/notebook/data/dtp_cleaned_outlier_handled.csv'
            },
            provide_context=True
        )
        
        # Task 4: Load to PostgreSQL
        load_to_postgres = PythonOperator(
            task_id='load_to_postgres',
            python_callable=load_to_postgres_task,
            op_kwargs={
                'input_path': '/opt/airflow/notebook/data/integrated_data.csv',
                'table_name': 'cleaned_trades'
            },
            provide_context=True
        )
        
        # Define task dependencies within Stage 1
        clean_missing_values >> detect_outliers >> integrate_datasets >> load_to_postgres
            