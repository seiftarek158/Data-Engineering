"""
Unified Stock Portfolio Pipeline DAG - Team 55_0654
===================================================

This unified DAG orchestrates the complete data engineering pipeline with all stages
organized into TaskGroups as per Milestone 3 requirements.

Pipeline Stages:
- Stage 1: Data Cleaning & Integration
- Stage 2: Encoding & Stream Preparation  
- Stage 3: Kafka Streaming
- Stage 4: Spark Analytics
- Stage 6: AI Agent Query Processing

Each stage is organized as a TaskGroup for better visualization and management.
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
# STAGE 1: DATA CLEANING & INTEGRATION FUNCTIONS
# ============================================================================

def clean_missing_values_task(dtp_input_path, trades_input_path, **context):
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
    
    return output_path


def detect_outliers_task(trades_input_path, dtp_input_path, dim_customer_input_path, **context):
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
    
    return dim_customer_output_path


def integrate_datasets_task(trades_input_path, dim_customer_input_path, dim_date_input_path,
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
    
    return output_path


def load_to_postgres_task(input_path, table_name, **context):
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
    return table_name


# ============================================================================
# STAGE 2: ENCODING & STREAM PREPARATION FUNCTIONS
# ============================================================================

def prepare_streaming_data_task(**context):
    """Prepare 5% sample for streaming, 95% for batch"""
    import pandas as pd
    import numpy as np
    import warnings
    warnings.filterwarnings("ignore")
    
    print("="*70)
    print("STAGE 2 TASK 1: PREPARE STREAMING DATA")
    print("="*70)
    
    data_path = "/opt/airflow/notebook/data/"
    output_path = "notebook/data/"
    
    dtp = pd.read_csv(os.path.join(data_path, "daily_trade_prices.csv"))
    dc = pd.read_csv(os.path.join(data_path, "dim_customer.csv"))
    dd = pd.read_csv(os.path.join(data_path, "dim_date.csv"))
    ds = pd.read_csv(os.path.join(data_path, "dim_stock.csv"))
    trades = pd.read_csv(os.path.join(data_path, "trades.csv"))
    
    print(f"Loaded all datasets")
    
    # Fill nulls in dtp
    trades["date_obj"] = pd.to_datetime(trades["timestamp"]).dt.date
    estimated_prices = (
        trades.groupby([trades["date_obj"], "stock_ticker"])
        .apply(
            lambda x: (x["cumulative_portfolio_value"] / x["quantity"] * x["quantity"]).sum()
            / x["quantity"].sum()
        )
        .reset_index(name="price")
    )
    stock_prices_map = estimated_prices.set_index(["date_obj", "stock_ticker"])["price"]
    
    dtp["date_obj"] = pd.to_datetime(dtp["date"]).dt.date
    for col in dtp.columns:
        if col not in ["date", "date_obj"]:
            missing_mask = dtp[col].isnull()
            if missing_mask.any():
                dates_for_mapping = dtp.loc[missing_mask, "date_obj"]
                map_index = pd.MultiIndex.from_tuples(
                    [(date, col) for date in dates_for_mapping],
                    names=["date_obj", "stock_ticker"],
                )
                dtp.loc[missing_mask, col] = map_index.map(stock_prices_map)
            
            dtp[col] = dtp[col].fillna(method="ffill")
    dtp = dtp.drop(columns=["date_obj"])
    
    # Convert to long format
    dtp["date"] = pd.to_datetime(dtp["date"])
    dtp_long = pd.melt(
        dtp,
        id_vars=["date"],
        var_name="stock_ticker",
        value_name="stock_price",
    ).dropna()
    
    # Integrate data
    trades["date"] = pd.to_datetime(trades["timestamp"]).dt.date
    dtp_long["date"] = pd.to_datetime(dtp_long["date"]).dt.date
    dd["date"] = pd.to_datetime(dd["date"]).dt.date
    
    merged_df = trades.merge(dc, on="customer_id", how="left")
    merged_df = merged_df.merge(ds, on="stock_ticker", how="left")
    merged_df = merged_df.merge(dd, on="date", how="left")
    df = merged_df.merge(dtp_long, on=["date", "stock_ticker"], how="left")
    
    # Extract 5% random sample for streaming
    stream_df = df.sample(frac=0.05, random_state=42)
    batch_df = df.drop(stream_df.index)
    
    stream_stats = stream_df.describe()
    stream_stats.to_csv(os.path.join(output_path, "stream_stats.csv"))
    
    stream_df.to_csv(os.path.join(output_path, "stream.csv"), index=False)
    batch_df.to_csv(os.path.join(output_path, "batch_data_for_encoding.csv"), index=False)
    
    print(f"✓ Saved {len(stream_df)} streaming records")
    print(f"✓ Saved {len(batch_df)} batch records")
    print("✓ STAGE 2 TASK 1 COMPLETED")


def encode_categorical_data_task(**context):
    """Encode categorical columns and generate lookup tables"""
    import pandas as pd
    import numpy as np
    import json
    
    print("="*70)
    print("STAGE 2 TASK 2: ENCODE CATEGORICAL DATA")
    print("="*70)
    
    data_path = "/opt/airflow/notebook/data/"
    lookups_path = os.path.join(data_path, "lookups")
    os.makedirs(lookups_path, exist_ok=True)
    
    batch_df = pd.read_csv(os.path.join(data_path, "batch_data_for_encoding.csv"))
    
    categorical_cols = batch_df.select_dtypes(include=["object", "bool"]).columns
    master_encoding_lookup = {}
    encoded_df = batch_df.copy()
    
    for col in categorical_cols:
        encoding_map = {
            category: i for i, category in enumerate(batch_df[col].unique())
        }
        encoded_df[col] = batch_df[col].map(encoding_map)
        
        lookup_map = {}
        for category, i in encoding_map.items():
            if isinstance(category, np.bool_):
                lookup_map[i] = bool(category)
            else:
                lookup_map[i] = category
        master_encoding_lookup[col] = lookup_map
        lookup_df = pd.DataFrame(
            list(lookup_map.items()), columns=["encoded_value", "original_value"]
        )
        lookup_df.to_csv(
            os.path.join(lookups_path, f"encoding_lookup_{col}.csv"), index=False
        )
    
    with open(os.path.join(lookups_path, "master_encoding_lookup.json"), "w") as f:
        json.dump(master_encoding_lookup, f, indent=4)
    
    encoded_df.to_csv(
        os.path.join(data_path, "integrated_encoded_trades_data.csv"), index=False
    )
    
    print(f"✓ Encoded {len(categorical_cols)} categorical columns")
    print(f"✓ Saved lookup tables to {lookups_path}")
    print("✓ STAGE 2 TASK 2 COMPLETED")


# ============================================================================
# STAGE 3: KAFKA STREAMING FUNCTIONS
# ============================================================================

def consume_and_process_stream_task(**context):
    """Consume data from Kafka, apply encoding, save processed data"""
    import pandas as pd
    import json
    from kafka import KafkaConsumer
    
    print("="*70)
    print("STAGE 3 TASK 2: CONSUME AND PROCESS STREAM")
    print("="*70)
    
    data_path = "/opt/airflow/notebook/data/"
    lookups_path = os.path.join(data_path, "lookups")
    output_file = os.path.join(data_path, "FINAL_STOCKS.csv")
    
    print(f"Loading lookup tables from: {lookups_path}")
    
    lookup_files = [f for f in os.listdir(lookups_path) if f.startswith("encoding_lookup_") and f.endswith(".csv")]
    
    if not lookup_files:
        raise FileNotFoundError(f"No encoding lookup files found in {lookups_path}")
    
    lookups = {}
    for file in lookup_files:
        col_name = file.replace("encoding_lookup_", "").replace(".csv", "")
        df = pd.read_csv(os.path.join(lookups_path, file))
        lookups[col_name] = dict(zip(df.original_value, df.encoded_value))
        print(f"Loaded lookup for: {col_name} ({len(lookups[col_name])} mappings)")
    
    print("Connecting to Kafka consumer...")
    consumer = KafkaConsumer(
        "55_0654_Topic",
        bootstrap_servers="kafka:9092",
        auto_offset_reset="earliest",
        consumer_timeout_ms=60000,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        group_id="stage_3_consumer",
        enable_auto_commit=True,
    )
    
    processed_records = []
    message_count = 0
    
    print("Starting to consume messages from Kafka...")
    for message in consumer:
        message_count += 1
        
        if isinstance(message.value, dict) and message.value.get("EOS"):
            print(f"End of Stream message received after processing {message_count - 1} messages.")
            break
        if message.value == "EOS":
            print(f"End of Stream message received after processing {message_count - 1} messages.")
            break
        
        record = message.value
        
        for col, lookup_map in lookups.items():
            if col in record and record[col] in lookup_map:
                record[col] = lookup_map[record[col]]
        
        processed_records.append(record)
        
        if message_count % 100 == 0:
            print(f"Processed {message_count} messages...")
    
    consumer.close()
    print(f"Kafka consumer closed. Total messages processed: {len(processed_records)}")
    
    if processed_records:
        print("Creating DataFrame from processed records...")
        final_df = pd.DataFrame(processed_records)
        
        all_cols = [
            'transaction_id', 'timestamp', 'customer_id', 'stock_ticker',
            'transaction_type', 'quantity', 'average_trade_size',
            'cumulative_portfolio_value', 'date', 'customer_key', 'account_type',
            'avg_trade_size_baseline', 'stock_key', 'company_name',
            'liquidity_tier', 'sector', 'industry', 'date_key', 'day', 'month',
            'month_name', 'quarter', 'year', 'day_of_week', 'day_name',
            'is_weekend', 'is_holiday', 'stock_price'
        ]
        
        for col in all_cols:
            if col not in final_df.columns:
                final_df[col] = None
                print(f"Warning: Column '{col}' was missing, filled with None")
        
        final_df = final_df[all_cols]
        final_df.to_csv(output_file, index=False)
        print(f"✓ Successfully saved {len(processed_records)} records to {output_file}")
    else:
        print("⚠ Warning: No records were processed")
        raise ValueError("No records were consumed from Kafka stream")
    
    print("✓ STAGE 3 TASK 2 COMPLETED")


def save_final_to_postgres_task(**context):
    """Save the final processed streaming data to PostgreSQL"""
    import pandas as pd
    from sqlalchemy import create_engine
    
    print("="*70)
    print("STAGE 3 TASK 3: SAVE FINAL TO POSTGRES")
    print("="*70)
    
    data_path = "/opt/airflow/notebook/data/"
    input_file = os.path.join(data_path, "FINAL_STOCKS.csv")
    
    if not os.path.exists(input_file):
        raise FileNotFoundError(f"The file {input_file} was not found")
    
    print("Reading processed data from CSV...")
    final_df = pd.read_csv(input_file)
    print(f"Loaded {len(final_df)} records with {len(final_df.columns)} columns")
    
    db_host = "pgdatabase"
    db_port = 5432
    db_user = "postgres"
    db_password = "postgres"
    db_name = "Trades_Database"
    
    connection_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    engine = create_engine(connection_string)
    
    table_name = "final_stocks"
    print(f"Saving data to table '{table_name}'...")
    final_df.to_sql(table_name, engine, if_exists="replace", index=False)
    
    print(f"✓ Successfully saved {len(final_df)} records to PostgreSQL table '{table_name}'")
    
    engine.dispose()
    print("✓ STAGE 3 TASK 3 COMPLETED")


# ============================================================================
# STAGE 4: SPARK ANALYTICS FUNCTIONS
# ============================================================================

def initialize_spark_session_task(**context):
    """
    Task 1: Initialize Spark Session
    
    Creates a Spark session connected to the Spark master node with
    the team-specific application name.
    """
    # Import PySpark inside function to avoid slow DAG parsing
    from pyspark.sql import SparkSession
    
    print("="*70)
    print("INITIALIZING SPARK SESSION")
    print("="*70)
    
    # Load Spark configuration
    spark_master = 'spark://spark-master:7077'
    spark_app_name = 'M3_SPARK_APP_55_0654'
    
    print(f"\nSpark Configuration:")
    print(f"  Master: {spark_master}")
    print(f"  App Name: {spark_app_name}")
    
    # Create Spark session with PostgreSQL JDBC driver
    # Use shared path accessible by both Airflow and Spark containers
    jdbc_jar = "/opt/airflow/notebook/data/jars/postgresql-42.7.1.jar"
    spark = SparkSession.builder \
        .appName(spark_app_name) \
        .master(spark_master) \
        .config("spark.jars", jdbc_jar) \
        .config("spark.driver.extraClassPath", jdbc_jar) \
        .config("spark.executor.extraClassPath", jdbc_jar) \
        .getOrCreate()
    
    # Set log level to reduce verbosity
    spark.sparkContext.setLogLevel("ERROR")
    
    print(f"\n✓ Spark Session initialized successfully")
    print(f"  Spark Version: {spark.version}")
    print(f"  Master URL: {spark.sparkContext.master}")
    print(f"  App Name: {spark.sparkContext.appName}")
    
    # Store spark session info in XCom
    context['task_instance'].xcom_push(key='spark_app_name', value=spark_app_name)
    context['task_instance'].xcom_push(key='spark_master', value=spark_master)
    
    # Don't stop the session - it will be reused by next task
    print("\n✓ Spark session ready for analytics tasks")
    print("="*70)
    
    return spark_app_name


def run_spark_analytics_task(input_path, **context):
    """
    Task 2: Run Spark Analytics

    This function:
    1. Creates a new Spark session
    2. Reads FULL_STOCKS.csv into Spark DataFrame
    3. Executes all 5 Spark DataFrame operations from Milestone 2
    4. Executes all 5 Spark SQL queries from Milestone 2
    5. Saves each result to PostgreSQL analytics tables
    """
    # Import heavy libraries inside function to avoid slow DAG parsing
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as fn
    from sqlalchemy import create_engine

    print("="*70)
    print("RUNNING SPARK ANALYTICS")
    print("="*70)

    # Spark configuration
    spark_master = 'spark://spark-master:7077'
    spark_app_name = 'M3_SPARK_APP_55_0654'

    from dotenv import load_dotenv
    load_dotenv()
    # Load database configuration from environment
    db_host = os.getenv('DB_HOST', 'pgdatabase')
    db_user = os.getenv('DB_USER', 'postgres')
    db_password = os.getenv('DB_PASSWORD', 'postgres')
    db_port = os.getenv('DB_PORT', '5432')
    db_name = os.getenv('DB_NAME', 'Trades_Database')

    # Create new Spark session with PostgreSQL JDBC driver
    # Use shared path accessible by both Airflow and Spark containers
    jdbc_jar = "/opt/airflow/notebook/data/jars/postgresql-42.7.1.jar"
    print(f"\nCreating new Spark session: {spark_app_name}")

    # Stop any existing Spark sessions first to avoid conflicts
    try:
        SparkSession.getActiveSession().stop()
        print("✓ Stopped existing Spark session")
    except:
        pass

    spark = SparkSession.builder \
        .appName(spark_app_name) \
        .master(spark_master) \
        .config("spark.jars", jdbc_jar) \
        .config("spark.driver.extraClassPath", jdbc_jar) \
        .config("spark.executor.extraClassPath", jdbc_jar) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    print("✓ Created new Spark session")
    print(f"  Spark Version: {spark.version}")
    print(f"  Master URL: {spark.sparkContext.master}")
    
    # Read FULL_STOCKS.csv into Spark DataFrame
    print(f"\nReading data from: {input_path}")
    df = spark.read.csv(input_path, header=True, inferSchema=True)
    
    print(f"✓ Loaded CSV file successfully")
    print(f"  Total records: {df.count()}")
    print(f"  Total columns: {len(df.columns)}")
    
    # Database connection string
    jdbc_url = f"jdbc:postgresql://{db_host}:{db_port}/{db_name}"
    connection_properties = {
        "user": db_user,
        "password": db_password,
        "driver": "org.postgresql.Driver"
    }
    
    # Alternative: SQLAlchemy for pandas compatibility
    sqlalchemy_url = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
    engine = create_engine(sqlalchemy_url)
    
    print("\n" + "="*70)
    print("EXECUTING SPARK DATAFRAME ANALYTICS")
    print("="*70)
    
    # ========================================================================
    # SPARK DATAFRAME QUESTIONS (from Milestone 2)
    # ========================================================================
    
    # Question 1: Total trading volume for each stock ticker
    print("\n[1/5] Total trading volume for each stock ticker...")
    q1_result = df.groupBy("stock_ticker") \
        .agg(fn.sum("quantity").alias("total_volume"))
    
    q1_result.show(5)
    q1_pandas = q1_result.toPandas()
    q1_pandas.to_sql('spark_analytics_1', con=engine, if_exists='replace', index=False)
    print(f"✓ Saved {len(q1_pandas)} records to table: spark_analytics_1")
    
    # Question 2: Average stock price by sector
    print("\n[2/5] Average stock price by sector...")
    q2_result = df.groupBy("stock_sector") \
        .agg(fn.avg("stock_price").alias("avg_stock_price"))
    
    q2_result.show()
    q2_pandas = q2_result.toPandas()
    q2_pandas.to_sql('spark_analytics_2', con=engine, if_exists='replace', index=False)
    print(f"✓ Saved {len(q2_pandas)} records to table: spark_analytics_2")
    
    # Question 3: Buy vs Sell transactions on weekends
    print("\n[3/5] Buy vs Sell transactions on weekends...")
    q3_result = df.filter(fn.col("is_weekend") == 1) \
        .groupBy("transaction_type") \
        .agg(fn.count("transaction_id").alias("transaction_count"))
    
    q3_result.show()
    q3_pandas = q3_result.toPandas()
    q3_pandas.to_sql('spark_analytics_3', con=engine, if_exists='replace', index=False)
    print(f"✓ Saved {len(q3_pandas)} records to table: spark_analytics_3")
    
    # Question 4: Customers with more than 10 transactions
    print("\n[4/5] Customers with more than 10 transactions...")
    q4_result = df.groupBy("customer_id") \
        .agg(fn.count("transaction_id").alias("transaction_count")) \
        .filter(fn.col("transaction_count") > 10)
    
    print(f"Total customers with >10 transactions: {q4_result.count()}")
    q4_result.show(5)
    q4_pandas = q4_result.toPandas()
    q4_pandas.to_sql('spark_analytics_4', con=engine, if_exists='replace', index=False)
    print(f"✓ Saved {len(q4_pandas)} records to table: spark_analytics_4")
    
    # Question 5: Total trade amount per day of the week (highest to lowest)
    print("\n[5/5] Total trade amount per day of the week...")
    day_cols = ["day_Monday", "day_Tuesday", "day_Wednesday", "day_Thursday", "day_Friday"]

    # More efficient approach: compute all day totals in a single pass
    q5_result = df.select(
        fn.when(fn.col("day_Monday") == 1, fn.lit("Monday"))
          .when(fn.col("day_Tuesday") == 1, fn.lit("Tuesday"))
          .when(fn.col("day_Wednesday") == 1, fn.lit("Wednesday"))
          .when(fn.col("day_Thursday") == 1, fn.lit("Thursday"))
          .when(fn.col("day_Friday") == 1, fn.lit("Friday"))
          .alias("day"),
        "total_trade_amount"
    ).groupBy("day") \
     .agg(fn.sum("total_trade_amount").alias("total_trade_amount")) \
     .orderBy(fn.desc("total_trade_amount"))
    
    q5_result.show()
    q5_pandas = q5_result.toPandas()
    q5_pandas.to_sql('spark_analytics_5', con=engine, if_exists='replace', index=False)
    print(f"✓ Saved {len(q5_pandas)} records to table: spark_analytics_5")
    
    print("\n✓ All Spark DataFrame analytics completed and saved to PostgreSQL")
    
    # ========================================================================
    # SPARK SQL QUESTIONS (from Milestone 2)
    # ========================================================================
    
    print("\n" + "="*70)
    print("EXECUTING SPARK SQL ANALYTICS")
    print("="*70)
    
    # Register DataFrame as temporary SQL table
    df.createOrReplaceTempView("trades")
    print("✓ Created temporary view: trades")
    
    # SQL Question 1: Top 5 most traded stock tickers by total quantity
    print("\n[1/5] Top 5 most traded stock tickers by total quantity...")
    sql1_result = spark.sql("""
        SELECT stock_ticker, 
               SUM(quantity) as total_quantity
        FROM trades
        GROUP BY stock_ticker
        ORDER BY total_quantity DESC
        LIMIT 5
    """)
    
    sql1_result.show()
    sql1_pandas = sql1_result.toPandas()
    sql1_pandas.to_sql('spark_sql_1', con=engine, if_exists='replace', index=False)
    print(f"✓ Saved {len(sql1_pandas)} records to table: spark_sql_1")
    
    # SQL Question 2: Average trade amount by customer account type
    print("\n[2/5] Average trade amount by customer account type...")
    sql2_result = spark.sql("""
        SELECT 
            CASE customer_account_type
                WHEN 0 THEN 'Institutional'
                ELSE 'Retail'
            END as account_type,
            AVG(total_trade_amount) as avg_trade_amount
        FROM trades
        GROUP BY customer_account_type
    """)
    
    sql2_result.show()
    sql2_pandas = sql2_result.toPandas()
    sql2_pandas.to_sql('spark_sql_2', con=engine, if_exists='replace', index=False)
    print(f"✓ Saved {len(sql2_pandas)} records to table: spark_sql_2")
    
    # SQL Question 3: Transactions during holidays vs non-holidays
    print("\n[3/5] Transactions during holidays vs non-holidays...")
    sql3_result = spark.sql("""
        SELECT 
            CASE 
                WHEN is_holiday = 1 THEN 'Holiday'
                ELSE 'Non-Holiday'
            END as period_type,
            COUNT(transaction_id) as transaction_count
        FROM trades
        GROUP BY is_holiday
    """)
    
    sql3_result.show()
    sql3_pandas = sql3_result.toPandas()
    sql3_pandas.to_sql('spark_sql_3', con=engine, if_exists='replace', index=False)
    print(f"✓ Saved {len(sql3_pandas)} records to table: spark_sql_3")
    
    # SQL Question 4: Stock sectors with highest total trading volume on weekends
    print("\n[4/5] Stock sectors with highest trading volume on weekends...")
    sql4_result = spark.sql("""
        SELECT stock_sector,
               SUM(quantity) as total_volume
        FROM trades
        WHERE is_weekend = 1
        GROUP BY stock_sector
        ORDER BY total_volume DESC
    """)
    
    sql4_result.show()
    sql4_pandas = sql4_result.toPandas()
    sql4_pandas.to_sql('spark_sql_4', con=engine, if_exists='replace', index=False)
    print(f"✓ Saved {len(sql4_pandas)} records to table: spark_sql_4")
    
    # SQL Question 5: Total buy vs sell amount for each stock liquidity tier
    print("\n[5/5] Total buy vs sell amount for each liquidity tier...")
    sql5_result = spark.sql("""
        SELECT stock_liquidity_tier,
               CASE transaction_type
                   WHEN 0 THEN 'BUY'
                   ELSE 'SELL'
               END as transaction_type,
               SUM(total_trade_amount) as total_amount
        FROM trades
        GROUP BY stock_liquidity_tier, transaction_type
        ORDER BY stock_liquidity_tier, transaction_type
    """)
    
    sql5_result.show()
    sql5_pandas = sql5_result.toPandas()
    sql5_pandas.to_sql('spark_sql_5', con=engine, if_exists='replace', index=False)
    print(f"✓ Saved {len(sql5_pandas)} records to table: spark_sql_5")
    
    print("\n✓ All Spark SQL analytics completed and saved to PostgreSQL")
    
    # Summary
    print("\n" + "="*70)
    print("ANALYTICS SUMMARY")
    print("="*70)
    print("\nSpark DataFrame Analytics Tables:")
    print("  • spark_analytics_1: Total trading volume by stock ticker")
    print("  • spark_analytics_2: Average stock price by sector")
    print("  • spark_analytics_3: Buy vs Sell transactions on weekends")
    print("  • spark_analytics_4: Customers with >10 transactions")
    print("  • spark_analytics_5: Total trade amount per day of week")
    print("\nSpark SQL Analytics Tables:")
    print("  • spark_sql_1: Top 5 most traded stock tickers")
    print("  • spark_sql_2: Average trade amount by account type")
    print("  • spark_sql_3: Transactions during holidays vs non-holidays")
    print("  • spark_sql_4: Stock sectors trading volume on weekends")
    print("  • spark_sql_5: Buy vs sell amount by liquidity tier")
    
    print("\n" + "="*70)
    print("✓ SPARK ANALYTICS COMPLETED SUCCESSFULLY")
    print("="*70)
    
    # Stop Spark session
    spark.stop()
    
    return "analytics_complete"


# ============================================================================
# STAGE 6: AI AGENT FUNCTIONS
# ============================================================================

def setup_agent_volume_task(**context):
    """Create agent volume directory and test queries file"""
    import json
    from pathlib import Path
    
    print("="*70)
    print("STAGE 6 TASK 1: SETUP AGENT VOLUME")
    print("="*70)
    
    agents_dir = Path('/opt/airflow/dags/agents')
    logs_dir = Path('/opt/airflow/dags/agent_logs')
    
    agents_dir.mkdir(parents=True, exist_ok=True)
    logs_dir.mkdir(parents=True, exist_ok=True)
    
    test_queries = [
        "What was the total trading volume for technology stocks last month?",
        "Show me the top 10 customers by trade amount",
        "What is the average trade size for retail accounts?",
        "How many transactions occurred on weekends?",
        "Which stock sector has the highest liquidity?",
        "Show me all transactions for customer with ID 4747",
        "What are the total buy versus sell transaction amounts?",
        "Which day of the week has the most trading activity?",
        "Show me all stocks in the high liquidity tier",
        "What is the transaction count by customer account type?",
        "Which stock ticker had the highest trade volume?",
        "What percentage of trades happened during holidays?",
        "Show me the average stock price by sector",
        "How many unique customers made transactions?",
        "What is the total trade amount by transaction type?"
    ]
    
    queries_file = agents_dir / 'user_query_test.txt'
    with open(queries_file, 'w') as f:
        for query in test_queries:
            f.write(query + '\n')
    
    print(f"✅ Created agents directory: {agents_dir}")
    print(f"✅ Created test queries file with {len(test_queries)} questions")
    
    context['ti'].xcom_push(key='queries_file', value=str(queries_file))
    context['ti'].xcom_push(key='logs_dir', value=str(logs_dir))
    
    print("✓ STAGE 6 TASK 1 COMPLETED")


def process_with_ai_agent_task(**context):
    """Initialize LangChain agent and process test queries"""
    import time
    import pandas as pd
    from langchain_google_genai import ChatGoogleGenerativeAI
    from langchain_community.utilities import SQLDatabase
    from sqlalchemy import create_engine
    
    print("="*70)
    print("STAGE 6 TASK 2: PROCESS WITH AI AGENT")
    print("="*70)
    
    ti = context['ti']
    queries_file = ti.xcom_pull(task_ids='stage_6_ai_agent.setup_agent_volume', key='queries_file')
    
    gemini_api_key = os.getenv('GEMINI_API_KEY')
    if not gemini_api_key:
        raise ValueError("GEMINI_API_KEY not found in environment variables")
    
    DB_HOST = os.getenv('DB_HOST', 'pgdatabase')
    DB_USER = os.getenv('DB_USER', 'postgres')
    DB_PASSWORD = os.getenv('DB_PASSWORD', 'postgres')
    DB_PORT = os.getenv('DB_PORT', '5432')
    DB_NAME = os.getenv('DB_NAME', 'Trades_Database')
    
    print("🔧 Initializing database connection...")
    connection_string = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
    engine = create_engine(connection_string)
    db = SQLDatabase(engine)
    
    print("🤖 Initializing Gemini LLM...")
    llm = ChatGoogleGenerativeAI(
        model="gemini-2.5-flash",
        temperature=0,
        google_api_key=gemini_api_key
    )
    
    print("⛓️ Getting database schema...")
    table_info = db.get_table_info()
    
    with open(queries_file, 'r') as f:
        queries = [line.strip() for line in f if line.strip()]
    
    print(f"📝 Processing {len(queries)} test queries...")
    
    results = []
    for idx, question in enumerate(queries, 1):
        try:
            print(f"\n🔍 Query {idx}/{len(queries)}: {question}")
            
            prompt = f"""Given the following database schema:
{table_info}

Generate a SQL query to answer this question: {question}

Return ONLY the SQL query without any explanation or markdown formatting."""
            
            response = llm.invoke(prompt)
            sql_query = response.content if hasattr(response, 'content') else str(response)
            cleaned_sql = sql_query.strip().replace('```sql', '').replace('```', '').strip()
            
            print(f"📊 Generated SQL: {cleaned_sql[:100]}...")
            
            if cleaned_sql.lower().startswith("select"):
                result_df = pd.read_sql(cleaned_sql, engine)
                response = result_df.to_dict('records')
                row_count = len(result_df)
                print(f"✅ Query returned {row_count} rows")
            else:
                response = "Non-SELECT query generated"
                row_count = 0
            
            results.append({
                'query_number': idx,
                'user_query': question,
                'sql_generated': cleaned_sql,
                'row_count': row_count,
                'agent_response': str(response)[:500],
                'status': 'success',
                'timestamp': datetime.now().isoformat()
            })
            
            time.sleep(1)
            
        except Exception as e:
            print(f"❌ Error processing query {idx}: {str(e)}")
            results.append({
                'query_number': idx,
                'user_query': question,
                'sql_generated': 'ERROR',
                'row_count': 0,
                'agent_response': f'Error: {str(e)}',
                'status': 'failed',
                'timestamp': datetime.now().isoformat()
            })
    
    ti.xcom_push(key='agent_results', value=results)
    
    success_count = sum(1 for r in results if r['status'] == 'success')
    print(f"\n📊 Agent Processing Complete:")
    print(f"   Total queries: {len(results)}")
    print(f"   Successful: {success_count}")
    print(f"   Failed: {len(results) - success_count}")
    
    print("✓ STAGE 6 TASK 2 COMPLETED")
    return results


def log_agent_responses_task(**context):
    """Log agent results to JSON file and PostgreSQL"""
    import json
    import pandas as pd
    from pathlib import Path
    from sqlalchemy import create_engine
    
    print("="*70)
    print("STAGE 6 TASK 3: LOG AGENT RESPONSES")
    print("="*70)
    
    ti = context['ti']
    results = ti.xcom_pull(task_ids='stage_6_ai_agent.process_with_ai_agent', key='agent_results')
    logs_dir = ti.xcom_pull(task_ids='stage_6_ai_agent.setup_agent_volume', key='logs_dir')
    
    year_dir = Path(logs_dir) / '2025'
    year_dir.mkdir(parents=True, exist_ok=True)
    
    log_file = year_dir / f'agent_volume_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
    
    with open(log_file, 'w') as f:
        json.dump({
            'execution_date': context['execution_date'].isoformat(),
            'dag_run_id': context['dag_run'].run_id,
            'total_queries': len(results),
            'queries': results
        }, f, indent=2)
    
    print(f"✅ Agent logs saved to: {log_file}")
    
    try:
        DB_HOST = os.getenv('DB_HOST', 'pgdatabase')
        DB_USER = os.getenv('DB_USER', 'postgres')
        DB_PASSWORD = os.getenv('DB_PASSWORD', 'postgres')
        DB_PORT = os.getenv('DB_PORT', '5432')
        DB_NAME = os.getenv('DB_NAME', 'Trades_Database')
        
        connection_string = f'postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
        engine = create_engine(connection_string)
        
        df = pd.DataFrame(results)
        df['dag_run_id'] = context['dag_run'].run_id
        df.to_sql('agent_query_results', engine, if_exists='append', index=False)
        
        print(f"✅ Results also saved to PostgreSQL table 'agent_query_results'")
    except Exception as e:
        print(f"⚠️ Could not save to PostgreSQL: {str(e)}")
    
    print("✓ STAGE 6 TASK 3 COMPLETED")
    return str(log_file)


# ============================================================================
# DAG DEFINITION
# ============================================================================

with DAG(
    dag_id='unified_portfolio_pipeline',
    default_args=default_args,
    description='Unified Stock Portfolio Data Engineering Pipeline - All Stages with TaskGroups',
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2025, 12, 12),
    catchup=False,
    tags=['unified', 'data-engineering', 'stock-portfolio', 'milestone-3', 'team-55-0654'],
    doc_md="""
    # Unified Stock Portfolio Pipeline
    
    This DAG orchestrates the complete data engineering pipeline with all stages:
    
    - **Stage 1**: Data Cleaning & Integration
    - **Stage 2**: Encoding & Stream Preparation
    - **Stage 3**: Kafka Streaming
    - **Stage 4**: Spark Analytics (reads from PostgreSQL final_stocks table)
    - **Stage 6**: AI Agent Query Processing
    
    All stages are organized as TaskGroups for better visualization.
    """
) as dag:
    
    # ========================================================================
    # STAGE 1: DATA CLEANING & INTEGRATION
    # ========================================================================
    with TaskGroup(group_id='stage_1_data_cleaning_integration') as stage_1:
        
        clean_missing_values = PythonOperator(
            task_id='clean_missing_values',
            python_callable=clean_missing_values_task,
            op_kwargs={
                'dtp_input_path': '/opt/airflow/notebook/data/daily_trade_prices.csv',
                'trades_input_path': '/opt/airflow/notebook/data/trades.csv'
            },
            provide_context=True,
        )
        
        detect_outliers = PythonOperator(
            task_id='detect_outliers',
            python_callable=detect_outliers_task,
            op_kwargs={
                'trades_input_path': '/opt/airflow/notebook/data/trades.csv',
                'dtp_input_path': '/opt/airflow/notebook/data/dtp_cleaned.csv',
                'dim_customer_input_path': '/opt/airflow/notebook/data/dim_customer.csv'
            },
            provide_context=True,
        )
        
        integrate_datasets = PythonOperator(
            task_id='integrate_datasets',
            python_callable=integrate_datasets_task,
            op_kwargs={
                'trades_input_path': '/opt/airflow/notebook/data/trades_outliers_handled.csv',
                'dim_customer_input_path': '/opt/airflow/notebook/data/dim_customer_outlier_handled.csv',
                'dim_date_input_path': '/opt/airflow/notebook/data/dim_date.csv',
                'dim_stock_input_path': '/opt/airflow/notebook/data/dim_stock.csv',
                'dtp_input_path': '/opt/airflow/notebook/data/dtp_cleaned_outlier_handled.csv'
            },
            provide_context=True
        )
        
        load_to_postgres = PythonOperator(
            task_id='load_to_postgres',
            python_callable=load_to_postgres_task,
            op_kwargs={
                'input_path': '/opt/airflow/notebook/data/integrated_data.csv',
                'table_name': 'cleaned_trades'
            },
            provide_context=True
        )
        
        clean_missing_values >> detect_outliers >> integrate_datasets >> load_to_postgres
    
    # ========================================================================
    # STAGE 2: ENCODING & STREAM PREPARATION
    # ========================================================================
    with TaskGroup(group_id='stage_2_data_processing') as stage_2:
        
        prepare_streaming_data = PythonOperator(
            task_id='prepare_streaming_data',
            python_callable=prepare_streaming_data_task,
            provide_context=True,
        )
        
        encode_categorical_data = PythonOperator(
            task_id='encode_categorical_data',
            python_callable=encode_categorical_data_task,
            provide_context=True,
        )
        
        prepare_streaming_data >> encode_categorical_data
    
    # ========================================================================
    # STAGE 3: KAFKA STREAMING
    # ========================================================================
    with TaskGroup(group_id='stage_3_kafka_streaming') as stage_3:
        
        start_kafka_producer = BashOperator(
            task_id='start_kafka_producer',
            bash_command='python /opt/airflow/plugins/kafka_producer.py',
        )
        
        consume_and_process_stream = PythonOperator(
            task_id='consume_and_process_stream',
            python_callable=consume_and_process_stream_task,
            provide_context=True,
        )
        
        save_final_to_postgres = PythonOperator(
            task_id='save_final_to_postgres',
            python_callable=save_final_to_postgres_task,
            provide_context=True,
        )
        
        start_kafka_producer >> consume_and_process_stream >> save_final_to_postgres
    
    # ========================================================================
    # STAGE 4: SPARK ANALYTICS (READS FROM POSTGRESQL)
    # ========================================================================
    with TaskGroup(group_id='stage_4_spark_analytics') as stage_4:
        
        initialize_spark_session_task = PythonOperator(
            task_id='initialize_spark_session',
            python_callable=initialize_spark_session_task,
            provide_context=True,
        )
        
        run_spark_analytics_task = PythonOperator(
            task_id='run_spark_analytics',
            python_callable=run_spark_analytics_task,
            op_kwargs={
                # Use Airflow container path (driver runs in Airflow, executors run in Spark)
                'input_path': '/opt/airflow/notebook/data/FULL_STOCKS.csv'
            },
            provide_context=True,
        )
        
        initialize_spark_session_task >> run_spark_analytics_task
    
    # ========================================================================
    # STAGE 6: AI AGENT QUERY PROCESSING
    # ========================================================================
    with TaskGroup(group_id='stage_6_ai_agent') as stage_6:
        
        setup_agent_volume = PythonOperator(
            task_id='setup_agent_volume',
            python_callable=setup_agent_volume_task,
            provide_context=True,
        )
        
        process_with_ai_agent = PythonOperator(
            task_id='process_with_ai_agent',
            python_callable=process_with_ai_agent_task,
            provide_context=True,
        )
        
        log_agent_responses = PythonOperator(
            task_id='log_agent_responses',
            python_callable=log_agent_responses_task,
            provide_context=True,
        )
        
        setup_agent_volume >> process_with_ai_agent >> log_agent_responses
    
    # ========================================================================
    # CROSS-STAGE DEPENDENCIES
    # ========================================================================
    stage_1 >> stage_2 >> stage_3 >> stage_4 >> stage_6
