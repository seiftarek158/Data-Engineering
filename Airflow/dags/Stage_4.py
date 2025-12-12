"""
Stock Portfolio Pipeline - Stage 4: Spark Analytics
====================================================

This stage implements Spark analytics tasks:
- initialize_spark_session: Create Spark session connected to master node
- run_spark_analytics: Execute Spark DataFrame and SQL queries, save results to PostgreSQL
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
import os

# Heavy imports (PySpark, SQLAlchemy) moved inside functions to speed up DAG parsing

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
# STAGE 4: SPARK ANALYTICS TASKS
# ============================================================================

def initialize_spark_session(**context):
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
    
    # Create Spark session
    spark = SparkSession.builder \
        .appName(spark_app_name) \
        .master(spark_master) \
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


def run_spark_analytics(input_path, **context):
    """
    Task 2: Run Spark Analytics
    
    This function:
    1. Connects to existing Spark session
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
    
    # Connect to Spark session
    print(f"\nConnecting to Spark session: {spark_app_name}")
    spark = SparkSession.builder \
        .appName(spark_app_name) \
        .master(spark_master) \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    print("✓ Connected to Spark session")
    
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
    
    # Compute total trade amount for each day
    sums = []
    for col in day_cols:
        total = df.filter(fn.col(col) == 1) \
                  .agg(fn.sum("total_trade_amount").alias("total")) \
                  .collect()[0]["total"]
        total = float(total) if total is not None else 0.0
        sums.append((col.replace("day_", ""), total))
    
    q5_result = spark.createDataFrame(sums, ["day", "total_trade_amount"]) \
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
# DAG DEFINITION
# ============================================================================

# Create the DAG
with DAG(
    dag_id='stage_4_spark_analytics_55_0654',
    default_args=default_args,
    description='Stage 4: Spark Analytics - DataFrame and SQL Queries',
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2025, 12, 12),
    catchup=False,
    tags=['data-engineering', 'spark', 'analytics', 'milestone-2', 'team-55-0654', 'stage-4'],
) as dag:
    
    # Task Group: Stage 4 - Spark Analytics
    with TaskGroup(group_id='stage_4_spark_analytics') as stage_4:
        
        # Task 1: Initialize Spark Session
        task_init_spark = PythonOperator(
            task_id='initialize_spark_session',
            python_callable=initialize_spark_session,
            provide_context=True,
            
        )
        
        # Task 2: Run Spark Analytics
        task_run_analytics = PythonOperator(
            task_id='run_spark_analytics',
            python_callable=run_spark_analytics,
            op_kwargs={
                'input_path': '/opt/airflow/notebook/data/FULL_STOCKS.csv'
            },
            provide_context=True,
            
        )
        
        # Define task dependencies
        task_init_spark >> task_run_analytics