import pyspark  

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as fn
# Set environment variables before importing Spark
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

# Initialize Spark with proper configuration for Windows
spark = SparkSession.builder \
.appName("SparkAppName") \
.master("spark://spark-master:7077") \
.getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("ERROR")

print("Spark Session initialized successfully.")

# 1. Read the CSV file using Spark as a Spark DataFrame
csv_path = "notebook/data/integrated_main.csv"
df = spark.read.csv(csv_path, header=True, inferSchema=True)

print(f"\nLoaded CSV file: {csv_path}")
print(f"Total records: {df.count()}")
print(f"Total columns: {len(df.columns)}")

# 2. Show the first 10 records to ensure all data are correctly loaded
print("\nFirst 10 records:")
df.show(10)

print("\n" + "="*80)
print("SPARK ANALYSIS QUESTIONS")
print("="*80)

# Question 1: What is the total trading volume for each stock ticker?
print("\n1. Total trading volume for each stock ticker:")
q1_result = df.groupBy("stock_ticker") \
    .agg(fn.sum("quantity").alias("total_volume")) \
    .orderBy(fn.desc("total_volume"))
q1_result.show()

# Question 2: What is the average stock price by sector?
print("\n2. Average stock price by sector:")
q2_result = df.groupBy("stock_sector") \
    .agg(fn.avg("stock_price").alias("avg_stock_price")) \
    .orderBy(fn.desc("avg_stock_price"))
q2_result.show()

# Question 3: How many buy vs sell transactions occurred on weekends?
print("\n3. Buy vs Sell transactions on weekends:")
q3_result = df.filter(fn.col("is_weekend") == 1) \
    .groupBy("transaction_type") \
    .agg(fn.count("transaction_id").alias("transaction_count")) \
    .orderBy("transaction_type")
q3_result.show()

# Question 4: Which customers have made more than 10 transactions?
print("\n4. Customers with more than 10 transactions:")
q4_result = df.groupBy("customer_id") \
    .agg(fn.count("transaction_id").alias("transaction_count")) \
    .filter(fn.col("transaction_count") > 10) \
    .orderBy(fn.desc("transaction_count"))
print(f"Total customers with >10 transactions: {q4_result.count()}")
q4_result.show()

# Question 5: What is the total trade amount per day of the week, ordered from highest to lowest?
print("\n5. Total trade amount per day of the week (highest to lowest):")
q5_result = df.groupBy("day_name") \
    .agg(fn.sum("total_trade_amount").alias("total_trade_amount")) \
    .orderBy(fn.desc("total_trade_amount"))
q5_result.show()

print("\n" + "="*80)
print("ANALYSIS COMPLETE")
print("="*80)

# Register the DataFrame as a temporary SQL table
df.createOrReplaceTempView("trades")

print("\n" + "="*80)
print("SPARK SQL ANALYSIS QUESTIONS")
print("="*80)

# SQL Question 1: What are the top 5 most traded stock tickers by total quantity?
print("\nSQL 1. Top 5 most traded stock tickers by total quantity:")
sql1_result = spark.sql("""
    SELECT stock_ticker, 
           SUM(quantity) as total_quantity
    FROM trades
    GROUP BY stock_ticker
    ORDER BY total_quantity DESC
    LIMIT 5
""")
sql1_result.show()

# SQL Question 2: What is the average trade amount by customer account type?
print("\nSQL 2. Average trade amount by customer account type:")
sql2_result = spark.sql("""
    SELECT customer_account_type,
           AVG(total_trade_amount) as avg_trade_amount,
           COUNT(*) as transaction_count
    FROM trades
    GROUP BY customer_account_type
    ORDER BY avg_trade_amount DESC
""")
sql2_result.show()

# SQL Question 3: How many transactions occurred during holidays vs non-holidays?
print("\nSQL 3. Transactions during holidays vs non-holidays:")
sql3_result = spark.sql("""
    SELECT 
        CASE 
            WHEN is_holiday = 1 THEN 'Holiday'
            ELSE 'Non-Holiday'
        END as period_type,
        COUNT(transaction_id) as transaction_count
    FROM trades
    GROUP BY is_holiday
    ORDER BY is_holiday DESC
""")
sql3_result.show()

# SQL Question 4: Which stock sectors had the highest total trading volume on weekends?
print("\nSQL 4. Stock sectors with highest total trading volume on weekends:")
sql4_result = spark.sql("""
    SELECT stock_sector,
           SUM(quantity) as total_volume,
           COUNT(transaction_id) as transaction_count
    FROM trades
    WHERE is_weekend = 1
    GROUP BY stock_sector
    ORDER BY total_volume DESC
""")
sql4_result.show()

# SQL Question 5: What is the total buy vs sell amount for each stock liquidity tier?
print("\nSQL 5. Total buy vs sell amount for each stock liquidity tier:")
sql5_result = spark.sql("""
    SELECT stock_liquidity_tier,
           transaction_type,
           SUM(total_trade_amount) as total_amount,
           COUNT(transaction_id) as transaction_count
    FROM trades
    GROUP BY stock_liquidity_tier, transaction_type
    ORDER BY stock_liquidity_tier, transaction_type
""")
sql5_result.show()

print("\n" + "="*80)
print("SPARK SQL ANALYSIS COMPLETE")
print("="*80)


