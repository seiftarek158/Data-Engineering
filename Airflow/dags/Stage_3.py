from __future__ import annotations

import json
import os
from datetime import datetime

import pandas as pd
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from sqlalchemy import create_engine


@dag(
    dag_id="stage_3_kafka_streaming",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=["kafka", "stage_3", "streaming"],
    doc_md="""
    # Stage 3: Kafka Streaming Pipeline
    
    This DAG handles real-time streaming data processing:
    1. Produces streaming data to Kafka topic
    2. Consumes and encodes the stream using lookup tables
    3. Saves final processed data to PostgreSQL
    
    **Dependencies:** Kafka, Zookeeper, PostgreSQL must be running
    """,
)
def stage_3_kafka_streaming_dag():
    """
    This DAG represents Stage 3 of the data pipeline: Kafka Streaming.
    It produces data to a Kafka topic, consumes and processes it, and saves it to PostgreSQL.
    """

    start_kafka_producer = BashOperator(
        task_id="start_kafka_producer",
        bash_command="python /opt/airflow/plugins/kafka_producer.py",
        doc_md="""
        #### Task Details
        **BashOperator** executes the Kafka producer script.
        
        The producer:
        - Reads from the `stream.csv` file
        - Sends records to Kafka topic `stock-trades-topic`
        - Sends an 'EOS' (End of Stream) message upon completion
        
        **Note:** Adjust the path if kafka_producer.py is in a different location.
        Common paths: /opt/airflow/plugins/ or /opt/airflow/dags/
        """,
    )

    @task
    def consume_and_process_stream():
        """
        Consumes data from Kafka, applies encoding, and saves the processed data.
        
        This task:
        1. Connects to Kafka consumer
        2. Loads encoding lookup tables
        3. Processes each message and applies categorical encoding
        4. Saves final encoded data to CSV
        """
        from kafka import KafkaConsumer

        data_path = "/opt/airflow/notebook/data/"
        lookups_path = os.path.join(data_path, "lookups")
        output_file = os.path.join(data_path, "FINAL_STOCKS.csv")

        print(f"Loading lookup tables from: {lookups_path}")
        
        # Load all lookup tables for encoding
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
            "stock-trades-topic",
            bootstrap_servers="kafka:9092",
            auto_offset_reset="earliest",
            consumer_timeout_ms=60000,  # Timeout after 60 seconds of inactivity
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        )

        processed_records = []
        message_count = 0
        
        print("Starting to consume messages from Kafka...")
        for message in consumer:
            message_count += 1
            
            if message.value == "EOS":
                print(f"End of Stream message received after processing {message_count - 1} messages.")
                break
            
            record = message.value
            
            # Apply encoding to categorical columns
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
            
            # Ensure all required columns are present, fill with NaN if not
            # This is a safety measure if some messages are malformed
            all_cols = [
                'transaction_id', 'timestamp', 'customer_id', 'stock_ticker',
                'transaction_type', 'quantity', 'average_trade_size',
                'cumulative_portfolio_value', 'date', 'customer_key', 'account_type',
                'avg_trade_size_baseline', 'stock_key', 'company_name',
                'liquidity_tier', 'sector', 'industry', 'date_key', 'day', 'month',
                'month_name', 'quarter', 'year', 'day_of_week', 'day_name',
                'is_weekend', 'is_holiday', 'stock_price'
            ]
            
            # Add missing columns
            for col in all_cols:
                if col not in final_df.columns:
                    final_df[col] = None
                    print(f"Warning: Column '{col}' was missing, filled with None")

            final_df = final_df[all_cols]  # Ensure column order
            final_df.to_csv(output_file, index=False)
            print(f"✓ Successfully saved {len(processed_records)} records to {output_file}")
        else:
            print("⚠ Warning: No records were processed. Check Kafka producer and topic.")
            raise ValueError("No records were consumed from Kafka stream")


    @task
    def save_final_to_postgres():
        """
        Saves the final processed streaming data to PostgreSQL database.
        
        This task:
        1. Reads the processed CSV file
        2. Connects to PostgreSQL using db_utils
        3. Saves data to 'final_stocks' table (replaces if exists)
        """
        data_path = "/opt/airflow/notebook/data/"
        input_file = os.path.join(data_path, "FINAL_STOCKS.csv")
        
        print(f"Looking for file: {input_file}")
        
        if not os.path.exists(input_file):
            raise FileNotFoundError(
                f"The file {input_file} was not found. "
                "The Kafka consumer task might have failed or produced no data."
            )

        print("Reading processed data from CSV...")
        final_df = pd.read_csv(input_file)
        print(f"Loaded {len(final_df)} records with {len(final_df.columns)} columns")
        
        # Connect to PostgreSQL database
        # Database connection details from docker-compose
        print("Connecting to PostgreSQL database...")
        db_host = "pgdatabase"
        db_port = 5432
        db_user = "postgres"
        db_password = "postgres"
        db_name = "Trades_Database"
        
        connection_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        engine = create_engine(connection_string)

        # Save to PostgreSQL
        table_name = "final_stocks"
        print(f"Saving data to table '{table_name}'...")
        final_df.to_sql(table_name, engine, if_exists="replace", index=False)
        
        print(f"✓ Successfully saved {len(final_df)} records to PostgreSQL table '{table_name}'.")
        print(f"  Database: {db_name}")
        print(f"  Host: {db_host}")
        
        engine.dispose()
        print("Database connection closed.")

    # Define Task Dependencies
    consume_task = consume_and_process_stream()
    save_task = save_final_to_postgres()

    start_kafka_producer >> consume_task >> save_task

stage_3_dag = stage_3_kafka_streaming_dag()
