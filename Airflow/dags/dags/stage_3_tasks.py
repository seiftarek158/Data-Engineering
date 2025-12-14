"""
Stage 3: Kafka Streaming Task Functions
========================================

This module contains all task functions for Stage 3 of the pipeline:
- consume_and_process_stream_task: Consume data from Kafka, apply encoding, save processed data
- save_final_to_postgres_task: Save the final processed streaming data to PostgreSQL
"""

import os


def consume_and_process_stream(**context):
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
    return final_df


def save_final_to_postgres(**context):
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
