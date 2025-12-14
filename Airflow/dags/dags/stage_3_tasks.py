"""
Stage 3: Kafka Streaming Task Functions
========================================

This module contains all task functions for Stage 3 of the pipeline:
- consume_and_process_stream_task: Consume data from Kafka, apply encoding, save processed data
- save_final_to_postgres_task: Save the final processed streaming data to PostgreSQL
"""

import os


def encode_row(row, encoding_lookups):
    """
    Encodes a single row/record with the same logic as encode_data but for individual rows.
    
    Parameters:
    -----------
    row : dict
        A single record/row to encode
    encoding_lookups : dict
        Dictionary containing all encoding mappings:
        - 'stock_ticker': dict mapping ticker names to encoded values
        - 'transaction_type': dict mapping transaction types to encoded values
        - 'customer_account_type': dict mapping account types to encoded values
        - 'stock_sector': dict mapping sectors to encoded values
        - 'stock_industry': dict mapping industries to encoded values
        - 'day_names': list of all possible day names
    
    Returns:
    --------
    dict
        The encoded row with all transformations applied
    """
    encoded_row = row.copy()
    
    # Label Encoding - Modify values directly using lookup dictionaries
    if 'stock_ticker' in encoded_row and encoded_row['stock_ticker'] is not None:
        encoded_row['stock_ticker'] = encoding_lookups['stock_ticker'].get(row['stock_ticker'], -1)
    
    if 'transaction_type' in encoded_row and encoded_row['transaction_type'] is not None:
        encoded_row['transaction_type'] = encoding_lookups['transaction_type'].get(row['transaction_type'], -1)
    
    if 'customer_account_type' in encoded_row and encoded_row['customer_account_type'] is not None:
        encoded_row['customer_account_type'] = encoding_lookups['customer_account_type'].get(row['customer_account_type'], -1)
    
    if 'stock_sector' in encoded_row and encoded_row['stock_sector'] is not None:
        encoded_row['stock_sector'] = encoding_lookups['stock_sector'].get(row['stock_sector'], -1)
    
    if 'stock_industry' in encoded_row and encoded_row['stock_industry'] is not None:
        encoded_row['stock_industry'] = encoding_lookups['stock_industry'].get(row['stock_industry'], -1)
    
    # One-Hot Encoding - Create new columns and remove original
    if 'day_name' in encoded_row and encoded_row['day_name'] is not None:
        day_value = encoded_row['day_name']
        for day in encoding_lookups['day_names']:
            encoded_row[f'day_{day}'] = 1 if day_value == day else 0
        del encoded_row['day_name']  # Remove original column
    
    # Boolean to Binary - Modify values directly
    if 'is_weekend' in encoded_row:
        encoded_row['is_weekend'] = int(encoded_row['is_weekend']) if encoded_row['is_weekend'] is not None else 0
    
    if 'is_holiday' in encoded_row:
        encoded_row['is_holiday'] = int(encoded_row['is_holiday']) if encoded_row['is_holiday'] is not None else 0
    
    return encoded_row


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
    master_lookup_file = os.path.join(lookups_path, "master_encoding_lookup.csv")
    output_file = os.path.join(data_path, "FINAL_STOCKS.csv")
    
    print(f"Loading encoding lookups from master lookup table...")
    
    if not os.path.exists(master_lookup_file):
        raise FileNotFoundError(f"Master lookup file not found: {master_lookup_file}")
    
    lookup_df = pd.read_csv(master_lookup_file)
    
    encoding_lookups = {}
    
    # Create lookup dictionaries for label encoding
    label_encoded = lookup_df[lookup_df['Encoded Value'].notna()]
    
    for col_name in ['stock_ticker', 'transaction_type', 'customer_account_type', 'stock_sector', 'stock_industry']:
        col_data = label_encoded[label_encoded['Column Name'] == col_name]
        encoding_lookups[col_name] = dict(zip(
            col_data['Original Value'],
            col_data['Encoded Value'].astype(int)
        ))
    
    # Create lists for one-hot encoding
    onehot_encoded = lookup_df[lookup_df['Encoded Column'].notna() & (lookup_df['Encoded Column'] != '')]
    encoding_lookups['day_names'] = sorted(
        onehot_encoded[onehot_encoded['Column Name'] == 'day_name']['Original Value'].tolist()
    )
    
    print("✓ Encoding lookups loaded successfully")
    print(f"  - Label encoded: stock_ticker ({len(encoding_lookups['stock_ticker'])}), "
          f"transaction_type ({len(encoding_lookups['transaction_type'])}), "
          f"customer_account_type ({len(encoding_lookups['customer_account_type'])}), "
          f"stock_sector ({len(encoding_lookups['stock_sector'])}), "
          f"stock_industry ({len(encoding_lookups['stock_industry'])})")
    print(f"  - One-hot encoded: day_names ({len(encoding_lookups['day_names'])})")
    
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
        
        # Apply encoding using the encode_row function
        encoded_record = encode_row(record, encoding_lookups)
        processed_records.append(encoded_record)
        
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
