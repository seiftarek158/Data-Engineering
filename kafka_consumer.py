"""
Kafka Consumer Script
Processes streamed data with encoding and appends to final dataset
"""

import pandas as pd
import json
from kafka import KafkaConsumer
from sklearn.preprocessing import LabelEncoder

# Configuration
KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
TOPIC_NAME = '55_0654_Topic'  # Replace with your actual ID
FINAL_OUTPUT_FILE = 'notebook/data/FULL_STOCKS.csv'
MAIN_DATA_PATH = 'notebook/data/integrated_main.csv'

if __name__ == "__main__":
    print("="*70)
    print("KAFKA CONSUMER - RECEIVING STREAMED DATA")
    print("="*70)
    print(f"Topic: {TOPIC_NAME}")
    print("="*70 + "\n")

    try:
        # Load and fit encoders based on the main dataset
        print("Loading encoders from main dataset...")
        main_data = pd.read_csv(MAIN_DATA_PATH)
        
        encoders = {}
        encoding_lookups = {}

        encoders['stock_ticker'] = LabelEncoder()
        encoders['transaction_type'] = LabelEncoder()
        encoders['customer_account_type'] = LabelEncoder()
        encoders['stock_sector'] = LabelEncoder()
        encoders['stock_industry'] = LabelEncoder()
        
        encoders['stock_ticker'].fit(main_data['stock_ticker'])
        encoders['transaction_type'].fit(main_data['transaction_type'])
        encoders['customer_account_type'].fit(main_data['customer_account_type'])
        encoders['stock_sector'].fit(main_data['stock_sector'])
        encoders['stock_industry'].fit(main_data['stock_industry'])
        
        encoding_lookups['day_names'] = main_data['day_name'].unique().tolist()
        
        print("✓ Encoders loaded successfully")

        # Load main dataset
        print(f"\nLoading main dataset from {MAIN_DATA_PATH}...")
        print(f"Main dataset: {len(main_data)} records")

        # Create consumer
        consumer = KafkaConsumer(
            TOPIC_NAME,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='stock-data-consumer',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )

        print(f"✓ Consumer subscribed to topic: {TOPIC_NAME}")
        print("Waiting for streamed data...")
        print("="*70)

        processed_records = []
        record_count = 0

        for message in consumer:
            record = message.value

            if 'EOS' in record and record['EOS']:
                print("="*70)
                print("✓ Received EOS (End of Stream) message")
                break

            # Process the record
            processed = record.copy()
            
            if 'stock_ticker' in processed and processed['stock_ticker'] is not None:
                processed['stock_ticker_encoded'] = int(encoders['stock_ticker'].transform([processed['stock_ticker']])[0])
            
            if 'transaction_type' in processed and processed['transaction_type'] is not None:
                processed['transaction_type_encoded'] = int(encoders['transaction_type'].transform([processed['transaction_type']])[0])
            
            if 'customer_account_type' in processed and processed['customer_account_type'] is not None:
                processed['customer_account_type_encoded'] = int(encoders['customer_account_type'].transform([processed['customer_account_type']])[0])
            
            if 'stock_sector' in processed and processed['stock_sector'] is not None:
                processed['stock_sector_encoded'] = int(encoders['stock_sector'].transform([processed['stock_sector']])[0])
            
            if 'stock_industry' in processed and processed['stock_industry'] is not None:
                processed['stock_industry_encoded'] = int(encoders['stock_industry'].transform([processed['stock_industry']])[0])
            
            if 'day_name' in processed and processed['day_name'] is not None:
                for day in encoding_lookups['day_names']:
                    processed[f'day_{day}'] = 1 if processed['day_name'] == day else 0
            
            if 'is_weekend' in processed:
                processed['is_weekend_encoded'] = int(processed['is_weekend']) if processed['is_weekend'] is not None else 0
            
            if 'is_holiday' in processed:
                processed['is_holiday_encoded'] = int(processed['is_holiday']) if processed['is_holiday'] is not None else 0

            processed_records.append(processed)
            record_count += 1
            
            print(f"Processed record {record_count}: Transaction ID {record.get('transaction_id')}")

        consumer.close()
        print("✓ Consumer closed")

        if processed_records:
            streamed_df = pd.DataFrame(processed_records)
            print(f"\n✓ Processed {len(streamed_df)} streamed records")
            
            full_dataset = pd.concat([main_data, streamed_df], ignore_index=True)
            print(f"✓ Combined dataset: {len(full_dataset)} total records")
            
            full_dataset.to_csv(FINAL_OUTPUT_FILE, index=False)
            print(f"✓ Saved to {FINAL_OUTPUT_FILE}")
            
            print(f"\n✓ Streaming processing completed!")
            print(f"Final dataset shape: {full_dataset.shape}")
        else:
            print("\n⚠ No records received from stream")

    except Exception as e:
        print(f"\n✗ Error occurred: {str(e)}")
        print("Make sure Kafka is running and producer has sent data")