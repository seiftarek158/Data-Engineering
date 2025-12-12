"""
Kafka Producer Script
Streams data ONLY from stream.csv to Kafka topic with 300ms delay between records
Sends EOS (End of Stream) message when complete
"""

import pandas as pd
import json
import time
from kafka import KafkaProducer

# Configuration
KAFKA_BOOTSTRAP_SERVERS = ['kafka:9092']
TOPIC_NAME = '55_0654_Topic'  # Replace with your actual ID
STREAM_FILE = '/opt/airflow/notebook/data/stream.csv'
SLEEP_TIME = 0.3  # 300 milliseconds

if __name__ == "__main__":
    print("="*70)
    print("KAFKA PRODUCER - STREAMING DATA")
    print("="*70)
    print(f"Topic: {TOPIC_NAME}")
    print(f"Delay between records: {SLEEP_TIME * 1000}ms")
    print("="*70 + "\n")

    try:
        # Load streaming data
        print(f"Loading streaming data from {STREAM_FILE}...")
        stream_data = pd.read_csv(STREAM_FILE)
        print(f"Loaded {len(stream_data)} records to stream")

        # Create producer
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print(f"Producer created. Streaming to topic: {TOPIC_NAME}")
        print("="*70)

        # Stream each record
        for idx, row in stream_data.iterrows():
            # Convert row to dictionary
            record = row.to_dict()

            # Convert any NaN values to None for JSON serialization
            record = {k: (None if pd.isna(v) else v) for k, v in record.items()}

            # Send to Kafka
            producer.send(TOPIC_NAME, value=record)

            print(f"Sent record {idx + 1}/{len(stream_data)}: Transaction ID {record.get('transaction_id')}")

            # Sleep for 300ms
            time.sleep(SLEEP_TIME)

        # Send End of Stream message
        eos_message = {"EOS": True, "message": "End of Stream"}
        producer.send(TOPIC_NAME, value=eos_message)
        print("="*70)
        print("✓ Sent EOS (End of Stream) message")

        # Flush and close producer
        producer.flush()
        producer.close()
        print("✓ Producer closed")
        print(f"\nTotal records streamed: {len(stream_data)}")
        print("\n✓ Streaming completed successfully!")

    except FileNotFoundError:
        print(f"\n✗ Error: Could not find {STREAM_FILE}")
        print("Make sure the file exists before running the producer.")
    except Exception as e:
        print(f"\n✗ Error occurred: {str(e)}")
        print("Make sure Kafka is running and accessible at kafka:9092")