"""
Stage 2: Encoding & Stream Preparation Task Functions
======================================================

This module contains all task functions for Stage 2 of the pipeline:
- prepare_streaming_data_task: Prepare 5% sample for streaming, 95% for batch
- encode_categorical_data_task: Encode categorical columns and generate lookup tables
"""

import os


def prepare_streaming_data(**context):
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
    
    df = pd.read_csv(os.path.join(data_path, "integrated_data.csv"))
    
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


def encode_categorical_data(**context):
    """Encode categorical columns and generate lookup tables"""
    import pandas as pd
    import numpy as np
    from sklearn.preprocessing import LabelEncoder
    
    print("="*70)
    print("STAGE 2 TASK 2: ENCODE CATEGORICAL DATA")
    print("="*70)
    
    data_path = "/opt/airflow/notebook/data/"
    lookups_path = os.path.join(data_path, "lookups")
    os.makedirs(lookups_path, exist_ok=True)
    
    batch_df = pd.read_csv(os.path.join(data_path, "batch_data_for_encoding.csv"))
    
    print(f"Loaded batch data: {len(batch_df)} records")
    
    # Create a copy to avoid modifying the original dataframe
    encoded_df = batch_df.copy()
    
    # Initialize label encoders
    le_stock = LabelEncoder()
    le_transaction = LabelEncoder()
    le_account = LabelEncoder()
    le_sector = LabelEncoder()
    le_industry = LabelEncoder()
    
    # Apply Label Encoding - MODIFY ORIGINAL COLUMNS
    # 1. stock_ticker
    encoded_df['stock_ticker'] = le_stock.fit_transform(encoded_df['stock_ticker'])
    
    # 2. transaction_type
    encoded_df['transaction_type'] = le_transaction.fit_transform(encoded_df['transaction_type'])
    
    # 3. customer_account_type
    encoded_df['customer_account_type'] = le_account.fit_transform(encoded_df['customer_account_type'])
    
    # 4. stock_sector
    encoded_df['stock_sector'] = le_sector.fit_transform(encoded_df['stock_sector'])
    
    # 5. stock_industry
    encoded_df['stock_industry'] = le_industry.fit_transform(encoded_df['stock_industry'])
    
    # Apply One-Hot Encoding for day_name
    day_dummies = pd.get_dummies(encoded_df['day_name'], prefix='day', dtype=int)
    encoded_df = pd.concat([encoded_df, day_dummies], axis=1)
    encoded_df.drop('day_name', axis=1, inplace=True)  # Remove original column
    
    # Boolean to Binary - MODIFY ORIGINAL COLUMNS
    encoded_df['is_weekend'] = encoded_df['is_weekend'].astype(int)
    encoded_df['is_holiday'] = encoded_df['is_holiday'].astype(int)
    
    # Store the encoders as attributes
    encoded_df.attrs['encoders'] = {
        'stock_ticker': le_stock,
        'transaction_type': le_transaction,
        'customer_account_type': le_account,
        'stock_sector': le_sector,
        'stock_industry': le_industry
    }
    
    print("✓ Encoding completed")
    
    # Create lookup tables
    lookup_tables = create_encoding_lookup_tables(encoded_df)
    
    # Save lookup tables
    save_lookup_tables(lookup_tables, lookups_path)
    
    # Save encoded data
    encoded_df.to_csv(
        os.path.join(data_path, "integrated_encoded_trades_data.csv"), index=False
    )
    
    print(f"✓ Encoded data saved")
    print(f"✓ Saved lookup tables to {lookups_path}")
    print("✓ STAGE 2 TASK 2 COMPLETED")
    return encoded_df


def create_encoding_lookup_tables(df_encoded):
    """
    Creates lookup tables for all encoded columns showing the mapping
    between original and encoded values.
    """
    import pandas as pd
    
    lookup_tables = {}
    
    # Get the encoders from the encoded dataframe attributes
    encoders = df_encoded.attrs.get('encoders', {})
    
    # 1. Stock Ticker Lookup (Label Encoding)
    if 'stock_ticker' in encoders:
        le_stock = encoders['stock_ticker']
        stock_lookup = pd.DataFrame({
            'Column Name': 'stock_ticker',
            'Original Value': le_stock.classes_,
            'Encoded Value': le_stock.transform(le_stock.classes_)
        })
        lookup_tables['stock_ticker'] = stock_lookup.sort_values('Encoded Value')
    
    # 2. Transaction Type Lookup (Label Encoding)
    if 'transaction_type' in encoders:
        le_trans = encoders['transaction_type']
        trans_lookup = pd.DataFrame({
            'Column Name': 'transaction_type',
            'Original Value': le_trans.classes_,
            'Encoded Value': le_trans.transform(le_trans.classes_)
        })
        lookup_tables['transaction_type'] = trans_lookup.sort_values('Encoded Value')
    
    # 3. Customer Account Type Lookup (Label Encoding)
    if 'customer_account_type' in encoders:
        le_account = encoders['customer_account_type']
        account_lookup = pd.DataFrame({
            'Column Name': 'customer_account_type',
            'Original Value': le_account.classes_,
            'Encoded Value': le_account.transform(le_account.classes_)
        })
        lookup_tables['customer_account_type'] = account_lookup.sort_values('Encoded Value')
    
    # 4. Stock Sector Lookup (Label Encoding)
    if 'stock_sector' in encoders:
        le_sector = encoders['stock_sector']
        sector_lookup = pd.DataFrame({
            'Column Name': 'stock_sector',
            'Original Value': le_sector.classes_,
            'Encoded Value': le_sector.transform(le_sector.classes_)
        })
        lookup_tables['stock_sector'] = sector_lookup.sort_values('Encoded Value')
    
    # 5. Stock Industry Lookup (Label Encoding)
    if 'stock_industry' in encoders:
        le_industry = encoders['stock_industry']
        industry_lookup = pd.DataFrame({
            'Column Name': 'stock_industry',
            'Original Value': le_industry.classes_,
            'Encoded Value': le_industry.transform(le_industry.classes_)
        })
        lookup_tables['stock_industry'] = industry_lookup.sort_values('Encoded Value')
    
    # 6. Day Name Lookup (One-Hot Encoding)
    day_cols = [col for col in df_encoded.columns if col.startswith('day_')]
    day_lookup_data = []
    for day_col in sorted(day_cols):
        day_name = day_col.replace('day_', '')
        day_lookup_data.append({
            'Column Name': 'day_name',
            'Original Value': day_name,
            'Encoded Column': day_col
        })
    if day_lookup_data:
        day_lookup = pd.DataFrame(day_lookup_data)
        lookup_tables['day_name'] = day_lookup
    
    # 7. Is Weekend Lookup (Boolean to Binary)
    weekend_lookup = pd.DataFrame({
        'Column Name': ['is_weekend', 'is_weekend'],
        'Original Value': [False, True],
        'Encoded Value': [0, 1]
    })
    lookup_tables['is_weekend'] = weekend_lookup
    
    # 8. Is Holiday Lookup (Boolean to Binary)
    holiday_lookup = pd.DataFrame({
        'Column Name': ['is_holiday', 'is_holiday'],
        'Original Value': [False, True],
        'Encoded Value': [0, 1]
    })
    lookup_tables['is_holiday'] = holiday_lookup
    
    return lookup_tables


def save_lookup_tables(lookup_tables, lookups_path):
    """
    Saves all lookup tables to CSV files.
    """
    import pandas as pd
    
    # Save individual encoding lookup tables
    for name, table in lookup_tables.items():
        filename = os.path.join(lookups_path, f'encoding_lookup_{name}.csv')
        table.to_csv(filename, index=False)
        print(f"Saved: {filename}")
    
    # Collect all encoding lookup tables: label-encoded, one-hot and binary
    label_keys = ['stock_ticker', 'stock_sector', 'stock_industry', 'transaction_type', 'customer_account_type']
    onehot_keys = ['day_name']
    binary_keys = ['is_weekend', 'is_holiday']
    selected_keys = label_keys + onehot_keys + binary_keys
    
    # Keep only keys that exist in lookup_tables
    selected_tables = [v for k, v in lookup_tables.items() if k in selected_keys]
    if selected_tables:
        master_lookup = pd.concat(selected_tables, ignore_index=True)
        master_lookup_file = os.path.join(lookups_path, 'master_encoding_lookup.csv')
        master_lookup.to_csv(master_lookup_file, index=False)
        print(f"Saved: {master_lookup_file}")
