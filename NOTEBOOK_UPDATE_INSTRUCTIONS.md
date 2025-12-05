# Notebook Update Instructions

## Required Changes for `data_final_notebook copy.ipynb`

### 1. After the 5% Split Cell (around line 3644)
**Current code:**
```python
integrated_main.to_csv('data/integrated_main.csv', index=False)
print(f"✓ Main dataset saved to: data/integrated_main.csv")

# Stream data is already saved as stream.csv
```

**Replace with:**
```python
# Save unencoded main data for encoding lookup creation in Kafka consumer
integrated_main.to_csv('data/integrated_main_unencoded.csv', index=False)
print(f"✓ Unencoded data saved for lookups: data/integrated_main_unencoded.csv")

# Encode the 95% main dataset
print("\n" + "="*70)
print("ENCODING MAIN DATASET (95%)")
print("="*70)
integrated_main_encoded = encode_data(integrated_main)
print(f"✓ Encoded main dataset: {len(integrated_main_encoded)} rows")

# Save encoded main data for Kafka consumer to use
integrated_main_encoded.to_csv('data/integrated_main.csv', index=False)
print(f"✓ Encoded data saved to: data/integrated_main.csv")
print("="*70)

# Stream data is already saved as stream.csv
```

---

### 2. Consumer Function Signature (around line 4703)
**Current:**
```python
def consume_kafka_stream(topic_name='55_0654_Topic', bootstrap_servers=['localhost:9092'], 
                         main_data_path='data/integrated_main.csv', 
                         output_file='data/FULL_STOCKS.csv'):
```

**Replace with:**
```python
def consume_kafka_stream(topic_name='55_0654_Topic', bootstrap_servers=['localhost:9092'], 
                         main_data_path='data/integrated_main.csv',
                         unencoded_data_path='data/integrated_main_unencoded.csv',
                         output_file='data/FULL_STOCKS.csv'):
```

---

### 3. Consumer Function Docstring
**Current:**
```python
    main_data_path : str
        Path to the main dataset for fitting encoders
    output_file : str
```

**Replace with:**
```python
    main_data_path : str
        Path to the encoded main dataset for final combination
    unencoded_data_path : str
        Path to the unencoded main dataset for creating encoding lookups
    output_file : str
```

---

### 4. Consumer Function - Loading Data
**Current:**
```python
    try:
        # Load main dataset and create encoding lookups
        print("Loading encoding lookups from main dataset...")
        main_data = pd.read_csv(main_data_path)
        
        encoding_lookups = {}
        
        # Create lookup dictionaries for label encoding
        encoding_lookups['stock_ticker'] = {val: idx for idx, val in enumerate(sorted(main_data['stock_ticker'].unique()))}
        encoding_lookups['transaction_type'] = {val: idx for idx, val in enumerate(sorted(main_data['transaction_type'].unique()))}
        encoding_lookups['customer_account_type'] = {val: idx for idx, val in enumerate(sorted(main_data['customer_account_type'].unique()))}
        encoding_lookups['stock_sector'] = {val: idx for idx, val in enumerate(sorted(main_data['stock_sector'].unique()))}
        
        # Create lists for one-hot encoding
        encoding_lookups['day_names'] = sorted(main_data['day_name'].unique().tolist())
        encoding_lookups['industry_names'] = sorted(main_data['stock_industry'].unique().tolist())
        
        print("✓ Encoding lookups created successfully")
        print(f"Main dataset: {len(main_data)} records")
```

**Replace with:**
```python
    try:
        # Load unencoded data to create encoding lookups
        print("Loading encoding lookups from unencoded main dataset...")
        unencoded_data = pd.read_csv(unencoded_data_path)
        
        encoding_lookups = {}
        
        # Create lookup dictionaries for label encoding
        encoding_lookups['stock_ticker'] = {val: idx for idx, val in enumerate(sorted(unencoded_data['stock_ticker'].unique()))}
        encoding_lookups['transaction_type'] = {val: idx for idx, val in enumerate(sorted(unencoded_data['transaction_type'].unique()))}
        encoding_lookups['customer_account_type'] = {val: idx for idx, val in enumerate(sorted(unencoded_data['customer_account_type'].unique()))}
        encoding_lookups['stock_sector'] = {val: idx for idx, val in enumerate(sorted(unencoded_data['stock_sector'].unique()))}
        
        # Create lists for one-hot encoding
        encoding_lookups['day_names'] = sorted(unencoded_data['day_name'].unique().tolist())
        encoding_lookups['industry_names'] = sorted(unencoded_data['stock_industry'].unique().tolist())
        
        print("✓ Encoding lookups created successfully")
        
        # Load encoded main data for final combination
        print("Loading encoded main dataset...")
        main_data = pd.read_csv(main_data_path)
        print(f"Main dataset: {len(main_data)} records")
```

---

### 5. Consumer Function - Sorting Before Save
**Current:**
```python
            full_dataset = pd.concat([main_data, streamed_df], ignore_index=True)
            print(f"✓ Combined dataset: {len(full_dataset)} total records")
            
            full_dataset.to_csv(output_file, index=False)
            print(f"✓ Saved to {output_file}")
```

**Replace with:**
```python
            full_dataset = pd.concat([main_data, streamed_df], ignore_index=True)
            print(f"✓ Combined dataset: {len(full_dataset)} total records")
            
            # Sort by transaction_id before saving
            print("\n✓ Sorting dataset by transaction_id...")
            full_dataset = full_dataset.sort_values(by='transaction_id').reset_index(drop=True)
            print(f"✓ Dataset sorted successfully")
            
            full_dataset.to_csv(output_file, index=False)
            print(f"✓ Saved to {output_file}")
```

---

### 6. Consumer Function Call (around line 4852)
**Current:**
```python
#     main_data_path='data/integrated_main.csv',
#     output_file='data/FULL_STOCKS.csv'
# )
```

**Replace with:**
```python
#     main_data_path='data/integrated_main.csv',
#     unencoded_data_path='data/integrated_main_unencoded.csv',
#     output_file='data/FULL_STOCKS.csv'
# )
```

---

## Summary of Changes

1. **Save unencoded 95% data** as `integrated_main_unencoded.csv` BEFORE encoding
2. **Encode the 95% main dataset** and save as `integrated_main.csv`
3. **Update consumer function** to accept `unencoded_data_path` parameter
4. **Load unencoded data** for creating encoding lookups (to access `day_name` and `stock_industry` columns)
5. **Load encoded data** for final combination with streamed records
6. **Sort by transaction_id** before saving final FULL_STOCKS.csv
7. **Update function call** with the new parameter

These changes ensure that:
- Encoding lookups are created from unencoded data (which still has the original categorical columns)
- The 95% main dataset is properly encoded before being combined
- The final dataset is sorted by transaction_id for proper ordering
