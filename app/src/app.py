import pandas as pd
from scipy.stats import mstats
import numpy as np
from db_utils import save_to_db

def extract_data(data_path):
  return pd.read_csv(data_path)

def impute_missing_data(dtp, trades):
    dtp_copy = dtp.copy()
    trades_copy = trades.copy()
    # First we will convert the timestamp to date in order to compare it with the dtp
    trades_copy['date'] = pd.to_datetime(trades_copy['timestamp']).dt.date

    #Then we will calculate the estimated prices per date per stock from the trades dataframe
    estimated_prices_per_date_per_stock = trades_copy.groupby(['date', 'stock_ticker']).apply(
        lambda x: (x['cumulative_portfolio_value'] / x['quantity'] * x['quantity']).sum() / x['quantity'].sum()
    ).reset_index(name='price')
    len(estimated_prices_per_date_per_stock)

    for col in dtp_copy.columns:
        if col == 'date':
            continue
        
        # Get trade prices for this stock
        stock_prices = estimated_prices_per_date_per_stock[estimated_prices_per_date_per_stock['stock_ticker'] == col].set_index('date')['price']
        
        # Fill missing values from trades by checking the null rows and then mapping it to the correct stockprice using the date
        missing_mask = dtp_copy[col].isnull()
        dtp_copy.loc[missing_mask, col] = dtp_copy.loc[missing_mask, 'date'].map(stock_prices)
        
        # Fill any remaining with forward fill
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


    return pd.DataFrame(rows),trades_copy

def outlier_checker_IQR(column)->bool:
    series = column
    n = len(series)


    # Calculate IQR-based outliers
    q1 = series.quantile(0.25)
    q3 = series.quantile(0.75)
    iqr = q3 - q1
    lower = q1 - 1.5 * iqr
    upper = q3 + 1.5 * iqr
    outliers_mask = (series < lower) | (series > upper)
    n_outliers = outliers_mask.sum()
    pct_outliers = (n_outliers / n) * 100
    
    return pct_outliers<10  

def outlier_checker_Zscore(column)->bool:
    series = column
    n = len(series)

    # Calculate Z-score
    mean = series.mean()
    std = series.std()
    z_scores = (series - mean) / std if std else 0
    n_outliers = (abs(z_scores) > 3).sum()
    pct_outliers = (n_outliers / n) * 100

    return pct_outliers<10

def handle_outliers_logtransformation(df,col):
    df_copy = df.copy()
    if(df[col].min() <= 0):
        df_copy[col+"_log"] = np.log1p(df_copy[col])
    else:
        df_copy[col+"_log"] = np.log(df_copy[col])
    return df_copy

def handle_outliers_winsorizecapped(df,col):
    df_copy = df.copy()
    df_copy[col+"_winsorized"] = mstats.winsorize(df_copy[col], limits=[0.15, 0.15])
    return df_copy

def integrate_data(dtp, trades, dc, dd, ds):
   
    trades_joined_with_dc= trades.merge(dc, how='left', left_on='customer_id', right_on='customer_id')
    trades_and_dc_joined_with_ds= trades_joined_with_dc.merge(ds, how='left', left_on='stock_ticker', right_on='stock_ticker')
    dd['date'] = pd.to_datetime(dd['date']).dt.date
    trades_and_dc_and_ds_joined_with_dd= trades_and_dc_joined_with_ds.merge(dd[['date', 'day_name', 'is_weekend', 'is_holiday']], how='left', left_on='date', right_on='date')
    integrated=trades_and_dc_and_ds_joined_with_dd.copy()
    integrated = integrated.rename(columns={
    'account_type': 'customer_account_type',
    'liquidity_tier': 'stock_liquidity_tier',
    'sector': 'stock_sector',
    'industry': 'stock_industry'
        })
    # Merge integrated with dtp to get stock_price
    dtp['date'] = pd.to_datetime(dtp['date']).dt.date
    integrated = integrated.merge(dtp, 
                                on=['date', 'stock_ticker'], 
                                how='left')

    # Drop the temporary date column
    integrated.drop('date', axis=1, inplace=True)
    # Calculate total_trade_amount
    integrated['total_trade_amount'] = integrated['stock_price'] * integrated['quantity']
    required_columns = ['transaction_id', 'timestamp', 'customer_id', 'stock_ticker', 
                   'transaction_type', 'quantity', 'average_trade_size_winsorized', 'stock_price_log',
                   'total_trade_amount', 'customer_account_type', 'day_name', 
                   'is_weekend', 'is_holiday', 'stock_liquidity_tier', 
                   'stock_sector', 'stock_industry']
    integrated = integrated[required_columns]
    integrated.columns = [col.lower() for col in integrated.columns]
    #stock_price_log change to stock_price
    integrated = integrated.rename(columns={'stock_price_log': 'stock_price',
                                            'average_trade_size_winsorized': 'average_trade_size'})
    return integrated

from sklearn.preprocessing import LabelEncoder
import pandas as pd
import json
from kafka import KafkaConsumer

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
        - 'day_names': list of all possible day names
        - 'industry_names': list of all possible industry names
    
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

def process_stream(record, encoding_lookups):
    """
    Processes a single streamed record by encoding it.
    This function is called for each row as it arrives from the Kafka stream.
    
    Parameters:
    -----------
    record : dict
        A single raw streamed record to process and encode
    encoding_lookups : dict
        Dictionary containing all encoding mappings
    
    Returns:
    --------
    dict
        The encoded record
    """
    try:
        # Encode the record using the encode_row function
        processed = encode_row(record, encoding_lookups)
        return processed
        
    except Exception as e:
        print(f"\n✗ Error in process_stream: {str(e)}")
        raise


def consume_kafka_stream(topic_name='55_0654_Topic', 
                         bootstrap_servers=['localhost:9092'],
                         main_data_path='data/integrated_main.csv',
                         lookup_path='../data/lookups/master_encoding_lookup.csv',
                         output_file='data/FULL_STOCKS.csv'):
    """
    Subscribes to Kafka topic and streams the latest data.
    Processes and saves each record immediately as it arrives (row-by-row streaming).
    
    Parameters:
    -----------
    topic_name : str
        Kafka topic name to consume from
    bootstrap_servers : list
        List of Kafka bootstrap servers
    main_data_path : str
        Path to the encoded main dataset (95% from milestone 1)
    lookup_path : str
        Path to the master encoding lookup table
    output_file : str
        Path to save the final combined dataset (FULL_STOCKS.csv)
    
    Returns:
    --------
    pandas.DataFrame
        Final combined dataset with all streamed data
    """
    print("="*70)
    print("KAFKA CONSUMER - STREAMING LATEST DATA (ROW-BY-ROW)")
    print("="*70)
    print(f"Topic: {topic_name}")
    print(f"Bootstrap Servers: {bootstrap_servers}")
    print("="*70)

    try:
        # Load encoding lookups from master lookup table
        print("\nLoading encoding lookups from master lookup table...")
        lookup_df = pd.read_csv(lookup_path)
        
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
        
        # Load encoded main data (95%)
        print("\nLoading encoded main dataset (95%)...")
        main_data = pd.read_csv(main_data_path)
        print(f"✓ Main dataset loaded: {len(main_data)} records")
        
        # Create consumer that subscribes to the topic
        consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='stock-data-consumer',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )

        print(f"\n✓ Consumer subscribed to topic: {topic_name}")
        print("✓ Listening for latest messages...")
        print("✓ Processing streamed records...\n")
        print("="*70)

        record_count = 0
        batch_size = 100
        batch_records = []

        # Stream messages from Kafka - process and batch append to main_data
        for message in consumer:
            record = message.value

            # Check for End of Stream signal
            if 'EOS' in record and record['EOS']:
                print("\n" + "="*70)
                print("✓ Received EOS (End of Stream) signal")
                print("="*70)
                break

            # Process the record and add to batch
            processed = process_stream(record, encoding_lookups)
            batch_records.append(processed)
            record_count += 1
            
            print(f"✓ Processed record {record_count}: Transaction ID {record.get('transaction_id')}")
            
            # Append batch to main_data every 100 records
            if len(batch_records) >= batch_size:
                main_data = pd.concat([main_data, pd.DataFrame(batch_records)], ignore_index=True)
                print(f"✓ Appended batch of {len(batch_records)} records to dataset")
                batch_records = []
        
        # Append any remaining records in the final batch
        if batch_records:
            main_data = pd.concat([main_data, pd.DataFrame(batch_records)], ignore_index=True)
            print(f"✓ Appended final batch of {len(batch_records)} records to dataset")

        # Close the consumer
        consumer.close()
        print("\n✓ Kafka consumer closed")
        print(f"✓ Total records processed and appended: {record_count}")
        
        # Sort by transaction_id and save final dataset
        if record_count > 0:
            print("\nFinalizing FULL_STOCKS.csv...")
            print(f"✓ Total records in dataset: {len(main_data)}")
            
            # Sort by transaction_id
            print("Sorting by transaction_id...")
            main_data = main_data.sort_values(by='transaction_id').reset_index(drop=True)
            print("✓ Dataset sorted successfully")
            
            # Save final dataset as FULL_STOCKS.csv
            main_data.to_csv(output_file, index=False)
            print(f"\n✓ FULL_STOCKS.csv saved with {len(main_data)} records")
        else:
            print("\n⚠ No streamed records received")
            main_data.to_csv(output_file, index=False)
            print(f"✓ FULL_STOCKS.csv saved with main dataset only: {len(main_data)} records")
        
        print("\n" + "="*70)
        print("STREAMING COMPLETE")
        print("="*70)
        print(f"✓ Final dataset shape: {main_data.shape}")
        print(f"✓ Location: {output_file}")
        print("="*70)
        
        return main_data

    except Exception as e:
        print(f"\n✗ Error in consume_kafka_stream: {str(e)}")
        print("Make sure Kafka is running and producer has sent data")
        raise

def encode_data(df):
    """
    Encodes categorical columns in the integrated dataframe.
    
    Label Encoding (modifies original columns):
    - stock_ticker, transaction_type, customer_account_type, stock_sector,stock_industry
    
    One-Hot Encoding (creates new columns, removes original):
    - day_name
    
    Boolean to Binary (modifies original columns):
    - is_weekend, is_holiday
    
    Parameters:
    -----------
    df : pandas.DataFrame
        The integrated dataframe from milestone 1
    
    Returns:
    --------
    pandas.DataFrame
        The dataframe with encoded columns
    """
    # Create a copy to avoid modifying the original dataframe
    df_encoded = df.copy()
    
    # Initialize label encoders
    le_stock = LabelEncoder()
    le_transaction = LabelEncoder()
    le_account = LabelEncoder()
    le_sector = LabelEncoder()
    le_industry=LabelEncoder()
    
    # Apply Label Encoding - MODIFY ORIGINAL COLUMNS
    # 1. stock_ticker - 20 unique values (STK001-STK020)
    df_encoded['stock_ticker'] = le_stock.fit_transform(df_encoded['stock_ticker'])
    
    # 2. stock_sector - Multiple sectors
    df_encoded['stock_sector'] = le_sector.fit_transform(df_encoded['stock_sector'])
    
    # 3. stock_industry
    df_encoded['stock_industry'] = le_industry.fit_transform(df_encoded['stock_industry'])

    # Apply One-Hot Encoding
    # 4. day_name - One-Hot Encoding (nominal categorical with 7 categories)
    day_dummies = pd.get_dummies(df_encoded['day_name'], prefix='day', dtype=int)
    df_encoded = pd.concat([df_encoded, day_dummies], axis=1)
    df_encoded.drop('day_name', axis=1, inplace=True)  # Remove original column
    
    
    
    # Boolean to Binary - MODIFY ORIGINAL COLUMNS
    # 5. is_weekend - Convert boolean to binary (True/False → 1/0)
    df_encoded['is_weekend'] = df_encoded['is_weekend'].astype(int)
    
    # 6. is_holiday - Convert boolean to binary (True/False → 1/0)
    df_encoded['is_holiday'] = df_encoded['is_holiday'].astype(int)
    
    # 7. transaction_type - 2 unique values (buy/sell)
    df_encoded['transaction_type'] = le_transaction.fit_transform(df_encoded['transaction_type'])
    
    # 8. customer_account_type - 2 unique values (Institutional/Retail)
    df_encoded['customer_account_type'] = le_account.fit_transform(df_encoded['customer_account_type'])
    
    # Store the encoders as attributes for potential inverse transformation
    df_encoded.attrs['encoders'] = {
        'stock_ticker': le_stock,
        'transaction_type': le_transaction,
        'customer_account_type': le_account,
        'stock_sector': le_sector,
        'stock_industry': le_industry
    }
    
    return df_encoded
def create_encoding_lookup_tables(df_encoded):
    """
    Creates lookup tables for all encoded columns showing the mapping
    between original and encoded values.
    
    Since label encoding now modifies original columns, we use the encoders
    stored in df_encoded.attrs['encoders'] to retrieve the mappings.
    
    Parameters:
    -----------
    df_encoded : pandas.DataFrame
        The encoded dataframe with encoded columns
    
    Returns:
    --------
    dict
        Dictionary of lookup tables for each encoded column
    """
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

def save_lookup_tables(lookup_tables, save_format='csv'):
    """
    Saves all lookup tables to files or database.
    
    Parameters:
    -----------
    lookup_tables : dict
        Dictionary of encoding lookup tables
    imputation_lookup : pandas.DataFrame
        Imputation lookup table
    save_format : str
        'csv' or 'db' for database
    """
    import os
    
    # Create lookups directory if it doesn't exist
    os.makedirs('../data/lookups', exist_ok=True)
    
    if save_format == 'csv':
        # Save encoding lookup tables
        for name, table in lookup_tables.items():
            filename = f'../data/lookups/encoding_lookup_{name}.csv'
            table.to_csv(filename, index=False)
            print(f"Saved: {filename}")
        
        # Collect all encoding lookup tables: label-encoded, one-hot and binary
        label_keys = ['stock_ticker','stock_sector','stock_industry', 'transaction_type', 'customer_account_type']
        onehot_keys = ['day_name']
        binary_keys = ['is_weekend', 'is_holiday']
        selected_keys = label_keys + onehot_keys + binary_keys

        # Keep only keys that exist in lookup_tables
        label_encoded_tables = [v for k, v in lookup_tables.items() if k in selected_keys]
        if label_encoded_tables:
            master_lookup = pd.concat(label_encoded_tables, ignore_index=True)
            master_lookup.to_csv('../data/lookups/master_encoding_lookup.csv', index=False)
            print(f"Saved: ../data/lookups/master_encoding_lookup.csv")
        
    elif save_format == 'db':
        from db_utils import save_to_db
        
        # Save each lookup table to database
        for name, table in lookup_tables.items():
            save_to_db(table, f'lookup_encoding_{name}')
            print(f"Saved to DB: lookup_encoding_{name}")
        
        # Save master lookup (only label-encoded columns)
        label_encoded_tables = [v for k, v in lookup_tables.items() 
                                if k in ['stock_ticker', 'transaction_type', 'customer_account_type', 'stock_sector','stock_industry']]
        if label_encoded_tables:
            master_lookup = pd.concat(label_encoded_tables, ignore_index=True)
            save_to_db(master_lookup, 'lookup_master_encoding')
            print(f"Saved to DB: lookup_master_encoding")

if __name__ == '__main__':
    # Use correct paths relative to app/src directory
    dtp = extract_data('../data/daily_trade_prices.csv')
    trades = extract_data('../data/trades.csv')
    dc = extract_data('../data/dim_customer.csv')
    dd = extract_data('../data/dim_date.csv')
    ds = extract_data('../data/dim_stock.csv')


    dtp_imputed, trades = impute_missing_data(dtp, trades)

    cols = ['STK001','STK002','STK003','STK004','STK005','STK006','STK007','STK008','STK009','STK010',
        'STK011','STK012','STK013','STK014','STK015','STK016','STK017','STK018','STK019','STK020']
    num_of_outlier_columns = 0
    for col in cols:
        series = dtp_imputed.loc[dtp_imputed['stock_ticker'] == col, 'stock_price']
        if not outlier_checker_IQR(series):
            num_of_outlier_columns += 1
    if(num_of_outlier_columns > 0):
        dtp_imputed = handle_outliers_logtransformation(dtp_imputed, 'stock_price')
  
    if not outlier_checker_IQR(trades['cumulative_portfolio_value']):
        trades = handle_outliers_logtransformation(trades, 'cumulative_portfolio_value')

    if not outlier_checker_IQR(trades['quantity']):
        trades = handle_outliers_logtransformation(trades, 'quantity')

    if not outlier_checker_IQR(trades['average_trade_size']):
        trades = handle_outliers_winsorizecapped(trades, 'average_trade_size')

    if not outlier_checker_Zscore(dc['avg_trade_size_baseline']):
        dc = handle_outliers_winsorizecapped(dc, 'avg_trade_size_baseline')
    
    # Add date column to trades for integration
    
    
    integrated = integrate_data(dtp_imputed, trades, dc, dd, ds)

    # ===================================================================
    # 2.1 Streaming Preparation: Split 5% for streaming BEFORE encoding
    # ===================================================================
    print("\n" + "="*70)
    print("STREAMING PREPARATION - Splitting 5% for stream.csv")
    print("="*70)
    
    total_rows = len(integrated)
    stream_size = int(total_rows * 0.05)
    
    print(f"Total rows in integrated dataset: {total_rows}")
    print(f"Rows to extract for streaming (5%): {stream_size}")
    
    # Randomly sample 5% of the data for streaming
    stream_data = integrated.sample(n=stream_size, random_state=42)
    
    # Get the remaining 95% of the data
    integrated_main = integrated.drop(stream_data.index)
    
    print(f"\nDataset split completed:")
    print(f"- Main dataset (95%): {len(integrated_main)} rows")
    print(f"- Stream dataset (5%): {len(stream_data)} rows")
    
    # Save the streaming data to stream.csv
    stream_data.to_csv('../data/stream.csv', index=False)
    print(f"\n✓ Streaming data saved to: ../data/stream.csv")
    
    # Verify no overlap between datasets
    overlap = set(integrated_main.index).intersection(set(stream_data.index))
    print(f"✓ Verification: Overlap between datasets = {len(overlap)} rows (should be 0)")
    print("="*70)
    
    # Update the integrated dataframe to use only the main dataset (95%)
    integrated = integrated_main.copy()
    print(f"\n✓ Updated 'integrated' dataframe to use main dataset: {len(integrated)} rows")
    print("✓ This 95% dataset will be used for encoding")

    # Try to save to database, but continue if it fails
    try:
        save_to_db(integrated, 'integrated_trades_data')
        print("✓ Data saved to PostgreSQL database")
    except Exception as e:
        print(f"⚠ Warning: Could not save to database: {str(e)}")
        print("⚠ Make sure Docker containers are running: docker compose up -d")
        print("✓ Continuing without database save...")
    
    sample=pd.DataFrame(integrated.sample(10))
    sample.to_csv('../data/sample_data.csv', index=False)
    
    # Save unencoded main data for encoding lookup creation in Kafka consumer
    integrated.to_csv('../data/integrated_main_unencoded.csv', index=False)
    print(f"✓ Saved unencoded data for lookups: integrated_main_unencoded.csv")
    
    # ===================================================================
    # 2.2 Encode the 95% main dataset
    # ===================================================================
    print("\n" + "="*70)
    print("ENCODING MAIN DATASET (95%)")
    print("="*70)
    encoded = encode_data(integrated)
    lookup = create_encoding_lookup_tables(encoded)
    save_lookup_tables(lookup, save_format='csv')
    print(f"✓ Encoded main dataset: {len(encoded)} rows")
    
    # Save encoded main data to integrated_main.csv for Kafka consumer to use
    encoded.to_csv('../data/integrated_main.csv', index=False)
    print(f"✓ Saved encoded integrated_main.csv: {len(encoded)} records")
    print("="*70)
    
    # Consume Kafka stream and get final dataset with streamed data
    print("\n" + "="*70)
    print("STARTING KAFKA CONSUMER")
    print("="*70)
    print("⚠ Make sure to run the Kafka producer first in a separate terminal:")
    print("  python kafka_producer.py")
    print("="*70)
    
    final_dataset = consume_kafka_stream(
        topic_name='55_0654_Topic',
        bootstrap_servers=['localhost:9092'],
        main_data_path='../data/integrated_main.csv',
        lookup_path='../data/lookups/master_encoding_lookup.csv',
        output_file='../data/FULL_STOCKS.csv'
    )
    
    print("\n" + "="*70)
    print("FINAL DATASET SAVED")
    print("="*70)
    print(f"✓ FULL_STOCKS.csv created with {len(final_dataset)} total records")
    print(f"✓ Location: data/FULL_STOCKS.csv")

    