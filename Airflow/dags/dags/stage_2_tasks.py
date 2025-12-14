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
    
    dtp = pd.read_csv(os.path.join(data_path, "daily_trade_prices.csv"))
    dc = pd.read_csv(os.path.join(data_path, "dim_customer.csv"))
    dd = pd.read_csv(os.path.join(data_path, "dim_date.csv"))
    ds = pd.read_csv(os.path.join(data_path, "dim_stock.csv"))
    trades = pd.read_csv(os.path.join(data_path, "trades.csv"))
    
    print(f"Loaded all datasets")
    
    # Fill nulls in dtp
    trades["date_obj"] = pd.to_datetime(trades["timestamp"]).dt.date
    estimated_prices = (
        trades.groupby([trades["date_obj"], "stock_ticker"])
        .apply(
            lambda x: (x["cumulative_portfolio_value"] / x["quantity"] * x["quantity"]).sum()
            / x["quantity"].sum()
        )
        .reset_index(name="price")
    )
    stock_prices_map = estimated_prices.set_index(["date_obj", "stock_ticker"])["price"]
    
    dtp["date_obj"] = pd.to_datetime(dtp["date"]).dt.date
    for col in dtp.columns:
        if col not in ["date", "date_obj"]:
            missing_mask = dtp[col].isnull()
            if missing_mask.any():
                dates_for_mapping = dtp.loc[missing_mask, "date_obj"]
                map_index = pd.MultiIndex.from_tuples(
                    [(date, col) for date in dates_for_mapping],
                    names=["date_obj", "stock_ticker"],
                )
                dtp.loc[missing_mask, col] = map_index.map(stock_prices_map)
            
            dtp[col] = dtp[col].fillna(method="ffill")
    dtp = dtp.drop(columns=["date_obj"])
    
    # Convert to long format
    dtp["date"] = pd.to_datetime(dtp["date"])
    dtp_long = pd.melt(
        dtp,
        id_vars=["date"],
        var_name="stock_ticker",
        value_name="stock_price",
    ).dropna()
    
    # Integrate data
    trades["date"] = pd.to_datetime(trades["timestamp"]).dt.date
    dtp_long["date"] = pd.to_datetime(dtp_long["date"]).dt.date
    dd["date"] = pd.to_datetime(dd["date"]).dt.date
    
    merged_df = trades.merge(dc, on="customer_id", how="left")
    merged_df = merged_df.merge(ds, on="stock_ticker", how="left")
    merged_df = merged_df.merge(dd, on="date", how="left")
    df = merged_df.merge(dtp_long, on=["date", "stock_ticker"], how="left")
    
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
    import json
    
    print("="*70)
    print("STAGE 2 TASK 2: ENCODE CATEGORICAL DATA")
    print("="*70)
    
    data_path = "/opt/airflow/notebook/data/"
    lookups_path = os.path.join(data_path, "lookups")
    os.makedirs(lookups_path, exist_ok=True)
    
    batch_df = pd.read_csv(os.path.join(data_path, "batch_data_for_encoding.csv"))
    
    categorical_cols = batch_df.select_dtypes(include=["object", "bool"]).columns
    master_encoding_lookup = {}
    encoded_df = batch_df.copy()
    
    for col in categorical_cols:
        encoding_map = {
            category: i for i, category in enumerate(batch_df[col].unique())
        }
        encoded_df[col] = batch_df[col].map(encoding_map)
        
        lookup_map = {}
        for category, i in encoding_map.items():
            if isinstance(category, np.bool_):
                lookup_map[i] = bool(category)
            else:
                lookup_map[i] = category
        master_encoding_lookup[col] = lookup_map
        lookup_df = pd.DataFrame(
            list(lookup_map.items()), columns=["encoded_value", "original_value"]
        )
        lookup_df.to_csv(
            os.path.join(lookups_path, f"encoding_lookup_{col}.csv"), index=False
        )
    
    with open(os.path.join(lookups_path, "master_encoding_lookup.json"), "w") as f:
        json.dump(master_encoding_lookup, f, indent=4)
    
    encoded_df.to_csv(
        os.path.join(data_path, "integrated_encoded_trades_data.csv"), index=False
    )
    
    print(f"✓ Encoded {len(categorical_cols)} categorical columns")
    print(f"✓ Saved lookup tables to {lookups_path}")
    print("✓ STAGE 2 TASK 2 COMPLETED")
    return encoded_df, master_encoding_lookup
