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
