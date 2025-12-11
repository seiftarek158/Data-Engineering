from __future__ import annotations

import json
import os
import warnings
from datetime import datetime

import numpy as np
import pandas as pd
from airflow.decorators import dag, task

warnings.filterwarnings("ignore")


@dag(
    dag_id="stage_2_data_processing",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=["data_processing", "stage_2"],
)
def stage_2_data_processing_dag():
    @task
    def prepare_streaming_data():
        """
        Prepares the data for streaming by sampling 5% of the integrated data.
        It performs the following steps:
        1. Load all raw data tables.
        2. Preprocess and integrate them into a single DataFrame.
        3. Extract a 5% random sample for streaming simulation.
        4. Save streaming data and its statistics.
        5. Save the remaining 95% of the data for batch processing.
        """
        # Define paths
        data_path = "/opt/airflow/notebook/data/"
        output_path = "notebook/data/"

        # Load data
        dtp = pd.read_csv(os.path.join(data_path, "daily_trade_prices.csv"))
        dc = pd.read_csv(os.path.join(data_path, "dim_customer.csv"))
        dd = pd.read_csv(os.path.join(data_path, "dim_date.csv"))
        ds = pd.read_csv(os.path.join(data_path, "dim_stock.csv"))
        trades = pd.read_csv(os.path.join(data_path, "trades.csv"))

        # --- Preprocessing based on notebook ---

        # Fill Nulls in daily_trade_prices (dtp)
        trades["date_obj"] = pd.to_datetime(trades["timestamp"]).dt.date
        estimated_prices = (
            trades.groupby([trades["date_obj"], "stock_ticker"])
            .apply(
                lambda x: (x["cumulative_portfolio_value"] / x["quantity"] * x["quantity"]).sum()
                / x["quantity"].sum()
            )
            .reset_index(name="price")
        )
        stock_prices_map = estimated_prices.set_index(["date_obj", "stock_ticker"])[
            "price"
        ]

        dtp["date_obj"] = pd.to_datetime(dtp["date"]).dt.date
        for col in dtp.columns:
            if col not in ["date", "date_obj"]:
                # Map prices for missing values
                missing_mask = dtp[col].isnull()
                if missing_mask.any():
                    dates_for_mapping = dtp.loc[missing_mask, "date_obj"]
                    map_index = pd.MultiIndex.from_tuples(
                        [(date, col) for date in dates_for_mapping],
                        names=["date_obj", "stock_ticker"],
                    )
                    dtp.loc[missing_mask, col] = map_index.map(stock_prices_map)

                # Forward fill any remaining NaNs
                dtp[col] = dtp[col].fillna(method="ffill")
        dtp = dtp.drop(columns=["date_obj"])

        # Convert dtp to long format
        dtp["date"] = pd.to_datetime(dtp["date"])
        dtp_long = pd.melt(
            dtp,
            id_vars=["date"],
            var_name="stock_ticker",
            value_name="stock_price",
        ).dropna()

        # Integrate data into one DataFrame
        trades["date"] = pd.to_datetime(trades["timestamp"]).dt.date
        dtp_long["date"] = pd.to_datetime(dtp_long["date"]).dt.date
        dd["date"] = pd.to_datetime(dd["date"]).dt.date

        merged_df = trades.merge(dc, on="customer_id", how="left")
        merged_df = merged_df.merge(ds, on="stock_ticker", how="left")
        merged_df = merged_df.merge(dd, on="date", how="left")
        df = merged_df.merge(dtp_long, on=["date", "stock_ticker"], how="left")

        # --- Task Logic ---
        # 1. Extract 5% random sample for streaming
        stream_df = df.sample(frac=0.05, random_state=42)
        batch_df = df.drop(stream_df.index)

        # 2. Save statistics for stream processing
        stream_stats = stream_df.describe()
        stream_stats.to_csv(os.path.join(output_path, "stream_stats.csv"))

        # Save the streaming data
        stream_df.to_csv(os.path.join(output_path, "stream.csv"), index=False)

        # Save batch_df for the next task
        batch_df.to_csv(
            os.path.join(output_path, "batch_data_for_encoding.csv"), index=False
        )

    @task
    def encode_categorical_data():
        """
        Encodes categorical columns of the batch data and generates lookup tables.
        """
        data_path = "/opt/airflow/notebook/data/"
        lookups_path = os.path.join(data_path, "lookups")
        os.makedirs(lookups_path, exist_ok=True)

        batch_df = pd.read_csv(
            os.path.join(data_path, "batch_data_for_encoding.csv")
        )

        categorical_cols = batch_df.select_dtypes(
            include=["object", "bool"]
        ).columns
        master_encoding_lookup = {}
        encoded_df = batch_df.copy()

        for col in categorical_cols:
            # Create encoding map
            encoding_map = {
                category: i for i, category in enumerate(batch_df[col].unique())
            }
            encoded_df[col] = batch_df[col].map(encoding_map)

            # Create and save individual lookup table
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

        # Save master lookup
        with open(os.path.join(lookups_path, "master_encoding_lookup.json"), "w") as f:
            json.dump(master_encoding_lookup, f, indent=4)

        # Save encoded dataframe
        encoded_df.to_csv(
            os.path.join(data_path, "integrated_encoded_trades_data.csv"), index=False
        )

    # Define task dependencies
    prepare_streaming_data() >> encode_categorical_data()


stage_2_data_processing_dag()
