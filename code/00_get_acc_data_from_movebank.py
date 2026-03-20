## dowmload and merge with previous acc data from movebank, save as parquet file
import pandas as pd
import numpy as np
import os
from pathlib import Path
from tqdm import tqdm
import pyarrow.parquet as pq
import pyarrow as pa
from datetime import datetime, timedelta
import subprocess
import glob
from dotenv import load_dotenv
import sys
sys.path.append('functions')
from fetch_movebank_data import MovebankDataFetcher

# Define parameters for data retrieval
study_id = 3445611111
# Find and read the most recent MBRP_acc_ CSV file
acc_folder = "/mnt/EAS_shared/baboon/working/data/raw/2025/acc/"
pattern = os.path.join(acc_folder, "MBRP_acc_*.parquet")
matching_files = glob.glob(pattern)

if matching_files:
    most_recent_file = max(matching_files, key=os.path.getmtime)
    print(f"Reading most recent file: {most_recent_file}")
    old_acc = pd.read_parquet(most_recent_file)
    old_acc.columns = old_acc.columns.str.replace('-', '_')
    old_acc = old_acc.dropna(axis=1, how='all')
    old_acc.columns = old_acc.columns.str.replace(':', '_')
    # old_acc = pd.read_csv("Baboons MBRP Mpala Kenya.csv")
else:
    raise FileNotFoundError("No MBRP_acc_*.parquet files found")

## add new data from movebank 
load_dotenv()
data_start = pd.to_datetime(old_acc['timestamp'].min()).strftime('%Y%m%d')
new_end_date = datetime.now().strftime('%Y%m%d')
new_start_date = (datetime.now() - timedelta(days=120)).strftime('%Y%m%d')  # ~4 months

# Initialize fetcher (credentials from .env or environment variables)
fetcher = MovebankDataFetcher()

# Fetch acceleration data (sensor_type_id=2365683 for acceleration)
new_acc = fetcher.get_event_data(
    study_id=study_id,
    sensor_type_id=2365683,  # acceleration sensor
    timestamp_start=new_start_date,
    timestamp_end=new_end_date
)
new_acc.columns = new_acc.columns.str.replace('-', '_')

print(f"Fetched {len(new_acc) if new_acc is not None else 0} records")

# Remove extra columns from new_acc before concatenating
cols_to_drop_new = ['sensor_type_id', 'tag_id', 'individual_id', 'study_id', 'deployment_id']
new_acc = new_acc.drop(columns=[col for col in cols_to_drop_new if col in new_acc.columns])

# Remove extra columns from old_acc
cols_to_drop_old = ['sensor_type', 'study_name']
old_acc = old_acc.drop(columns=[col for col in cols_to_drop_old if col in old_acc.columns])

# Concatenate old and new data
acc_data = pd.concat([old_acc, new_acc], ignore_index=True)
acc_data['timestamp'] = pd.to_datetime(acc_data['timestamp'])
# Remove duplicates based on key columns
#acc_data = acc_data.drop_duplicates(subset=['individual-local-identifier', 'tag-local-identifier', 'timestamp'])
acc_data = acc_data.drop_duplicates(subset=['event_id'])
# Save the processed data for the current animal to a Parquet file with date range
print(f"Combined data shape: {acc_data.shape}")


new_data_path = f"/mnt/EAS_shared/baboon/working/data/raw/2025/acc/MBRP_acc_{data_start}_{new_end_date}.parquet"
acc_data.to_parquet(new_data_path, index=False) 

if most_recent_file != new_data_path:
    os.remove(most_recent_file)
    print(f"Removed old file: {most_recent_file}")


# acc_data.shape
# acc_data.isna().sum()


# metadata = pd.read_csv('movebank_metadata.csv')
# acc_data = acc_data.merge(metadata[['individual_local_identifier', 'group_id', 'sex']], 
#                           on='individual_local_identifier', how='left')
