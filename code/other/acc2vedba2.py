# ---- Auto-install required packages if missing ----
import subprocess
import sys

required = ['pyreadr', 'numpy', 'pandas', 'tqdm', 'pyarrow','time']

for package in required:
    try:
        __import__(package)
    except ImportError:
        print(f"Package '{package}' not found. Installing...")
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', package])

import time
import pyreadr
import numpy as np
import pandas as pd
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import as_completed
import ast

import os
import pyarrow.parquet as pq

# ---- Helper function: dy_acc ----
def dy_acc(vect, win_size=7):
    """
    Calculate the dynamic acceleration (dy_acc) for a given vector.
    """
    if vect is None or len(vect) == 0:
        raise ValueError("Input vector is empty, contains None, or invalid values.")
    
  
    pad_size = int(win_size / 2 - 0.5)
    padded = np.pad(vect, (pad_size, pad_size), constant_values=np.nan)
    acc_vec = np.empty(len(vect))
    acc_vec[:] = np.nan

    for i in range(len(vect)):
        window = padded[i : i + (2 * pad_size + 1)]
        m_ave = np.nanmean(window)
        acc_vec[i] = vect[i] - m_ave
    
    return acc_vec

# ---- Vector Calculation Function ----
def process_chunk(chunk):
    # Convert 'x_cal', 'y_cal', 'z_cal' dict columns to numpy arrays
    chunk['x_cal_array'] = chunk['x_cal'].apply(
        lambda d: np.array(list(d.values())) if isinstance(d, dict) else np.nan
    )
    chunk['y_cal_array'] = chunk['y_cal'].apply(
        lambda d: np.array(list(d.values())) if isinstance(d, dict) else np.nan
    )
    chunk['z_cal_array'] = chunk['z_cal'].apply(
        lambda d: np.array(list(d.values())) if isinstance(d, dict) else np.nan
    )
    
    # Filter out rows with invalid arrays
    chunk = chunk.dropna(subset=['x_cal_array', 'y_cal_array', 'z_cal_array'])
    
    # Process each row in the chunk
    results = [process_row(row) for _, row in chunk.iterrows()]
    ave_vedba, ave_pitch = zip(*results)
    chunk['ave_vedba'] = ave_vedba
    chunk['ave_pitch'] = ave_pitch
    return chunk

def process_row(row):
    """
    Process a single row to calculate dynamic acceleration components and derived metrics.
    """
    # Ensure the input arrays are valid
    if row['x_cal_array'] is None or row['y_cal_array'] is None or row['z_cal_array'] is None:
        return np.nan, np.nan

    # Calculate dynamic acceleration components
    x_component = np.abs(dy_acc(row['x_cal_array']))
    y_component = np.abs(dy_acc(row['y_cal_array']))
    z_component = np.abs(dy_acc(row['z_cal_array']))

    # Calculate vectorial sum and average VEDBA
    vectorial_sum = np.sqrt(x_component**2 + y_component**2 + z_component**2)
    ave_vedba_value = np.nanmean(vectorial_sum)

    # Calculate pitch and average pitch
    pitch = np.arctan2(x_component, np.sqrt(y_component**2 + z_component**2))
    ave_pitch = np.nanmean(pitch)

    return ave_vedba_value, ave_pitch

# ---- Main Processing ----
# Load the data
######################################
import pyarrow.parquet as pq
import pandas as pd
import numpy as np
from concurrent.futures import ProcessPoolExecutor

# Function to process a chunk of data
def process_chunk(chunk):
    # Convert 'x_cal', 'y_cal', 'z_cal' dict columns to numpy arrays
    chunk['x_cal_array'] = chunk['x_cal'].apply(
        lambda d: np.array(list(d.values())) if isinstance(d, dict) else np.nan
    )
    chunk['y_cal_array'] = chunk['y_cal'].apply(
        lambda d: np.array(list(d.values())) if isinstance(d, dict) else np.nan
    )
    chunk['z_cal_array'] = chunk['z_cal'].apply(
        lambda d: np.array(list(d.values())) if isinstance(d, dict) else np.nan
    )
    # Process each row in the chunk
    results = [process_row(row) for _, row in chunk.iterrows()]
    ave_vedba, ave_pitch = zip(*results)
    chunk['ave_vedba'] = ave_vedba
    chunk['ave_pitch'] = ave_pitch
    return chunk

# Measure the start time
start_time = time.time()

# Read the Parquet file in chunks
file_path = 'data/acc_v1.parquet'  # Replace with your file path
parquet_file = pq.ParquetFile(file_path)

# Process the file in chunks
chunk_size = 15000  # Define the number of rows per chunk
results = []

with ProcessPoolExecutor(max_workers=80) as executor:
    futures = []
    for i in range(0, parquet_file.num_row_groups):
        # Read a chunk (row group)
        table = parquet_file.read_row_group(i)
        chunk = table.to_pandas()
        # Submit the chunk for parallel processing
        futures.append(executor.submit(process_chunk, chunk))
    
    # Collect results as they complete
    for future in futures:
        results.append(future.result())

# Combine all processed chunks into a single DataFrame
acc_data_trim = pd.concat(results, ignore_index=True)

# Save the processed data
#acc_data_trim.to_parquet('data/processed_acc_v1.parquet', index=False)
#print("Data saved to 'data/processed_acc_v1.parquet'")
######################################

# Log-transform like in R
acc_data_trim['log_vedba'] = np.log(acc_data_trim['ave_vedba'])

# Select relevant columns
filtered_data = acc_data_trim[['individual_local_identifier', 
                               'local_timestamp', 
                               'tag_local_identifier', 
                               'log_vedba', 
                               'ave_pitch']]

# Save the processed data
filtered_data.to_parquet('data/acc_vedba.parquet', index=False)
print("Data saved to 'data/acc_vedba.parquet'")

# Measure the end time
end_time = time.time()

# Calculate and print the elapsed time
elapsed_time = end_time - start_time
print(f"Process completed in {elapsed_time:.2f} seconds")