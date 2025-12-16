# ---- Auto-install required packages if missing ----
import subprocess
import sys

required = ['pyreadr', 'numpy', 'pandas', 'tqdm', 'pyarrow']

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
        raise ValueError("Input vector is empty or invalid.")
    
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
def process_row(row):
    """
    Process a single row to calculate dynamic acceleration components and derived metrics.
    """
    x_component = np.abs(dy_acc(row['x_cal_array']))
    y_component = np.abs(dy_acc(row['y_cal_array']))
    z_component = np.abs(dy_acc(row['z_cal_array']))

    vectorial_sum = np.sqrt(x_component**2 + y_component**2 + z_component**2)
    ave_vedba_value = np.nanmean(vectorial_sum)

    pitch = np.arctan2(x_component, np.sqrt(y_component**2 + z_component**2))
    ave_pitch = np.nanmean(pitch)

    return ave_vedba_value, ave_pitch

# ---- Main Processing ----
# Load the data
start_time = time.time()

acc_data_trim = pd.read_parquet('data/acc_v1.parquet')

# Example: assuming acc_data_trim is your DataFrame

# Convert 'x_cal' dict column to list of numpy arrays
acc_data_trim['x_cal_array'] = acc_data_trim['x_cal'].apply(
    lambda d: np.array(list(d.values())) if isinstance(d, dict) else np.nan
)

# Convert 'y_cal' dict column to list of numpy arrays
acc_data_trim['y_cal_array'] = acc_data_trim['y_cal'].apply(
    lambda d: np.array(list(d.values())) if isinstance(d, dict) else np.nan
)

# Convert 'z_cal' dict column to list of numpy arrays
acc_data_trim['z_cal_array'] = acc_data_trim['z_cal'].apply(
    lambda d: np.array(list(d.values())) if isinstance(d, dict) else np.nan
)

acc_data_trim = acc_data_trim[
    acc_data_trim['x_cal_array'].apply(lambda x: all(v is not None for v in x)) &
    acc_data_trim['y_cal_array'].apply(lambda x: all(v is not None for v in x)) &
    acc_data_trim['z_cal_array'].apply(lambda x: all(v is not None for v in x))
]

# # If you want to check the shape:
# print(acc_data_trim['x_cal_array'].iloc[0])
# print(type(acc_data_trim['x_cal_array'].iloc[0]))  # should print: <class 'numpy.ndarray'>
# Parallel processing
results = []
with ProcessPoolExecutor(max_workers=12) as executor:
    futures = {executor.submit(process_row, row): i for i, row in acc_data_trim.iterrows()}
    
    for f in tqdm(as_completed(futures), total=len(futures), desc="Collecting results"):
        results.append(f.result())


# Store back the results
ave_vedba, ave_pitch = zip(*results)
acc_data_trim['ave_vedba'] = ave_vedba
acc_data_trim['ave_pitch'] = ave_pitch

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

end_time = time.time()

# Calculate and print the elapsed time
elapsed_time = end_time - start_time
print(f"Process completed in {elapsed_time:.2f} seconds")