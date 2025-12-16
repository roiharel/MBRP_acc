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
        
import pyreadr
import numpy as np
import pandas as pd
from tqdm import tqdm
import ast

import os
import pyarrow.parquet as pq

import glob
import os
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, as_completed
import time

# Create the vedba directory if it doesn't exist
data_folder = '/mnt/EAS_shared/baboon/working/data/processed/2025/acc/acc_v1'
vedba_dir = '/mnt/EAS_shared/baboon/working/data/processed/2025/acc/vedba'
os.makedirs(vedba_dir, exist_ok=True)

def process_animal_file_parallel(file_path):
    """
    Process a single animal file and create individual VEDBA output (parallel version)
    This function will be run in separate processes
    """
    import pandas as pd
    import numpy as np
    import os
    
    # Re-define dy_acc function for parallel processing (each process needs its own copy)
    def dy_acc(vect, win_size=15):
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
    
    def process_group_simple_parallel(group_data):
        """Simplified processing for parallel execution"""
        x_values = group_data['X'].values.astype(float)
        y_values = group_data['Y'].values.astype(float)
        z_values = group_data['Z'].values.astype(float)
        
        x_component = np.abs(dy_acc(x_values))
        y_component = np.abs(dy_acc(y_values))
        z_component = np.abs(dy_acc(z_values))
        
        vectorial_sum = np.sqrt(x_component**2 + y_component**2 + z_component**2)
        ave_vedba = np.nanmean(vectorial_sum)
        
        return pd.Series({
            'tag': group_data['tag'].iloc[0],
            'timestamp': group_data['timestamp'].iloc[0], 
            'vedba': ave_vedba,
            'n_samples': len(group_data)
        })
    
    try:
        filename = os.path.basename(file_path)
        animal_id = filename.replace('.parquet', '')
        
        # Load the animal's accelerometer data
        acc_data = pd.read_parquet(file_path)
        
        acc_data['tag'] = animal_id
        # Create grouping column
        acc_data['group_id'] = acc_data['tag'] + '_' + acc_data['burst_timestamp'].astype(str)
        
        # Process all groups for this animal
        results = acc_data.groupby('group_id', group_keys=False).apply(process_group_simple_parallel).reset_index(drop=True)
        
        # Add log transformation
        results['logvedba'] = np.log(results['vedba'])
        
        # Sort by timestamp
        results = results.sort_values('timestamp').reset_index(drop=True)
        
        # Select final columns
        vedba_data = results[['timestamp', 'vedba', 'logvedba']]
        
        # Save individual VEDBA file
        output_file = f"{animal_id}.parquet"
        output_path = os.path.join(vedba_dir, output_file)
        vedba_data.to_parquet(output_path, index=False)
        
        return {
            'animal_id': animal_id,
            'input_file': filename,
            'output_file': output_file,
            'records': len(vedba_data),
            'date_range': f"{vedba_data['timestamp'].min()} to {vedba_data['timestamp'].max()}",
            'status': 'success'
        }
        
    except Exception as e:
        return {
            'animal_id': os.path.basename(file_path).replace('.parquet', ''),
            'input_file': os.path.basename(file_path),
            'error': str(e),
            'status': 'error'
        }

# Get all input files
parquet_files = glob.glob(os.path.join(data_folder, '*.parquet'))

# Determine optimal number of workers (use 75% of available cores)
n_cores = mp.cpu_count()
n_workers = max(1, int(n_cores * 0.75))  # Use 75% of cores, minimum 1

start_time = time.time()
summary_list = []
failed_files = []

# Process files in parallel
with ProcessPoolExecutor(max_workers=n_workers) as executor:
    # Submit all jobs
    future_to_file = {executor.submit(process_animal_file_parallel, file_path): file_path 
                     for file_path in parquet_files}
    
    # Collect results as they complete
    for i, future in enumerate(as_completed(future_to_file), 1):
        file_path = future_to_file[future]
        result = future.result()
        
        if result['status'] == 'success':
            summary_list.append(result)
            print(f" [{i}/{len(parquet_files)}] {result['animal_id']}: {result['records']} records")
        else:
            failed_files.append(result)
            print(f" [{i}/{len(parquet_files)}] {result['animal_id']}: ERROR - {result['error']}")

end_time = time.time()
processing_time = end_time - start_time

if summary_list:
    total_records = sum(s['records'] for s in summary_list)
   
# Show successful processing summary
if summary_list:
    print(f"\n Successfully processed animals:")
    for summary in summary_list[:10]:  # Show first 10
        print(f"   {summary['animal_id']}: {summary['records']:,} records")
    if len(summary_list) > 10:
        print(f"   ... and {len(summary_list) - 10} more animals")

# Show any failures
if failed_files:
    for failed in failed_files:
        print(f"   {failed['animal_id']}: {failed['error']}")

# Verify output
vedba_files = [f for f in os.listdir(vedba_dir) if f.endswith('.parquet')]
print(f"\n Output verification:")
print(f"   {len(vedba_files)} VEDBA files created in {vedba_dir}/")

print(f"\n Parallel processing complete! Each animal's VEDBA data is in: {vedba_dir}/[AnimalID].parquet")