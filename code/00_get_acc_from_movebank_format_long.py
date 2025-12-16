import pandas as pd
import numpy as np
import os
from pathlib import Path
from tqdm import tqdm
import pyarrow.parquet as pq
import pyarrow as pa
from datetime import timedelta
import subprocess

# Define parameters for data retrieval
study_id = 3445611111

# Read acceleration data using pandas
acc_data = pd.read_csv("/mnt/EAS_shared/baboon/working/data/raw/2025/acc/Baboons MBRP Mpala Kenya_acc_20240701_20251110.csv")

# Replace hyphens with underscores in column names
acc_data.columns = acc_data.columns.str.replace('-', '_')

# Change working directory
os.chdir("/mnt/EAS_shared/baboon/working/data/processed/2025/acc")

# metadata = pd.read_csv('movebank_metadata.csv')
# acc_data = acc_data.merge(metadata[['individual_local_identifier', 'group_id', 'sex']], 
#                           on='individual_local_identifier', how='left')

# Clean and process the data - rename columns for clarity
acc_data = acc_data.rename(columns={
    'individual_local_identifier': 'animal',
    'tag_local_identifier': 'tag',
    'eobs:accelerations_raw': 'eobs_accelerations_raw_str'
})

# Select necessary columns and ensure timestamp is in datetime format
acc_data = acc_data[['animal', 'tag', 'timestamp', 'eobs_accelerations_raw_str']]
acc_data['timestamp'] = pd.to_datetime(acc_data['timestamp'], utc=True)

# Filter out rows with empty acceleration data
acc_data = acc_data[acc_data['eobs_accelerations_raw_str'] != '']

# Create output directory
output_dir = "acc_v1_parquet"
Path(output_dir).mkdir(exist_ok=True)

# Read calibration data upfront
acc_calib = pd.read_csv('acc_calib.csv')
acc_calib['tag'] = acc_calib['tag'].astype(str)
acc_calib['x0'] = pd.to_numeric(acc_calib['x0'])
acc_calib['y0'] = pd.to_numeric(acc_calib['y0'])
acc_calib['z0'] = pd.to_numeric(acc_calib['z0'])
acc_calib['Sx'] = pd.to_numeric(acc_calib['Sx'])
acc_calib['Sy'] = pd.to_numeric(acc_calib['Sy'])
acc_calib['Sz'] = pd.to_numeric(acc_calib['Sz'])

# Loop over each unique animal with progress bar
unique_animals = acc_data['animal'].unique()

for current_animal in tqdm(unique_animals, desc="Processing animals"):
    # Filter data for the current animal (may have multiple tags)
    animal_acc_data = acc_data[acc_data['animal'] == current_animal].copy()
    
    # Get all tags for this animal
    animal_tags = animal_acc_data['tag'].unique()
    print(f"Processing animal: {current_animal} with tags: {animal_tags}")
    
    # Store data from all tags for this animal
    all_tag_data = []
    
    # Process each tag separately
    for current_tag in animal_tags:
        # Filter data for the current tag
        individual_acc_data = animal_acc_data[animal_acc_data['tag'] == current_tag].copy()
        
        # Remove high res data, more than a single burst per minute or long bursts
        # Create a temporary minute column for grouping
        individual_acc_data['minute_temp'] = individual_acc_data['timestamp'].dt.strftime('%Y-%m-%d %H:%M')
        
        # Select first row per minute
        individual_acc_data = individual_acc_data.sort_values('timestamp').groupby('minute_temp').first().reset_index()
        
        # Remove the temporary minute column
        individual_acc_data = individual_acc_data.drop(columns=['minute_temp'])
        
        # Split the acceleration string into columns
        acc_cols = individual_acc_data['eobs_accelerations_raw_str'].str.split(' ', expand=True)
        
        # Take first 120 columns
        if acc_cols.shape[1] > 120:
            acc_cols = acc_cols.iloc[:, :120]
        
        # Convert to numeric
        acc_cols = acc_cols.apply(pd.to_numeric, errors='coerce')
        
        col_count = acc_cols.shape[1]
        
        # If there's no acceleration data for this tag, skip to the next
        if col_count == 0:
            continue
        
        # Dynamically name the new columns (x1, y1, z1, x2, y2, z2, ...)
        n_samples = col_count // 3
        xyz_names = [f"{axis}{i+1}" for i in range(n_samples) for axis in ['x', 'y', 'z']]
        acc_cols.columns = xyz_names[:col_count]
        
        # Add burst_timestamp and tag info
        acc_cols['burst_timestamp'] = individual_acc_data['timestamp'].values
        acc_cols['tag'] = current_tag
        
        # Reshape data from wide to long format
        # Create index columns for x, y, z
        x_cols = [col for col in acc_cols.columns if col.startswith('x') and col != 'x']
        y_cols = [col for col in acc_cols.columns if col.startswith('y') and col != 'y']
        z_cols = [col for col in acc_cols.columns if col.startswith('z') and col != 'z']
        
        # Melt for each axis
        id_vars = ['burst_timestamp', 'tag']
        
        tag_long_data = pd.DataFrame()
        
        for i in range(len(x_cols)):
            temp_df = acc_cols[id_vars + [x_cols[i], y_cols[i], z_cols[i]]].copy()
            temp_df.columns = ['burst_timestamp', 'tag', 'X', 'Y', 'Z']
            temp_df['index'] = i + 1
            tag_long_data = pd.concat([tag_long_data, temp_df], ignore_index=True)
        
        # Calculate timestamp: burst_timestamp + (index - 1) * 0.05 seconds
        tag_long_data['timestamp'] = tag_long_data['burst_timestamp'] + \
                                      pd.to_timedelta((tag_long_data['index'] - 1) * 0.05, unit='s')
        
        # Apply calibration (commented out as in original)
        # calib_row = acc_calib[acc_calib['tag'] == str(current_tag)]
        # if len(calib_row) > 0:
        #     tag_long_data['X'] = (tag_long_data['X'] - calib_row['x0'].iloc[0]) * calib_row['Sx'].iloc[0] * 9.81
        #     tag_long_data['Y'] = (tag_long_data['Y'] - calib_row['y0'].iloc[0]) * calib_row['Sy'].iloc[0] * -9.81
        #     tag_long_data['Z'] = (tag_long_data['Z'] - calib_row['z0'].iloc[0]) * calib_row['Sz'].iloc[0] * 9.81
        
        # Sort data for this tag
        tag_long_data = tag_long_data.sort_values(['timestamp', 'burst_timestamp', 'index', 'X', 'Y', 'Z'])
        
        # Add to list of all tag data for this animal
        all_tag_data.append(tag_long_data)
    
    # Skip if no valid data for any tag
    if len(all_tag_data) == 0:
        continue
    
    # Combine data from all tags for this animal
    animal_combined_data = pd.concat(all_tag_data, ignore_index=True)
    
    # Sort the combined data by timestamp
    animal_combined_data = animal_combined_data.sort_values(['timestamp'])
    # animal_combined_data = animal_combined_data.sort_values(['timestamp', 'X', 'Y', 'Z', 'burst_timestamp', 'index'])
    
    # Save the processed data for the current animal to a Parquet file
    output_filename = os.path.join(output_dir, f"{current_animal}.parquet")
    animal_combined_data.to_parquet(output_filename, index=False)

print("Processing complete!")
