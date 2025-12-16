#!/usr/bin/env python3
"""
Combine all individual VEDBA parquet files into one master file
"""

import pandas as pd
import glob
import os
from pathlib import Path

def combine_vedba_files():
    """
    Read all VEDBA parquet files and combine them into one master file
    """
    
    # Define paths
    vedba_dir = 'data/vedba'
    output_file = 'data/vedba_all.parquet'
    
    print(" COMBINING VEDBA FILES")
    print("=" * 50)
    
    # Get all parquet files in vedba directory
    vedba_files = glob.glob(os.path.join(vedba_dir, '*.parquet'))
    
    if not vedba_files:
        print(f" No parquet files found in {vedba_dir}/")
        return
    
    print(f" Found {len(vedba_files)} VEDBA files to combine")
    print(f" Input directory: {vedba_dir}/")
    print(f" Output file: {output_file}")
    
    # Read and combine all files
    print(f"\n Reading files...")
    all_dataframes = []
    
    for i, file_path in enumerate(vedba_files, 1):
        filename = os.path.basename(file_path)
        animal_id = filename.replace('.parquet', '')
        
        # Read the file
        df = pd.read_parquet(file_path)
        
        all_dataframes.append(df)
        
        print(f"  [{i:2d}/{len(vedba_files)}] {animal_id}: {len(df):,} records")
    
    # Combine all dataframes
    print(f"\n Combining all data...")
    combined_df = pd.concat(all_dataframes, ignore_index=True)
    
    # Sort by tag and timestamp for better organization
    print(f" Sorting data...")
    combined_df = combined_df.sort_values(['tag', 'timestamp']).reset_index(drop=True)
    
    # Save the combined file
    print(f" Saving combined file...")
    combined_df.to_parquet(output_file, index=False)
    
    # Calculate file size
    file_size = os.path.getsize(output_file) / (1024 * 1024)  # MB
    
    print(f"\n SUCCESS!")
    print(f"=" * 50)
    print(f" Combined dataset statistics:")
    print(f"   Total records: {len(combined_df):,}")
    print(f"   Unique animals: {combined_df['tag'].nunique()}")
    print(f"   Columns: {', '.join(combined_df.columns)}")
    print(f"   File size: {file_size:.1f} MB")
    print(f"   Date range: {combined_df['timestamp'].min()} to {combined_df['timestamp'].max()}")
    
    # Show sample of data
    print(f"\n Sample of combined data:")
    print(combined_df.head())
    
    # Show summary by animal
    print(f"\n🐾 Records per animal:")
    animal_counts = combined_df['tag'].value_counts().sort_index()
    for animal, count in animal_counts.head(10).items():
        print(f"   {animal}: {count:,} records")
    if len(animal_counts) > 10:
        print(f"   ... and {len(animal_counts) - 10} more animals")
    
    print(f"\n🎉 Combined VEDBA file created: {output_file}")
    return combined_df

if __name__ == "__main__":
    try:
        result = combine_vedba_files()
        print(f"\n Script completed successfully!")
    except Exception as e:
        print(f"\n Error: {str(e)}")
        import traceback
        traceback.print_exc()
