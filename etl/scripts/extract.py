import pandas as pd
import os

def extract_data():
    raw_folder = "/opt/airflow/data/raw"
    combined_path = "/opt/airflow/data/telco_combined.csv"
    
    # Create combined directory if it doesn't exist
    os.makedirs(os.path.dirname(combined_path), exist_ok=True)
    
    all_dfs = []
    
    # Read and combine all CSV files from raw folder
    for file in os.listdir(raw_folder):
        if file.endswith(".csv"):
            df = pd.read_csv(os.path.join(raw_folder, file))
            df['source_file'] = file  # track file source
            all_dfs.append(df)
    
    if not all_dfs:
        raise ValueError("No CSV files found in raw folder")
    
    # Combine all dataframes
    df_all = pd.concat(all_dfs, ignore_index=True)
    
    # Save combined dataframe
    df_all.to_csv(combined_path, index=False)
    print(f"Combined data saved to {combined_path}")
    print(f"Total rows: {len(df_all)}")
