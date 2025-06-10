import pandas as pd
import os

def transform_data():
    combined_path = "/opt/airflow/data/telco_combined.csv"
    clean_path = "/opt/airflow/data/clean/telco_clean.csv"
    log_path = "/opt/airflow/data/quality_  logs/quality_log.csv"

    # Read the combined dataframe
    df_all = pd.read_csv(combined_path)

    # Clean up: Trim whitespace from all string fields
    df_all = df_all.map(lambda x: x.strip() if isinstance(x, str) else x)

    # Rule 1: Remove rows with any null or NA values
    df_all.replace(['NA', 'N/A', '', ' '], pd.NA, inplace=True)
    df_clean = df_all.dropna()

    removed_nulls = df_all[df_all.isna().any(axis=1)].copy()
    removed_nulls['reason'] = 'null_or_na'

    # Rule 2: Remove duplicated customerID (keep first)
    df_clean = df_clean.drop_duplicates(subset=['customerID'], keep='first')

    removed_dupes = df_all[df_all.duplicated(subset=['customerID'], keep='first')].copy()
    removed_dupes['reason'] = 'duplicate_customerID'

    # Combine logs
    log_df = pd.concat([removed_nulls, removed_dupes], ignore_index=True)

    # Rule 3: Convert TotalCharges to numeric
    df_clean['TotalCharges'] = pd.to_numeric(df_clean['TotalCharges'], errors='coerce')

    def tenure_group(t):
        if t < 12:
            return '<1 year'
        elif t < 24:
            return '1–2 years'
        elif t < 48:
            return '2–4 years'
        else:
            return '>4 years'

    # Create TenureGroup column
    df_clean['TenureGroup'] = df_clean['tenure'].apply(tenure_group)
    
    total_rows = len(df_all)
    removed_rows = len(log_df)
    percent_removed = (removed_rows / total_rows) * 100

    # Save clean & log
    df_clean.to_csv(clean_path, index=False)
    log_df.to_csv(log_path, index=False)

    print(f"Cleaned data saved to {clean_path}")
    print(f"Quality issues logged at {log_path}")
    print(f"Removed {removed_rows} / {total_rows} rows ({percent_removed:.2f}%)")

    # Alert condition: if more than 10% of rows are removed, raise an error
    if percent_removed > 10:
        raise ValueError(f"❌ Alert: {percent_removed:.2f}% rows removed. Task failed.")