import pandas as pd


def transform_data():
    path = '/opt/airflow/data/telco_raw_working.csv'
    df = pd.read_csv(path)

    df['TotalCharges'] = pd.to_numeric(df['TotalCharges'], errors='coerce')
    df.dropna(subset=['TotalCharges'], inplace=True)

    def tenure_group(t):
        if t < 12:
            return '<1 year'
        elif t < 24:
            return '1–2 years'
        elif t < 48:
            return '2–4 years'
        else:
            return '>4 years'

    df['TenureGroup'] = df['tenure'].apply(tenure_group)
    out = '/opt/airflow/data/telco_clean.csv'
    df.to_csv(out, index=False)
    print(f'Cleaned data written to {out}')
