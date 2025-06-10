import pandas as pd
from sqlalchemy import create_engine
import os


def load_data():
    df = pd.read_csv('/opt/airflow/data/clean/telco_clean.csv')
    
    # PostgreSQL connection for data storage (assumes telco_data database exists)
    postgres_url = "postgresql+psycopg2://airflow:airflow@postgres:5432/telco_data"
    
    try:
        engine = create_engine(postgres_url)
        
        # Test connection first
        with engine.connect() as conn:
            conn.execute("SELECT 1")
        
        # Load data into the telco_data database
        df.to_sql('telco_customers', engine, if_exists='replace', index=False)
        print(f'Loaded {len(df)} rows into PostgreSQL telco_data.telco_customers')
        
        engine.dispose()
        
    except Exception as e:
        print(f"Error loading data to PostgreSQL: {e}")
        raise e
