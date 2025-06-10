import pandas as pd
from sqlalchemy import create_engine


def load_data():
    df = pd.read_csv('/opt/airflow/data/telco_clean.csv')
    engine = create_engine('sqlite:////opt/airflow/data/telco.db')
    df.to_sql('telco_customers', engine, if_exists='replace', index=False)
    print('Loaded data into SQLite telco.db')
