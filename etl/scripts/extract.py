import shutil


def extract_data():
    src = '/opt/airflow/data/WA_Fn-UseC_-Telco-Customer-Churn.csv'
    dst = '/opt/airflow/data/telco_raw_working.csv'
    shutil.copy(src, dst)
    print(f'copy data to {dst}')
