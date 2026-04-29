from sqlalchemy import create_engine
import pandas as pd

def load(file_path):
    df = pd.read_csv(file_path)

    engine = create_engine("postgresql://airflow:airflow@postgres:5432/airflow")

    df.to_sql("orders", engine, if_exists="replace", index=False)