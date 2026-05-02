import sys
import os
from datetime import datetime

# Memastikan direktori root Airflow terbaca oleh Python di dalam container
sys.path.append(os.path.abspath(os.environ.get('AIRFLOW_HOME', '/opt/airflow')))

from airflow import DAG
from airflow.operators.python import PythonOperator

# Mengimpor modul Star Schema yang baru saja kita buat
from scripts.extract_dwh import extract_dwh
from scripts.transform_dwh import transform_dwh
from scripts.load_dwh import load_dwh

# --- Definisi Task Functions ---

def run_extract_dwh(**context):
    """Menjalankan ekstraksi dan mengembalikan dictionary berisi path file raw."""
    raw_file_paths = extract_dwh()
    return raw_file_paths

def run_transform_dwh(**context):
    """Menarik path raw dari XCom, menjalankan transformasi, dan mengembalikan path processed."""
    # Menarik (pull) data dari return value task extract
    raw_file_paths = context['ti'].xcom_pull(task_ids='extract_dwh_task')
    
    processed_file_paths = transform_dwh(raw_file_paths)
    return processed_file_paths

def run_load_dwh(**context):
    """Menarik path processed dari XCom dan memuatnya ke PostgreSQL."""
    # Menarik (pull) data dari return value task transform
    processed_file_paths = context['ti'].xcom_pull(task_ids='transform_dwh_task')
    
    load_dwh(processed_file_paths)

# --- Definisi DAG ---

with DAG(
    dag_id='star_schema_dwh_pipeline',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['data_warehouse', 'star_schema', 'portfolio'],
    description='ETL Pipeline untuk membangun Star Schema Data Warehouse'
) as dag:

    # 1. Task Extract
    extract_task = PythonOperator(
        task_id='extract_dwh_task',
        python_callable=run_extract_dwh,
    )

    # 2. Task Transform
    transform_task = PythonOperator(
        task_id='transform_dwh_task',
        python_callable=run_transform_dwh,
    )

    # 3. Task Load
    load_task = PythonOperator(
        task_id='load_dwh_task',
        python_callable=run_load_dwh,
    )

    # --- Mengatur Urutan Eksekusi (Dependencies) ---
    extract_task >> transform_task >> load_task