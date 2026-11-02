from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys

# Memastikan jalur Python mendeteksi root direktori di dalam container
sys.path.append('/opt/airflow')
from scripts.etl_process import jalankan_etl

# Argumen default dengan kebijakan retry yang lebih ketat jika terjadi gangguan database
default_args = {
    'owner': 'yusuf_cahyo',
    'depends_on_past': False,
    'start_date': datetime(2026, 10, 26), 
    'retries': 2,                             # Dicoba ulang 2 kali jika eror
    'retry_delay': timedelta(minutes=2),       # Jeda waktu antar retry adalah 2 minut
}

with DAG(
    'dag_transjakarta_etl',            
    default_args=default_args,
    description='Pipeline ETL Profesional dan Modular untuk Data Tiket TransJakarta',
    schedule='@daily',                         # Penulisan standar untuk Airflow 2.x terbaru
    template_searchpath=['/opt/airflow/sql'], 
    catchup=False                       
) as dag:

    # TASK 1: Membuat struktur tabel database jika belum ada
    task_buat_tabel = SQLExecuteQueryOperator(
        task_id='create_database_table',
        conn_id='postgres_data_conn',   
        sql='creatte_table_transjakarta.sql', 
    )

    # TASK 2: Mengeksekusi pembersihan data tingkat lanjut menggunakan Pandas
    task_proses_etl = PythonOperator(
        task_id='run_pandas_etl_process',
        python_callable=jalankan_etl,   
    )

    # Alur kerja orkestrasi pipeline
    task_buat_tabel >> task_proses_etl
