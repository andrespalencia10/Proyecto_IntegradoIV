from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os

# Asegúrate de poder importar desde la carpeta src
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))

# Importa las funciones de extracción, transformación y carga
from extract import extract_data
from transform import transform_data
from load import load_data

default_args = {
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='elt_pipeline_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description='Pipeline ELT con Airflow',
) as dag:

    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data
    )

    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data
    )

    extract_task >> transform_task >> load_task
