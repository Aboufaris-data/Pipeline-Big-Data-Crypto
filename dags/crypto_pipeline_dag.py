from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
import sys
import logging

sys.path.append('/opt/airflow/')

from src.ingest_bronze import run as run_bronze
from src.transform_silver import run as run_silver
from src.load_gold import run as run_gold

# 1. Alerte en cas d'échec
def on_failure_alert(context):
    task_id = context['task_instance'].task_id
    logging.error(f"❌ Airflow Alert: La tâche [{task_id}] a échoué.")

# 2. Configuration des Retries (2 tentatives, 5 min d'attente)
default_args = {
    'owner': 'data_engineering',
    'start_date': datetime(2026, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': on_failure_alert,
}

# 3. Définition du Workflow
with DAG(
    'cryptopipelinedag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    ingest_bronze = PythonOperator(
        task_id='ingestbronze',
        python_callable=run_bronze,
    )

    transform_silver = PythonOperator(
        task_id='transformsilver',
        python_callable=run_silver,
    )

    build_gold_model = PythonOperator(
        task_id='buildgoldmodel',
        python_callable=run_gold,
    )

    # L'enchaînement des tâches
    ingest_bronze >> transform_silver >> build_gold_model