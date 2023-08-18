from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from spotify_etl import main_spotify_etl


default_args = {
    'owner': 'Jonatan',
    'depends_on_past': False,
    'start_date': days_ago(0,0,0,0,0),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'spotify_etl_dag',
    default_args=default_args,
    description='A daily Spotify ETL',
    schedule_interval=timedelta(days=1),
)



run_etl = PythonOperator(
    task_id='whole_spotify_etl',
    python_callable=main_spotify_etl,
    dag=dag,
)

run_etl