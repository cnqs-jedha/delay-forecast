from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def ping():
    print("DAG is alive")

with DAG(
    dag_id="delay_forecast_daily",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["poc"],
) as dag:
    PythonOperator(task_id="ping", python_callable=ping)
