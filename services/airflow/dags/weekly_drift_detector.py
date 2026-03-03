from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os
import smtplib
from email.mime.text import MIMEText

def drift_monitoring():
    AIRFLOW_SMTP_USER = os.getenv("AIRFLOW_SMTP_USER")
    AIRFLOW_SMTP_PASSWORD = os.getenv("AIRFLOW_SMTP_PASSWORD")
    
    drift_value = 5
    print(drift_value)
    if drift_value > 1:
        print("value > 1")
        # Construire le mail
        msg = MIMEText(f"Drift détecté ! Valeur : {drift_value}")
        msg["Subject"] = "Alerte Drift"
        msg["From"] = AIRFLOW_SMTP_USER
        msg["To"] = AIRFLOW_SMTP_USER

        # Envoyer via SMTP
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(AIRFLOW_SMTP_USER, AIRFLOW_SMTP_PASSWORD)
            server.send_message(msg)

        print(f"Mail envoyé — value={drift_value}")

with DAG(
    dag_id="weekly_drift_detector",
    start_date=datetime(2026, 3, 2),
    schedule="@weekly",
    catchup=False,
    tags=["drift", "monitoring"],
) as dag:

    PythonOperator(
        task_id="weekly_drift_detector",
        python_callable=drift_monitoring,
    )
