"""
DAG Delay Forecast - Pipeline quotidien
Orchestre : Ingestion -> ETL -> Chargement DB -> Entraînement
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import sys
import os

# Ajouter le chemin des scripts pipeline
sys.path.insert(0, "/opt/project/pipeline")

# Configuration par défaut du DAG
default_args = {
    "owner": "delay-forecast",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Coordonnées Stockholm
LAT, LON = 59.3251172, 18.0710935


def task_ingestion_meteo(**context):
    """Ingestion des données météo depuis l'API Open-Meteo"""
    from call_api_meteo import fetch_weather_data
    
    # Archive météo
    path_archive = fetch_weather_data(LAT, LON, mode="archive", filename="weather_stockholm_archive.json")
    print(f"Météo archive téléchargée : {path_archive}")
    
    # Prévisions météo
    path_forecast = fetch_weather_data(LAT, LON, mode="forecast", filename="weather_stockholm_forecast.json")
    print(f"Météo prévisions téléchargées : {path_forecast}")
    
    return {"archive": path_archive, "forecast": path_forecast}


def task_ingestion_transport(**context):
    """Ingestion des données transport depuis l'API Trafiklab"""
    from call_api_transport import fetch_transport_realtime
    
    path_rt = fetch_transport_realtime()
    print(f"Transport temps réel téléchargé : {path_rt}")
    
    return {"realtime": path_rt}


def task_etl_meteo(**context):
    """Transformation des données météo"""
    from transform_meteo_archives import process_etl_meteo
    from transform_meteo_previsions import process_etl_previsions
    
    try:
        process_etl_meteo("weather_stockholm_archive.json")
        print("ETL météo archive terminé")
    except Exception as e:
        print(f"Erreur ETL météo archive : {e}")
    
    try:
        process_etl_previsions("weather_stockholm_forecast.json")
        print("ETL météo prévisions terminé")
    except Exception as e:
        print(f"Erreur ETL météo prévisions : {e}")


def task_etl_transport(**context):
    """Transformation des données transport"""
    from transform_transport_reel import process_etl_transport_live
    
    ti = context['ti']
    transport_paths = ti.xcom_pull(task_ids='ingestion_transport')
    
    if transport_paths and transport_paths.get('realtime'):
        try:
            process_etl_transport_live(
                transport_paths['realtime'], 
                os.path.join("/opt/project/data", "sweden_data")
            )
            print("ETL transport temps réel terminé")
        except Exception as e:
            print(f"Erreur ETL transport : {e}")


def task_load_to_neon(**context):
    """Chargement des données transformées vers Neon DB"""
    from load_to_neon import load_to_neon
    
    try:
        load_to_neon()
        print("Chargement vers Neon DB terminé")
    except Exception as e:
        print(f"Erreur chargement Neon : {e}")
        raise


def task_train_model(**context):
    """Entraînement du modèle ML et logging dans MLflow"""
    from train_model import train_model
    
    try:
        train_model()
        print("Entraînement du modèle terminé")
    except Exception as e:
        print(f"Erreur entraînement : {e}")
        raise


def task_trigger_evidently(**context):
    """Déclenche le calcul de drift Evidently"""
    import requests
    
    try:
        response = requests.post("http://evidently:8001/analyze/drift", timeout=60)
        if response.status_code == 200:
            print(f"Analyse Evidently terminée : {response.json()}")
        else:
            print(f"Erreur Evidently : {response.status_code}")
    except Exception as e:
        print(f"Erreur appel Evidently : {e}")


# Définition du DAG
with DAG(
    dag_id="delay_forecast_daily",
    default_args=default_args,
    description="Pipeline quotidien de prédiction des retards de transport",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["delay-forecast", "ml", "etl"],
) as dag:
    
    # Début du pipeline
    start = EmptyOperator(task_id="start")
    
    # Tâches d'ingestion (en parallèle)
    ingestion_meteo = PythonOperator(
        task_id="ingestion_meteo",
        python_callable=task_ingestion_meteo,
    )
    
    ingestion_transport = PythonOperator(
        task_id="ingestion_transport",
        python_callable=task_ingestion_transport,
    )
    
    # Tâches ETL
    etl_meteo = PythonOperator(
        task_id="etl_meteo",
        python_callable=task_etl_meteo,
    )
    
    etl_transport = PythonOperator(
        task_id="etl_transport",
        python_callable=task_etl_transport,
    )
    
    # Chargement vers DB
    load_db = PythonOperator(
        task_id="load_to_neon",
        python_callable=task_load_to_neon,
    )
    
    # Entraînement du modèle
    train = PythonOperator(
        task_id="train_model",
        python_callable=task_train_model,
    )
    
    # Monitoring Evidently
    monitoring = PythonOperator(
        task_id="trigger_evidently",
        python_callable=task_trigger_evidently,
    )
    
    # Fin du pipeline
    end = EmptyOperator(task_id="end")
    
    # Définition des dépendances
    #
    # start
    #   ├── ingestion_meteo ──► etl_meteo ─────┐
    #   │                                      ├──► load_db ──► train ──► monitoring ──► end
    #   └── ingestion_transport ──► etl_transport ┘
    #
    
    start >> [ingestion_meteo, ingestion_transport]
    
    ingestion_meteo >> etl_meteo
    ingestion_transport >> etl_transport
    
    [etl_meteo, etl_transport] >> load_db >> train >> monitoring >> end
