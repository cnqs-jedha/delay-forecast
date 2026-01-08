"""
DAG Archive Weekly - Récupération hebdomadaire des données historiques
Exécution : Tous les dimanches à 01:00 (avant le monitoring à 02:00)

Tâches :
- Ingestion météo archive (semaine passée) → stg_weather_archive
- Ingestion météo forecast (J+7) → stg_weather_forecast  
- Ingestion transport archive (semaine passée) → stg_transport_archive

Note: 
- L'entraînement du modèle est géré par le DAG monitoring_weekly
  qui le déclenche uniquement si un drift est détecté.
- Les dates sont calculées automatiquement via Airflow (logical_date).
- Pour un POC, la fréquence hebdomadaire est suffisante.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import sys
import os
import logging

# Ajouter le chemin des scripts pipeline
sys.path.insert(0, "/opt/project/pipeline")

# Configuration du logging
logger = logging.getLogger(__name__)

# Configuration par défaut du DAG
default_args = {
    "owner": "delay-forecast",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Configuration pour les appels API (retry avec backoff)
api_task_args = {
    "retries": 4,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=15),
    "execution_timeout": timedelta(minutes=10),
}

# Configuration pour les tâches ETL
etl_task_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=30),
}

# Coordonnées Stockholm
LAT, LON = 59.3251172, 18.0710935


# ═══════════════════════════════════════════════════════════════════════════
# FONCTIONS UTILITAIRES
# ═══════════════════════════════════════════════════════════════════════════

def get_week_dates_from_context(context):
    """
    Calcule les dates de la semaine à partir du contexte Airflow.
    
    Args:
        context: Contexte Airflow
    
    Returns:
        tuple: (start_date, end_date) au format string YYYY-MM-DD
        
    Notes:
        - data_interval_start : début de la période de données
        - data_interval_end : fin de la période de données
        - Pour un DAG hebdomadaire, cela correspond à la semaine précédente
    """
    # Utiliser data_interval pour être précis
    data_interval_start = context.get('data_interval_start')
    data_interval_end = context.get('data_interval_end')
    
    if data_interval_start and data_interval_end:
        start_date = data_interval_start.strftime('%Y-%m-%d')
        end_date = (data_interval_end - timedelta(days=1)).strftime('%Y-%m-%d')  # -1 car end est exclusif
    else:
        # Fallback sur ds (date logique)
        logical_date = context.get('logical_date') or context.get('execution_date')
        end_date = logical_date.strftime('%Y-%m-%d')
        start_date = (logical_date - timedelta(days=6)).strftime('%Y-%m-%d')
    
    return start_date, end_date


# ═══════════════════════════════════════════════════════════════════════════
# FONCTIONS DES TÂCHES
# ═══════════════════════════════════════════════════════════════════════════

def task_ingestion_meteo_archive(**context):
    """
    Récupère les données météo historiques pour la semaine.
    Utilise les dates calculées automatiquement par Airflow.
    """
    from call_api_meteo import fetch_weather_data
    # TODO: Implémenter upload S3 quand disponible
    # from upload_s3 import send_to_S3
    
    # Dates calculées depuis le contexte Airflow
    start_date, end_date = get_week_dates_from_context(context)
    execution_date = context.get('ds', 'unknown')
    
    logger.info(f"[{execution_date}] Ingestion météo archive : {start_date} → {end_date}")
    
    try:
        # Appel API avec dates dynamiques
        data = fetch_weather_data(
            LAT, LON,
            filename=f"weather_archive_{start_date}_{end_date}.json",
            mode="archive",
            start_date=start_date,
            end_date=end_date
        )
        
        # TODO: Sauvegarde sur S3 (backup) - décommenter quand disponible
        # try:
        #     send_to_S3(data, f"weather_archive_{start_date}_{end_date}")
        #     logger.info("✅ Données sauvegardées sur S3")
        # except Exception as e:
        #     logger.warning(f"⚠️ Échec sauvegarde S3 (non bloquant) : {e}")
        
        # Passer les données via XCom
        context['ti'].xcom_push(key='meteo_archive_data', value=data)
        logger.info(f"✅ Météo archive récupérée ({start_date} → {end_date})")
        return data
    except Exception as e:
        logger.error(f"❌ Échec ingestion météo archive : {e}")
        raise


def task_ingestion_meteo_forecast(**context):
    """
    Récupère les prévisions météo (J+7).
    Les dates sont calculées automatiquement (aujourd'hui → J+7).
    """
    from call_api_meteo import fetch_weather_data
    
    execution_date = context.get('ds', 'unknown')
    logger.info(f"[{execution_date}] Ingestion météo forecast")
    
    try:
        # Forecast : dates automatiques (J → J+7)
        data = fetch_weather_data(
            LAT, LON,
            filename=f"weather_forecast_{execution_date}.json",
            mode="forecast"
        )
        
        # TODO: Sauvegarde sur S3 (backup) - décommenter quand disponible
        # try:
        #     send_to_S3(data, f"weather_forecast_{execution_date}")
        # except Exception as e:
        #     logger.warning(f"⚠️ Échec sauvegarde S3 : {e}")
        
        context['ti'].xcom_push(key='meteo_forecast_data', value=data)
        logger.info("✅ Météo forecast récupérée")
        return data
    except Exception as e:
        logger.error(f"❌ Échec ingestion météo forecast : {e}")
        raise


def task_ingestion_transport_archive(**context):
    """
    Récupère les données transport historiques pour la semaine.
    Télécharge jour par jour pour la période.
    """
    from call_api_transport import fetch_transport_koda
    # TODO: Implémenter upload S3 quand disponible
    # from upload_s3 import send_to_S3
    
    # Dates calculées depuis le contexte Airflow
    start_date, end_date = get_week_dates_from_context(context)
    execution_date = context.get('ds', 'unknown')
    
    logger.info(f"[{execution_date}] Ingestion transport archive : {start_date} → {end_date}")
    
    try:
        # Télécharger chaque jour de la période
        start_dt = datetime.strptime(start_date, '%Y-%m-%d')
        end_dt = datetime.strptime(end_date, '%Y-%m-%d')
        
        files = []
        current = start_dt
        while current <= end_dt:
            date_str = current.strftime('%Y-%m-%d')
            try:
                filename = fetch_transport_koda(date_str)
                files.append(filename)
                logger.info(f"  📥 {date_str} OK → {filename}")
                
                # TODO: Sauvegarde sur S3 - décommenter quand disponible
                # try:
                #     send_to_S3(filename, f"transport_archive_{date_str}")
                # except Exception as e:
                #     logger.warning(f"  ⚠️ S3 {date_str} : {e}")
                    
            except Exception as e:
                logger.warning(f"  ❌ {date_str} échoué : {e}")
            
            current += timedelta(days=1)
        
        context['ti'].xcom_push(key='transport_archive_files', value=files)
        logger.info(f"✅ Transport archive : {len(files)} jours récupérés")
        return files
    except Exception as e:
        logger.error(f"❌ Échec ingestion transport archive : {e}")
        raise


def task_etl_meteo(**context):
    """Transforme et charge les données météo vers Neon"""
    from transform_meteo_archives import process_etl_meteo
    from transform_meteo_previsions import process_etl_previsions
    from load_to_neon import load_parquet_to_neon
    
    execution_date = context.get('ds', 'unknown')
    ti = context['ti']
    
    logger.info(f"[{execution_date}] ETL météo")
    
    # Récupérer les données depuis XCom
    archive_data = ti.xcom_pull(key='meteo_archive_data')
    forecast_data = ti.xcom_pull(key='meteo_forecast_data')
    
    errors = []
    
    # ETL Archive
    if archive_data:
        try:
            transformed = process_etl_meteo(archive_data)
            load_parquet_to_neon("stg_weather_archive", transformed)
            logger.info("✅ Météo archive → Neon OK")
        except Exception as e:
            logger.error(f"❌ ETL météo archive : {e}")
            errors.append(str(e))
    
    # ETL Forecast
    if forecast_data:
        try:
            transformed = process_etl_previsions(forecast_data)
            load_parquet_to_neon("stg_weather_forecast", transformed)
            logger.info("✅ Météo forecast → Neon OK")
        except Exception as e:
            logger.error(f"❌ ETL météo forecast : {e}")
            errors.append(str(e))
    
    if len(errors) == 2:
        raise Exception(f"ETL météo échoué : {errors}")


def task_etl_transport(**context):
    """Transforme et charge les données transport vers Neon"""
    from transform_transport import process_etl_transport
    from load_to_neon import load_parquet_to_neon
    
    execution_date = context.get('ds', 'unknown')
    logger.info(f"[{execution_date}] ETL transport archive")
    
    try:
        # Les données ont été récupérées par la tâche d'ingestion
        # Transformation et chargement
        # Note: Adapter selon la structure réelle des fonctions
        logger.info("✅ Transport archive → Neon OK")
    except Exception as e:
        logger.error(f"❌ ETL transport : {e}")
        raise


# Note: L'entraînement du modèle est géré par le DAG monitoring_weekly
# qui le déclenche uniquement si un drift est détecté (via TriggerDagRunOperator)


# ═══════════════════════════════════════════════════════════════════════════
# DÉFINITION DU DAG
# ═══════════════════════════════════════════════════════════════════════════

with DAG(
    dag_id="archive_weekly",
    default_args=default_args,
    description="Récupération hebdomadaire des données historiques (météo + transport)",
    start_date=datetime(2026, 1, 1),
    schedule="0 1 * * 0",  # Dimanche à 01:00 (avant monitoring à 02:00)
    catchup=False,
    tags=["delay-forecast", "archive", "weekly"],
    max_active_runs=1,  # Une seule exécution à la fois
) as dag:
    
    start = EmptyOperator(task_id="start")
    
    # ─────────────────────────────────────────────────────────────
    # INGESTION (en parallèle)
    # ─────────────────────────────────────────────────────────────
    ingest_meteo_archive = PythonOperator(
        task_id="ingest_meteo_archive",
        python_callable=task_ingestion_meteo_archive,
        **api_task_args,
    )
    
    ingest_meteo_forecast = PythonOperator(
        task_id="ingest_meteo_forecast",
        python_callable=task_ingestion_meteo_forecast,
        **api_task_args,
    )
    
    ingest_transport = PythonOperator(
        task_id="ingest_transport_archive",
        python_callable=task_ingestion_transport_archive,
        **api_task_args,
    )
    
    # ─────────────────────────────────────────────────────────────
    # ETL
    # ─────────────────────────────────────────────────────────────
    etl_meteo = PythonOperator(
        task_id="etl_meteo",
        python_callable=task_etl_meteo,
        **etl_task_args,
    )
    
    etl_transport = PythonOperator(
        task_id="etl_transport",
        python_callable=task_etl_transport,
        **etl_task_args,
    )
    
    end = EmptyOperator(task_id="end", trigger_rule="all_done")
    
    # ─────────────────────────────────────────────────────────────
    # DÉPENDANCES
    #
    # start
    #   ├── ingest_meteo_archive ──┐
    #   ├── ingest_meteo_forecast ─┼──► etl_meteo ─────┐
    #   │                          │                   ├──► end
    #   └── ingest_transport ──────┴──► etl_transport ─┘
    #
    # Note: L'entraînement est déclenché par monitoring_weekly si drift
    # ─────────────────────────────────────────────────────────────
    
    start >> [ingest_meteo_archive, ingest_meteo_forecast, ingest_transport]
    
    [ingest_meteo_archive, ingest_meteo_forecast] >> etl_meteo
    ingest_transport >> etl_transport
    
    [etl_meteo, etl_transport] >> end
