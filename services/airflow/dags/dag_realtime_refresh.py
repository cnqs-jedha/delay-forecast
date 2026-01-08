"""
DAG Realtime Refresh - Mise à jour temps réel des données transport
Exécution : Toutes les 5 minutes

Tâches :
- Récupération position/retard actuel des bus → stg_transport_realtime
- (Futur) Récupération trafic routier → stg_traffic_realtime

Note: La fréquence de 5 minutes est un compromis entre :
- Précision (temps moyen entre 2 arrêts de bus ≈ 2-3 min)
- Charge sur les APIs externes
- Coût de stockage

Pour ajuster : modifier le schedule "*/5 * * * *"
- */2 = toutes les 2 minutes (heures de pointe)
- */10 = toutes les 10 minutes (heures creuses)
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

# Configuration par défaut
default_args = {
    "owner": "delay-forecast",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(seconds=30),  # Retry rapide pour le temps réel
}

# Configuration pour les appels API temps réel
realtime_task_args = {
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=3),  # Timeout court pour ne pas bloquer
}

# Configuration bus
BUS_NUMBER = "541"
MAX_BUS_PER_HOUR = 3


# ═══════════════════════════════════════════════════════════════════════════
# FONCTIONS DES TÂCHES
# ═══════════════════════════════════════════════════════════════════════════

def task_fetch_transport_realtime(**context):
    """
    Récupère les données transport en temps réel depuis l'API.
    TRUNCATE + INSERT dans stg_transport_realtime.
    """
    from transport.utils.call_api_transport import call_rt_history_api, call_rt_reference_api
    from transport.utils.read_data_transport import read_koda_reference_data
    from transport.utils.filter_route_transport import filter_by_bus_route
    from transport.utils.transform_data_transport import transform_S3_to_neon
    from transport.utils.load_to_neon_transport import load_parquet_to_neon
    from google.transit import gtfs_realtime_pb2
    
    execution_time = context.get('ts', 'unknown')
    logger.info(f"[{execution_time}] Fetch transport realtime")
    
    try:
        # 1. Appel API temps réel
        r_history = call_rt_history_api()
        
        # 2. Parser les données GTFS
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(r_history.content)
        history_entities = list(feed.entity)
        
        logger.info(f"📡 {len(history_entities)} entités reçues de l'API")
        
        # 3. Récupérer les références
        r_reference = call_rt_reference_api()
        reference_routes = read_koda_reference_data(r_reference, "routes")
        reference_trips = read_koda_reference_data(r_reference, "trips")
        
        # 4. Filtrer par bus
        filtered_data = filter_by_bus_route(
            BUS_NUMBER, 
            reference_routes, 
            reference_trips, 
            history_entities, 
            MAX_BUS_PER_HOUR
        )
        
        logger.info(f"🚌 {len(filtered_data)} observations pour bus {BUS_NUMBER}")
        
        # 5. Transformer
        data_transformed = transform_S3_to_neon(filtered_data)
        
        # 6. Charger (TRUNCATE + INSERT)
        load_parquet_to_neon("stg_transport_realtime", data_transformed, realtime=True)
        
        logger.info("✅ Transport realtime mis à jour")
        return {"count": len(filtered_data)}
        
    except Exception as e:
        logger.error(f"❌ Échec fetch transport realtime : {e}")
        raise


def task_fetch_traffic_realtime(**context):
    """
    (FUTUR) Récupère les données trafic routier en temps réel.
    À implémenter quand l'API trafic sera intégrée.
    """
    execution_time = context.get('ts', 'unknown')
    logger.info(f"[{execution_time}] Fetch traffic realtime - NON IMPLÉMENTÉ")
    
    # TODO: Implémenter quand l'API trafic sera disponible
    # 1. Appeler l'API trafic
    # 2. Transformer les données
    # 3. TRUNCATE + INSERT dans stg_traffic_realtime
    
    logger.warning("⚠️ API trafic non implémentée - tâche ignorée")
    return {"status": "not_implemented"}


def task_health_check(**context):
    """Vérifie que les services sont disponibles"""
    import requests
    
    services = {
        "api": "http://api:8000/",
        "mlflow": "http://mlflow:5000/health",
    }
    
    results = {}
    for name, url in services.items():
        try:
            response = requests.get(url, timeout=5)
            results[name] = response.status_code == 200
        except:
            results[name] = False
    
    logger.info(f"🏥 Health check: {results}")
    return results


# ═══════════════════════════════════════════════════════════════════════════
# DÉFINITION DU DAG
# ═══════════════════════════════════════════════════════════════════════════

with DAG(
    dag_id="realtime_refresh",
    default_args=default_args,
    description="Mise à jour temps réel des données transport (toutes les 5 min)",
    start_date=datetime(2026, 1, 1),
    schedule="*/5 * * * *",  # Toutes les 5 minutes
    catchup=False,
    tags=["delay-forecast", "realtime", "transport"],
    max_active_runs=1,  # Éviter les exécutions parallèles
) as dag:
    
    start = EmptyOperator(task_id="start")
    
    # ─────────────────────────────────────────────────────────────
    # TRANSPORT TEMPS RÉEL
    # ─────────────────────────────────────────────────────────────
    fetch_transport = PythonOperator(
        task_id="fetch_transport_realtime",
        python_callable=task_fetch_transport_realtime,
        **realtime_task_args,
    )
    
    # ─────────────────────────────────────────────────────────────
    # TRAFIC TEMPS RÉEL (futur)
    # ─────────────────────────────────────────────────────────────
    fetch_traffic = PythonOperator(
        task_id="fetch_traffic_realtime",
        python_callable=task_fetch_traffic_realtime,
        retries=0,  # Pas de retry car non implémenté
    )
    
    end = EmptyOperator(task_id="end", trigger_rule="all_done")
    
    # ─────────────────────────────────────────────────────────────
    # DÉPENDANCES (parallèle)
    #
    # start
    #   ├── fetch_transport ──┐
    #   │                     ├──► end
    #   └── fetch_traffic ────┘
    #
    # ─────────────────────────────────────────────────────────────
    
    start >> [fetch_transport, fetch_traffic] >> end
