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

PRÉREQUIS :
- Variable d'environnement API_GTFS_RT_KEY
- Fichiers statiques GTFS dans /opt/project/data/sweden_data/ (routes.txt)
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


# ═══════════════════════════════════════════════════════════════════════════
# FONCTIONS DES TÂCHES
# ═══════════════════════════════════════════════════════════════════════════

def task_fetch_transport_realtime(**context):
    """
    Récupère les données transport en temps réel depuis l'API GTFS-RT.
    
    Étapes :
    1. Télécharge le fichier .pb depuis l'API
    2. Parse et transforme les données
    3. Charge dans stg_transport_realtime (TRUNCATE + INSERT)
    
    Prérequis : Fichiers statiques GTFS (routes.txt) dans /opt/project/data/sweden_data/
    """
    from call_api_transport import fetch_transport_realtime
    from transform_transport_reel import process_etl_transport_live
    from load_to_neon import load_parquet_to_neon
    import pandas as pd
    
    execution_time = context.get('ts', 'unknown')
    logger.info(f"[{execution_time}] Fetch transport realtime")
    
    # Chemins
    DATA_DIR = "/opt/project/data"
    STATIC_DATA_PATH = os.path.join(DATA_DIR, "sweden_data")
    
    try:
        # 1. Télécharger le fichier .pb temps réel
        logger.info("Téléchargement du flux GTFS-RT...")
        filename = fetch_transport_realtime()
        file_path = os.path.join(DATA_DIR, filename)
        logger.info(f"Fichier téléchargé : {filename}")
        
        # 2. Vérifier que les fichiers statiques existent
        routes_file = os.path.join(STATIC_DATA_PATH, "routes.txt")
        if not os.path.exists(routes_file):
            logger.warning(f"Fichiers statiques GTFS manquants : {routes_file}")
            logger.warning("La transformation ne peut pas être effectuée.")
            logger.warning("Téléchargez les fichiers statiques GTFS depuis Trafiklab.")
            return {"status": "incomplete", "reason": "missing_static_files", "file": filename}
        
        # 3. Transformer les données
        logger.info("Transformation des données...")
        df_transformed = process_etl_transport_live(file_path, STATIC_DATA_PATH)
        
        if df_transformed.empty:
            logger.warning("Aucune donnée de bus extraite du flux temps réel")
            return {"status": "empty", "file": filename}
        
        logger.info(f"{len(df_transformed)} lignes de bus extraites")
        
        # 4. Sauvegarder en parquet temporaire
        parquet_path = file_path.replace(".pb", "_processed.parquet")
        df_transformed.to_parquet(parquet_path, index=False)
        
        # 5. Charger dans Neon (TRUNCATE + INSERT via if_exists='replace')
        logger.info("Chargement vers Neon (stg_transport_realtime)...")
        load_parquet_to_neon(parquet_path, "stg_transport_realtime", if_exists="replace")
        
        logger.info("Transport realtime mis a jour")
        return {"status": "success", "count": len(df_transformed), "file": filename}
        
    except Exception as e:
        logger.error(f"Echec fetch transport realtime : {e}")
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
