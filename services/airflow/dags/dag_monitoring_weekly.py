"""
DAG Monitoring Weekly - Analyse de drift et qualité du modèle
Exécution : Tous les dimanches à 02:00

Tâches :
- Chargement des données de référence (entraînement)
- Chargement des données récentes (production)
- Analyse de drift (Evidently)
- Génération de rapport
- Alerte si drift détecté
- (Optionnel) Déclenchement du réentraînement si drift > seuil
"""

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
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
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Seuils de drift
DRIFT_THRESHOLD = 0.1  # 10% de colonnes avec drift
R2_MIN_THRESHOLD = 0.6  # Score R² minimum acceptable


# ═══════════════════════════════════════════════════════════════════════════
# FONCTIONS DES TÂCHES
# ═══════════════════════════════════════════════════════════════════════════

def task_load_reference_data(**context):
    """
    Charge les données de référence (utilisées pour l'entraînement).
    Ces données représentent le "comportement normal" du modèle.
    """
    from sqlalchemy import create_engine, text
    import pandas as pd
    import os
    
    execution_date = context.get('ds', 'unknown')
    logger.info(f"[{execution_date}] Chargement données de référence")
    
    db_url = os.getenv("DATABASE_URL")
    engine = create_engine(db_url)
    
    try:
        with engine.connect() as conn:
            # Charger un échantillon des données d'archive (derniers 30 jours d'entraînement)
            query = """
                SELECT * FROM stg_weather_archive 
                ORDER BY timestamp_rounded DESC 
                LIMIT 1000
            """
            df = pd.read_sql(text(query), conn)
        
        # Sauvegarder pour la tâche suivante
        reference_path = "/tmp/reference_data.parquet"
        df.to_parquet(reference_path)
        
        logger.info(f"✅ Données de référence chargées : {len(df)} lignes")
        return {"path": reference_path, "count": len(df)}
        
    except Exception as e:
        logger.error(f"❌ Échec chargement référence : {e}")
        raise


def task_load_current_data(**context):
    """
    Charge les données récentes (production).
    Ces données seront comparées aux données de référence.
    """
    from sqlalchemy import create_engine, text
    import pandas as pd
    import os
    
    execution_date = context.get('ds', 'unknown')
    logger.info(f"[{execution_date}] Chargement données récentes")
    
    db_url = os.getenv("DATABASE_URL")
    engine = create_engine(db_url)
    
    try:
        with engine.connect() as conn:
            # Charger les prédictions récentes (dernière semaine)
            query = """
                SELECT * FROM prediction_logs 
                WHERE timestamp >= NOW() - INTERVAL '7 days'
                ORDER BY timestamp DESC
            """
            df_predictions = pd.read_sql(text(query), conn)
            
            # Charger les données météo récentes
            query_weather = """
                SELECT * FROM stg_weather_archive 
                WHERE timestamp_rounded >= NOW() - INTERVAL '7 days'
            """
            df_weather = pd.read_sql(text(query_weather), conn)
        
        # Sauvegarder
        predictions_path = "/tmp/current_predictions.parquet"
        weather_path = "/tmp/current_weather.parquet"
        
        df_predictions.to_parquet(predictions_path)
        df_weather.to_parquet(weather_path)
        
        logger.info(f"✅ Données récentes : {len(df_predictions)} prédictions, {len(df_weather)} météo")
        return {
            "predictions_path": predictions_path,
            "weather_path": weather_path,
            "predictions_count": len(df_predictions),
            "weather_count": len(df_weather)
        }
        
    except Exception as e:
        logger.error(f"❌ Échec chargement données récentes : {e}")
        raise


def task_analyze_drift(**context):
    """
    Analyse le drift via Evidently.
    Compare les données de référence aux données récentes.
    """
    import requests
    import pandas as pd
    
    execution_date = context.get('ds', 'unknown')
    ti = context['ti']
    
    logger.info(f"[{execution_date}] Analyse de drift")
    
    # Récupérer les chemins des données
    ref_info = ti.xcom_pull(task_ids='load_reference_data')
    current_info = ti.xcom_pull(task_ids='load_current_data')
    
    if not ref_info or not current_info:
        logger.warning("⚠️ Données manquantes pour l'analyse")
        return {"drift_detected": False, "error": "missing_data"}
    
    try:
        # Charger les données
        df_reference = pd.read_parquet(ref_info['path'])
        df_current = pd.read_parquet(current_info['weather_path'])
        
        # Appeler le service Evidently
        response = requests.post(
            "http://evidently:8001/reference",
            json={"data": df_reference.head(500).to_dict(orient='records')},
            timeout=60
        )
        
        if response.status_code != 200:
            logger.warning(f"⚠️ Échec définition référence Evidently : {response.status_code}")
        
        # Analyser le drift
        response = requests.post(
            "http://evidently:8001/drift/report",
            json={"data": df_current.head(500).to_dict(orient='records')},
            timeout=120
        )
        
        if response.status_code == 200:
            result = response.json()
            drift_detected = result.get('drift_detected', False)
            drift_share = result.get('drift_share', 0)
            
            logger.info(f"📊 Résultat drift : detected={drift_detected}, share={drift_share:.2%}")
            
            if drift_detected:
                logger.warning(f"⚠️ DRIFT DÉTECTÉ ! {drift_share:.2%} des colonnes ont drifté")
            else:
                logger.info("✅ Pas de drift significatif détecté")
            
            return {
                "drift_detected": drift_detected,
                "drift_share": drift_share,
                "threshold": DRIFT_THRESHOLD,
                "report_filename": result.get('report_filename')
            }
        else:
            logger.error(f"❌ Erreur Evidently : {response.status_code}")
            return {"drift_detected": False, "error": f"evidently_error_{response.status_code}"}
            
    except requests.exceptions.ConnectionError:
        logger.warning("⚠️ Service Evidently non disponible")
        return {"drift_detected": False, "error": "evidently_unavailable"}
    except Exception as e:
        logger.error(f"❌ Erreur analyse drift : {e}")
        return {"drift_detected": False, "error": str(e)}


def task_decide_retrain(**context):
    """
    Décide si un réentraînement est nécessaire.
    Retourne le task_id de la branche à suivre.
    """
    ti = context['ti']
    drift_result = ti.xcom_pull(task_ids='analyze_drift')
    
    if not drift_result:
        logger.info("Pas de résultat de drift - skip retrain")
        return "skip_retrain"
    
    drift_detected = drift_result.get('drift_detected', False)
    drift_share = drift_result.get('drift_share', 0)
    
    # Décision basée sur le seuil
    if drift_detected and drift_share > DRIFT_THRESHOLD:
        logger.warning(f"🔄 Réentraînement requis ! Drift share: {drift_share:.2%}")
        return "trigger_retrain"
    else:
        logger.info("✅ Pas de réentraînement nécessaire")
        return "skip_retrain"


def task_generate_report(**context):
    """Génère un rapport de monitoring consolidé"""
    import json
    from datetime import datetime
    
    ti = context['ti']
    execution_date = context.get('ds', 'unknown')
    
    drift_result = ti.xcom_pull(task_ids='analyze_drift')
    current_info = ti.xcom_pull(task_ids='load_current_data')
    
    report = {
        "date": execution_date,
        "generated_at": datetime.now().isoformat(),
        "drift_analysis": drift_result,
        "data_volume": {
            "predictions": current_info.get('predictions_count', 0) if current_info else 0,
            "weather_records": current_info.get('weather_count', 0) if current_info else 0,
        },
        "thresholds": {
            "drift": DRIFT_THRESHOLD,
            "r2_min": R2_MIN_THRESHOLD,
        },
        "recommendation": "RETRAIN" if drift_result and drift_result.get('drift_detected') else "CONTINUE"
    }
    
    # Sauvegarder le rapport
    report_path = f"/tmp/monitoring_report_{execution_date}.json"
    with open(report_path, 'w') as f:
        json.dump(report, f, indent=2, default=str)
    
    logger.info(f"📄 Rapport généré : {report_path}")
    logger.info(f"📊 Résumé : {report['recommendation']}")
    
    return report


def task_send_alert(**context):
    """Envoie une alerte si nécessaire (drift détecté)"""
    ti = context['ti']
    drift_result = ti.xcom_pull(task_ids='analyze_drift')
    
    if drift_result and drift_result.get('drift_detected'):
        drift_share = drift_result.get('drift_share', 0)
        
        # TODO: Implémenter l'envoi réel (email, Slack, webhook)
        alert_message = f"""
        🚨 ALERTE DRIFT DÉTECTÉ 🚨
        
        Date: {context.get('ds')}
        Drift share: {drift_share:.2%}
        Seuil: {DRIFT_THRESHOLD:.2%}
        
        Action recommandée: Réentraînement du modèle
        """
        
        logger.warning(alert_message)
        
        # Exemple d'envoi webhook (à configurer)
        # import requests
        # webhook_url = os.getenv("ALERT_WEBHOOK_URL")
        # if webhook_url:
        #     requests.post(webhook_url, json={"text": alert_message})
        
        return {"alert_sent": True, "message": alert_message}
    
    logger.info("✅ Pas d'alerte à envoyer")
    return {"alert_sent": False}


# ═══════════════════════════════════════════════════════════════════════════
# DÉFINITION DU DAG
# ═══════════════════════════════════════════════════════════════════════════

with DAG(
    dag_id="monitoring_weekly",
    default_args=default_args,
    description="Analyse hebdomadaire de drift et qualité du modèle (Evidently)",
    start_date=datetime(2026, 1, 1),
    schedule="0 2 * * 0",  # Dimanche à 02:00
    catchup=False,
    tags=["delay-forecast", "monitoring", "evidently", "weekly"],
    max_active_runs=1,
) as dag:
    
    start = EmptyOperator(task_id="start")
    
    # ─────────────────────────────────────────────────────────────
    # CHARGEMENT DES DONNÉES
    # ─────────────────────────────────────────────────────────────
    load_reference = PythonOperator(
        task_id="load_reference_data",
        python_callable=task_load_reference_data,
    )
    
    load_current = PythonOperator(
        task_id="load_current_data",
        python_callable=task_load_current_data,
    )
    
    # ─────────────────────────────────────────────────────────────
    # ANALYSE DE DRIFT
    # ─────────────────────────────────────────────────────────────
    analyze_drift = PythonOperator(
        task_id="analyze_drift",
        python_callable=task_analyze_drift,
        execution_timeout=timedelta(minutes=10),
    )
    
    # ─────────────────────────────────────────────────────────────
    # DÉCISION : RÉENTRAÎNER OU NON
    # ─────────────────────────────────────────────────────────────
    decide = BranchPythonOperator(
        task_id="decide_retrain",
        python_callable=task_decide_retrain,
    )
    
    # Branche : Déclencher réentraînement
    trigger_retrain = TriggerDagRunOperator(
        task_id="trigger_retrain",
        trigger_dag_id="archive_weekly",  # Déclenche le DAG d'archive qui inclut l'entraînement
        wait_for_completion=False,
        reset_dag_run=True,
    )
    
    # Branche : Skip réentraînement
    skip_retrain = EmptyOperator(task_id="skip_retrain")
    
    # ─────────────────────────────────────────────────────────────
    # RAPPORT ET ALERTES
    # ─────────────────────────────────────────────────────────────
    generate_report = PythonOperator(
        task_id="generate_report",
        python_callable=task_generate_report,
        trigger_rule="all_done",  # S'exécute quelle que soit la branche
    )
    
    send_alert = PythonOperator(
        task_id="send_alert",
        python_callable=task_send_alert,
    )
    
    end = EmptyOperator(task_id="end", trigger_rule="all_done")
    
    # ─────────────────────────────────────────────────────────────
    # DÉPENDANCES
    #
    # start
    #   ├── load_reference ──┐
    #   │                    ├──► analyze_drift ──► decide
    #   └── load_current ────┘                       │
    #                                                ├──► trigger_retrain ──┐
    #                                                │                      ├──► generate_report ──► send_alert ──► end
    #                                                └──► skip_retrain ─────┘
    #
    # ─────────────────────────────────────────────────────────────
    
    start >> [load_reference, load_current]
    [load_reference, load_current] >> analyze_drift >> decide
    
    decide >> [trigger_retrain, skip_retrain]
    
    [trigger_retrain, skip_retrain] >> generate_report >> send_alert >> end
