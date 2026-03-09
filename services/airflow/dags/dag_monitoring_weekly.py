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

WEATHER_FEATURES = [
    "temperature_2m", "precipitation", "rain", "snowfall",
    "wind_speed_10m", "wind_gusts_10m", "weather_code", "cloud_cover",
    "dew_point_2m", "wind_direction_10m", "soleil_leve",
    "risque_gel_pluie", "risque_gel_neige", "neige_fondue",
    "est_weekend", "est_jour_ferie", "vacances_scolaires",
]

WEATHER_COLS_SQL = ", ".join(WEATHER_FEATURES)


def task_load_reference_data(**context):
    """
    Charge les features meteo de reference (entrainement) depuis
    stg_weather_archive, en excluant les 7 derniers jours.
    """
    from sqlalchemy import create_engine, text
    import pandas as pd
    import os

    execution_date = context.get('ds', 'unknown')
    logger.info(f"[{execution_date}] Chargement donnees de reference")

    db_url = os.getenv("DATABASE_URL")
    engine = create_engine(db_url)

    try:
        with engine.connect() as conn:
            query = f"""
                SELECT {WEATHER_COLS_SQL}
                FROM stg_weather_archive
                WHERE timestamp_rounded < NOW() - INTERVAL '7 days'
                ORDER BY timestamp_rounded DESC
                LIMIT 1000
            """
            df = pd.read_sql(text(query), conn)

        if len(df) < 50:
            logger.warning(f"Pas assez de donnees de reference ({len(df)} lignes, min 50)")
            return None

        reference_path = "/tmp/reference_data.parquet"
        df.to_parquet(reference_path)

        logger.info(f"Donnees de reference chargees : {len(df)} lignes")
        return {"path": reference_path, "count": len(df)}

    except Exception as e:
        logger.error(f"Echec chargement reference : {e}")
        raise


def task_load_current_data(**context):
    """
    Charge les donnees de production recentes :
    - Features meteo depuis prediction_logs (7 derniers jours) pour le drift
    - Predictions + ground_truth (30 derniers jours) pour le rapport de performance
    """
    from sqlalchemy import create_engine, text
    import pandas as pd
    import os

    execution_date = context.get('ds', 'unknown')
    logger.info(f"[{execution_date}] Chargement donnees recentes")

    db_url = os.getenv("DATABASE_URL")
    engine = create_engine(db_url)

    try:
        with engine.connect() as conn:
            query_drift = f"""
                SELECT {WEATHER_COLS_SQL}
                FROM prediction_logs
                WHERE timestamp >= NOW() - INTERVAL '7 days'
            """
            df_drift = pd.read_sql(text(query_drift), conn)

            query_perf = """
                SELECT p."prediction_P50", g.actual_delay
                FROM prediction_logs p
                JOIN ground_truth g ON p.id = g.prediction_log_id
                WHERE g.created_at >= NOW() - INTERVAL '30 days'
            """
            df_perf = pd.read_sql(text(query_perf), conn)

        drift_path = "/tmp/current_drift_features.parquet"
        perf_path = "/tmp/current_performance.parquet"

        df_drift.to_parquet(drift_path)
        df_perf.to_parquet(perf_path)

        logger.info(
            f"Donnees recentes : {len(df_drift)} features drift, {len(df_perf)} performance"
        )
        return {
            "drift_path": drift_path,
            "perf_path": perf_path,
            "drift_count": len(df_drift),
            "perf_count": len(df_perf),
        }

    except Exception as e:
        logger.error(f"Echec chargement donnees recentes : {e}")
        raise


def task_analyze_drift(**context):
    """
    Analyse le drift ET la performance via Evidently.
    1. Data drift : features meteo reference vs production
    2. Performance : prediction_P50 vs actual_delay (MAE, RMSE, R2)
    """
    import requests
    import pandas as pd

    execution_date = context.get('ds', 'unknown')
    ti = context['ti']

    logger.info(f"[{execution_date}] Analyse drift + performance")

    ref_info = ti.xcom_pull(task_ids='load_reference_data')
    current_info = ti.xcom_pull(task_ids='load_current_data')

    if not ref_info or not current_info:
        logger.warning("Donnees manquantes pour l'analyse")
        return {"drift_detected": False, "error": "missing_data"}

    drift_result = {"drift_detected": False, "drift_share": 0}
    perf_result = {}

    # --- 1. Data drift ---
    try:
        df_reference = pd.read_parquet(ref_info['path'])
        df_current = pd.read_parquet(current_info['drift_path'])

        if len(df_current) < 10:
            logger.warning("Pas assez de donnees courantes pour le drift (%d lignes)", len(df_current))
        else:
            common_cols = sorted(set(df_reference.columns) & set(df_current.columns))
            df_reference = df_reference[common_cols]
            df_current = df_current[common_cols]

            resp = requests.post(
                "http://evidently:8001/reference",
                json={"data": df_reference.head(500).to_dict(orient='records')},
                timeout=60,
            )
            if resp.status_code != 200:
                logger.warning("Echec definition reference Evidently : %s", resp.status_code)

            resp = requests.post(
                "http://evidently:8001/drift/report",
                json={"data": df_current.head(500).to_dict(orient='records')},
                timeout=120,
            )
            if resp.status_code == 200:
                r = resp.json()
                drift_result = {
                    "drift_detected": r.get("drift_detected", False),
                    "drift_share": r.get("drift_share", 0),
                    "threshold": DRIFT_THRESHOLD,
                    "drift_report": r.get("report_filename"),
                }
                logger.info(
                    "Drift : detected=%s, share=%.2f%%",
                    drift_result["drift_detected"],
                    drift_result["drift_share"] * 100,
                )
            else:
                logger.error("Erreur Evidently drift/report : %s", resp.status_code)

    except requests.exceptions.ConnectionError:
        logger.warning("Service Evidently non disponible pour le drift")
    except Exception as e:
        logger.error("Erreur analyse drift : %s", e)

    # --- 2. Performance report ---
    try:
        df_perf = pd.read_parquet(current_info['perf_path'])

        if len(df_perf) < 5:
            logger.warning("Pas assez de donnees ground_truth pour le rapport de performance (%d lignes)", len(df_perf))
        else:
            resp = requests.post(
                "http://evidently:8001/performance/report",
                json={
                    "data": df_perf.to_dict(orient='records'),
                    "prediction_column": "prediction_P50",
                    "target_column": "actual_delay",
                },
                timeout=120,
            )
            if resp.status_code == 200:
                perf_result = resp.json()
                logger.info("Rapport performance genere : %s", perf_result.get("report_filename"))
            else:
                logger.error("Erreur Evidently performance/report : %s", resp.status_code)

    except requests.exceptions.ConnectionError:
        logger.warning("Service Evidently non disponible pour la performance")
    except Exception as e:
        logger.error("Erreur analyse performance : %s", e)

    return {
        **drift_result,
        "performance_report": perf_result.get("report_filename"),
    }


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
    """Genere un rapport de monitoring consolide"""
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
            "drift_features": current_info.get('drift_count', 0) if current_info else 0,
            "performance_rows": current_info.get('perf_count', 0) if current_info else 0,
        },
        "thresholds": {
            "drift": DRIFT_THRESHOLD,
            "r2_min": R2_MIN_THRESHOLD,
        },
        "recommendation": "RETRAIN" if drift_result and drift_result.get('drift_detected') else "CONTINUE",
    }

    report_path = f"/tmp/monitoring_report_{execution_date}.json"
    with open(report_path, 'w') as f:
        json.dump(report, f, indent=2, default=str)

    logger.info(f"Rapport genere : {report_path}")
    logger.info(f"Resume : {report['recommendation']}")

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
