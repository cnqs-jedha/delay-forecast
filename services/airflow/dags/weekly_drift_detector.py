"""
DAG Weekly Drift Detector
Schedule : tous les dimanches a 23h00 UTC

1. Synchronise la table ground_truth (predictions vs retards reels)
2. Calcule la MAE recente (7j) et de reference (30j)
3. Envoie une alerte email si le drift depasse le seuil
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os
import smtplib
import psycopg2
import logging
from email.mime.text import MIMEText

sys.path.insert(0, "/opt/project/pipeline")

logger = logging.getLogger(__name__)

DRIFT_THRESHOLD = 1.5  # alerte si MAE recente > 1.5x MAE reference


def sync_ground_truth():
    """Alimente la table ground_truth en joignant prediction_logs et stg_transport_realtime."""
    from prediction_vs_ground_truth import sync_ground_truth_optimized
    sync_ground_truth_optimized()


def compute_drift():
    """
    Calcule le drift en comparant la MAE recente (7j) a la MAE de reference (30j).
    Retourne les metriques via XCom.
    """
    db_url = os.getenv("DATABASE_URL")
    conn = psycopg2.connect(db_url)
    cur = conn.cursor()

    try:
        cur.execute("""
            SELECT AVG(ABS(p."prediction_P50" - g.actual_delay))
            FROM prediction_logs p
            JOIN ground_truth g ON p.id = g.prediction_log_id
            WHERE g.created_at >= NOW() - INTERVAL '7 days'
        """)
        mae_recent = cur.fetchone()[0]

        cur.execute("""
            SELECT AVG(ABS(p."prediction_P50" - g.actual_delay))
            FROM prediction_logs p
            JOIN ground_truth g ON p.id = g.prediction_log_id
            WHERE g.created_at >= NOW() - INTERVAL '30 days'
        """)
        mae_reference = cur.fetchone()[0]

        cur.execute("""
            SELECT COUNT(*) FROM ground_truth
            WHERE created_at >= NOW() - INTERVAL '7 days'
        """)
        sample_count = cur.fetchone()[0]

        if mae_recent is None or mae_reference is None or mae_reference == 0:
            logger.warning("Pas assez de donnees pour calculer le drift")
            return {
                "mae_recent": None,
                "mae_reference": None,
                "drift_ratio": None,
                "sample_count": sample_count,
                "drift_detected": False,
            }

        drift_ratio = mae_recent / mae_reference

        logger.info(f"MAE recente (7j): {mae_recent:.2f}s")
        logger.info(f"MAE reference (30j): {mae_reference:.2f}s")
        logger.info(f"Drift ratio: {drift_ratio:.2f} (seuil: {DRIFT_THRESHOLD})")
        logger.info(f"Echantillons (7j): {sample_count}")

        return {
            "mae_recent": round(mae_recent, 2),
            "mae_reference": round(mae_reference, 2),
            "drift_ratio": round(drift_ratio, 2),
            "sample_count": sample_count,
            "drift_detected": drift_ratio > DRIFT_THRESHOLD,
        }
    finally:
        cur.close()
        conn.close()


def send_alert(**context):
    """Envoie un email d'alerte si un drift est detecte."""
    ti = context["ti"]
    metrics = ti.xcom_pull(task_ids="compute_drift")

    if not metrics:
        logger.info("Aucune metrique disponible, pas d'alerte")
        return

    drift_detected = metrics.get("drift_detected", False)
    mae_recent = metrics.get("mae_recent")
    mae_reference = metrics.get("mae_reference")
    drift_ratio = metrics.get("drift_ratio")
    sample_count = metrics.get("sample_count", 0)

    smtp_user = os.getenv("AIRFLOW_SMTP_USER")
    smtp_password = os.getenv("AIRFLOW_SMTP_PASSWORD")

    if not smtp_user or not smtp_password:
        logger.warning("SMTP non configure, alerte non envoyee")
        return

    if drift_detected:
        subject = "ALERTE DRIFT - Delay Forecast"
        body = (
            f"Drift detecte sur le modele de prediction.\n\n"
            f"MAE recente (7j) : {mae_recent}s\n"
            f"MAE reference (30j) : {mae_reference}s\n"
            f"Drift ratio : {drift_ratio} (seuil: {DRIFT_THRESHOLD})\n"
            f"Echantillons : {sample_count}\n\n"
            f"Action recommandee : re-entrainer le modele."
        )
    else:
        subject = "Drift Report OK - Delay Forecast"
        body = (
            f"Pas de drift detecte.\n\n"
            f"MAE recente (7j) : {mae_recent}s\n"
            f"MAE reference (30j) : {mae_reference}s\n"
            f"Drift ratio : {drift_ratio} (seuil: {DRIFT_THRESHOLD})\n"
            f"Echantillons : {sample_count}"
        )

    msg = MIMEText(body)
    msg["Subject"] = subject
    msg["From"] = smtp_user
    msg["To"] = smtp_user

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(smtp_user, smtp_password)
        server.send_message(msg)

    logger.info(f"Email envoye — drift_detected={drift_detected}, ratio={drift_ratio}")


with DAG(
    dag_id="weekly_drift_detector",
    start_date=datetime(2026, 3, 2),
    schedule="@weekly",
    catchup=False,
    tags=["drift", "monitoring"],
) as dag:

    t_sync = PythonOperator(
        task_id="sync_ground_truth",
        python_callable=sync_ground_truth,
    )

    t_compute = PythonOperator(
        task_id="compute_drift",
        python_callable=compute_drift,
    )

    t_alert = PythonOperator(
        task_id="send_alert",
        python_callable=send_alert,
    )

    t_sync >> t_compute >> t_alert
