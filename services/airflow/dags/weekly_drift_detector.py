from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from dotenv import load_dotenv
import psycopg2
import sys
import os
import smtplib
from email.mime.text import MIMEText


def drift_monitoring():
    AIRFLOW_SMTP_USER = os.getenv("AIRFLOW_SMTP_USER")
    AIRFLOW_SMTP_PASSWORD = os.getenv("AIRFLOW_SMTP_PASSWORD")

    # Récupère la prédiction de la table prediction log et récupère la vrai valeur dans la table transport_real_time 
    # pour l'insérer dans la ground_truth
    load_dotenv()
    DATABASE_URL = os.getenv("DATABASE_URL")

    conn = None
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()

        # matching par Month/DOW/Hour car les dates absolues 
        # ne correspondent pas forcément entre le staging et les logs.
        # Le casting ::text et ::int assure que la jointure ne casse pas.
        sync_query = """
        INSERT INTO ground_truth (prediction_log_id, actual_delay, created_at)
        SELECT DISTINCT ON (p.id)
            p.id, 
            r.departure_delay,
            NOW()
        FROM prediction_logs p
        JOIN stg_transport_realtime r ON 
            p.bus_nbr::text = r.bus_nbr::text 
            AND p.direction_id::text = r.direction_id::text 
            AND p.stop_sequence::int = r.stop_sequence::int
            -- Matching structurel sur le temps
            AND p.month = EXTRACT(MONTH FROM r.timestamp_rounded)
            AND p.day_of_week = EXTRACT(DOW FROM r.timestamp_rounded)
            AND p.hour = EXTRACT(HOUR FROM r.timestamp_rounded)
        WHERE p.bus_nbr = '541'
          AND NOT EXISTS (
              SELECT 1 FROM ground_truth g WHERE g.prediction_log_id = p.id
          )
        ORDER BY p.id, r.timestamp_rounded DESC;
        """

        cur.execute(sync_query)
        rows_inserted = cur.rowcount
        conn.commit()

        print(f"Synchronisation terminée : {rows_inserted} lignes ajoutées à ground_truth.")

        # Check de performance : on calcule le MAE uniquement si de nouvelles données ont été insérées
        if rows_inserted > 0:
            check_mae_weekly_query = """
            SELECT AVG(ABS(p."prediction_P50" - g.actual_delay)) as mae
            FROM prediction_logs p
            JOIN ground_truth g ON p.id = g.prediction_log_id
            WHERE p.bus_nbr = '541';
            """
            cur.execute(check_mae_weekly_query)
            mae_weekly = cur.fetchone()[0]
            print(f"MAE actuelle sur la ligne 541 : {mae_weekly:.2f} secondes")

            check_mae_avg_query = """
            SELECT AVG(ABS(p."prediction_P50")) as mae
            FROM prediction_logs p
            WHERE p.bus_nbr = '541';
            """
            cur.execute(check_mae_avg_query)
            mae_avg = cur.fetchone()[0]
            print(f"MAE moyenne sur la ligne 541 : {mae_avg:.2f} secondes")

            print(mae_weekly/mae_avg)

            # Calcul du drift et envoie d'un mail si drift suppérieur de 30%
            drift_value = mae_weekly/mae_avg
            print(drift_value)
            if drift_value > 1.3:
                # Construire le mail
                msg = MIMEText(f"Drift détecté ! Valeur : {drift_value}")
                msg["Subject"] = "Alerte Drift"
                msg["From"] = AIRFLOW_SMTP_USER
                msg["To"] = "cnsqjedha@gmail.com"

                # Envoyer via SMTP
                with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
                    server.login(AIRFLOW_SMTP_USER, AIRFLOW_SMTP_PASSWORD)
                    server.send_message(msg)

                print(f"Mail envoyé — value={drift_value}")


    except Exception as e:
        print(f" Erreur lors de l'exécution : {e}")
        if conn: conn.rollback()
    finally:
        if conn:
            cur.close()
            conn.close()

    

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
