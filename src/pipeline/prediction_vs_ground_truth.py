# delay_forecast/src/pipeline/prediction_vs_ground_truth.py
from dotenv import load_dotenv
import psycopg2
import os

# Conseil : utilise des variables d'environnement pour plus de sécurité
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

def sync_ground_truth_optimized():
    conn = None
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()

        sync_query = """
        INSERT INTO ground_truth (prediction_log_id, actual_delay, created_at)
        SELECT DISTINCT ON (p.id)
            p.id,
            r.departure_delay,
            NOW()
        FROM prediction_logs p
        JOIN stg_transport_archive r ON
            p.bus_nbr::text = r.bus_nbr::text
            AND p.direction_id::text = r.direction_id::text
            AND p.stop_sequence::int = r.stop_sequence::int
            AND p.month = EXTRACT(MONTH FROM r.timestamp_rounded)
            AND p.day = EXTRACT(DAY FROM r.timestamp_rounded)
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

        # Chexk de performance : on calcule le MAE uniquement si de nouvelles données ont été insérées
        if rows_inserted > 0:
            check_mae_query = """
            SELECT AVG(ABS(p."prediction_P50" - g.actual_delay)) as mae
            FROM prediction_logs p
            JOIN ground_truth g ON p.id = g.prediction_log_id
            WHERE p.bus_nbr = '541';
            """
            cur.execute(check_mae_query)
            mae = cur.fetchone()[0]
            print(f"MAE actuelle sur la ligne 541 : {mae:.2f} secondes")

    except Exception as e:
        print(f" Erreur lors de l'exécution : {e}")
        if conn: conn.rollback()
    finally:
        if conn:
            cur.close()
            conn.close()

if __name__ == "__main__":
    sync_ground_truth_optimized()


