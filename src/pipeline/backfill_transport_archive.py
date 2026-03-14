"""
Rattrapage stg_transport_archive.

Telecharge les archives KoDa jour par jour, les transforme
via le pipeline existant, et les charge dans Neon en append.

Usage (dans le conteneur Airflow) :
    python /opt/project/pipeline/backfill_transport_archive.py
    python /opt/project/pipeline/backfill_transport_archive.py --start 2026-02-01 --end 2026-03-07
"""

import argparse
import os
import logging
from datetime import datetime, timedelta

from call_api_transport import fetch_transport_koda
from transform_transport import process_etl_transport
from load_to_neon import load_parquet_to_neon

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("backfill")

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_DIR = os.path.join(BASE_DIR, "data")

TABLE_NAME = "stg_transport_archive"


def backfill(start_date: str, end_date: str, cleanup: bool = True):
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    total_days = (end_dt - start_dt).days + 1

    logger.info("Backfill %s -> %s (%d jours)", start_date, end_date, total_days)

    ok, fail, total_rows = 0, 0, 0
    current = start_dt

    while current <= end_dt:
        date_str = current.strftime("%Y-%m-%d")
        logger.info("--- %s (%d/%d) ---", date_str, ok + fail + 1, total_days)

        try:
            filename = fetch_transport_koda(date_str)
            df = process_etl_transport(filename)

            if df is not None and not df.empty:
                parquet_name = filename.replace(".7z", "_processed.parquet")
                parquet_path = os.path.join(DATA_DIR, parquet_name)
                load_parquet_to_neon(parquet_path, TABLE_NAME, if_exists="append")
                total_rows += len(df)
                logger.info("%s : %d lignes chargees", date_str, len(df))

                if cleanup:
                    for f in [os.path.join(DATA_DIR, filename), parquet_path]:
                        if os.path.exists(f):
                            os.remove(f)
            else:
                logger.warning("%s : aucune donnee apres transformation", date_str)

            ok += 1

        except Exception as exc:
            logger.error("%s : echec - %s", date_str, exc)
            fail += 1

        current += timedelta(days=1)

    logger.info("=== Bilan : %d OK / %d echecs / %d lignes inserees ===", ok, fail, total_rows)


def main():
    default_end = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    default_start = (datetime.now() - timedelta(days=28)).strftime("%Y-%m-%d")

    parser = argparse.ArgumentParser(description="Backfill stg_transport_archive")
    parser.add_argument("--start", default=default_start, help="Date debut (YYYY-MM-DD)")
    parser.add_argument("--end", default=default_end, help="Date fin (YYYY-MM-DD)")
    parser.add_argument("--no-cleanup", action="store_true", help="Garder les fichiers temporaires")
    args = parser.parse_args()

    backfill(args.start, args.end, cleanup=not args.no_cleanup)


if __name__ == "__main__":
    main()
