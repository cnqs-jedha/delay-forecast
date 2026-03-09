"""
Rattrapage stg_weather_archive.

Telecharge la meteo historique depuis Open-Meteo par blocs de 3 mois,
transforme via le pipeline existant, et charge dans Neon en append.

Usage (dans le conteneur Airflow) :
    python /opt/project/pipeline/backfill_weather_archive.py
    python /opt/project/pipeline/backfill_weather_archive.py --start 2024-04-01 --end 2026-03-07
"""

import argparse
import os
import logging
from datetime import datetime, timedelta

from call_api_meteo import fetch_weather_data
from transform_meteo_archives import process_etl_meteo
from load_to_neon import load_parquet_to_neon

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("backfill_weather")

LAT, LON = 59.3251172, 18.0710935
TABLE_NAME = "stg_weather_archive"
CHUNK_DAYS = 90


def backfill(start_date: str, end_date: str):
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    total_days = (end_dt - start_dt).days + 1

    logger.info("Backfill meteo %s -> %s (%d jours, blocs de %d j)", start_date, end_date, total_days, CHUNK_DAYS)

    chunk_start = start_dt
    chunk_num = 0
    total_rows = 0

    while chunk_start <= end_dt:
        chunk_end = min(chunk_start + timedelta(days=CHUNK_DAYS - 1), end_dt)
        chunk_num += 1
        s = chunk_start.strftime("%Y-%m-%d")
        e = chunk_end.strftime("%Y-%m-%d")

        logger.info("--- Bloc %d : %s -> %s ---", chunk_num, s, e)

        try:
            json_filename = f"weather_backfill_{s}_{e}.json"
            json_path = fetch_weather_data(
                LAT, LON,
                filename=json_filename,
                mode="archive",
                start_date=s,
                end_date=e,
            )

            if json_path is None:
                logger.error("Echec telechargement pour %s -> %s", s, e)
                chunk_start = chunk_end + timedelta(days=1)
                continue

            df = process_etl_meteo(json_filename)

            if df is not None and not df.empty:
                parquet_name = json_filename.replace(".json", "_processed.parquet")
                base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
                parquet_path = os.path.join(base_dir, "data", parquet_name)
                load_parquet_to_neon(parquet_path, TABLE_NAME, if_exists="append")
                total_rows += len(df)
                logger.info("Bloc %d : %d lignes chargees", chunk_num, len(df))

                for f in [json_path, parquet_path]:
                    if os.path.exists(f):
                        os.remove(f)
            else:
                logger.warning("Bloc %d : aucune donnee apres transformation", chunk_num)

        except Exception as exc:
            logger.error("Bloc %d echec : %s", chunk_num, exc)

        chunk_start = chunk_end + timedelta(days=1)

    logger.info("=== Bilan : %d blocs, %d lignes inserees ===", chunk_num, total_rows)


def main():
    default_start = "2024-04-01"
    default_end = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    parser = argparse.ArgumentParser(description="Backfill stg_weather_archive")
    parser.add_argument("--start", default=default_start, help="Date debut (YYYY-MM-DD)")
    parser.add_argument("--end", default=default_end, help="Date fin (YYYY-MM-DD)")
    args = parser.parse_args()

    backfill(args.start, args.end)


if __name__ == "__main__":
    main()
