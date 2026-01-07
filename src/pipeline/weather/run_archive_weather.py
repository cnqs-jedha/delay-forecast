import os
import sys
import logging
import sqlalchemy as sa

# Imports des modules locaux
from pipeline.weather.utils.call_api_meteo import fetch_weather_data
from pipeline.weather.utils.transform_meteo_archives import process_etl_meteo
from pipeline.weather.utils.transform_meteo_previsions import process_etl_previsions
from pipeline.weather.utils.s3_weather import send_to_S3
from pipeline.weather.utils.load_to_neon_weather import load_parquet_to_neon

# Configuration du logging pour le run principal
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logging.getLogger("sqlalchemy.engine.Engine").setLevel(logging.WARNING)
logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
logging.getLogger("sqlalchemy.pool").setLevel(logging.WARNING)
logger = logging.getLogger("RUN_PIPELINE")

def main():
    LAT, LON = 59.3251172, 18.0710935

    logger.info("Lancement du Pipeline Complet : Ingestion -> ETL -> Neon DB")

    # --- ÉTAPE 1 : INGESTION (API -> RAW FILES) ---
    logger.info("--- 1. Ingestion des données (Météo & Transport) ---")

    weather_archive_name = "weather_stockholm_archive"
    # weather_forecast_name = "weather_stockholm_sforecast"
    
    data_weather_archive = fetch_weather_data(LAT, LON, mode="archive")
    # data_weather_forecast = fetch_weather_data(LAT, LON, mode="forecast")

    # Envoyer à S3
    send_to_S3(data_weather_archive, weather_archive_name)
    # send_to_S3(data_weather_forecast, weather_forecast_name)

    # --- ÉTAPE 2 : ETL / TRANSFORMATION (RAW -> PARQUET) ---
    logger.info("--- 2. Transformation des données (ETL) ---")

    data_archive = process_etl_meteo(data_weather_archive)
    # data_forecast = process_etl_meteo(data_weather_forecast)

    # --- ÉTAPE 3 : CHARGEMENT VERS NEON DB ---
    logger.info("--- 3. Chargement des fichiers Processed vers Neon DB ---")
    
    try:
        load_parquet_to_neon("stg_weather_archive", data_archive)
        # load_parquet_to_neon("stg_weather_forecast", data_forecast)
        logger.info("Pipeline terminé avec succès. Les données sont dans Neon DB.")
    except sa.exc.DBAPIError as e:
        # e.orig = message Postgres / psycopg2/pg8000 (le vrai)
        logger.error("DBAPIError (Postgres): %s", e.orig)
    except sa.exc.SQLAlchemyError as e:
        logger.error("SQLAlchemyError: %s", e.__class__.__name__)

    logger.info("\nFin du run.")

if __name__ == "__main__":
    main()