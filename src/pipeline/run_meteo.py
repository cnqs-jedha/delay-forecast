import os
import sys
import logging

# Imports des modules locaux
from call_api_meteo import fetch_weather_data
from transform_meteo_archives import process_etl_meteo
from transform_meteo_previsions import process_etl_previsions
from load_to_neon import load_to_neon

# Configuration du logging pour le run principal
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("RUN_PIPELINE")

def main():
    LAT, LON = 59.3251172, 18.0710935

    logger.info("Lancement du Pipeline Complet : Ingestion -> ETL -> Neon DB")

    # --- ÉTAPE 1 : INGESTION (API -> RAW FILES) ---
    logger.info("--- 1. Ingestion des données (Météo & Transport) ---")
    
    path_weather_archive = fetch_weather_data(LAT, LON, mode="archive", filename="weather_stockholm_archive.json")
    path_weather_forecast = fetch_weather_data(LAT, LON, mode="forecast", filename="weather_stockholm_forecast.json")
    
    # --- ETAPE 2 : UPLOAD TO S3 OU DRIVE 


    # --- ÉTAPE 2 : ETL / TRANSFORMATION (RAW -> PARQUET) ---
    logger.info("--- 2. Transformation des données (ETL) ---")

    # Météo
    if path_weather_archive:
        process_etl_meteo("weather_stockholm_archive.json")
    if path_weather_forecast:
        process_etl_previsions("weather_stockholm_forecast.json")

if __name__ == "__main__":
    main()