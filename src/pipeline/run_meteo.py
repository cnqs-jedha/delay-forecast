import os
import sys
import logging

# Imports des modules locaux
from src.pipeline.weather.utils.call_api_meteo import fetch_weather_data
from transform_meteo_archives import process_etl_meteo
from transform_meteo_previsions import process_etl_previsions
from load_to_neon import load_to_neon

# Configuration du logging pour le run principal
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("RUN_PIPELINE")

def main():
    from datetime import datetime, timedelta
    
    LAT, LON = 59.3251172, 18.0710935
    
    # ═══════════════════════════════════════════════════════════════════════
    # MODE MANUEL - Décommenter pour initialisation de la base historique
    # ═══════════════════════════════════════════════════════════════════════
    # DATE_START = "2024-01-01"  # Date de début historique souhaitée
    # DATE_END = "2025-12-31"    # Date de fin historique souhaitée
    # DATE_HISTO = "2024-01-01"  # Date spécifique pour transport
    
    # ═══════════════════════════════════════════════════════════════════════
    # MODE AUTOMATIQUE (production) - dernière semaine
    # ═══════════════════════════════════════════════════════════════════════
    today = datetime.now()
    DATE_START = (today - timedelta(days=7)).strftime('%Y-%m-%d')  # J-7
    DATE_END = (today - timedelta(days=1)).strftime('%Y-%m-%d')    # Hier
    DATE_HISTO = DATE_END  # Télécharge les données de la veille
    
    logger.info(f"Période : {DATE_START} → {DATE_END}")
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