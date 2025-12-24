import os
import sys
import logging

# Imports des modules locaux
from call_api_meteo import fetch_weather_data
from src.pipeline.transform_meteo_archives import process_etl_meteo
from src.pipeline.transform_meteo_previsions import process_etl_previsions
from src.pipeline.call_api_transport import fetch_transport_koda, fetch_transport_realtime
from src.pipeline.transform_transport import process_etl_transport
from src.pipeline.transform_transport_reel import process_etl_transport_live
from src.pipeline.load_to_neon import load_to_neon

# Configuration du logging pour le run principal
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("RUN_PIPELINE")

def main():
    LAT, LON = 59.3251172, 18.0710935
    DATE_HISTO = "2025-03-04"
    
    logger.info("Lancement du Pipeline Complet : Ingestion -> ETL -> Neon DB")

    # --- ÉTAPE 1 : INGESTION (API -> RAW FILES) ---
    logger.info("--- 1. Ingestion des données (Météo & Transport) ---")
    
    path_weather_archive = fetch_weather_data(LAT, LON, mode="archive", filename="weather_stockholm_archive.json")
    path_weather_forecast = fetch_weather_data(LAT, LON, mode="forecast", filename="weather_stockholm_forecast.json")
    
    path_transport_archive = fetch_transport_koda(DATE_HISTO)
    path_transport_rt = fetch_transport_realtime()

    # --- ÉTAPE 2 : ETL / TRANSFORMATION (RAW -> PARQUET) ---
    logger.info("--- 2. Transformation des données (ETL) ---")

    # Météo
    if path_weather_archive:
        process_etl_meteo("weather_stockholm_archive.json")
    if path_weather_forecast:
        process_etl_previsions("weather_stockholm_forecast.json")

    # Transport Historique (Attention à la RAM - Utilise la version optimisée)
    if path_transport_archive:
        process_etl_transport(f"transport_koda_{DATE_HISTO}.7z")

    # Transport Temps Réel
    if path_transport_rt:
        # On passe le chemin du fichier .pb généré par fetch_transport_realtime
        process_etl_transport_live(path_transport_rt, os.path.join("data", "sweden_data"))

    # --- ÉTAPE 3 : CHARGEMENT VERS NEON DB ---
    logger.info("--- 3. Chargement des fichiers Processed vers Neon DB ---")
    
    try:
        load_to_neon()
        logger.info("Pipeline terminé avec succès. Les données sont dans Neon DB.")
    except Exception as e:
        logger.error(f"Erreur lors du chargement Neon : {e}")

    logger.info("\nFin du run.")

if __name__ == "__main__":
    main()