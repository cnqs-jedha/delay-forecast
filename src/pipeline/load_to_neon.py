import pandas as pd
import os
import logging
from sqlalchemy import create_engine
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("neon_loader")

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

def load_to_neon():
    # Utilisation de SQLAlchemy qui est plus flexible pour la création automatique de tables
    engine = create_engine(DATABASE_URL)
    
    # CONFIGURATION : On sépare tout - pas le plus recommandé, mais plus simple pour l'instant
    mapping = {
        "stg_transport_archive": "transport_koda_2025-03-04_processed.parquet",
        "stg_transport_realtime": "transport_rt_20251224_1119_processed.parquet",
        "stg_weather_archive": "weather_stockholm_archive_processed.parquet",
        "stg_weather_forecast": "weather_stockholm_forecast_processed.parquet"
    }

    for table_name, file_name in mapping.items():
        path = os.path.join("data", file_name)
        if not os.path.exists(path):
            logger.warning(f"Fichier manquant : {file_name}")
            continue
        
        logger.info(f"Lecture de {file_name}...")
        df = pd.read_parquet(path)
        
        logger.info(f"Injection vers {table_name} ({len(df)} lignes)...")
        
        try:
            # method='multi' et chunksize accélèrent l'insertion pour les gros volumes
            df.to_sql(
                table_name, 
                engine, 
                if_exists="replace", # On écrase et on recrée la structure exacte du Parquet
                index=False, 
                chunksize=10000,
                method='multi' 
            )
            logger.info(f"Table {table_name} créée et remplie avec succès.")
        except Exception as e:
            logger.error(f"Erreur sur {table_name}: {e}")

if __name__ == "__main__":
    load_to_neon()

def load_parquet_to_neon(table_name, data_array) -> None:
    engine = create_engine(DATABASE_URL)

    df = pd.DataFrame(data_array)

    df.to_sql(
        table_name,
        engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=10_000,
    )

    logger.info("OK: %s chargée", table_name)