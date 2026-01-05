import pandas as pd
import os
import logging
import uuid
from sqlalchemy import create_engine, text, types  # Ajout de types ici
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("neon_loader")

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

# Gestion des chemins
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_DIR = os.path.join(BASE_DIR, "data")

def load_to_neon():
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL manquante (env var).")
        
    engine = create_engine(DATABASE_URL)
    
    mapping = {
        "stg_weather_archive": "weather_stockholm_archive_processed.parquet",
        "stg_weather_forecast": "weather_stockholm_forecast_processed.parquet"
    }

    for table_name, file_name in mapping.items():
        path = os.path.join(DATA_DIR, file_name)
        
        if not os.path.exists(path):
            logger.warning(f"Fichier manquant à l'adresse : {path}")
            continue
        
        logger.info(f"Lecture de {file_name}...")
        df = pd.read_parquet(path)

        # 1. Vérification/Ajout de l'UUID si absent du Parquet
        if 'observation_uuid' not in df.columns:
            logger.info(f"Génération des UUIDs pour {table_name}...")
            df['observation_uuid'] = [str(uuid.uuid4()) for _ in range(len(df))]

        # 2. On met l'UUID en première colonne
        cols = ['observation_uuid'] + [c for c in df.columns if c != 'observation_uuid']
        df = df[cols]
        
        logger.info(f"Injection vers {table_name} ({len(df)} lignes) avec PK...")
        
        try:
            # On utilise engine.begin() pour que l'insertion et la PK soient une seule transaction
            with engine.begin() as connection:
                df.to_sql(
                    table_name, 
                    connection, 
                    if_exists="replace", 
                    index=False, 
                    chunksize=10000,
                    method='multi',
                    dtype={"observation_uuid": types.VARCHAR(36)}
                )
                
                # Ajout de la Primary Key
                connection.execute(text(f"ALTER TABLE {table_name} ADD PRIMARY KEY (observation_uuid);"))
                
            logger.info(f"Table {table_name} créée (PK ok).")
        except Exception as e:
            logger.error(f"Erreur sur {table_name}: {e}")

if __name__ == "__main__":
    load_to_neon()