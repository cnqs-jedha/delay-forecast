import pandas as pd
import os
import io
import py7zr
import tempfile
import shutil
import copy
import logging
from pathlib import Path
from google.transit import gtfs_realtime_pb2
from google.protobuf.message import DecodeError
from dotenv import load_dotenv

# Configuration Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger("gtfs")

load_dotenv()

# Configuration des chemins
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_DIR = os.path.join(BASE_DIR, "data")
# On pointe vers data/sweden_data comme tu l'as précisé
STATIC_DATA_DIR = os.path.join(DATA_DIR, "sweden_data") 

def process_etl_transport(filename, batch_size=200):
    input_path = os.path.join(DATA_DIR, filename)
    
    with open(input_path, "rb") as f:
        archive_bytes = io.BytesIO(f.read())

    tmpdir = tempfile.mkdtemp(prefix="koda_")
    tmp = Path(tmpdir)
    
    # CHANGEMENT MAJEUR : On ne stocke plus all_entities
    rows = [] 
    bad_files = []
    feed = gtfs_realtime_pb2.FeedMessage()

    archive_bytes.seek(0)
    with py7zr.SevenZipFile(archive_bytes, mode="r") as z:
        #candidates = [n for n in z.getnames() if n.lower().endswith(".pb")]
        candidates = [n for n in z.getnames() if n.lower().endswith(".pb")][:1000]

    for i in range(0, len(candidates), batch_size):
        batch = candidates[i:i+batch_size]
        logger.info(f"Batch {i + 1} / {len(candidates)}")

        try:
            archive_bytes.seek(0)
            with py7zr.SevenZipFile(archive_bytes, mode="r") as z:
                z.extract(path=tmpdir, targets=batch)
        except Exception:
            continue

        for name in batch:
            p = tmp / name
            try:
                raw = p.read_bytes()
                feed.Clear()
                feed.ParseFromString(raw)
                
                # ON EXTRAIT TOUT DE SUITE LES DONNÉES (Plus besoin de deepcopy)
                for e in feed.entity:
                    if e.HasField("trip_update"):
                        tu = e.trip_update
                        for stu in tu.stop_time_update:
                            rows.append({
                                "entity_id": e.id,
                                "trip_id": tu.trip.trip_id,
                                "route_id": tu.trip.route_id,
                                "stop_sequence": stu.stop_sequence,
                                "stop_id": stu.stop_id,
                                "stop_arrival_delay": stu.arrival.delay if stu.HasField("arrival") else None,
                                "timestamp": tu.timestamp if tu.timestamp else None
                            })
            except Exception as e:
                bad_files.append((name, str(e)))
            finally:
                if p.exists(): p.unlink()

    shutil.rmtree(tmpdir, ignore_errors=True)

    # 4. Création du DataFrame
    logger.info(f"Création du DataFrame avec {len(rows)} lignes...")
    df = pd.DataFrame(rows)
    # On vide la liste rows pour libérer de la mémoire
    rows = []

# 5. Merges Statiques
    logger.info("Merges GTFS Statiques...")
    try:
        trips = pd.read_csv(os.path.join(STATIC_DATA_DIR, "trips.txt"), dtype=str)
        routes = pd.read_csv(os.path.join(STATIC_DATA_DIR, "routes.txt"), dtype=str)
        
        # Sécurité : tout en string
        df['trip_id'] = df['trip_id'].astype(str)
        
        # Merge 1 : Récupérer le route_id depuis trips si manquant
        df = df.merge(trips[['trip_id', 'route_id']], on="trip_id", how="left", suffixes=('', '_static'))
        df['route_id'] = df['route_id'].fillna(df['route_id_static'])
        
        # Merge 2 : Récupérer les infos de la route
        df = df.merge(routes[['route_id', 'route_short_name', 'route_type']], on="route_id", how="left")
        
        # --- LE FIX : DIAGNOSTIC ---
        logger.info(f"Types de transport trouvés : {df['route_type'].unique()}")
        
        # On ne filtre QUE SI on trouve des bus, sinon on garde tout pour ne pas avoir 0 lignes
        if '700' in df['route_type'].values:
            df = df[df["route_type"] == "700"]
            logger.info("Filtre Bus (700) appliqué.")
        else:
            logger.warning("Aucun bus '700' trouvé. On garde toutes les données pour éviter le 0.")
            
    except Exception as e:
        logger.error(f"Erreur lors des merges : {e}")

    # 6. Time formatting pour Neon DB
    if 'timestamp' in df.columns and not df.empty:
        df['timestamp_dt'] = pd.to_datetime(df['timestamp'], unit='s')
        df['timestamp_rounded'] = df['timestamp_dt'].dt.floor('h').dt.tz_localize(None)

    print(df.columns)
    
    # Sauvegarde Parquet
    output_filename = filename.replace(".7z", "_processed.parquet")
    output_path = os.path.join(DATA_DIR, output_filename)
    df.to_parquet(output_path, index=False)
    
    logger.info(f" Terminé ! {len(df)} lignes de bus sauvegardées.")
    return df

if __name__ == "__main__":
    from datetime import datetime, timedelta
    
    # ═══════════════════════════════════════════════════════════════════════
    # MODE MANUEL - Décommenter pour traiter une date spécifique
    # ═══════════════════════════════════════════════════════════════════════
    # process_etl_transport("transport_koda_2024-01-01.7z", batch_size=100)
    
    # ═══════════════════════════════════════════════════════════════════════
    # MODE AUTOMATIQUE - Traite les données de la veille
    # ═══════════════════════════════════════════════════════════════════════
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    filename = f"transport_koda_{yesterday}.7z"
    print(f"Traitement de : {filename}")
    process_etl_transport(filename, batch_size=100)