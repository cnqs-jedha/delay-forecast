import pandas as pd
import gzip
import os
import logging
from google.transit import gtfs_realtime_pb2
from dotenv import load_dotenv

load_dotenv()

# Configuration du logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("gtfs_rt_local")

# Chemins
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_DIR = os.path.join(BASE_DIR, "data")
STATIC_DATA_PATH = os.path.join(DATA_DIR, "sweden_data")

def process_etl_transport_live(file_path, static_data_path):
    """Lit un fichier .pb local et le transforme en DataFrame filtré."""
    
    if not os.path.exists(file_path):
        logger.error(f"Fichier source introuvable : {file_path}")
        return pd.DataFrame()

    # 1. Lecture du contenu binaire
    with open(file_path, "rb") as f:
        raw_content = f.read()

    # 2. Décompression si GZIP
    if raw_content[:2] == b"\x1f\x8b":
        logger.info("Décompression GZIP détectée...")
        raw_content = gzip.decompress(raw_content)

    # 3. Parsing Protobuf
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(raw_content)
    
    rows = []
    for e in feed.entity:
        if not e.HasField("trip_update"):
            continue
        
        tu = e.trip_update
        tr = tu.trip
        
        for stu in tu.stop_time_update:
            rows.append({
                "entity_id": e.id,
                "trip_id": tr.trip_id,
                "route_id": tr.route_id,
                "stop_sequence": stu.stop_sequence,
                "stop_id": stu.stop_id,
                "stop_arrival_delay": stu.arrival.delay if stu.HasField("arrival") else None,
                "timestamp": tu.timestamp if tu.timestamp else None
            })

    df_rt = pd.DataFrame(rows)
    if df_rt.empty:
        logger.warning("Aucune donnée extraite du fichier Protobuf.")
        return df_rt
    
    # 4. Merges statiques
    try:
        routes = pd.read_csv(os.path.join(static_data_path, "routes.txt"), dtype=str)
        
        # SÉCURITÉ : On force tout en string et on nettoie les espaces
        df_rt['route_id'] = df_rt['route_id'].astype(str).str.strip()
        routes['route_id'] = routes['route_id'].astype(str).str.strip()
        
        logger.info(f"Exemples de route_id dans le RT : {df_rt['route_id'].unique()[:5]}")
        logger.info(f"Exemples de route_id dans routes.txt : {routes['route_id'].unique()[:5]}")

        df_merged = df_rt.merge(routes[['route_id', 'route_short_name', 'route_type']], on="route_id", how="left")
        
        # ANALYSE DES TYPES TROUVÉS
        types_trouves = df_merged['route_type'].unique()
        logger.info(f"Types de transport détectés dans le fichier : {types_trouves}")

        # On filtre sur 700, mais si c'est vide, on essaie de comprendre pourquoi
        df_bus_rt = df_merged[df_merged["route_type"].isin(["700", "401"])].copy()
        
        if df_bus_rt.empty:
            logger.warning("Aucun type 700 trouvé. Voici un échantillon des données après merge :")
            logger.warning(df_merged[['route_id', 'route_type']].dropna().head(10))
        logger.info(f"Succès : {len(df_bus_rt)} lignes de bus extraites.")
        return df_bus_rt

    except Exception as e:
        logger.error(f"Erreur lors du merge statique : {e}") 
        return df_rt


if __name__ == "__main__":
    # Nom de ton fichier déjà présent dans /data
    FILENAME = "transport_rt_20251224_1119.pb"
    input_file = os.path.join(DATA_DIR, FILENAME)
    
    # Lancement du traitement
    df_final = process_etl_transport_live(input_file, STATIC_DATA_PATH)

    # Sauvegarde du résultat
    if not df_final.empty:
        output_path = os.path.join(DATA_DIR, FILENAME.replace(".pb", "_processed.parquet"))
        df_final.to_parquet(output_path, index=False)
        logger.info(f"Fichier sauvegardé : {output_path}")