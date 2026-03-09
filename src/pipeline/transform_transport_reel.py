import pandas as pd
import gzip
import os
import logging
from google.transit import gtfs_realtime_pb2
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("gtfs_rt_local")

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_DIR = os.path.join(BASE_DIR, "data")
STATIC_DATA_PATH = os.path.join(DATA_DIR, "sweden_data")

# POC : filtrer sur la ligne 541 uniquement. Mettre a None pour tout garder.
POC_BUS_FILTER = "541"

FINAL_COLUMNS = [
    "direction_id", "stop_sequence", "arrival_delay",
    "departure_delay", "timestamp_rounded", "hour", "bus_nbr",
]


def process_etl_transport_live(file_path, static_data_path):
    """Lit un fichier .pb temps reel et retourne un DataFrame au schema cible."""

    if not os.path.exists(file_path):
        logger.error(f"Fichier source introuvable : {file_path}")
        return pd.DataFrame()

    with open(file_path, "rb") as f:
        raw_content = f.read()

    if raw_content[:2] == b"\x1f\x8b":
        logger.info("Décompression GZIP détectée...")
        raw_content = gzip.decompress(raw_content)

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
                "trip_id": tr.trip_id,
                "route_id": tr.route_id,
                "stop_sequence": stu.stop_sequence,
                "arrival_delay": stu.arrival.delay if stu.HasField("arrival") else None,
                "departure_delay": stu.departure.delay if stu.HasField("departure") else None,
                "timestamp": tu.timestamp if tu.timestamp else None,
            })

    df = pd.DataFrame(rows)
    if df.empty:
        logger.warning("Aucune donnée extraite du fichier Protobuf.")
        return df

    # --- Merge trips.txt pour direction_id + route_id fiable ---
    trips_path = os.path.join(static_data_path, "trips.txt")
    if not os.path.exists(trips_path):
        logger.error(f"Fichier trips.txt manquant : {trips_path}")
        return pd.DataFrame()

    trips = pd.read_csv(trips_path, dtype=str, usecols=["trip_id", "route_id", "direction_id"])
    df["trip_id"] = df["trip_id"].astype(str).str.strip()
    trips["trip_id"] = trips["trip_id"].astype(str).str.strip()

    df = df.merge(trips[["trip_id", "direction_id", "route_id"]],
                  on="trip_id", how="left", suffixes=("_pb", "_static"))
    df["route_id"] = df["route_id_static"].fillna(df["route_id_pb"])
    df.drop(columns=["route_id_pb", "route_id_static"], inplace=True)

    # --- Merge routes.txt pour route_short_name (filtre bus) ---
    routes_path = os.path.join(static_data_path, "routes.txt")
    if not os.path.exists(routes_path):
        logger.error(f"Fichier routes.txt manquant : {routes_path}")
        return pd.DataFrame()

    routes = pd.read_csv(routes_path, dtype=str, usecols=["route_id", "route_short_name"])
    df["route_id"] = df["route_id"].astype(str).str.strip()
    routes["route_id"] = routes["route_id"].astype(str).str.strip()

    df = df.merge(routes[["route_id", "route_short_name"]], on="route_id", how="left")

    # --- Filtre POC sur le numero de bus ---
    if POC_BUS_FILTER:
        df = df[df["route_short_name"] == POC_BUS_FILTER].copy()
        if df.empty:
            logger.warning(f"Aucune donnée pour la ligne {POC_BUS_FILTER}")
            return df

    df["bus_nbr"] = df["route_short_name"]

    # --- Calculs temporels (conversion UTC -> heure locale Stockholm) ---
    df["timestamp_dt"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
    df["timestamp_dt"] = df["timestamp_dt"].dt.tz_convert("Europe/Stockholm")
    df["timestamp_rounded"] = df["timestamp_dt"].dt.floor("h").dt.tz_localize(None)
    df["hour"] = df["timestamp_rounded"].dt.hour

    # --- Selection finale ---
    for col in FINAL_COLUMNS:
        if col not in df.columns:
            logger.error(f"Colonne manquante apres transformation : {col}")
            return pd.DataFrame()

    df = df[FINAL_COLUMNS].copy()
    logger.info(f"{len(df)} lignes extraites (bus {POC_BUS_FILTER or 'ALL'}).")
    return df


if __name__ == "__main__":
    FILENAME = "transport_rt_20251224_1119.pb"
    input_file = os.path.join(DATA_DIR, FILENAME)

    df_final = process_etl_transport_live(input_file, STATIC_DATA_PATH)

    if not df_final.empty:
        output_path = os.path.join(DATA_DIR, FILENAME.replace(".pb", "_processed.parquet"))
        df_final.to_parquet(output_path, index=False)
        logger.info(f"Fichier sauvegardé : {output_path}")
