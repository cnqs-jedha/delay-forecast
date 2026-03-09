import pandas as pd
import os
import io
import py7zr
import tempfile
import shutil
import logging
from pathlib import Path
from google.transit import gtfs_realtime_pb2
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger("gtfs")

load_dotenv()

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA_DIR = os.path.join(BASE_DIR, "data")
STATIC_DATA_DIR = os.path.join(DATA_DIR, "sweden_data")

# POC : filtrer sur la ligne 541 uniquement. Mettre a None pour tout garder.
POC_BUS_FILTER = "541"

FINAL_COLUMNS = [
    "direction_id", "stop_sequence", "arrival_delay",
    "departure_delay", "timestamp_rounded", "hour", "bus_nbr",
]


def _build_trip_filter(static_data_dir, bus_filter):
    """Pre-charge le mapping trip_id -> (direction_id, bus_nbr) pour filtrage rapide."""
    trips = pd.read_csv(os.path.join(static_data_dir, "trips.txt"), dtype=str,
                         usecols=["trip_id", "route_id", "direction_id"])
    routes = pd.read_csv(os.path.join(static_data_dir, "routes.txt"), dtype=str,
                          usecols=["route_id", "route_short_name"])

    merged = trips.merge(routes, on="route_id", how="left")
    if bus_filter:
        merged = merged[merged["route_short_name"] == bus_filter]

    lookup = {}
    for _, r in merged.iterrows():
        tid = str(r["trip_id"]).strip()
        lookup[tid] = {
            "direction_id": r["direction_id"],
            "bus_nbr": r["route_short_name"],
        }
    return lookup


def process_etl_transport(filename, batch_size=200):
    """Extrait une archive .7z KoDa et retourne un DataFrame au schema cible."""

    input_path = os.path.join(DATA_DIR, filename)

    # Pre-charger le filtre trip_id pour ne garder que le bus cible en memoire
    trip_lookup = _build_trip_filter(STATIC_DATA_DIR, POC_BUS_FILTER)
    logger.info(f"{len(trip_lookup)} trip_ids pour le bus {POC_BUS_FILTER or 'ALL'}")

    with open(input_path, "rb") as f:
        archive_bytes = io.BytesIO(f.read())

    tmpdir = tempfile.mkdtemp(prefix="koda_")
    tmp = Path(tmpdir)

    rows = []
    bad_files = []
    feed = gtfs_realtime_pb2.FeedMessage()

    archive_bytes.seek(0)
    with py7zr.SevenZipFile(archive_bytes, mode="r") as z:
        all_pb = [n for n in z.getnames() if n.lower().endswith(".pb")]
    # ~3 fichiers/heure × 24h = ~72 fichiers : couverture horaire complete, leger en RAM
    MAX_PB = 72
    step = max(1, len(all_pb) // MAX_PB)
    candidates = all_pb[::step]
    logger.info(f"{len(all_pb)} fichiers .pb, {len(candidates)} retenus (step={step})")

    for i in range(0, len(candidates), batch_size):
        batch = candidates[i:i + batch_size]
        if i % 1000 == 0:
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

                for e in feed.entity:
                    if e.HasField("trip_update"):
                        tu = e.trip_update
                        tid = str(tu.trip.trip_id).strip()
                        info = trip_lookup.get(tid)
                        if info is None:
                            continue
                        ts = tu.timestamp if tu.timestamp else None
                        for stu in tu.stop_time_update:
                            rows.append({
                                "direction_id": info["direction_id"],
                                "bus_nbr": info["bus_nbr"],
                                "stop_sequence": stu.stop_sequence,
                                "arrival_delay": stu.arrival.delay if stu.HasField("arrival") else None,
                                "departure_delay": stu.departure.delay if stu.HasField("departure") else None,
                                "timestamp": ts,
                            })
            except Exception as exc:
                bad_files.append((name, str(exc)))
            finally:
                if p.exists():
                    p.unlink()

    shutil.rmtree(tmpdir, ignore_errors=True)

    logger.info(f"Création du DataFrame avec {len(rows)} lignes...")
    df = pd.DataFrame(rows)
    rows = []

    if df.empty:
        logger.warning("Aucune donnée extraite de l'archive.")
        return df

    # --- Calculs temporels (conversion UTC -> heure locale Stockholm) ---
    df["timestamp_dt"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
    df["timestamp_dt"] = df["timestamp_dt"].dt.tz_convert("Europe/Stockholm")
    df["timestamp_rounded"] = df["timestamp_dt"].dt.floor("h").dt.tz_localize(None)
    df["hour"] = df["timestamp_rounded"].dt.hour

    # --- Deduplication : 1 ligne par (direction, stop, heure arrondie) ---
    group_cols = ["direction_id", "stop_sequence", "timestamp_rounded", "hour", "bus_nbr"]
    df = df.groupby(group_cols, as_index=False).agg(
        arrival_delay=("arrival_delay", "median"),
        departure_delay=("departure_delay", "median"),
    )
    logger.info(f"{len(df)} lignes apres deduplication")

    # --- Selection finale ---
    for col in FINAL_COLUMNS:
        if col not in df.columns:
            logger.error(f"Colonne manquante apres transformation : {col}")
            return pd.DataFrame()

    df = df[FINAL_COLUMNS].copy()

    output_filename = filename.replace(".7z", "_processed.parquet")
    output_path = os.path.join(DATA_DIR, output_filename)
    df.to_parquet(output_path, index=False)

    logger.info(f"{len(df)} lignes extraites (bus {POC_BUS_FILTER or 'ALL'}).")
    return df


if __name__ == "__main__":
    from datetime import datetime, timedelta

    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    filename = f"transport_koda_{yesterday}.7z"
    print(f"Traitement de : {filename}")
    process_etl_transport(filename, batch_size=100)
