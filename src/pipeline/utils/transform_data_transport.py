import json
from pathlib import Path
from datetime import datetime, timezone, timedelta

BASE_DIR = Path(__file__).resolve().parents[3]
DATA_DIR = BASE_DIR / "data" / "S3"

#json_path = DATA_DIR / "history_transport_2025-03-15-2025-03-16.json"

def transform_S3_to_neon(file_name):
    json_path = DATA_DIR / file_name
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    KEYS_TO_REMOVE = {"trip_id", "route_id", "start_date", "vehicle_id", "arrival_time", "departure_time"}

    for row in data:
        for key in KEYS_TO_REMOVE:
            row.pop(key, None)

        ts = row.get("timestamp")

        ts_hour = ((ts + 1800) // 3600) * 3600
        row["timestamp_hour"] = ts_hour

        row["timestamp_rounded"] = datetime.fromtimestamp(ts_hour, tz=timezone.utc).isoformat()
        row["hour"] = (row["timestamp_hour"] // 3600) % 24
        row.pop("timestamp_hour", None)
        row.pop("timestamp", None)
        
        row["bus_nbr"] = "541"
    
    return data

