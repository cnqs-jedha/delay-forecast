from datetime import datetime, timezone

def transform_S3_to_neon(data, bus_nbr):
    KEYS_TO_REMOVE = {"trip_id", "route_id", "start_date", "vehicle_id", "arrival_time", "departure_time"}

    for row in data:
        for key in KEYS_TO_REMOVE:
            row.pop(key, None)

        ts = row.get("timestamp")

        if not isinstance(ts, int):
            continue

        ts_hour = ((ts + 1800) // 3600) * 3600
        row["timestamp_hour"] = ts_hour

        row["timestamp_rounded"] = datetime.fromtimestamp(ts_hour, tz=timezone.utc).isoformat()
        row["hour"] = (row["timestamp_hour"] // 3600) % 24
        row.pop("timestamp_hour", None)
        row.pop("timestamp", None)
        
        row["bus_nbr"] = bus_nbr
    
    return data

