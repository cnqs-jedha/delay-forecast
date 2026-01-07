# Crée une tableu de correspondance basé sur une colonne
def corr_array_creation(reference_data, id_key, ref_fields:tuple):
    """
    Création d'un tableau de correspondance pour gagner du temps au merge des dict().
    `reference_data`: ,
    `id_key`: nom de la column à mettre en avant,
    `ref_fields`: liste des colonnes à prendre. ('nom_1', "nom_2",...)
    """
    ref = {}
    for r in reference_data:
        tid = r.get(id_key)
        if not tid:
            continue

        ref[tid] = {k: r.get(k) for k in ref_fields}
    
    return ref

#Choisi la structure de sortie
def flatten_history_entity_koda(history_items, trips_corr):
    # si pas de trip_update -> rien
    if not history_items.HasField("trip_update"):
        return

    tu = history_items.trip_update
    trip = tu.trip
    tid = trip.trip_id

    # MERGE + FILTRE ici
    corr = trips_corr.get(tid)
    if corr is None:
        return

    route_id = corr.get("route_id")
    direction_id = corr.get("direction_id")

    start_date = trip.start_date if trip.HasField("start_date") else None
    feed_ts = tu.timestamp if tu.HasField("timestamp") else None
    vehicle_id = tu.vehicle.id if tu.HasField("vehicle") else None

    for stu in tu.stop_time_update:
        yield {
            "timestamp": feed_ts,
            "trip_id": tid,
            "route_id": route_id,
            "direction_id": direction_id,
            "start_date": start_date,
            "vehicle_id": vehicle_id,
            "stop_sequence": stu.stop_sequence,
            "arrival_time": stu.arrival.time if stu.HasField("arrival") else None,
            "departure_time": stu.departure.time if stu.HasField("departure") else None,
            "arrival_delay": stu.arrival.delay if stu.HasField("arrival") else None,
            "departure_delay": stu.departure.delay if stu.HasField("departure") else None,
        }
