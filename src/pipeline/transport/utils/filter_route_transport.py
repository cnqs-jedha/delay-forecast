import logging
from datetime import datetime, timezone, timedelta
from collections import defaultdict
import random
import gc

from pipeline.transport.utils.collect_data_transport import corr_array_creation, flatten_history_entity_koda

def filter_by_bus_route(bus_nbr, r_routes, r_trips, history_data, bus_per_hour=2):
    """
    :param bus_nbr: Numéro du bus
    :param r_routes: La sortie de `call_koda_reference_api()` de reference_routes
    :param r_trips: La sortie de `call_koda_reference_api()` de reference_trips
    :param history_data: Les données historique filtrer
    :param route_id_selected: Le tableau des ids de routes choisies
    :param trip_bus_chosed: la liste des ids de trips choisis, via les route ids
    :param bus_per_hour: Nombre de bus par heure conservé dans les datas
    """
    final_data = []
    loop_data = []
    route_ids_chosed = []
    trip_bus_chosed = []
    
    routes_bus_chosed = [
            r for r in r_routes
            if str(r.get("route_short_name")) == bus_nbr
        ]

    #Récupère tous les route_id de bus choisis, un ou plusieurs
    for route in routes_bus_chosed:
        route_ids_chosed.append(route['route_id'])

    # Récupère les trips id des routes choisies
    for id in route_ids_chosed:
        current_route = [t for t in r_trips if t["route_id"] == id]
        trip_bus_chosed.extend(current_route)

    REF_TRIPS_CHOOSED_FIELDS = ("route_id", "direction_id")

    #Fait un tableau de corrélation avec trips.txt
    ref_trips_choosed_corr = corr_array_creation(trip_bus_chosed, "trip_id", REF_TRIPS_CHOOSED_FIELDS)
    valid_trip_ids = set(ref_trips_choosed_corr.keys())

    # Filtrer les données pour n'avoir que les trip_id correpondant au routes choisies
    filtered_history = [
        e for e in history_data
        if getattr(e, "trip_update", None)
        and getattr(e.trip_update, "trip", None)
        and e.trip_update.trip.trip_id in valid_trip_ids
    ]

    for e in filtered_history:
        final_data.extend(flatten_history_entity_koda(e, ref_trips_choosed_corr))

    #for row in final_data:
        #row.pop("trip_id", None)

        #### Ce qu'il y a en dessous c'est pour arrondir l'heure @Nadège
        #ts = row.get("timestamp")

        #ts_hour = ((ts + 1800) // 3600) * 3600
        #row["timestamp_hour"] = ts_hour

        #row["datetime_rounded"] = datetime.fromtimestamp(ts_hour, tz=timezone.utc).isoformat()
        #row["hour"] = (row["timestamp_hour"] // 3600) % 24
        #row.pop("timestamp_hour", None)
        #### Ce qu'il y a au-dessus c'est pour arrondir l'heure @Nadège

        # Mettre le numéro du bus pour entrainer le modèle avec plusieurs potentiel num de bus, à rendre dynamique si plusieurs
        #row['bus_nbr'] = bus_number

    counts = defaultdict(int)

    #Pour randomiser et ne pas prendre que les deux première data de la journée
    random.shuffle(final_data)

    for row in final_data:
        # Filtrer pour deux bus par heure et pas plus 
        key = (
            row.get("hour"),
            row.get("direction_id"),
            row.get("stop_sequence"),
        )

        if counts[key] < bus_per_hour:
            loop_data.append(row)
            counts[key] += 1

    return loop_data 
