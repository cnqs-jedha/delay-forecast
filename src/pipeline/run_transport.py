import logging
from datetime import datetime, timezone, timedelta
from collections import defaultdict
import random
import gc
import json
import pandas as pd
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

from utils.call_api_transport import call_koda_history_api, call_koda_reference_api
from utils.read_data_transport import read_koda_history_day_stream, read_koda_reference_data
from utils.collect_data_transport import corr_array_creation, flatten_history_entity_koda
from utils.filter_route_transport import filter_by_bus_route
from utils.transform_data_transport import transform_S3_to_neon
from load_to_neon import load_parquet_to_neon

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("RUN_TRANSPORT")

DATE_BEGIN = "2025-01-13"
DATE_END = "2025-01-19"

start = datetime.strptime(DATE_BEGIN, "%Y-%m-%d")
end = datetime.strptime(DATE_END, "%Y-%m-%d")

datas = []
bad_days = []

current = start

logger.info("RUN Transport")
while current <= end:
    #Choix du numéro de bus
    bus_number = "541"
    max_bus_per_hour = 2

    current_string = current.strftime("%Y-%m-%d")

    # New day
    logger.info(current_string)
    try:
        r_history = call_koda_history_api(current_string)
        r_reference = call_koda_reference_api(current_string)

        # Récupération des données
        # Lit les données d'historique par batch
        history_entities, bad_files = read_koda_history_day_stream(r_history, 500)

        # Lit les données de références de route et trips
        reference_routes = read_koda_reference_data(r_reference, "routes")
        reference_trips = read_koda_reference_data(r_reference, "trips")

        # Filtrer les données par ligne de bus
        filtered_data = filter_by_bus_route(bus_number, reference_routes, reference_trips, history_entities, max_bus_per_hour)
        datas.extend(filtered_data)

        if bad_files:
            logger.warning(
                "⚠️ %s — fichiers ignorés: %d",
                current_string,
                len(bad_files),
            )

    except Exception as e:
        logger.exception("❌ Jour %s ignoré", current_string)
        bad_days.append((current_string, repr(e)))

    finally:
        # Nettoie pour vider la RAM
        del r_history, r_reference
        del history_entities, bad_files
        del reference_routes, reference_trips
        del filtered_data

        gc.collect()
        # Ajoute un jour
        current += timedelta(days=1)

logger.info(datas[:20])

################
##ENVOYER A S3## TODO
################
#Regarder pour faire un fichier par semaine (ceci est l'envoi à S3)
with open(f"data/S3/history_transport_{DATE_BEGIN}-{DATE_END}.json", "w", encoding="utf-8") as f:
    json.dump(datas, f, ensure_ascii=False, indent=2)

logger.info("c'est enregistré")

# VERIFIER QU4IL ENVOIE TOUTES LES DONNEES EN DB !!!!
# Transform data to database
json_name = f"history_transport_{DATE_BEGIN}-{DATE_END}.json"
datas_S3 = transform_S3_to_neon(json_name)

logger.info(datas_S3[:20])

#LOAD TO NEON
logger = logging.getLogger("NEON LOADER")

load_parquet_to_neon("stg_transport_archive", datas_S3)

logger.warning("Jours ignorés: %s", bad_days)