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

from pipeline.transport.utils.call_api_transport import call_koda_history_api, call_koda_reference_api
from pipeline.transport.utils.read_data_transport import read_koda_history_day_stream, read_koda_reference_data
from pipeline.transport.utils.collect_data_transport import corr_array_creation, flatten_history_entity_koda
from pipeline.transport.utils.filter_route_transport import filter_by_bus_route
from pipeline.transport.utils.transform_data_transport import transform_S3_to_neon
from pipeline.transport.utils.s3_transport import send_to_S3
from pipeline.transport.utils.load_to_neon_transport import load_parquet_to_neon


def log_archive_sniff(logger, resp, label="reference"):
    data = resp.content
    logger.info(
        "%s sniff: status=%s content-type=%s dispo=%s len=%d sig2=%r sig6=%r",
        label,
        getattr(resp, "status_code", None),
        resp.headers.get("Content-Type", ""),
        resp.headers.get("Content-Disposition", ""),
        len(data),
        data[:2],
        data[:6],
    )

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("RUN_TRANSPORT")

DATE_BEGIN = "2025-01-06"
DATE_END = "2025-01-06"

start = datetime.strptime(DATE_BEGIN, "%Y-%m-%d")
end = datetime.strptime(DATE_END, "%Y-%m-%d")

datas = []
bad_days = []

current = start

logger.info("RUN Transport")
while current <= end:
    #Choix du numéro de bus
    bus_number = "1"
    max_bus_per_hour = 3

    current_string = current.strftime("%Y-%m-%d")

    r_history = r_reference = None
    history_entities = bad_files = None
    reference_routes = reference_trips = None
    filtered_data = None

    # New day
    logger.info(current_string)

    # try:
    #     r_history = call_koda_history_api(current_string)
    #     log_archive_sniff(logger, r_history, label=f"history {current_string}")

    #     #     # Récupération des données
    #     # Lit les données d'historique par batch
    #     history_entities, bad_files = read_koda_history_day_stream(r_history, 500)
    #     # logger.info(history_entities[:20])
    #     history_entities_array = list(history_entities)
    #     print(history_entities_array[:1])
    # except Exception as e:
    #     logger.exception("❌ HISTORY cassé %s", current_string)
    #     bad_days.append((current_string, "history", repr(e)))
    #     current += timedelta(days=1)
    #     continue

    # try:
    #     r_reference = call_koda_reference_api(current_string)

    #     log_archive_sniff(logger, r_reference, label=f"reference {current_string}")
    #     reference_routes = read_koda_reference_data(r_reference, "routes")
    #     reference_trips = read_koda_reference_data(r_reference, "trips")
    #     # if reference_routes is None or reference_trips is None:
    #     #     raise RuntimeError("read_koda_reference_data a retourné None (bug lecture reference)")
    #     logger.info("reference_routes type=%s len=%s", type(reference_routes), None if reference_routes is None else len(reference_routes))
    #     logger.info("reference_trips  type=%s len=%s", type(reference_trips),  None if reference_trips  is None else len(reference_trips))


    #     # Filtrer les données par ligne de bus
    #     try:
    #         filtered_data = filter_by_bus_route(bus_number, reference_routes, reference_trips, history_entities, max_bus_per_hour)
    #         print(filtered_data[:2])
    #         datas.extend(filtered_data)
    #     except Exception as e:
    #         logger.exception("❌ FILTRED DATA cassé %s", current_string)
    #         bad_days.append((current_string, "reference", repr(e)))
    #         current += timedelta(days=1)
    #         continue
    # except Exception as e:
    #     logger.exception("❌ REFERENCE cassé %s", current_string)
    #     bad_days.append((current_string, "reference", repr(e)))
    #     current += timedelta(days=1)
    #     continue

    try:
        try:
            r_history = call_koda_history_api(current_string)
        except Exception as e:
            logger.exception("❌ HISTORY CALL cassé %s", current_string)
            bad_days.append((current_string, "reference", repr(e)))
        
        try:
            r_reference = call_koda_reference_api(current_string)
        except Exception as e:
            logger.exception("❌ REFERENCE CALL cassé %s", current_string)
            bad_days.append((current_string, "reference", repr(e)))


        # Récupération des données
        # Lit les données d'historique par batch
        try:
            history_entities, bad_files = read_koda_history_day_stream(r_history, 500)
        except Exception as e:
            logger.exception("❌ NO READING DATA %s", current_string)
            bad_days.append((current_string, "reference", repr(e)))

        # Lit les données de références de route et trips
        try:
            reference_routes = read_koda_reference_data(r_reference, "routes")
            reference_trips = read_koda_reference_data(r_reference, "trips")
        except Exception as e:
            logger.exception("❌ REFERENCE route and trips cassé %s", current_string)
            bad_days.append((current_string, "reference", repr(e)))

        # Filtrer les données par ligne de bus
        try:
            filtered_data = filter_by_bus_route(bus_number, reference_routes, reference_trips, history_entities, max_bus_per_hour)
        except Exception as e:
            logger.exception("❌ FILTRED DATA cassé %s", current_string)
            bad_days.append((current_string, "reference", repr(e)))

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

logger.info(datas[:1])

file_name = f"history_transport_{DATE_BEGIN}-{DATE_END}"
# send_to_S3(datas, file_name)

# logger.info("c'est envoyé")

# # Transform data to database
# datas_S3 = transform_S3_to_neon(datas)

# logger.info(datas_S3[:20])

# # #LOAD TO NEON
# logger = logging.getLogger("NEON LOADER")

# load_parquet_to_neon("stg_transport_archive", datas_S3)

# logger.warning("Jours ignorés: %s", bad_days)