import requests
import os
from pathlib import Path
from dotenv import load_dotenv
import logging

load_dotenv()

KODA_KEY = os.getenv("API_KODA_KEY")
GTFS_RT_KEY = os.getenv("API_GTFS_RT_KEY")

logger = logging.getLogger("API")

def call_koda_api(base_url, date, operator = "sl", endpoint=""):
    if endpoint != "":
        api_url = f"https://api.koda.trafiklab.se/KoDa/api/v2/{base_url}/{operator}/{endpoint}"
    else:
        api_url = f"https://api.koda.trafiklab.se/KoDa/api/v2/{base_url}/{operator}"
        
    params = {
        "date": date, 
        "key": KODA_KEY
    }
    
    request = requests.get(f"{api_url}", params=params, timeout=30)
    
    logger.info(request)
    logger.info(f"{len(request.content)} BYTES")

    return request

def call_koda_history_api(date):
    logger.info("Call history")
    request = call_koda_api("gtfs-rt", date, endpoint="TripUpdates")
    return request

def call_koda_reference_api(date):
    logger.info("Call reference")
    request = call_koda_api("gtfs-static", date)
    return request