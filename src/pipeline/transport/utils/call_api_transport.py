import requests
import os
from pathlib import Path
from dotenv import load_dotenv
import logging

load_dotenv()

KODA_KEY = os.getenv("API_KODA_KEY")
GTFS_RT_KEY = os.getenv("API_GTFS_RT_KEY")
GTFS_REGIONAL_STATIC_KEY = os.getenv("GTFS_REGIONAL_STATIC_KEY")

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



#################
### REAL TIME ###
#################
def call_rt_history_api(operator = "sl"):
        # api_url = f"https://opendata.samtrafiken.se/gtfs-rt-sweden/{operator}/{endpoint}.pb"
    api_url = f"https://opendata.samtrafiken.se/gtfs-rt/{operator}/TripUpdates.pb"

    params = {
        "key": GTFS_RT_KEY
    }

    request = requests.get(api_url, params=params, timeout=60)
    request.raise_for_status()
    
    logger.info(request)
    logger.info(f"{len(request.content)} BYTES")

    return request


def call_rt_reference_api(operator = "sl"):
    api_url = f"https://opendata.samtrafiken.se/gtfs/{operator}/{operator}.zip"
        
    params = {
        "key": GTFS_REGIONAL_STATIC_KEY
    }

    request = requests.get(api_url, params=params, timeout=240)
    request.raise_for_status()
        
    logger.info(request)
    logger.info(f"{len(request.content)} BYTES")

    return request





    # "#def realtime_now_with_route_short_name(operator: str) -> pd.DataFrame:\n",
    # "    # 1) GTFS-RT live (souvent gzip/brut)\n",
    # "    #url_rt = f\"https://opendata.samtrafiken.se/gtfs-rt/{operator}/TripUpdates.pb\"\n",
    # "    #r_rt = requests.get(url_rt, params={\"key\": GTFS_RT_KEY}, timeout=60)\n",
    # "    #r_rt.raise_for_status()\n",
    # "\n",
    # "    #feed = parse_gtfs_rt_any(r_rt.content)\n",
    # "\n",
    # "    # 2) Static courant (ZIP)\n",
    # "    #url_static = f\"https://opendata.samtrafiken.se/gtfs/{operator}/{operator}.zip\"\n",
    # "   # r_static = requests.get(url_static, params={\"key\": GTFS_REGIONAL_STATIC_KEY}, timeout=240)\n",
    # "   # r_static.raise_for_status()\n",
    # "\n",
    # "   ## trip_map = build_trip_to_route_short_name_from_static_zip(r_static.content)\n",
    # "\n",
    # "    # 3) DataFrame enrichi\n",
    # "   # df = feed_tripupdates_to_df(feed, trip_map)\n",
    # "   # df[\"source\"] = \"realtime\"\n",
    # "   # df[\"day\"] = str(dt_date.today())\n",
    # "   # return df"












    # "api_GTS_RT_KEY = os.getenv(\"API_GTFS_RT_KEY\")\n",
    # "api_koda_key = os.getenv(\"API_KODA_KEY\")\n",
    # "api_RT_national = os.getenv(\"API_GTFS_STATIC\")\n",
    # "\n",
    # "host_api = \"https://opendata.samtrafiken.se/\"\n",
    # "base_path = \"/gtfs-rt-sweden\"\n",
    # "location_path = \"/sl\"\n",
    # "type_path = \"/TripUpdatesSweden.pb\"\n",
    # "api_url = host_api + base_path + location_path + type_path\n",
    # "\n",
    # "params = {\n",
    # "    \"key\": api_GTS_RT_KEY\n",
    # "}"

    #   "api_url_full = f\"https://opendata.samtrafiken.se/gtfs-rt/sl/TripUpdates.pb?&key={api_RT_national}\"\n",
    # "r = requests.get(f\"{api_url_full}\", timeout=20)\n",
    # "\n",
    # "print(r)"

    # "url = \"https://opendata.samtrafiken.se/gtfs-rt-sweden/sl/TripUpdatesSweden.pb\"\n",
    # "params = {\"key\": api_RT_national}  # ta clé Trafiklab\n",
    # "headers = {\"User-Agent\": \"gtfs-client/1.0\"}\n",
    # "\n",
    # "r = requests.get(url, params=params, headers=headers, timeout=30)\n",
    # "print(r.status_code, r.headers.get(\"content-type\"))\n",
    # "\n",
    # "if r.ok:\n",
    # "    with open(\"sweden_gtfs.zip\", \"wb\") as f:\n",
    # "        f.write(r.content)"


#         "import requests\n",
#     "from google.transit import gtfs_realtime_pb2\n",
#     "\n",
#     "operator = \"sl\"\n",
#     "apikey = api_RT_national\n",
#     "\n",
#     "url = f\"https://opendata.samtrafiken.se/gtfs-rt-sweden/{operator}/TripUpdatesSweden.pb\"\n",
#     "headers = {\n",
#     "    \"User-Agent\": \"gtfsrt-client/1.0\",\n",
#     "    \"Accept\": \"application/x-protobuf\",\n",
#     "    \"Accept-Encoding\": \"gzip\",\n",
#     "}\n",
#     "\n",
#     "r = requests.get(url, params={\"key\": apikey}, headers=headers, timeout=30)\n",
#     "r.raise_for_status()\n",
#     "\n",
#     "data = r.content  # déjà utilisable\n",
#     "\n",
#     "feed = gtfs_realtime_pb2.FeedMessage()\n",
#     "feed.ParseFromString(data)\n",
#     "\n",
#     "print(feed.header)\n",
#     "print(\"entities:\", len(feed.entity))\n"
#    ]

