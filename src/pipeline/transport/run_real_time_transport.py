import logging
from google.transit import gtfs_realtime_pb2
from datetime import datetime
from datetime import date

from pipeline.transport.utils.call_api_transport import call_rt_history_api, call_rt_reference_api
from pipeline.transport.utils.read_data_transport import read_koda_reference_data
from pipeline.transport.utils.filter_route_transport import filter_by_bus_route
from pipeline.transport.utils.transform_data_transport import transform_S3_to_neon
from pipeline.transport.utils.s3_transport import send_to_S3
from pipeline.transport.utils.load_to_neon_transport import load_parquet_to_neon

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("RUN_TRANSPORT")

bus_number = "541"
max_bus_per_hour = 3

# Realtime list
try:
    r_history = call_rt_history_api()
except Exception as e:
    logger.exception("❌ REALTIME API PROBLEME %s", e)

try:
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(r_history.content)
    history_entities = list(feed.entity)
except Exception as e:
    logger.exception("❌ REALTIME READING DATA PROBLEME %s", e)

# Reference
try:
    r_reference = call_rt_reference_api()
except Exception as e:
    logger.exception("❌ REFERENCE API PROBLEME %s", e)

try:
    reference_routes = read_koda_reference_data(r_reference, "routes")
    reference_trips = read_koda_reference_data(r_reference, "trips")
except Exception as e:
    logger.exception("❌ REFERENCE READING DATA PROBLEME %s", e)

try:
    filtered_data = filter_by_bus_route(bus_number, reference_routes, reference_trips, history_entities, max_bus_per_hour)
except Exception as e:
    logger.exception("❌ DATA NO FILTRED %s", e)

logger.info(filtered_data[:1])
logger.info("history entities len-> %s",len(history_entities))
logger.info("final data len -> %s", len(filtered_data))


today = date.today().strftime("%Y%m%d")
hour_str = datetime.now().strftime("%H-%M-%S")
file_name = f"realtime_transport_{today}_{hour_str}"

try:
    send_to_S3(filtered_data, "file_name")
except Exception as e:
    logger.exception("❌ NOT SENDED TO S3 %s", e)

print(file_name)

try:
    data_transformed = transform_S3_to_neon(filtered_data, bus_number)
except Exception as e:
    logger.exception("❌ DATA NOT TRANSFORMED %s", e)
logger.info(data_transformed[:2])

load_parquet_to_neon("stg_transport_realtime", data_transformed, realtime=True)