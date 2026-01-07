import logging

from pipeline.transport.utils.transform_data_transport import transform_S3_to_neon
from pipeline.transport.utils.s3_transport import get_S3_datas
from pipeline.transport.utils.load_to_neon_transport import load_parquet_to_neon

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("RUN_TRANSPORT")

DATE_BEGIN = "2025-07-28"
DATE_END = "2025-08-03"

logger.info("RUN Transport")

file_name = f"history_transport_{DATE_BEGIN}-{DATE_END}"
datas = get_S3_datas(file_name)

logger.info("c'est récupéré")

# Transform data to database
datas_S3 = transform_S3_to_neon(datas)

logger.info(datas_S3[:20])

# #LOAD TO NEON
logger = logging.getLogger("NEON LOADER")

load_parquet_to_neon("stg_transport_archive", datas_S3)
