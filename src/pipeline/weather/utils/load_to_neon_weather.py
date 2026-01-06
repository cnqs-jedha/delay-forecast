import pandas as pd
import os
import logging
from sqlalchemy import create_engine
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
logging.getLogger("sqlalchemy.pool").setLevel(logging.WARNING)
logger = logging.getLogger("neon_loader")

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

def load_parquet_to_neon(table_name, data_df) -> None:
    engine = create_engine(DATABASE_URL)

    # df = pd.DataFrame(data_array)

    data_df.to_sql(
        table_name,
        engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=1000,
    )

    logger.info("OK: %s chargée", table_name)