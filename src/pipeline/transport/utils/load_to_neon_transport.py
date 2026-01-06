import pandas as pd
import os
import logging
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("neon_loader")

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

def load_parquet_to_neon(table_name, data_array, if_exist="append", realtime=False) -> None:
    engine = create_engine(DATABASE_URL)

    if realtime == True:
        with engine.begin() as conn:
            conn.execute(text("""
                TRUNCATE TABLE stg_transport_realtime;
            """))

    df = pd.DataFrame(data_array)

    df.to_sql(
        table_name,
        engine,
        if_exists=if_exist,
        index=False,
        method="multi",
        chunksize=10_000,
    )

    logger.info("OK: %s chargée", table_name)