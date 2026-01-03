import requests
import os
import io
import py7zr
import zipfile
import csv
import tempfile
from pathlib import Path
from dotenv import load_dotenv
import shutil
import copy
from google.transit import gtfs_realtime_pb2
from google.protobuf.message import DecodeError
import logging
from datetime import datetime, timezone, timedelta
from collections import defaultdict
import random
import gc

def read_koda_history_day_stream(request, items_by_batch=400):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%H:%M:%S"
    )
    logger = logging.getLogger("gtfs")
    bad_files = []

    def _iter_entities():
        archive_bytes = io.BytesIO(request.content)
        tmpdir = tempfile.mkdtemp(prefix="koda_")
        tmp = Path(tmpdir)
        feed = gtfs_realtime_pb2.FeedMessage()

        try:
            archive_bytes.seek(0)
            with py7zr.SevenZipFile(archive_bytes, mode="r") as z:
                candidates = [n for n in z.getnames() if n.lower().endswith(".pb")]

            for i in range(0, len(candidates), items_by_batch):
                
                batch = candidates[i:i+items_by_batch]
                logger.info(
                    "Batch %d–%d / %d (%.1f%%)",
                    i + 1,
                    min(i + items_by_batch, len(candidates)),
                    len(candidates),
                    100 * (i + len(batch)) / len(candidates)
                )
                try:
                    archive_bytes.seek(0)
                    with py7zr.SevenZipFile(archive_bytes, mode="r") as z:
                        z.extract(path=tmpdir, targets=batch)
                except Exception as e:
                    for name in batch:
                        bad_files.append((name, f"ExtractError: {e!r}"))
                    continue

                for name in batch:
                    p = tmp / name
                    try:
                        raw = p.read_bytes()
                        feed.Clear()
                        feed.ParseFromString(raw)

                        for entity in feed.entity:
                            yield entity   # ✅ streaming

                    except Exception as e:
                        bad_files.append((name, f"Read/ParseError: {e!r}"))
                    finally:
                        p.unlink(missing_ok=True)

        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)
            logger.info(f"⚠️ Fichiers ignorés: {len(bad_files)}")

    return _iter_entities(), bad_files


# Lit les fichier de référence .txt
def read_koda_reference_data(request, file_name):
    archive_bytes = io.BytesIO(request.content)

    with zipfile.ZipFile(archive_bytes, "r") as z:
        with z.open(f"{file_name}.txt") as f:
            text = io.TextIOWrapper(f, encoding="utf-8")
            reader = csv.DictReader(text)
            return list(reader)