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

    def detect_archive_type(data: bytes) -> str:
        if data.startswith(b"7z\xbc\xaf'\x1c"):
            return "7z"
        if data.startswith(b"PK\x03\x04") or data.startswith(b"PK\x05\x06") or data.startswith(b"PK\x07\x08"):
            return "zip"
        return "unknown"

    def _pb_candidates(names):
        # On garde .pb, mais on rend ça plus robuste si le fournisseur change un peu.
        # Tu peux ajuster cette liste facilement.
        exts = (".pb", ".protobuf", ".bin")
        out = [n for n in names if n.lower().endswith(exts)]
        return out

    def _iter_entities():
        data = request.content
        sig = data[:16]
        kind = detect_archive_type(data)

        logger.info("Archive sniff: kind=%s size=%d sig=%r", kind, len(data), sig)

        tmpdir = tempfile.mkdtemp(prefix="koda_")
        tmp = Path(tmpdir)
        feed = gtfs_realtime_pb2.FeedMessage()

        try:
            if kind == "7z":
                archive_bytes = io.BytesIO(data)

                # 1) Liste les fichiers
                try:
                    archive_bytes.seek(0)
                    with py7zr.SevenZipFile(archive_bytes, mode="r") as z:
                        names = z.getnames()
                except py7zr.exceptions.Bad7zFile as e:
                    logger.error("❌ Archive 7z illisible (Bad7zFile) — jour ignoré")
                    bad_files.append(("__archive__", repr(e)))
                    return
                except Exception as e:
                    logger.exception("❌ Erreur ouverture 7z — jour ignoré")
                    bad_files.append(("__archive__", repr(e)))
                    return

                candidates = _pb_candidates(names)
                if not candidates:
                    # important: log les extensions présentes pour comprendre le changement de format
                    sample = names[:20]
                    logger.warning("⚠️ Aucun .pb/.bin trouvé dans le 7z. Exemple fichiers: %s", sample)
                    return

                # 2) Extraction batch comme tu faisais
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
                                yield entity
                        except DecodeError as e:
                            bad_files.append((name, f"DecodeError: {e!r}"))
                        except Exception as e:
                            bad_files.append((name, f"Read/ParseError: {e!r}"))
                        finally:
                            p.unlink(missing_ok=True)

            elif kind == "zip":
                # ZIP: on lit directement les entrées, sans extraire sur disque (encore plus simple)
                archive_bytes = io.BytesIO(data)
                try:
                    with zipfile.ZipFile(archive_bytes, "r") as z:
                        names = z.namelist()
                        candidates = _pb_candidates(names)

                        if not candidates:
                            sample = names[:20]
                            logger.warning("⚠️ Aucun .pb/.bin trouvé dans le ZIP. Exemple fichiers: %s", sample)
                            return

                        for i in range(0, len(candidates), items_by_batch):
                            batch = candidates[i:i+items_by_batch]
                            logger.info(
                                "Batch %d–%d / %d (%.1f%%)",
                                i + 1,
                                min(i + items_by_batch, len(candidates)),
                                len(candidates),
                                100 * (i + len(batch)) / len(candidates)
                            )

                            for name in batch:
                                try:
                                    raw = z.read(name)
                                    feed.Clear()
                                    feed.ParseFromString(raw)
                                    for entity in feed.entity:
                                        yield entity
                                except DecodeError as e:
                                    bad_files.append((name, f"DecodeError: {e!r}"))
                                except KeyError as e:
                                    bad_files.append((name, f"MissingInZip: {e!r}"))
                                except Exception as e:
                                    bad_files.append((name, f"Read/ParseError: {e!r}"))

                except zipfile.BadZipFile as e:
                    logger.error("❌ Archive ZIP illisible (BadZipFile) — jour ignoré")
                    bad_files.append(("__archive__", repr(e)))
                    return
                except Exception as e:
                    logger.exception("❌ Erreur ouverture ZIP — jour ignoré")
                    bad_files.append(("__archive__", repr(e)))
                    return

            else:
                # Ni zip ni 7z: souvent HTML/JSON d'erreur ou autre format
                logger.error("❌ Archive de type inconnu. sig=%r (premiers octets) — jour ignoré", sig)
                bad_files.append(("__archive__", f"Unknown archive type sig={sig!r}"))
                return

        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)
            logger.info("⚠️ Fichiers ignorés: %d", len(bad_files))

    return _iter_entities(), bad_files



# Lit les fichier de référence .txt
def read_koda_reference_data(request, file_name):
    data = request.content

    def detect_archive_type(data: bytes) -> str:
        if data.startswith(b"7z\xbc\xaf'\x1c"):
            return "7z"
        if data.startswith(b"PK\x03\x04") or data.startswith(b"PK\x05\x06") or data.startswith(b"PK\x07\x08"):
            return "zip"
        return "unknown"

    kind = detect_archive_type(data)
    archive_bytes = io.BytesIO(data)

    if kind == "zip":
        with zipfile.ZipFile(archive_bytes, "r") as z:
            with z.open(f"{file_name}.txt") as f:
                text = io.TextIOWrapper(f, encoding="utf-8")
                return list(csv.DictReader(text))

    if kind == "7z":
        # 7z: py7zr ne donne pas open() direct comme zipfile, on extrait vers mémoire ou temp
        with py7zr.SevenZipFile(archive_bytes, mode="r") as z:
            target = f"{file_name}.txt"
            names = z.getnames()
            # si chemin différent dans l'archive, on essaie de retrouver le bon
            matches = [n for n in names if n.endswith(target)]
            if not matches:
                raise FileNotFoundError(f"{target} introuvable dans l'archive 7z")
            chosen = matches[0]

            extracted = z.read([chosen])  # returns dict {name: bio}
            bio = extracted[chosen]
            text = io.TextIOWrapper(bio, encoding="utf-8")
            return list(csv.DictReader(text))

    raise ValueError(f"Archive type inconnu pour reference data: sig={data[:16]!r}")
