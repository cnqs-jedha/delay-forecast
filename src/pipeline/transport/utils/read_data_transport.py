import io
import py7zr
import zipfile
import csv
import tempfile
from pathlib import Path
import shutil
from google.transit import gtfs_realtime_pb2
from google.protobuf.message import DecodeError
import logging

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
                    sample = names[:20]
                    logger.warning("⚠️ Aucun .pb/.bin trouvé dans le 7z. Exemple fichiers: %s", sample)
                    return

                # 2) Extraction batch
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
                logger.error("❌ Archive de type inconnu. sig=%r (premiers octets) — jour ignoré", sig)
                bad_files.append(("__archive__", f"Unknown archive type sig={sig!r}"))
                return

        finally:
            shutil.rmtree(tmpdir, ignore_errors=True)
            logger.info("⚠️ Fichiers ignorés: %d", len(bad_files))

    return _iter_entities(), bad_files



def read_koda_reference_data(request, file_name):
    archive_bytes = io.BytesIO(request.content)

    with zipfile.ZipFile(archive_bytes, "r") as z:
        with z.open(f"{file_name}.txt") as f:
            text = io.TextIOWrapper(f, encoding="utf-8")
            reader = csv.DictReader(text)
            return list(reader)
