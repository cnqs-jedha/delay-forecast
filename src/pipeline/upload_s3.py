"""
Upload de fichiers vers AWS S3

Fonctions pour sauvegarder les donnees brutes sur S3 (backup).
Permet de rejouer les transformations sans refaire les appels API.
"""

import os
import json
import logging
from pathlib import Path
from datetime import datetime

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# Configuration
S3_BUCKET = os.getenv("S3_BUCKET")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "eu-west-3")

# Dossier local des donnees
BASE_DIR = Path(__file__).resolve().parent.parent.parent
DATA_DIR = BASE_DIR / "data"


def get_s3_client():
    """Cree un client S3 boto3."""
    return boto3.client(
        "s3",
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=AWS_REGION,
    )


def send_to_S3(data, key_prefix: str, bucket: str = None) -> bool:
    """
    Sauvegarde des donnees sur S3.
    
    Args:
        data: Donnees a sauvegarder (dict, str, ou chemin de fichier)
        key_prefix: Prefixe de la cle S3 (ex: "weather_archive_2026-01-01")
        bucket: Nom du bucket (utilise S3_BUCKET par defaut)
    
    Returns:
        bool: True si succes, False sinon
    
    Examples:
        # Sauvegarder un dict JSON
        send_to_S3({"temp": 20}, "weather_archive_2026-01-01")
        
        # Sauvegarder un fichier local
        send_to_S3("transport_koda_2026-01-01.7z", "transport_archive_2026-01-01")
    """
    bucket = bucket or S3_BUCKET
    
    if not bucket:
        logger.warning("S3_BUCKET non configure - backup ignore")
        return False
    
    try:
        s3 = get_s3_client()
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Cas 1: data est un dict (JSON)
        if isinstance(data, dict):
            key = f"{key_prefix}_{timestamp}.json"
            body = json.dumps(data, ensure_ascii=False, indent=2)
            s3.put_object(Bucket=bucket, Key=key, Body=body.encode('utf-8'))
            logger.info(f"[S3] JSON sauvegarde: s3://{bucket}/{key}")
            return True
        
        # Cas 2: data est un nom de fichier local
        elif isinstance(data, str):
            # Chercher le fichier dans DATA_DIR
            file_path = DATA_DIR / data
            if not file_path.exists():
                file_path = Path(data)  # Chemin absolu
            
            if file_path.exists():
                extension = file_path.suffix
                key = f"{key_prefix}_{timestamp}{extension}"
                s3.upload_file(str(file_path), bucket, key)
                logger.info(f"[S3] Fichier sauvegarde: s3://{bucket}/{key}")
                return True
            else:
                logger.warning(f"[S3] Fichier non trouve: {data}")
                return False
        
        else:
            logger.warning(f"[S3] Type de donnees non supporte: {type(data)}")
            return False
            
    except ClientError as e:
        logger.error(f"[S3] Erreur AWS: {e}")
        return False
    except Exception as e:
        logger.error(f"[S3] Erreur: {e}")
        return False


def list_s3_files(prefix: str = "", bucket: str = None) -> list:
    """Liste les fichiers dans S3 avec un prefixe donne."""
    bucket = bucket or S3_BUCKET
    if not bucket:
        return []
    
    try:
        s3 = get_s3_client()
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        return [obj['Key'] for obj in response.get('Contents', [])]
    except Exception as e:
        logger.error(f"[S3] Erreur listing: {e}")
        return []


def download_from_s3(key: str, local_path: str = None, bucket: str = None) -> str:
    """
    Telecharge un fichier depuis S3.
    
    Args:
        key: Cle S3 du fichier
        local_path: Chemin local de destination (optionnel)
        bucket: Nom du bucket
    
    Returns:
        str: Chemin local du fichier telecharge, ou None si erreur
    """
    bucket = bucket or S3_BUCKET
    if not bucket:
        return None
    
    try:
        s3 = get_s3_client()
        
        if local_path is None:
            local_path = DATA_DIR / Path(key).name
        
        s3.download_file(bucket, key, str(local_path))
        logger.info(f"[S3] Telecharge: {key} -> {local_path}")
        return str(local_path)
    except Exception as e:
        logger.error(f"[S3] Erreur download: {e}")
        return None


if __name__ == "__main__":
    # Test
    print(f"Bucket: {S3_BUCKET}")
    print(f"Region: {AWS_REGION}")
    
    if S3_BUCKET:
        files = list_s3_files()
        print(f"Fichiers dans le bucket: {len(files)}")
        for f in files[:5]:
            print(f"  - {f}")
    else:
        print("S3_BUCKET non configure")
