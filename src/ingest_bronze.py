import json
import logging
import os
from datetime import datetime, timezone
from io import BytesIO

import requests
from dotenv import load_dotenv

from src.utils import get_minio_client

load_dotenv()

API_URL = os.getenv("COINGECKO_API_URL")
BRONZE_BUCKET = os.getenv("BRONZE_BUCKET")

logging.basicConfig(
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("ingest_bronze")
logger.setLevel(logging.INFO)


def fetch_crypto_data():
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 100,
        "page": 1,
        "sparkline": "false"
    }
    response = requests.get(API_URL, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


# Modifina hna bch n-passiw l-date s-s7i7a
def save_raw_to_minio(data, execution_date: datetime):
    client = get_minio_client()

    if not client.bucket_exists(BRONZE_BUCKET):
        logger.info(f"Le bucket '{BRONZE_BUCKET}' n'existe pas. Création en cours...")
        client.make_bucket(BRONZE_BUCKET)
        logger.info(f"Bucket '{BRONZE_BUCKET}' créé avec succès.")
    # ----------------------------------------------------------------------

    object_name = f"{execution_date.year}/{execution_date.month:02d}/{execution_date.day:02d}/raw.json"

    payload = {
        "collected_at": execution_date.isoformat(),
        "data": data
    }

    json_bytes = json.dumps(payload, indent=2).encode("utf-8")

    client.put_object(
        bucket_name=BRONZE_BUCKET,
        object_name=object_name,
        data=BytesIO(json_bytes),
        length=len(json_bytes),
        content_type="application/json"
    )
    logger.info(f"Fichier sauvegardé : {BRONZE_BUCKET}/{object_name}")


# Kat-akhd **kwargs mn Airflow
def run(**kwargs):
    try:
        if kwargs and 'logical_date' in kwargs:
            now = kwargs['logical_date']
        else:
            now = datetime.now(tz=timezone.utc)

        data = fetch_crypto_data()
        save_raw_to_minio(data, now)
        logger.info("Ingestion Bronze terminée avec succès.")
    except Exception as e:
        logger.error(f"Erreur : {e}")
        raise


if __name__ == "__main__":
    run()