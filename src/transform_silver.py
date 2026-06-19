import json
import logging
import os
from datetime import datetime, timezone
from io import BytesIO

import pandas as pd
from dotenv import load_dotenv

from src.utils import get_minio_client

load_dotenv()

BRONZE_BUCKET = os.getenv("BRONZE_BUCKET")
SILVER_BUCKET = os.getenv("SILVER_BUCKET")

logging.basicConfig(
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("transform_silver")
logger.setLevel(logging.INFO)


def read_bronze(client, date: datetime) -> pd.DataFrame:
    object_name = f"{date.year}/{date.month:02d}/{date.day:02d}/raw.json"
    response = client.get_object(bucket_name=BRONZE_BUCKET, object_name=object_name)
    raw_bytes = response.read()
    response.close()
    response.release_conn()
    payload = json.loads(raw_bytes.decode("utf-8"))
    data = payload.get("data", payload)
    collected_at = payload.get("collected_at", date.isoformat())
    df = pd.DataFrame(data)
    df["collected_at"] = collected_at
    logger.info(f"{len(df)} lignes chargées depuis Bronze.")
    return df


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    columns_mapping = {
        "id":                           "coin_id",
        "symbol":                       "symbol",
        "name":                         "name",
        "market_cap_rank":              "market_cap_rank",
        "current_price":                "current_price",
        "high_24h":                     "price_high_24h",
        "low_24h":                      "price_low_24h",
        "total_volume":                 "volume_24h",
        "market_cap":                   "market_cap",
        "price_change_24h":             "price_change_24h",
        "price_change_percentage_24h":  "price_change_pct_24h",
        "collected_at":                 "collected_at",
    }
    existing_cols = [c for c in columns_mapping if c in df.columns]
    df = df[existing_cols].rename(columns=columns_mapping)
    df["current_price"]        = pd.to_numeric(df["current_price"],        errors="coerce")
    df["price_high_24h"]       = pd.to_numeric(df["price_high_24h"],       errors="coerce")
    df["price_low_24h"]        = pd.to_numeric(df["price_low_24h"],        errors="coerce")
    df["volume_24h"]           = pd.to_numeric(df["volume_24h"],           errors="coerce")
    df["market_cap"]           = pd.to_numeric(df["market_cap"],           errors="coerce")
    df["price_change_24h"]     = pd.to_numeric(df["price_change_24h"],      errors="coerce")
    df["price_change_pct_24h"] = pd.to_numeric(df["price_change_pct_24h"], errors="coerce")
    df["market_cap_rank"]      = pd.to_numeric(df["market_cap_rank"],       errors="coerce").astype("Int64")
    df["collected_at"]         = pd.to_datetime(df["collected_at"],         utc=True)
    df["coin_id"] = df["coin_id"].str.strip().str.lower()
    df["symbol"]  = df["symbol"].str.strip().str.upper()
    df["name"]    = df["name"].str.strip()
    df = df.drop_duplicates(subset=["coin_id", "collected_at"])
    df = df.dropna(subset=["coin_id", "current_price"])
    logger.info(f"{len(df)} lignes après nettoyage.")
    return df


def save_silver(client, df: pd.DataFrame, date: datetime):
    # --- التعديل هنا: التّأكد من وجود الـ Bucket وإنشائه أوتوماتيكياً إذا لم يكن موجوداً ---
    if not client.bucket_exists(SILVER_BUCKET):
        logger.info(f"Le bucket '{SILVER_BUCKET}' n'existe pas. Création en cours...")
        client.make_bucket(SILVER_BUCKET)
        logger.info(f"Bucket '{SILVER_BUCKET}' créé avec succès.")
    # ----------------------------------------------------------------------------------

    object_name = f"{date.year}/{date.month:02d}/{date.day:02d}/silver.parquet"
    buffer = BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)
    parquet_bytes = buffer.getvalue()
    client.put_object(
        bucket_name=SILVER_BUCKET,
        object_name=object_name,
        data=BytesIO(parquet_bytes),
        length=len(parquet_bytes),
        content_type="application/octet-stream",
    )
    logger.info(f"Parquet sauvegardé : {SILVER_BUCKET}/{object_name}")


def run(**kwargs):
    try:
        if kwargs and 'logical_date' in kwargs:
            now = kwargs['logical_date']
        else:
            now = datetime.now(tz=timezone.utc)

        client = get_minio_client()

        df_raw   = read_bronze(client, now)
        df_clean = clean_dataframe(df_raw)
        save_silver(client, df_clean, now)

        logger.info("Transformation Silver terminée avec succès.")

    except Exception as e:
        logger.error(f"Erreur : {e}")
        raise


if __name__ == "__main__":
    run()