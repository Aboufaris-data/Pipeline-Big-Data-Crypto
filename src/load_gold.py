import logging
import os
from datetime import datetime, timezone
from io import BytesIO

import pandas as pd
from dotenv import load_dotenv
import snowflake.connector

from src.utils import get_minio_client

load_dotenv()

SILVER_BUCKET = os.getenv("SILVER_BUCKET")
GOLD_BUCKET   = os.getenv("GOLD_BUCKET")

logging.basicConfig(
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("load_gold")
logger.setLevel(logging.INFO)


def read_silver(client, date: datetime) -> pd.DataFrame:
    object_name = f"{date.year}/{date.month:02d}/{date.day:02d}/silver.parquet"
    response = client.get_object(bucket_name=SILVER_BUCKET, object_name=object_name)
    parquet_bytes = response.read()
    response.close()
    response.release_conn()
    df = pd.read_parquet(BytesIO(parquet_bytes), engine="pyarrow")
    return df


def build_dim_crypto(df: pd.DataFrame) -> pd.DataFrame:
    dim = df[["coin_id", "name", "symbol", "market_cap_rank"]].drop_duplicates(subset=["coin_id"]).reset_index(drop=True)
    dim.insert(0, "crypto_id", range(1, len(dim) + 1))
    return dim


def build_dim_date(df: pd.DataFrame) -> pd.DataFrame:
    dim = df[["collected_at"]].drop_duplicates().reset_index(drop=True)
    dim["date"]  = dim["collected_at"].dt.date
    dim["year"]  = dim["collected_at"].dt.year
    dim["month"] = dim["collected_at"].dt.month
    dim["week"]  = dim["collected_at"].dt.isocalendar().week.astype(int)
    dim["day"]   = dim["collected_at"].dt.day
    dim["hour"]  = dim["collected_at"].dt.hour
    dim.insert(0, "date_id", range(1, len(dim) + 1))
    return dim


def build_fact_market_data(df: pd.DataFrame, dim_crypto: pd.DataFrame, dim_date: pd.DataFrame) -> pd.DataFrame:
    fact = df.merge(dim_crypto[["crypto_id", "coin_id"]], on="coin_id", how="left")
    fact = fact.merge(dim_date[["date_id", "collected_at"]], on="collected_at", how="left")
    fact = fact[[
        "crypto_id", "date_id", "current_price", "price_high_24h", "price_low_24h",
        "volume_24h", "market_cap", "price_change_24h", "price_change_pct_24h",
    ]].reset_index(drop=True)
    fact.insert(0, "fact_id", range(1, len(fact) + 1))
    return fact


def save_gold(client, df: pd.DataFrame, table_name: str):
    # Automatically verify and create MinIO bucket if it's missing
    if not client.bucket_exists(GOLD_BUCKET):
        logger.info(f"Le bucket '{GOLD_BUCKET}' n'existe pas. Création en cours...")
        client.make_bucket(GOLD_BUCKET)
        logger.info(f"Bucket '{GOLD_BUCKET}' créé avec succès.")

    object_name = f"{table_name}.parquet"
    buffer = BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)
    parquet_bytes = buffer.getvalue()
    client.put_object(
        bucket_name=GOLD_BUCKET,
        object_name=object_name,
        data=BytesIO(parquet_bytes),
        length=len(parquet_bytes),
        content_type="application/octet-stream",
    )
    logger.info(f"Parquet sauvegardé dans Gold : {GOLD_BUCKET}/{object_name}")


def load_dataframe_to_snowflake(df: pd.DataFrame, table_name: str):
    """
    Connects to Snowflake, automatically creates the Database, Schema, 
    and the Table with exact matching constraints, then appends the data.
    """
    try:
        # --- حل مشكلة الاختلاف بين حَجم الحروف في الـ Dataframe والـ Snowflake ---
        df_upper = df.copy()
        df_upper.columns = [col.upper() for col in df_upper.columns]
        # ----------------------------------------------------------------------

        # 1. Initial Connection Setup
        conn = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            role=os.getenv("SNOWFLAKE_ROLE", "SYSADMIN")
        )
        cursor = conn.cursor()
        
        logger.info("Connexion initiale à Snowflake réussie.")

        # 2. Infrastructure Creation Automation
        db_name = os.getenv("SNOWFLAKE_DATABASE", "CRYPTO_DB")
        schema_name = os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC")
        
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name};")
        cursor.execute(f"USE DATABASE {db_name};")
        cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name};")
        cursor.execute(f"USE SCHEMA {schema_name};")
        
        logger.info(f"Environnement Snowflake prêt : DATABASE={db_name}, SCHEMA={schema_name}")

        # 3. Explicit Data Warehouse DDL Mapping (تحديث الأعمدة لتكون UPPERCASE)
        structures = {
            "DIM_CRYPTO": """
                CREATE TABLE IF NOT EXISTS DIM_CRYPTO (
                    CRYPTO_ID NUMBER,
                    COIN_ID VARCHAR(100),
                    NAME VARCHAR(100),
                    SYMBOL VARCHAR(20),
                    MARKET_CAP_RANK NUMBER
                );
            """,
            "DIM_DATE": """
                CREATE TABLE IF NOT EXISTS DIM_DATE (
                    DATE_ID NUMBER,
                    COLLECTED_AT TIMESTAMP_TZ,
                    DATE DATE,
                    YEAR NUMBER,
                    MONTH NUMBER,
                    WEEK NUMBER,
                    DAY NUMBER,
                    HOUR NUMBER
                );
            """,
            "FACT_MARKET_DATA": """
                CREATE TABLE IF NOT EXISTS FACT_MARKET_DATA (
                    FACT_ID NUMBER,
                    CRYPTO_ID NUMBER,
                    DATE_ID NUMBER,
                    CURRENT_PRICE NUMBER(38, 8),
                    PRICE_HIGH_24H NUMBER(38, 8),
                    PRICE_LOW_24H NUMBER(38, 8),
                    VOLUME_24H NUMBER(38, 2),
                    MARKET_CAP NUMBER(38, 2),
                    PRICE_CHANGE_24H NUMBER(38, 8),
                    PRICE_CHANGE_PCT_24H NUMBER(38, 5)
                );
            """
        }

        # Validate and deploy DDL target structure
        upper_table = table_name.upper()
        if upper_table in structures:
            cursor.execute(structures[upper_table])
            logger.info(f"Table {upper_table} vérifiée/créée avec succès.")

        # 4. Stream and Append Data using optimized write_pandas
        from snowflake.connector.pandas_tools import write_pandas
        
        success, nchunks, nrows, _ = write_pandas(
            conn=conn, 
            df=df_upper,  
            table_name=upper_table, 
            database=db_name,
            schema=schema_name,
            auto_create_table=False,
            use_logical_type=True  
        )
        
        logger.info(f"Snowflake Loading Status: {success}, {nrows} lignes insérées dans {upper_table}.")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logger.error(f"Erreur lors du chargement de la table {table_name} sur Snowflake: {e}")
        raise


def run(**kwargs):
    try:
        if kwargs and 'logical_date' in kwargs:
            now = kwargs['logical_date']
        else:
            now = datetime.now(tz=timezone.utc)
            
        client = get_minio_client()

        # Lecture Silver
        df = read_silver(client, now)

        # Construction du modèle dimensionnel (Star Schema)
        dim_crypto  = build_dim_crypto(df)
        dim_date    = build_dim_date(df)
        fact        = build_fact_market_data(df, dim_crypto, dim_date)

        # Sauvegarde f MinIO Gold
        save_gold(client, dim_crypto, "dim_crypto")
        save_gold(client, dim_date,   "dim_date")
        save_gold(client, fact,        "fact_market_data")
        
        # Loadi les dimensions f Snowflake
        load_dataframe_to_snowflake(dim_crypto, "dim_crypto")
        load_dataframe_to_snowflake(dim_date, "dim_date")
        load_dataframe_to_snowflake(fact, "fact_market_data")

        logger.info("Modélisation Gold et chargement Snowflake terminés avec succès.")

    except Exception as e:
        logger.error(f"Erreur : {e}")
        raise


if __name__ == "__main__":
    run()