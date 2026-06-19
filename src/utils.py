import logging
import os

from dotenv import load_dotenv
from minio import Minio
from minio.error import S3Error

load_dotenv()

logger = logging.getLogger("utils")


# ─────────────────────────────────────────────
#  Connexion MinIO
# ─────────────────────────────────────────────

def get_minio_client() -> Minio:
    """
    Crée et retourne un client MinIO connecté.
    Lit les credentials depuis le fichier .env

    Variables .env requises :
        MINIO_ENDPOINT  — ex: localhost:9000
        MINIO_ACCESS_KEY
        MINIO_SECRET_KEY
        MINIO_SECURE    — "true" ou "false"
    """
    endpoint   = os.getenv("MINIO_ENDPOINT",   "localhost:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
    secure     = os.getenv("MINIO_SECURE",     "false").lower() == "true"

    client = Minio(
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=secure,
    )

    logger.debug(f"Client MinIO créé — endpoint: {endpoint}, secure: {secure}")
    return client


# ─────────────────────────────────────────────
#  Création automatique des buckets
# ─────────────────────────────────────────────

def ensure_bucket_exists(client: Minio, bucket_name: str):
    """
    Crée le bucket s'il n'existe pas encore.
    Appelé au démarrage de chaque étape du pipeline.
    """
    try:
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            logger.info(f"Bucket '{bucket_name}' créé.")
        else:
            logger.debug(f"Bucket '{bucket_name}' existe déjà.")
    except S3Error as e:
        logger.error(f"Erreur MinIO sur le bucket '{bucket_name}' : {e}")
        raise


# ─────────────────────────────────────────────
#  Lecture config.yaml (optionnel)
# ─────────────────────────────────────────────

def load_config(config_path: str = None) -> dict:
    """
    Charge le fichier config.yaml si besoin.
    Par défaut cherche config/config.yaml depuis la racine du projet.
    """
    import yaml

    if config_path is None:
        base_dir    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        config_path = os.path.join(base_dir, "config", "config.yaml")

    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    logger.debug(f"Config chargée depuis : {config_path}")
    return config


# ─────────────────────────────────────────────
#  Test de connexion rapide
# ─────────────────────────────────────────────

def test_minio_connection():
    """
    Vérifie que MinIO est accessible.
    Utile pour déboguer avant de lancer le pipeline.
    """
    try:
        client = get_minio_client()
        buckets = client.list_buckets()
        print("✅ Connexion MinIO réussie.")
        print(f"   Buckets existants : {[b.name for b in buckets]}")
    except Exception as e:
        print(f"❌ Connexion MinIO échouée : {e}")


if __name__ == "__main__":
    test_minio_connection()