# Pipeline Big Data Crypto

A end-to-end **Big Data pipeline** that collects cryptocurrency market data from the CoinGecko API, processes it through a **Medallion Architecture** (Bronze → Silver → Gold), stores each layer in **MinIO** (S3-compatible object storage), loads the final model into **Snowflake**, and visualizes it with a **Tableau** dashboard. The pipeline is fully orchestrated with **Apache Airflow**.

---

## 📐 Architecture Overview

```
CoinGecko API
      │
      ▼
┌─────────────┐     ┌──────────────┐     ┌────────────┐
│   BRONZE    │ ──► │    SILVER    │ ──► │    GOLD    │
│  (raw JSON) │     │  (Parquet)   │     │ (Parquet)  │
└─────────────┘     └──────────────┘     └────────────┘
       │                   │                    │
       └───────────────────┴────────────────────┘
                           │
                        MinIO
                    (Object Storage)
                           │
                           ▼
                       Snowflake
                  (Data Warehouse)
                           │
                           ▼
                   Tableau Dashboard
```

The pipeline runs **daily** via an Airflow DAG with automatic retries and failure alerts.

---

## 🗂️ Project Structure

```
Pipeline Big Data Crypto/
│
├── config/
│   └── config.yaml             # API, MinIO & pipeline configuration
│
├── dags/
│   └── crypto_pipeline_dag.py  # Airflow DAG definition
│
├── dashboard/
│   └── Projet_Crypto.twbx      # Tableau workbook
│
├── logs/                       # Airflow / pipeline logs
│
├── src/
│   ├── ingest_bronze.py        # Layer 1 — Fetch & store raw JSON
│   ├── transform_silver.py     # Layer 2 — Clean & convert to Parquet
│   ├── load_gold.py            # Layer 3 — Build star schema & load Snowflake
│   └── utils.py                # Shared helpers (MinIO client, etc.)
│
├── .env                        # Environment variables (secrets — not committed)
├── .gitignore
├── ERD.pdf                     # Entity-Relationship Diagram
├── requirements.txt
├── run_pipeline.py             # Run the full pipeline locally
└── setup_minio.py              # Download & start MinIO on Windows
```

---

## 🧱 Medallion Architecture

### 🥉 Bronze — Raw Ingestion (`src/ingest_bronze.py`)
- Calls the CoinGecko `/coins/markets` endpoint.
- Fetches the **top 100 cryptocurrencies** by market cap (in USD).
- Stores the raw response as a timestamped JSON file in MinIO:
  ```
  crypto-bronze/{year}/{month}/{day}/raw.json
  ```

### 🥈 Silver — Cleaning & Transformation (`src/transform_silver.py`)
- Reads the raw JSON from the Bronze bucket.
- Selects and renames relevant fields (price, volume, market cap, etc.).
- Casts types, normalizes strings, and drops duplicates/nulls.
- Saves a clean **Parquet** file to MinIO:
  ```
  crypto-silver/{year}/{month}/{day}/silver.parquet
  ```

### 🥇 Gold — Star Schema & Loading (`src/load_gold.py`)
- Reads the Silver Parquet and builds a **star schema**:
  - `DIM_CRYPTO` — coin metadata (id, name, symbol, market cap rank)
  - `DIM_DATE` — date/time dimension (year, month, week, day, hour)
  - `FACT_MARKET_DATA` — price, volume, market cap, 24h changes
- Saves each table as Parquet in the Gold MinIO bucket.
- Loads all three tables into **Snowflake** (auto-creates database, schema, and tables if missing).

---

## 🗄️ Snowflake Data Model

```
DIM_CRYPTO          FACT_MARKET_DATA         DIM_DATE
──────────          ────────────────         ────────
crypto_id  ◄──┐    fact_id                   date_id
coin_id        ├── crypto_id           ┌──── date_id
name           │   date_id   ──────────┘     collected_at
symbol         │   current_price             date
market_cap_rank┘   price_high_24h            year
                   price_low_24h             month
                   volume_24h                week
                   market_cap                day
                   price_change_24h          hour
                   price_change_pct_24h
```

---

## ⚙️ Prerequisites

| Tool | Version | Notes |
|---|---|---|
| Python | 3.10+ | 3.14 used in dev |
| Apache Airflow | 2.x | For scheduled runs |
| MinIO | Latest | Local object storage |
| Snowflake | — | Cloud account required |
| Tableau Desktop | — | To open `.twbx` |

---

## 🚀 Getting Started

### 1. Clone the repository

```bash
git clone https://github.com/<your-username>/Pipeline-Big-Data-Crypto.git
cd Pipeline-Big-Data-Crypto
```

### 2. Install Python dependencies

```bash
pip install -r requirements.txt
```

### 3. Configure environment variables

Copy the example below into a `.env` file at the project root and fill in your credentials:

```env
# CoinGecko
COINGECKO_API_URL=https://api.coingecko.com/api/v3/coins/markets

# MinIO
MINIO_ENDPOINT=localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_SECURE=false

# Buckets (auto-created if missing)
BRONZE_BUCKET=crypto-bronze
SILVER_BUCKET=crypto-silver
GOLD_BUCKET=crypto-gold

# Snowflake
SNOWFLAKE_ACCOUNT=<your-account>
SNOWFLAKE_USER=<your-user>
SNOWFLAKE_PASSWORD=<your-password>
SNOWFLAKE_DATABASE=CRYPTO_DB
SNOWFLAKE_SCHEMA=PUBLIC
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_ROLE=SYSADMIN
```

> ⚠️ **Never commit your `.env` file.** It is listed in `.gitignore`.

### 4. Start MinIO (Windows)

```bash
python setup_minio.py
```

This will download the MinIO binary and start the server at:
- **API:** `http://localhost:9000`
- **Web Console:** `http://localhost:9001`

For Linux/macOS, install MinIO via your package manager or Docker:
```bash
docker run -p 9000:9000 -p 9001:9001 \
  minio/minio server /data --console-address ":9001"
```

---

## ▶️ Running the Pipeline

### Option A — Run Locally (without Airflow)

```bash
python run_pipeline.py
```

This runs all three stages sequentially: Bronze → Silver → Gold → Snowflake.

### Option B — Run with Apache Airflow

1. Place the project folder inside your Airflow home directory (or configure `sys.path` in the DAG).
2. Copy the DAG file:
   ```bash
   cp dags/crypto_pipeline_dag.py $AIRFLOW_HOME/dags/
   ```
3. Start Airflow:
   ```bash
   airflow scheduler &
   airflow webserver
   ```
4. Enable the `crypto_pipeline_dag` DAG in the Airflow UI.

The DAG runs **daily** with 2 automatic retries (5-minute delay) and logs errors on failure.

---

## 📊 Dashboard

Open `dashboard/Projet_Crypto.twbx` in Tableau Desktop.

The dashboard connects to Snowflake and visualizes:
- Top cryptocurrencies by market cap
- 24h price changes and volume
- Historical trends across the date dimension

---

## 📦 Key Dependencies

| Package | Purpose |
|---|---|
| `requests` | CoinGecko API calls |
| `pandas` | Data transformation |
| `pyarrow` | Parquet read/write |
| `minio` | MinIO object storage client |
| `snowflake-connector-python` | Snowflake loading |
| `apache-airflow` | Pipeline orchestration |
| `python-dotenv` | Environment variable management |

Install all with:
```bash
pip install -r requirements.txt
```

---

## 📁 MinIO Bucket Layout

```
crypto-bronze/
└── {year}/{month}/{day}/raw.json

crypto-silver/
└── {year}/{month}/{day}/silver.parquet

crypto-gold/
├── dim_crypto.parquet
├── dim_date.parquet
└── fact_market_data.parquet
```

---

## 🔒 Security Notes

- Credentials are loaded from `.env` via `python-dotenv` — never hardcoded.
- `.env` is excluded from version control via `.gitignore`.
- Rotate your Snowflake credentials before sharing or deploying this project.

---

## 📄 License

This project is for educational and personal portfolio purposes.

---