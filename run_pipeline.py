import os
import sys
from datetime import datetime, timezone

# Bach Python i-chouf l-dossier src
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

from src.ingest_bronze import run as run_bronze
from src.transform_silver import run as run_silver
from src.load_gold import run as run_gold

if __name__ == "__main__":
    print("🚀 --- Lancement du Pipeline Local ---")
    
    # Simulation dial logical_date b7al dial Airflow (Aujourd'hui)
    context = {'logical_date': datetime.now(tz=timezone.utc)}
    
    print("\n1️⃣ Ingestion Bronze...")
    run_bronze(**context)
    
    print("\n2️⃣ Transformation Silver...")
    run_silver(**context)
    
    print("\n3️⃣ Modélisation Gold & Load Snowflake...")
    run_gold(**context)
    
    print("\n✅ --- Pipeline terminé avec succès ! ---")