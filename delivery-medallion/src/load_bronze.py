
import pandas as pd
from pathlib import Path
from db import get_connection

# =========================================================
# PATH SETUP
# =========================================================
BASE_PATH = Path(__file__).resolve().parents[1]
BRONZE_PATH = BASE_PATH / "bronze_inputs"

# =========================================================
# FUNCTION: Load Single CSV into Bronze Table
# =========================================================
def load_csv_to_bronze(table_name, file_name):
    conn = get_connection()
    file_path = BRONZE_PATH / file_name

    if not file_path.exists():
        print(f" File not found: {file_path.name}. Skipping {table_name}.")
        conn.close()
        return

    try:
        df = pd.read_csv(file_path)
        df.to_sql(table_name, conn, if_exists="replace", index=False)
        print(f" Loaded {table_name}: {len(df)} rows")
    except Exception as e:
        print(f" Failed to load {file_path.name} → {e}")
    finally:
        conn.close()

# =========================================================
# FUNCTION: Load All Bronze Tables
# =========================================================
def load_all_bronze():
    print("🔹 Loading all Bronze Layer CSVs...\n")

def load_all_bronze():
    load_csv_to_bronze("bronze_orders", "Capstone - Orders (1).csv")
    load_csv_to_bronze("bronze_kitchen_orders", "Capstone - Kitchen_Log.csv")
    load_csv_to_bronze("bronze_delivery_log", "Capstone - Delivery_Log.csv")
    load_csv_to_bronze("bronze_external_factors", "Capstone - External_Factors.csv")
    load_csv_to_bronze("bronze_feedback_errors", "Capstone - Feedback_Errors.csv")


    print("\n All Bronze tables loaded successfully.")

# =========================================================
# MAIN ENTRY POINT
# =========================================================
if __name__ == "__main__":
    load_all_bronze()
