from db import get_connection
import sqlite3

def validate_bronze():
    conn = get_connection()
    tables = [
        "bronze_orders",
        "bronze_kitchen_orders",
        "bronze_delivery_log",
        "bronze_external_factors",
        "bronze_feedback_errors"
    ]

    print("🔍 Validating Bronze Layer Table Row Counts...\n")

    for table in tables:
        try:
            cur = conn.execute(f"SELECT COUNT(*) FROM {table}")
            count = cur.fetchone()[0]
            print(f"📦 {table}: {count} rows")
        except sqlite3.Error as e:
            print(f"⚠️ {table}: Error reading table → {e}")

    conn.close()
    print("\n✅ Bronze Layer validation completed.\n")


if __name__ == "__main__":
    validate_bronze()
