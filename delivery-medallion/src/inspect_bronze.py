import sqlite3
from db import get_connection

def inspect_bronze():
    conn = get_connection()

    tables = [
        "bronze_orders",
        "bronze_kitchen_orders",
        "bronze_delivery_log",
        "bronze_external_factors",
        "bronze_feedback_errors"
    ]

    print("🔍 Inspecting Bronze Layer Tables...\n")

    for table in tables:
        print(f"📦 Table: {table}")
        try:
            rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
            if not rows:
                print("  ⚠️ Table not found or has no columns.\n")
                continue

            print("  Columns:")
            for row in rows:
                cid, name, col_type, notnull, default, pk = row
                print(f"   - {name} ({col_type}){' [PK]' if pk else ''}")
            print()

            # Optional: preview 3 sample rows
            sample = conn.execute(f"SELECT * FROM {table} LIMIT 3").fetchall()
            if sample:
                print("  Sample Rows:")
                for s in sample:
                    print("   ", s)
                print()
            else:
                print("  (No data rows yet)\n")

        except sqlite3.Error as e:
            print(f"  ❌ Error inspecting {table}: {e}\n")

    conn.close()
    print("✅ Bronze Layer inspection complete.\n")


if __name__ == "__main__":
    inspect_bronze()
