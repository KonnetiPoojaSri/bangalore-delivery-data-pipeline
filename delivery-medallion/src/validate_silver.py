from db import get_connection
conn = get_connection()
tables = [
    "silver_orders",
    "silver_deliveries",
    "silver_drivers",
    "silver_hubs",
    "silver_delivery_issues"
]
for t in tables:
    count = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
    print(f"{t}: {count} rows")
conn.close()