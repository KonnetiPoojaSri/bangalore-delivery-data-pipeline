import pandas as pd
from db import get_connection

# =========================================================
# MASTER FUNCTION
# =========================================================
def build_silver():
    conn = get_connection()
    print("Starting Silver Layer...")

    build_silver_orders(conn)
    build_silver_kitchen_orders(conn)
    build_silver_delivery_log(conn)
    build_silver_external_factors(conn)
    build_silver_feedback_errors(conn)

    conn.close()
    print("Silver Layer completed successfully.")

# =========================================================
# 1️⃣ ORDERS
# =========================================================
def build_silver_orders(conn):
    df = pd.read_sql("SELECT * FROM bronze_orders", conn)
    before = len(df)

    # Type enforcement
    df["order_time"] = pd.to_datetime(df["order_time"], errors="coerce")
    df["order_status"] = df["order_status"].str.lower().str.strip()

    # Duplicates and null checks
    df = df.drop_duplicates(subset=["order_id"])
    df = df.dropna(subset=["order_id", "customer_id", "order_time"])

    # Range & data sanity
    df = df[df["total_amount"] >= 0]
    df = df[df["prep_time_est_mins"] >= 0]

    after = len(df)
    print(f"silver_orders: removed {before - after} invalid/duplicate rows")

    df.to_sql("silver_orders", conn, if_exists="replace", index=False)

# =========================================================
# 2️⃣ KITCHEN ORDERS
# =========================================================
def build_silver_kitchen_orders(conn):
    df = pd.read_sql("SELECT * FROM bronze_kitchen_orders", conn)
    before = len(df)

    # Convert time fields
    df["prep_start"] = pd.to_datetime(df["prep_start"], errors="coerce")
    df["prep_end"] = pd.to_datetime(df["prep_end"], errors="coerce")

    # Duration checks
    df = df[df["prep_end"] >= df["prep_start"]]
    df["actual_prep_mins"] = (df["prep_end"] - df["prep_start"]).dt.total_seconds() / 60

    # Standardization
    df["cuisine"] = df["cuisine"].str.title().str.strip()

    # Duplicates and nulls
    df = df.drop_duplicates(subset=["order_id"])
    df = df.dropna(subset=["order_id", "restaurant_id"])

    after = len(df)
    print(f"silver_kitchen_orders: removed {before - after} invalid rows")

    df.to_sql("silver_kitchen_orders", conn, if_exists="replace", index=False)

# =========================================================
# 3️⃣ DELIVERY LOG
# =========================================================
def build_silver_delivery_log(conn):
    df = pd.read_sql("SELECT * FROM bronze_delivery_log", conn)
    before = len(df)

    # Time conversions
    df["rider_arrival"] = pd.to_datetime(df["rider_arrival"], errors="coerce")
    df["handover_time"] = pd.to_datetime(df["handover_time"], errors="coerce")
    df["delivered_time"] = pd.to_datetime(df["delivered_time"], errors="coerce")

    # Time validation
    df = df.dropna(subset=["delivery_id", "order_id", "rider_id"])
    df = df[df["delivered_time"] >= df["handover_time"]]

    # Recalculate delivery delay in minutes
    df["delivery_delay_mins"] = (
        df["delivered_time"] - df["handover_time"]
    ).dt.total_seconds() / 60

    # Range filter (remove negative or unrealistic delays)
    df = df[df["delivery_delay_mins"].between(0, 180)]

    # FK checks
    orders = pd.read_sql("SELECT order_id FROM silver_orders", conn)
    df = df[df["order_id"].isin(orders["order_id"])]

    after = len(df)
    print(f"silver_delivery_log: removed {before - after} invalid rows")

    df.to_sql("silver_delivery_log", conn, if_exists="replace", index=False)

# =========================================================
# 4️⃣ EXTERNAL FACTORS
# =========================================================
def build_silver_external_factors(conn):
    df = pd.read_sql("SELECT * FROM bronze_external_factors", conn)
    before = len(df)

    # Clean categorical fields
    for col in ["weather", "zone", "traffic_level"]:
        df[col] = df[col].apply(lambda x: str(x).title().strip() if pd.notnull(x) else None)

    # Null & duplicate handling
    df = df.drop_duplicates(subset=["order_id"])
    df = df.dropna(subset=["order_id", "weather", "traffic_level"])

    # FK check
    orders = pd.read_sql("SELECT order_id FROM silver_orders", conn)
    df = df[df["order_id"].isin(orders["order_id"])]

    after = len(df)
    print(f"silver_external_factors: removed {before - after} invalid rows")

    df.to_sql("silver_external_factors", conn, if_exists="replace", index=False)

# =========================================================
# 5️⃣ FEEDBACK ERRORS
# =========================================================
def build_silver_feedback_errors(conn):
    df = pd.read_sql("SELECT * FROM bronze_feedback_errors", conn)
    before = len(df)

    # Standardization with safe string conversion
    for col in ["complaint_tag", "refund_issued"]:
        df[col] = df[col].apply(lambda x: str(x).title().strip() if pd.notnull(x) else None)

    # Type enforcement
    df["rating_stars"] = pd.to_numeric(df["rating_stars"], errors="coerce")

    # Duplicates and nulls
    df = df.drop_duplicates(subset=["feedback_id"])
    df = df.dropna(subset=["feedback_id", "order_id", "rating_stars"])

    # Range checks
    df = df[df["rating_stars"].between(0, 5)]

    # FK check
    orders = pd.read_sql("SELECT order_id FROM silver_orders", conn)
    df = df[df["order_id"].isin(orders["order_id"])]

    after = len(df)
    print(f"silver_feedback_errors: removed {before - after} invalid rows")

    df.to_sql("silver_feedback_errors", conn, if_exists="replace", index=False)

# =========================================================
# MAIN ENTRY POINT
# =========================================================
if __name__ == "__main__":
    build_silver()
