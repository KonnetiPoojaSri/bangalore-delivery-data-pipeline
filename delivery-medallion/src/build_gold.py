import pandas as pd
from db import get_connection

# =========================================================
# MASTER FUNCTION
# =========================================================
def build_gold():
    conn = get_connection()
    print("Starting Gold Layer...")

    build_gold_order_performance(conn)
    build_gold_restaurant_performance(conn)
    build_gold_rider_performance(conn)
    build_gold_issue_analysis(conn)
    build_gold_external_factors_impact(conn)

    conn.close()
    print("Gold Layer completed successfully.")

# =========================================================
# 1️⃣ ORDER PERFORMANCE
# =========================================================
def build_gold_order_performance(conn):
    df_orders = pd.read_sql("SELECT * FROM silver_orders", conn)
    df_kitchen = pd.read_sql("SELECT * FROM silver_kitchen_orders", conn)
    df_delivery = pd.read_sql("SELECT * FROM silver_delivery_log", conn)

    # Join all three
    df = (
        df_orders
        .merge(df_kitchen, on="order_id", how="left")
        .merge(df_delivery, on="order_id", how="left")
    )

    # Convert time columns
    for col in ["order_time", "prep_start", "prep_end", "handover_time", "delivered_time"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col])

    # Derived metrics
    df["prep_duration"] = (df["prep_end"] - df["prep_start"]).dt.total_seconds() / 60
    df["delivery_duration"] = (df["delivered_time"] - df["handover_time"]).dt.total_seconds() / 60
    df["total_order_time"] = (df["delivered_time"] - df["order_time"]).dt.total_seconds() / 60

    gold = df.groupby("order_status").agg(
        total_orders=("order_id", "count"),
        avg_prep_minutes=("prep_duration", "mean"),
        avg_delivery_minutes=("delivery_duration", "mean"),
        avg_total_order_minutes=("total_order_time", "mean"),
    ).reset_index()

    gold.to_sql("gold_order_performance", conn, if_exists="replace", index=False)
    print("gold_order_performance created")

# =========================================================
# 2️⃣ RESTAURANT PERFORMANCE
# =========================================================
def build_gold_restaurant_performance(conn):
    df = pd.read_sql("SELECT * FROM silver_kitchen_orders", conn)
    df["prep_start"] = pd.to_datetime(df["prep_start"])
    df["prep_end"] = pd.to_datetime(df["prep_end"])
    df["actual_prep_mins"] = (df["prep_end"] - df["prep_start"]).dt.total_seconds() / 60

    gold = df.groupby("restaurant_id").agg(
        total_orders=("order_id", "count"),
        avg_prep_minutes=("actual_prep_mins", "mean"),
        late_preps=("actual_prep_mins", lambda x: (x > 30).sum()),
    ).reset_index()

    gold["late_prep_pct"] = gold["late_preps"] / gold["total_orders"] * 100
    gold.to_sql("gold_restaurant_performance", conn, if_exists="replace", index=False)
    print("gold_restaurant_performance created")

# =========================================================
# 3️⃣ RIDER PERFORMANCE
# =========================================================
def build_gold_rider_performance(conn):
    df = pd.read_sql("SELECT * FROM silver_delivery_log", conn)
    df["rider_arrival"] = pd.to_datetime(df["rider_arrival"])
    df["handover_time"] = pd.to_datetime(df["handover_time"])
    df["delivered_time"] = pd.to_datetime(df["delivered_time"])

    df["delivery_duration"] = (df["delivered_time"] - df["handover_time"]).dt.total_seconds() / 60
    df["arrival_to_handover"] = (df["handover_time"] - df["rider_arrival"]).dt.total_seconds() / 60

    rider_perf = df.groupby("rider_id").agg(
        total_deliveries=("delivery_id", "count"),
        avg_delivery_minutes=("delivery_duration", "mean"),
        avg_wait_before_handover=("arrival_to_handover", "mean"),
        delayed_deliveries=("delivery_delay_mins", lambda x: (x > 15).sum())
    ).reset_index()

    rider_perf["delay_pct"] = rider_perf["delayed_deliveries"] / rider_perf["total_deliveries"] * 100
    rider_perf.to_sql("gold_rider_performance", conn, if_exists="replace", index=False)
    print("gold_rider_performance created")

# =========================================================
# 4️⃣ ISSUE / FEEDBACK ANALYSIS
# =========================================================
def build_gold_issue_analysis(conn):
    df = pd.read_sql("SELECT * FROM silver_feedback_errors", conn)
    gold = df.groupby("complaint_tag").agg(
        total_issues=("feedback_id", "count"),
        avg_rating=("rating_stars", "mean"),
        refund_count=("refund_issued", lambda x: (x == 'Yes').sum()),
    ).reset_index()

    gold["refund_rate_pct"] = gold["refund_count"] / gold["total_issues"] * 100
    gold.to_sql("gold_issue_analysis", conn, if_exists="replace", index=False)
    print("gold_issue_analysis created")

# =========================================================
# 5️⃣ EXTERNAL FACTORS IMPACT
# =========================================================
def build_gold_external_factors_impact(conn):
    df = pd.read_sql("""
        SELECT e.order_id, e.weather, e.zone, e.traffic_level, d.delivery_delay_mins
        FROM silver_external_factors e
        JOIN silver_delivery_log d ON e.order_id = d.order_id
    """, conn)

    gold = df.groupby(["weather", "traffic_level"]).agg(
        avg_delay_mins=("delivery_delay_mins", "mean"),
        order_count=("order_id", "count")
    ).reset_index()

    gold.to_sql("gold_external_factors_impact", conn, if_exists="replace", index=False)
    print("gold_external_factors_impact created")

# =========================================================
# MAIN ENTRY POINT
# =========================================================
if __name__ == "__main__":
    build_gold()
