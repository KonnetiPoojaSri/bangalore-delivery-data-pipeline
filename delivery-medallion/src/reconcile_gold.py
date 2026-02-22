import pandas as pd
from db import get_connection

def reconcile_gold():
    conn = get_connection()
    print(" Reconciling Silver vs Gold Layer totals...\n")

    # =========================================================
    # 1️⃣ TOTAL ORDERS CHECK
    # =========================================================
    silver_orders = pd.read_sql("SELECT COUNT(*) AS total FROM silver_orders", conn)["total"][0]
    gold_orders = pd.read_sql("SELECT SUM(total_orders) AS total FROM gold_order_performance", conn)["total"][0]

    print(f" Silver total orders: {silver_orders}")
    print(f" Gold total orders (aggregated): {gold_orders}")
    print(f" Orders match: {silver_orders == gold_orders}\n")

    # =========================================================
    # 2️⃣ DELIVERY LOG CHECK (RIDER LEVEL)
    # =========================================================
    silver_deliveries = pd.read_sql("SELECT COUNT(*) AS total FROM silver_delivery_log", conn)["total"][0]
    gold_deliveries = pd.read_sql("SELECT SUM(total_deliveries) AS total FROM gold_rider_performance", conn)["total"][0]

    print(f"Silver total deliveries: {silver_deliveries}")
    print(f" Gold total deliveries (aggregated): {gold_deliveries}")
    print(f"Deliveries match: {silver_deliveries == gold_deliveries}\n")

    # =========================================================
    # 3️⃣ FEEDBACK / ISSUES CHECK
    # =========================================================
    silver_feedbacks = pd.read_sql("SELECT COUNT(*) AS total FROM silver_feedback_errors", conn)["total"][0]
    gold_issues = pd.read_sql("SELECT SUM(total_issues) AS total FROM gold_issue_analysis", conn)["total"][0]

    print(f" Silver total feedback issues: {silver_feedbacks}")
    print(f" Gold total feedback issues (aggregated): {gold_issues}")
    print(f" Feedback issues match: {silver_feedbacks == gold_issues}\n")

    # =========================================================
    # CLEANUP
    # =========================================================
    conn.close()
    print(" Reconciliation completed successfully.\n")


if __name__ == "__main__":
    reconcile_gold()
