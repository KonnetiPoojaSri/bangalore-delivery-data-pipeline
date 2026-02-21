import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from db import get_connection

sns.set(style="whitegrid")

# =========================================================
# 1️⃣ ORDER PERFORMANCE
# =========================================================
conn = get_connection()
df_order = pd.read_sql("SELECT * FROM gold_order_performance", conn)
conn.close()

plt.figure(figsize=(8, 5))
sns.barplot(x="order_status", y="avg_total_order_minutes", data=df_order, palette="Blues_d")
plt.title("Average Total Order Time by Order Status (mins)")
plt.xlabel("Order Status")
plt.ylabel("Avg Total Order Time (mins)")
plt.show()

# =========================================================
# 2️⃣ RESTAURANT PERFORMANCE
# =========================================================
conn = get_connection()
df_restaurant = pd.read_sql("SELECT * FROM gold_restaurant_performance", conn)
conn.close()

plt.figure(figsize=(10, 5))
sns.barplot(x="restaurant_id", y="avg_prep_minutes", data=df_restaurant, palette="Greens_d")
plt.title("Average Prep Time per Restaurant (mins)")
plt.xlabel("Restaurant ID")
plt.ylabel("Avg Prep Time (mins)")
plt.show()

plt.figure(figsize=(10, 5))
sns.barplot(x="restaurant_id", y="late_prep_pct", data=df_restaurant, palette="Reds_d")
plt.title("Late Preparation % per Restaurant")
plt.xlabel("Restaurant ID")
plt.ylabel("Late Prep %")
plt.show()

# =========================================================
# 3️⃣ RIDER PERFORMANCE
# =========================================================
conn = get_connection()
df_rider = pd.read_sql("""
    SELECT rider_id, avg_delivery_minutes, delay_pct
    FROM gold_rider_performance
    ORDER BY avg_delivery_minutes ASC
    LIMIT 5
""", conn)
conn.close()

plt.figure(figsize=(10, 5))
sns.barplot(x="rider_id", y="avg_delivery_minutes", data=df_rider, palette="Purples_d")
plt.title("Top 5 Fastest Riders (Avg Delivery Time)")
plt.xlabel("Rider ID")
plt.ylabel("Avg Delivery Time (mins)")
plt.show()

plt.figure(figsize=(10, 5))
sns.barplot(x="rider_id", y="delay_pct", data=df_rider, palette="Oranges_d")
plt.title("Top 5 Riders by Delay Percentage")
plt.xlabel("Rider ID")
plt.ylabel("Delay %")
plt.show()

# =========================================================
# 4️⃣ FEEDBACK / ISSUE ANALYSIS
# =========================================================
conn = get_connection()
df_issues = pd.read_sql("SELECT * FROM gold_issue_analysis", conn)
conn.close()

plt.figure(figsize=(10, 5))
sns.barplot(x="complaint_tag", y="total_issues", data=df_issues, palette="coolwarm")
plt.title("Number of Complaints by Tag")
plt.xlabel("Complaint Tag")
plt.ylabel("Total Complaints")
plt.xticks(rotation=45)
plt.show()

plt.figure(figsize=(10, 5))
sns.barplot(x="complaint_tag", y="refund_rate_pct", data=df_issues, palette="mako")
plt.title("Refund Rate by Complaint Tag (%)")
plt.xlabel("Complaint Tag")
plt.ylabel("Refund %")
plt.xticks(rotation=45)
plt.show()

# =========================================================
# 5️⃣ EXTERNAL FACTORS IMPACT
# =========================================================
conn = get_connection()
df_external = pd.read_sql("SELECT * FROM gold_external_factors_impact", conn)
conn.close()

plt.figure(figsize=(10, 6))
sns.barplot(x="weather", y="avg_delay_mins", hue="traffic_level", data=df_external, palette="Spectral")
plt.title("Average Delivery Delay by Weather and Traffic Level")
plt.xlabel("Weather Condition")
plt.ylabel("Avg Delivery Delay (mins)")
plt.legend(title="Traffic Level")
plt.show()
