import sqlite3
from pathlib import Path

# =========================================================
# Database Path Setup
# =========================================================
# Project Root → one folder above current script (e.g., /src/../)
DB_DIR = Path(__file__).resolve().parents[1]
DB_PATH = DB_DIR / "delivery_medallion.db"

# =========================================================
# Connection Function
# =========================================================
def get_connection():
    """
    Create and return a SQLite connection to the delivery_medallion.db file.
    Ensures that the database file is stored in the project root directory.
    """
    # Ensure directory exists
    DB_DIR.mkdir(exist_ok=True)

    # Connect to SQLite
    conn = sqlite3.connect(DB_PATH)

    # Enable foreign key constraints for data consistency
    conn.execute("PRAGMA foreign_keys = ON;")

    return conn

# =========================================================
# Optional: Quick connection test
# =========================================================
if __name__ == "__main__":
    conn = get_connection()
    print(f"✅ Connected to database: {DB_PATH.name}")
    conn.close()
