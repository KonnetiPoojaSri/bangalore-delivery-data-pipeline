import schedule
import time
import logging
from pathlib import Path
from etl import run_etl 

LOG_DIR = Path(__file__).resolve().parents[1] / "logs"
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    filename=LOG_DIR / "etl_automation.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

def run_etl_with_logging():
    logging.info("ETL Job Triggered")
    try:
        run_etl()

        logging.info("ETL Job Completed Successfully")

    except Exception as e:
        logging.error(f"ETL Job Failed: {e}")


run_etl_with_logging()

schedule.every().day.at("05:00").do(run_etl_with_logging)

logging.info("ETL Automation Service Started. Waiting for scheduled runs...")

while True:
    schedule.run_pending()
    time.sleep(60) 
