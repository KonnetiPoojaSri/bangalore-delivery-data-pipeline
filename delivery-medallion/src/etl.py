import time
import logging
import os
from load_bronze import load_all_bronze
from build_silver import build_silver
from build_gold import build_gold

LOG_DIR = os.path.join(os.path.dirname(__file__), "../logs")
os.makedirs(LOG_DIR, exist_ok=True)

LOG_PATH = os.path.join(LOG_DIR, "etl_pipeline.log")

logging.basicConfig(
    filename=LOG_PATH,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

def run_etl():
    start_time = time.time()
    logging.info(" ETL Pipeline started")
    print("ETL Pipeline started...")

    try:

        logging.info("Starting Bronze Layer...")
        print("Running Bronze Layer...")
        load_all_bronze()
        logging.info(" Bronze Layer completed successfully.")
        print("Bronze Layer completed.")

        logging.info("Starting Silver Layer...")
        print("Running Silver Layer...")
        build_silver()
        logging.info(" Silver Layer completed successfully.")
        print("Silver Layer completed.")


        logging.info("Starting Gold Layer...")
        print("Running Gold Layer...")
        build_gold()
        logging.info(" Gold Layer completed successfully.")
        print("Gold Layer completed.")

        end_time = time.time()
        elapsed = end_time - start_time
        logging.info(f"ETL Pipeline completed in {elapsed:.2f} seconds.")
        print(f"ETL Pipeline completed in {elapsed:.2f} seconds.")

    except Exception as e:
        logging.error(f"ETL Pipeline failed: {str(e)}", exc_info=True)
        print(f"ETL failed: {str(e)}")


if __name__ == "__main__":
    run_etl()
