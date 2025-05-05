from ingest import ingest_data
from logging_utils import setup_logging

import logging
import os

if __name__ == "__main__":
    log_file = setup_logging()

    data_files = [file for file in os.listdir("./trip-data") if file.endswith(".parquet")]
    for idx, file in enumerate(data_files):
        logging.info(f"Ingesting {idx+1} of {len(data_files)} files")
        logging.info(f"Ingesting {file}")
        ingest_data(file)
        logging.info("Data Ingested")
