from ingest import ingest_data, write_idx, read_idx
from logging_utils import setup_logging

import logging
import os

parquet_files_fmt = 'fhvhv_tripdata_{}-{}.parquet' 
last_file = './trip-data/last_file.txt'
last_idx_file = './trip-data/last_idx.txt'

if __name__ == "__main__":
    log_file = setup_logging()

    if not os.path.exists(last_file):
        write_idx(last_file, '202301')

    start_file = read_idx(last_file)
    start_year = int(start_file[:4])
    start_month = int(start_file[4:])

    for year in range(start_year, 2024):
        month_start = start_month if year == start_year else 1
        for month in range(month_start, 13):
            file = parquet_files_fmt.format(year, f"{month:02d}")
            logging.info(f"Ingesting {file}")
            ingest_data(file)
            logging.info("Data Ingested")
            write_idx(last_file, str(year)+str(f"{month:02d}"))
            write_idx(last_idx_file, 0)
