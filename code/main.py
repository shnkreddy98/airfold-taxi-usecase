from ingest import ingest_data, write_idx, read_idx
from logging_utils import setup_logging

import logging
import os
import sys

files = ["fhvhv_tripdata_2023-01.parquet", "fhvhv_tripdata_2023-02.parquet", 
         "fhvhv_tripdata_2023-03.parquet", "fhvhv_tripdata_2023-04.parquet", 
         "fhvhv_tripdata_2023-05.parquet", "fhvhv_tripdata_2023-06.parquet", 
         "fhvhv_tripdata_2023-07.parquet", "fhvhv_tripdata_2023-08.parquet", 
         "fhvhv_tripdata_2023-09.parquet", "fhvhv_tripdata_2023-10.parquet", 
         "fhvhv_tripdata_2023-11.parquet", "fhvhv_tripdata_2023-12.parquet", 
         "fhvhv_tripdata_2024-01.parquet", "fhvhv_tripdata_2024-02.parquet", 
         "fhvhv_tripdata_2024-03.parquet", "fhvhv_tripdata_2024-04.parquet", 
         "fhvhv_tripdata_2024-05.parquet", "fhvhv_tripdata_2024-06.parquet", 
         "fhvhv_tripdata_2024-07.parquet", "fhvhv_tripdata_2024-08.parquet", 
         "fhvhv_tripdata_2024-09.parquet", "fhvhv_tripdata_2024-10.parquet", 
         "fhvhv_tripdata_2024-11.parquet", "fhvhv_tripdata_2024-12.parquet"]

last_file = './trip-data/last_file_{}.txt'
last_idx_file = './trip-data/last_idx_{}.txt'

if __name__ == "__main__":
    log_file = setup_logging()
    part = sys.argv[1]
    last_file = last_file.format(part)
    last_idx_file = last_idx_file.format(part)

    if not os.path.exists(last_file):
        write_idx(last_file, int(part)*6)

    start_idx = int(read_idx(last_file))

    part_files = files[start_idx:int(part)*6+6]

    for idx, file in enumerate(part_files):
        logging.info(f"Ingesting {part_files[idx]}")
        ingest_data(file, last_idx_file, part)
        logging.info("Data Ingested")
        write_idx(last_file, idx)
        write_idx(last_idx_file, 0)

# if __name__ == "__main__":
#     log_file = setup_logging()

#     if not os.path.exists(last_file):
#         write_idx(last_file, '202301')

#     start_file = read_idx(last_file)
#     start_year = int(start_file[:4])
#     start_month = int(start_file[4:])

#     for year in range(start_year, 2024):
#         month_start = start_month if year == start_year else 1
#         for month in range(month_start, 13):
#             file = parquet_files_fmt.format(year, f"{month:02d}")
#             logging.info(f"Ingesting {file}")
#             ingest_data(file)
#             logging.info("Data Ingested")
#             write_idx(last_file, str(year)+str(f"{month:02d}"))
#             write_idx(last_idx_file, 0)
