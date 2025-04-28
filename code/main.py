from ingest import ingest_data
from logging_utils import setup_logging

import logging
import os

def read_idx():
    with open("file_idx.txt", "r") as f:
        idx = int(f.read())
    return idx

def write_idx(idx):
    with open("file_idx.txt", "w+") as f:
        f.write(idx)

if __name__ == "__main__":
    data_files = ["fhvhv_tripdata_2023-01.parquet", "fhvhv_tripdata_2023-02.parquet", 
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

    log_file = setup_logging()

    try:
        idx = read_idx()
    except:
        idx = 0
        write_idx(idx)

    file = data_files[idx]

    logging.info(f"Ingesting {file} to airfold")
    ingest_data(file)

    logging.info("Data Ingested")
    write_idx(idx+1)