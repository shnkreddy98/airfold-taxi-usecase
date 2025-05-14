from dask.distributed import Client
from dotenv import load_dotenv
from pandas.tseries.holiday import USFederalHolidayCalendar as calendar

import logging
import json
import numpy as np
import os
import pandas as pd
import requests
import time

load_dotenv()

data_dir = './trip-data'
transformed_data_dir = os.path.join(data_dir, "json_data")
if not os.path.exists(transformed_data_dir):
    os.makedirs(transformed_data_dir)

def read_idx(file):
    with open(file, "r") as f:
        return f.read()

def write_idx(file, idx):
    with open(file, "w+") as f:
        f.write(str(idx))

def append_source(table, filename):
    logging.info(f"Appending Data {filename} to Airfold")
    auth = os.getenv('auth_code')

    with open(filename, 'r') as f:
        data = json.load(f)
    url = 'https://api.us.airfold.co/v1/events/{}'

    res = requests.post(
        url.format(table),
        headers={
            'Authorization': 'Bearer {}'.format(auth),
            'Content-Type': 'application/json',
            'Connection': 'keep-alive'
        },
        json=data
    )
    time.sleep(0.5)
    if not str(res.status_code).startswith('2'):
        logging.error(f"Data not appended for file {filename}")

def ingest_data(file, last_idx_file, part):
    data = pd.read_parquet(os.path.join(data_dir, file))
    data = data.dropna().reset_index(drop=True)
    data = data.drop_duplicates().reset_index(drop=True)
    data = data[data['hvfhs_license_num']=='HV0003'].copy()

    step = 10000
    total_rows = data.shape[0]
    if not os.path.exists(last_idx_file):
        write_idx(last_idx_file, 0)

    start_idx = int(read_idx(last_idx_file))
    avg_time = 0
    sum_time = 0

    for idx, i in enumerate(range(start_idx, total_rows, step)):
        batch_start_time = time.time()
        try:
            logging.info(f"Ingesting data part {idx} of {total_rows//step}")
            new_data = data[i:i+step]
            filename = os.path.join(transformed_data_dir, f"data_{part}_{i}_{i+step}.json")
            new_data.to_json(filename, orient='records')
            append_source("trip_data", filename)
            write_idx(last_idx_file, i)
            os.remove(filename)
            logging.info(f"Appended {filename} successfuly")
            sum_time = sum_time+(time.time()-batch_start_time)
            avg_time = sum_time/(idx+1)

            logging.info(f"Average time for the batch {avg_time} seconds")
            logging.info(f"Total time needed for current month {(avg_time)*(total_rows//step)} seconds")
        except Exception as e:
            logging.error(f"Error logging {file} at {i}")
            logging.error(e)
            write_idx(last_idx_file, i)
            exit()

if __name__=="__main__":
    ingest_data()