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
last_idx_file = os.path.join(data_dir, "last_idx.txt")

def read_idx():
    with open(last_idx_file, "r") as f:
        return int(f.read())

def write_idx(idx):
    with open(last_idx_file, "w+") as f:
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
            'Content-Type': 'application/json'
        },
        json=data
    )
    if not str(res.status_code).startswith('2'):
        logging.error(f"Data not appended for file {filename}")

def true_false_values(tablename, df):
    if tablename == "dim_time":
        return df.map(lambda x: 1 if x else 0) if hasattr(df, "applymap") else df.apply(lambda x: 1 if x else 0)
    else:
        return df.map(lambda x: 1 if x == 'Y' else 0) if hasattr(df, "applymap") else df.apply(lambda x: 1 if x == 'Y' else 0)

def transform_data(data):
    # dim_time
    dim_time = data[['request_datetime', 'on_scene_datetime', 'pickup_datetime', 'dropoff_datetime']].melt(
        value_name='full_datetime'
    )[['full_datetime']]
    dim_time = dim_time.drop_duplicates().reset_index(drop=True)

    dim_time['date'] = dim_time.full_datetime.dt.date
    # dim_time['time'] = dim_time.full_datetime.dt.time
    dim_time['hour'] = dim_time.full_datetime.dt.hour
    dim_time['minute'] = dim_time.full_datetime.dt.minute
    dim_time['second'] = dim_time.full_datetime.dt.second
    dim_time['year'] = dim_time.full_datetime.dt.year
    dim_time['quarter'] = dim_time.full_datetime.dt.quarter
    dim_time['month'] = dim_time.full_datetime.dt.month
    dim_time['day_of_month'] = dim_time.full_datetime.dt.day
    dim_time['day_of_week'] = dim_time.full_datetime.dt.day_of_week
    dim_time['day_name'] = dim_time.full_datetime.dt.day_name()
    dim_time['is_weekend'] = dim_time.day_of_week > 4
    cal = calendar()
    holidays = cal.holidays(start = dim_time['date'].min(),
                            end = dim_time['date'].max())

    dim_time['is_holiday'] = dim_time['date'].isin(holidays)

    dim_time[['full_datetime', 'date']] = dim_time[['full_datetime', 'date']].astype('str')
    dim_time[["is_weekend", "is_holiday"]] = true_false_values("dim_time", dim_time[["is_weekend", "is_holiday"]])

    # fact_trips
    data.rename(columns={
        'dispatching_base_num': 'dispatching_base_id', 
        'originating_base_num': 'originating_base_id',
        'request_datetime': 'request_time_id', 
        'on_scene_datetime': 'on_scene_time_id', 
        'pickup_datetime': 'pickup_time_id',
        'dropoff_datetime': 'dropoff_time_id', 
        'PULocationID': 'pickup_location_id', 
        'DOLocationID': 'dropoff_location_id'
        }, inplace=True)

    fact_trips = data[['trip_id', 'hvfhs_license_num', 'dispatching_base_id', 
                    'originating_base_id', 'request_time_id', 'on_scene_time_id', 
                    'pickup_time_id', 'dropoff_time_id', 'pickup_location_id', 
                    'dropoff_location_id', 'trip_miles', 'trip_time', 
                    'base_passenger_fare', 'tolls', 'bcf', 'sales_tax',
                    'congestion_surcharge', 'airport_fee', 'tips', 'driver_pay',
                    'shared_request_flag', 'shared_match_flag', 'access_a_ride_flag',
                    'wav_request_flag', 'wav_match_flag']].drop_duplicates().reset_index(drop=True)

    fact_trips[['shared_request_flag', 'shared_match_flag', 'access_a_ride_flag',
                'wav_request_flag', 'wav_match_flag']] = true_false_values("fact_trips", 
                                                                           fact_trips[['shared_request_flag', 'shared_match_flag', 
                                                                                       'access_a_ride_flag', 'wav_request_flag', 
                                                                                       'wav_match_flag']])

    # Write to csv
    filename = "{}{}{}"
    max_time = str(dim_time.full_datetime.max())
    max_time = max_time.replace("-", "").replace(" ", "_").replace(":", "")

    dim_time_filename = os.path.join(transformed_data_dir, filename.format("dim_time_", max_time, ".json"))
    fact_trips_filename = os.path.join(transformed_data_dir, filename.format("fact_trips_", max_time, ".json"))

    dim_time.to_json(dim_time_filename, orient='records')
    fact_trips.to_json(fact_trips_filename, orient='records')
    # dim_time.to_csv(dim_time_filename, index=False)
    # fact_trips.to_csv(fact_trips_filename, index=False)

    # Call af source append
    # append_source("dim_time", dim_time_filename)
    # append_source("fact_trips", fact_trips_filename)


def ingest_data(file):
    data = pd.read_parquet(os.path.join(data_dir, file))
    data = data.dropna().reset_index(drop=True)
    data = data.drop_duplicates().reset_index(drop=True)
    data = data[data['hvfhs_license_num']=='HV0003'].copy()
    data = data.sample(10000, random_state=1)

    step = 1000
    total_rows = data.shape[0]
    if not os.path.exists(last_idx_file):
        write_idx(0)

    start_idx = read_idx()
    end_idx = start_idx+total_rows
    data['trip_id'] = np.arange(start_idx, end_idx)

    # client = Client(n_workers=2, threads_per_worker=1)

    # logging.info("Starting parallel transformation")
    # futures = []
    for idx, i in enumerate(range(0, total_rows, step)):
        end_i = min(i + step, total_rows)
        logging.info(f"Ingesting data part {idx} of {total_rows//step}")
        # transform_data(data[i:end_i].copy())
        # data[i:end_i].to_json(os.path.join(transformed_data_dir, f"data_{i}_{end_i}.json"))
        new_data = data[i:end_i]
        filename = os.path.join(transformed_data_dir, f"data_{start_idx+i}_{start_idx+end_i}.json")
        new_data.to_json(filename, orient='records')
        append_source("trip_data", filename)
        time.sleep(2)
    write_idx(end_idx)
    #     futures.append(future)
    
    # for future in futures:
    #     future.result()
        
if __name__=="__main__":
    ingest_data()