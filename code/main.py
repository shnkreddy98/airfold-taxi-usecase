from dask.distributed import Client
from dotenv import load_dotenv
import json
import os
import pandas as pd
from pandas.tseries.holiday import USFederalHolidayCalendar as calendar
import requests
import sys

load_dotenv()

data_dir = './trip-data'
transformed_data_dir = os.path.join(data_dir, "new_data")
if not os.path.exists(transformed_data_dir):
    os.makedirs(transformed_data_dir)

def append_source(table, filename):
    print("Appending {} data to airfold to table {}".format(filename,table))
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
    if 200<=res.status_code<300:
        print("Data Appended")
    else:
        print("Error %s", res.status_code)

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
    append_source("dim_time", dim_time_filename)
    append_source("fact_trips", fact_trips_filename)


def main():
    file = sys.argv[1]
    data = pd.read_parquet(os.path.join(data_dir, file))
    data = data.dropna().reset_index(drop=True)
    data = data.drop_duplicates().reset_index(drop=True)
    data['trip_id'] = data.index

    client = Client()
    step = 10000
    end = data.shape[0]

    print("Starting parallel transformation")
    for start in range(0, end, step):
        if start+step>end:
            client.submit(transform_data, data[start:end].copy())
            # transform_data(data[start:end].copy())
        else:
            client.submit(transform_data, data[start:start+step].copy())
            # transform_data(data[start:start+step].copy())
    
if __name__=="__main__":
    main()