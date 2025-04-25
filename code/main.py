import os
import pandas as pd
import sys

data_dir = './trip-data'

if __name__=="__main__":
    file = sys.argv[1]
    data = pd.read_parquet(os.path.join(data_dir, file))
    data = data.dropna().reset_index(drop=True)
    data = data.drop_duplicates().reset_index(drop=True)
    data['trip_id'] = data.index

    # dim_time
    # fact_trips
    
