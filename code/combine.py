import pandas as pd
import os

data_dir = "trip-data/new_data"
files = os.listdir(data_dir)

df_old = pd.DataFrame()

for file in files:
    if file.endswith(".csv"):
        df_new = pd.read_csv(os.path.join(data_dir, file))
        df_old = pd.concat([df_old, df_new], ignore_index=True)

df_old.to_csv(os.path.join(data_dir, "combined_data.csv"), index=False)