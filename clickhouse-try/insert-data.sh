#!/bin/sh

for year in $(seq 2023 2024); do
	for month in $(seq 1 12); do
		echo "Inserting $year-$month data"
		if [ "$month" -lt 10 ]; then
			/usr/local/bin/clickhouse-client --query="INSERT INTO trip_data FROM INFILE '/Users/reddy/Documents/GitHub/airfold-taxi-usecase/trip-data/fhvhv_tripdata_$year-0$month.parquet' FORMAT Parquet"
		else
            /usr/local/bin/clickhouse-client --query="INSERT INTO trip_data FROM INFILE '/Users/reddy/Documents/GitHub/airfold-taxi-usecase/trip-data/fhvhv_tripdata_$year-$month.parquet' FORMAT Parquet"
		fi
		echo "Inserted"
	done
done

# for month in $(seq 4 12); do
# 	echo "Inserting 2024-$month data"
# 	if [ "$month" -lt 10 ]; then
# 		/usr/local/bin/clickhouse-client --query="INSERT INTO trip_data FROM INFILE '/Users/reddy/Documents/GitHub/airfold-taxi-usecase/trip-data/fhvhv_tripdata_2024-0$month.parquet' FORMAT Parquet"
# 	else
# 		/usr/local/bin/clickhouse-client --query="INSERT INTO trip_data FROM INFILE '/Users/reddy/Documents/GitHub/airfold-taxi-usecase/trip-data/fhvhv_tripdata_2024-$month.parquet' FORMAT Parquet"
# 	fi
# 	echo "Inserted"
# done
