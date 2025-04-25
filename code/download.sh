#!/bin/sh

for year in $(seq 2023 2024); do
	for month in $(seq 1 12); do
		if [ "$month" -lt 10 ]; then
			wget "https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_$year-0$month.parquet"
		else
			wget "https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_$year-$month.parquet"
		fi
	done
done
