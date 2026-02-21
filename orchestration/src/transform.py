#!/usr/bin/env python3
"""Transform raw csv to a transformed parquet file."""

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
from shared import *

COLUMNS_TO_KEEP = [
	"FlightDate",
	"Year",
	"Month",
	"DayOfMonth",
	"DayOfWeek",
	"Operating_Airline",
	"Flight_Number_Marketing_Airline",
	"Tail_Number",
	"Distance",
	"Origin",
	"OriginCityName",
	"OriginStateName",
	"Dest",
	"DestCityName",
	"DestStateName",
	"CRSElapsedTime",
	"ActualElapsedTime",
	"Cancelled",
	"CancellationCode",
	"Diverted",
	"ArrDelayMinutes",
	"DepDelayMinutes",
	"CarrierDelay",
	"WeatherDelay",
	"NASDelay",
	"SecurityDelay",
	"LateAircraftDelay",
	"Div1Airport",
	"Div2Airport",
	"Div3Airport",
	"Div4Airport",
	"Div5Airport",
]

def main():
	spark = None
	try:
		spark = build_spark_session("Transform Raw Data")

		source_path = "/data/raw/Flights_*.csv"
		df = spark.read.csv(source_path, header=True, inferSchema=True)
		new_column_names = [c.strip() for c in df.columns]
		df = df.toDF(*new_column_names)

		df = df.withColumn("file_year", F.regexp_extract(F.input_file_name(), r"Flights_(\d{4})", 1))

		years_list = [row[0] for row in df.select("file_year").distinct().collect() if row[0]]

		for year in years_list:
				print(f"Processing year: {year}...")

				(df.filter(F.col("file_year") == year)
					.select(COLUMNS_TO_KEEP)
					.orderBy("FlightDate")
					.coalesce(1)
					.write
					.mode("overwrite")
					.parquet(f"/data/transformed/Flights_Curated_{year}.parquet"))

		print(f"✓ Done! Files are available in HDFS at /data/transformed")
	except Exception as e:
		print(f"✗ Failed to process data: {e}")
	finally:
		if spark:
			spark.stop()

if __name__ == "__main__":
	main()