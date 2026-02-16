#!/usr/bin/env python3
"""Process data from HDFS and save to MongoDB."""

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.ml.feature import Bucketizer
from shared import *

def calculate_airline_stats(spark: SparkSession, df: DataFrame):
	print("Calculating airline stats...")

	mongo_collection = "airline_stats"
	airline_stats = df.groupBy(F.col("Operating_Airline").alias("AirlineCode")).agg(
		F.avg(
			F.abs(F.col("ActualElapsedTime") - F.col("CRSElapsedTime"))
		).alias("ErrorMinutes"),
		F.count("*").alias("FlightCount"),
		F.count(F.when(F.col("ArrDelayMinutes") > DELAY_THRESHOLD, 1)).alias("DelayedFlightCount"),
	) \
	.withColumn(
		"OnTimePerformance",
		(1.0 - (F.col("DelayedFlightCount") / F.col("FlightCount"))) * 100
	) \
	.drop("FlightCount", "DelayedFlightCount") \
	.orderBy(F.col("OnTimePerformance").desc())

	airline_stats = join_with_airlines(spark, airline_stats)

	save_to_mongodb(airline_stats, mongo_collection)

def calculate_airport_departure_delays(spark: SparkSession, df: DataFrame):
	print("Calculating airport departure delays...")

	mongo_collection = "airport_departure_delays"
	airport_departure_delays_result = df.groupBy("Origin", "OriginCityName", "OriginStateName") \
		.agg(F.avg(F.col("DepDelayMinutes")).alias("AvgDepDelayMinutes")) \
		.orderBy(F.col("AvgDepDelayMinutes").desc())

	airport_departure_delays_result = airport_departure_delays_result \
		.withColumnRenamed("Origin", "AirportCode")

	save_to_mongodb(airport_departure_delays_result, mongo_collection)

def calculate_busiest_airports(spark: SparkSession, df: DataFrame):
	print("Calculating busiest airports...")

	mongo_collection = "busiest_airports"
	busiest_airports_result = df.groupBy("Origin", "OriginCityName", "OriginStateName") \
		.agg(F.count(F.col("Origin")).alias("FlightCount")) \
		.orderBy(F.col("FlightCount").desc())

	busiest_airports_result = busiest_airports_result \
		.withColumnRenamed("Origin", "AirportCode")

	save_to_mongodb(busiest_airports_result, mongo_collection)

def calculate_busiest_weeks(spark: SparkSession, df: DataFrame):
	print("Calculating busiest weeks...")

	mongo_collection = "busiest_weeks"
	busiest_weeks_result = df.groupBy(
			F.col("Month"),
			F.weekofyear(F.col("FlightDate")).alias("WeekOfYear")) \
		.agg(
			F.count("*").alias("FlightCount"),
			F.count(F.when(F.col("ArrDelayMinutes") > 15.0, 1)).alias("DelayedFlightCount"),
		) \
		.withColumn(
			"DelayedFlightPercent",
			F.when(F.col("FlightCount") > 0, F.col("DelayedFlightCount") / F.col("FlightCount") * 100).otherwise(None)
		) \
		.orderBy(F.col("FlightCount").desc())

	save_to_mongodb(busiest_weeks_result, mongo_collection)

def calculate_delay_reasons(spark: SparkSession, df: DataFrame):
	print("Calculating delay reasons...")

	mongo_collection = "delay_reasons"
	delay_reasons_result = df.agg(
		F.avg("CarrierDelay").alias("AvgCarrierDelayMinutes"),
		F.avg("WeatherDelay").alias("AvgWeatherDelayMinutes"),
		F.avg("NASDelay").alias("AvgNASDelayMinutes"),
		F.avg("SecurityDelay").alias("AvgSecurityDelayMinutes"),
		F.avg("LateAircraftDelay").alias("AvgLateAircraftDelayMinutes"),
	)

	save_to_mongodb(delay_reasons_result, mongo_collection)

def calculate_days_with_cancellations(spark: SparkSession, df: DataFrame):
	print("Calculating days with cancellations...")

	mongo_collection = "days_with_cancellations"
	days_with_cancellations_result = df.groupBy("DayOfWeek") \
		.agg(
			F.count("*").alias("FlightCount"),
			F.count(F.when(F.col("Cancelled") == 1, 1)).alias("CancellationCount")) \
		.withColumn(
			"CancellationPercent",
			F.when(F.col("FlightCount") > 0, F.col("CancellationCount") / F.col("FlightCount") * 100).otherwise(None)
		) \
		.orderBy(F.col("CancellationPercent").desc())

	save_to_mongodb(days_with_cancellations_result, mongo_collection)

def calculate_distance_delay_correlation(spark: SparkSession, df: DataFrame):
	print("Calculating distance delay correlation...")

	mongo_collection = "distance_delay_correlation"
	splits = [0, 250, 500, 750, 1000, 1250, 1500, 1750, 2000, 2250, 2500, float('inf')]

	bucketizer = Bucketizer(splits=splits, inputCol="Distance", outputCol="DistanceCategory")
	df_bucketed = bucketizer.transform(df)

	bucket_labels = [f"{int(splits[i])}-{int(splits[i+1])}" for i in range(len(splits) - 2)]
	bucket_labels.append(f"{int(splits[-2])}+")
	label_expr = F.coalesce(
		*[F.when(F.col("DistanceCategory") == i, label) for i, label in enumerate(bucket_labels)]
	)
	df_bucketed = df_bucketed.withColumn("DistanceBucket", label_expr)

	result = (df_bucketed.groupBy("DistanceCategory", "DistanceBucket")
		.agg(
			F.count(F.when(F.col("ArrDelayMinutes") > DELAY_THRESHOLD, 1)).alias("DelayedFlightCount"),
			F.avg("Distance").alias("AvgDistance"),
			F.count("*").alias("FlightCount"))
		.withColumn("ProbabilityOfDelay", F.col("DelayedFlightCount") / F.col("FlightCount"))
		.drop("DelayedFlightCount", "FlightCount")
		.orderBy("DistanceCategory"))

	save_to_mongodb(result, mongo_collection)

def main():
	spark = None
	try:
		spark = build_spark_session("Process Flight Data")

		df = spark.read.parquet("/data/transformed/*.parquet", header=True, inferSchema=True)

		calculate_airline_stats(spark, df)
		calculate_airport_departure_delays(spark, df)
		calculate_busiest_airports(spark, df)
		calculate_busiest_weeks(spark, df)
		calculate_delay_reasons(spark, df)
		calculate_days_with_cancellations(spark, df)
		calculate_distance_delay_correlation(spark, df)
	except Exception as e:
		print(f"✗ Failed to process data: {e}")
	finally:
		if spark:
			spark.stop()

if __name__ == "__main__":
	main()