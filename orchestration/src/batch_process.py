#!/usr/bin/env python3
"""Process data from HDFS and save to MongoDB."""

from pyspark.sql import SparkSession, Window
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
		).alias("AvgErrorMinutes"),
		F.count("*").alias("FlightCount"),
		F.count(F.when(F.col("ArrDelayMinutes") > DELAY_THRESHOLD, 1)).alias("DelayedFlightCount"),
	) \
	.filter(F.col("FlightCount") > MIN_FLIGHTS_THRESHOLD) \
	.withColumn(
		"OnTimePerformance",
		(1.0 - (F.col("DelayedFlightCount") / F.col("FlightCount"))) * 100
	) \
	.drop("FlightCount", "DelayedFlightCount") \
	.orderBy(F.col("OnTimePerformance").desc())

	airline_stats = join_with_airlines_metadata(spark, airline_stats)

	save_to_mongodb(airline_stats, mongo_collection)

def calculate_airport_departure_delays(spark: SparkSession, df: DataFrame):
	print("Calculating airport departure delays...")

	mongo_collection = "airport_departure_delays"
	airport_departure_delays_result = df.groupBy("Origin", "OriginCityName", "OriginStateName") \
		.agg(
			F.avg(F.col("DepDelayMinutes")).alias("AvgDepDelayMinutes"),
			F.count("*").alias("FlightCount")
		) \
	.filter(F.col("FlightCount") > MIN_FLIGHTS_THRESHOLD) \
	.drop("FlightCount") \
	.orderBy(F.col("AvgDepDelayMinutes").desc())

	airport_departure_delays_result = airport_departure_delays_result \
		.withColumnRenamed("Origin", "AirportCode")

	airport_departure_delays_result = join_with_airports_metadata(spark, airport_departure_delays_result)

	save_to_mongodb(airport_departure_delays_result, mongo_collection)

def calculate_busiest_airports(spark: SparkSession, df: DataFrame):
	print("Calculating busiest airports...")

	mongo_collection = "busiest_airports"
	busiest_airports_result = df.groupBy("Origin", "OriginCityName", "OriginStateName") \
		.agg(F.count(F.col("Origin")).alias("FlightCount")) \
		.filter(F.col("FlightCount") > MIN_FLIGHTS_THRESHOLD) \
		.orderBy(F.col("FlightCount").desc()) \
		.withColumnRenamed("Origin", "AirportCode") \
		.withColumnRenamed("OriginCityName", "CityName") \
		.withColumnRenamed("OriginStateName", "StateName")

	w = Window.partitionBy()
	busiest_airports_result = busiest_airports_result \
		.withColumn("_min", F.min("FlightCount").over(w)) \
		.withColumn("_max", F.max("FlightCount").over(w)) \
		.withColumn("_total", F.sum("FlightCount").over(w)) \
		.withColumn(
			"BusynessScorePercent",
			F.when(F.col("_max") == F.col("_min"), 100.0).otherwise(
				(F.col("FlightCount") - F.col("_min")) / (F.col("_max") - F.col("_min")) * 100
			)
		) \
		.withColumn(
			"ShareOfTotalFlightsPercent",
			F.when(F.col("_total") > 0, F.col("FlightCount") / F.col("_total") * 100).otherwise(0.0)
		) \
		.drop("_min", "_max", "_total") \
		.orderBy(F.col("BusynessScorePercent").desc())

	busiest_airports_result = join_with_airports_metadata(spark, busiest_airports_result)

	top_10_busiest_airports_result = busiest_airports_result.limit(10)

	top_10_busiest_airports_result.write \
		.mode("overwrite") \
		.parquet("/data/curated/busiest_airports.parquet")

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
		.filter(F.col("FlightCount") > MIN_FLIGHTS_THRESHOLD) \
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
	).select(
		F.expr(
			"stack(5, "
			"'CarrierDelay', AvgCarrierDelayMinutes, "
			"'WeatherDelay', AvgWeatherDelayMinutes, "
			"'NASDelay', AvgNASDelayMinutes, "
			"'SecurityDelay', AvgSecurityDelayMinutes, "
			"'LateAircraftDelay', AvgLateAircraftDelayMinutes"
			") as (DelayReason, AvgDelayMinutes)"
		)
	).select("DelayReason", "AvgDelayMinutes")

	save_to_mongodb(delay_reasons_result, mongo_collection)

def calculate_days_with_cancellations(spark: SparkSession, df: DataFrame):
	print("Calculating days with cancellations...")

	mongo_collection = "days_with_cancellations"
	days_with_cancellations_result = df.groupBy("DayOfWeek") \
		.agg(
			F.count("*").alias("FlightCount"),
			F.count(F.when(F.col("Cancelled") == 1, 1)).alias("CancellationCount")) \
		.filter(F.col("FlightCount") > MIN_FLIGHTS_THRESHOLD) \
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
			F.count("*").alias("FlightCount")) \
		.filter(F.col("FlightCount") > MIN_FLIGHTS_THRESHOLD) \
		.withColumn("ProbabilityOfDelay", F.col("DelayedFlightCount") / F.col("FlightCount"))
		.drop("DelayedFlightCount", "FlightCount")
		.orderBy("DistanceCategory"))

	save_to_mongodb(result, mongo_collection)

def calculate_problematic_routes(spark: SparkSession, df: DataFrame):
	print("Calculating problematic routes...")

	mongo_collection = "problematic_routes"
	total_agg = df.agg(
		F.count("*").alias("_total"),
		F.count(F.when(F.col("ArrDelayMinutes") > DELAY_THRESHOLD, 1)).alias("_delayed"),
	).collect()[0]
	overall_delay_pct = (total_agg["_delayed"] / total_agg["_total"] * 100) if total_agg["_total"] > 0 else 0.0

	route_stats = df.groupBy(F.col("Origin").alias("OriginCode"), F.col("Dest").alias("DestCode"), F.col("OriginCityName"), F.col("DestCityName")).agg(
		F.count("*").alias("FlightCount"),
		F.count(F.when(F.col("ArrDelayMinutes") > DELAY_THRESHOLD, 1)).alias("DelayedFlightCount"),
	).filter(F.col("FlightCount") > MIN_FLIGHTS_THRESHOLD)

	route_delay_pct = F.col("DelayedFlightCount") / F.col("FlightCount") * 100
	problematic = route_stats \
		.withColumn("Route", F.concat(F.col("OriginCode"), F.lit("-"), F.col("DestCode"))) \
		.withColumn(
			"ProblematicScore",
			F.when(F.lit(overall_delay_pct) > 0, route_delay_pct / F.lit(overall_delay_pct)).otherwise(F.lit(1.0))
		) \
		.filter(F.col("ProblematicScore") >= 1.2) \
		.orderBy(F.col("ProblematicScore").desc()) \
		.select("Route", "OriginCode", "DestCode", "OriginCityName", "DestCityName", "FlightCount", "ProblematicScore")

	save_to_mongodb(problematic, mongo_collection)

def calculate_diverted_flights_by_airport(spark: SparkSession, df: DataFrame):
	print("Calculating diverted flights by airport...")

	mongo_collection = "diverted_flights_by_airport"
	div_cols = ["Div1Airport", "Div2Airport", "Div3Airport", "Div4Airport", "Div5Airport"]

	diverted = df.filter(F.col("Diverted") == 1)
	stack_expr = ", ".join(
		f"'{c}', {c}" for c in div_cols
	)
	unpivoted = diverted.select(
		F.expr(f"stack(5, {stack_expr}) as (_, AirportCode)")
	).select("AirportCode")

	diverted_counts = (
		unpivoted
		.filter(F.col("AirportCode").isNotNull() & (F.trim(F.col("AirportCode")) != ""))
		.groupBy("AirportCode")
		.agg(F.count("*").alias("DivertedFlightCount"))
		.orderBy(F.col("DivertedFlightCount").desc())
	).filter(F.col("DivertedFlightCount") > MIN_FLIGHTS_THRESHOLD)

	diverted_counts = join_with_airports_metadata(spark, diverted_counts)
	save_to_mongodb(diverted_counts, mongo_collection)


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
		calculate_problematic_routes(spark, df)
		calculate_diverted_flights_by_airport(spark, df)
	except Exception as e:
		print(f"✗ Failed to process data: {e}")
	finally:
		if spark:
			spark.stop()

if __name__ == "__main__":
	main()