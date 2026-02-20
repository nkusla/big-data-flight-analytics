#!/usr/bin/env python3
"""
Script to calculate flights lookup data and write to Kafka for Global KTable.

Reads from HDFS /data/transformed/*.parquet
"""

import json
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from shared import build_spark_session
from confluent_kafka import Producer

FLIGHTS_LOOKUP_TOPIC = os.environ.get("FLIGHTS_LOOKUP_TOPIC")
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
AIRCRAFT_DB_PATH = os.environ.get("AIRCRAFT_DB_PATH")

def run_aggregation(spark: SparkSession, normalize_delay: bool = True, min_flights: int = 0):
	"""
	Aggregate CarrierDelay by aircraft (Tail_Number) and join to icao24.
	CarrierDelay = DOT/BTS delay attributed to the carrier (maintenance, crew, etc.), in minutes.
	If normalize_delay=True, adds DelayScore01: min-max normalized 0-1 (1 = most problematic).
	"""

	df = spark.read.parquet("/data/transformed/*.parquet")

	agg = (
		df.filter(F.col("Tail_Number").isNotNull() & (F.col("Tail_Number") != ""))
		.groupBy(F.col("Tail_Number").alias("TailNumber"))
		.agg(
			F.avg(F.col("CarrierDelay")).alias("AvgCarrierDelayMinutes"),
			F.count("*").alias("FlightCount"),
		)
		.withColumn("AvgCarrierDelayMinutes", F.round(F.col("AvgCarrierDelayMinutes"), 2))
	)

	# Join with aircraft database: registration = Tail_Number, keep only icao24
	aircraft = spark.read.option("header", True).csv(AIRCRAFT_DB_PATH).select(
		F.col("registration").alias("TailNumber"),
		F.col("icao24"),
	).dropDuplicates(["TailNumber"])

	result = (
		agg.join(aircraft, "TailNumber", "inner")
		.select("icao24", "AvgCarrierDelayMinutes", "FlightCount")
		.filter(F.col("FlightCount") >= min_flights)
		.orderBy("icao24")
	)

	if normalize_delay:
		stats = result.agg(
			F.min("AvgCarrierDelayMinutes").alias("min_d"),
			F.max("AvgCarrierDelayMinutes").alias("max_d"),
		).collect()[0]

		min_d, max_d = float(stats["min_d"] or 0), float(stats["max_d"] or 0)
		span = max_d - min_d if max_d > min_d else 1.0

		result = result.withColumn(
			"DelayScore01",
			F.round((F.col("AvgCarrierDelayMinutes") - min_d) / span, 4),
		)

	return result

def send_to_kafka(rows, topic: str, bootstrap_servers: str):
	conf = {
		"bootstrap.servers": bootstrap_servers,
		"client.id": "flights-lookup-producer",
	}
	producer = Producer(conf)

	def delivery_callback(err, msg):
		if err:
			print(f"Delivery failed: {err}")

	for row in rows:
		key = str(row.icao24)
		payload = {
			"icao24": key,
			"AvgCarrierDelayMinutes": float(row.AvgCarrierDelayMinutes) if row.AvgCarrierDelayMinutes is not None else None,
			"FlightCount": int(row.FlightCount),
		}
		if hasattr(row, "DelayScore01") and row.DelayScore01 is not None:
			payload["DelayScore01"] = round(float(row.DelayScore01), 4)
		value = json.dumps(payload)

		producer.produce(topic, key=key.encode("utf-8"), value=value.encode("utf-8"), callback=delivery_callback)

	producer.flush()
	print(f"✓ Written {len(rows)} records to Kafka topic '{topic}' (for Global KTable)")


def main():
	spark = None
	try:
		spark = build_spark_session("Flights Lookup to Kafka Global KTable")
		result_df = run_aggregation(spark)
		rows = result_df.collect()
		if not rows:
			print("No rows to send (empty aggregation or no data).")
			return
		send_to_kafka(rows, FLIGHTS_LOOKUP_TOPIC, KAFKA_BOOTSTRAP_SERVERS)
	except Exception as e:
		print(f"✗ Failed: {e}")
		raise
	finally:
		if spark:
			spark.stop()


if __name__ == "__main__":
	main()
