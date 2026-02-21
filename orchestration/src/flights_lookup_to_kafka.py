#!/usr/bin/env python3

import json
import os
from pyspark.sql import SparkSession
from pyspark.sql import Window
import pyspark.sql.functions as F
from shared import build_spark_session, refresh_topic, AIRLINES_DB_PATH
from confluent_kafka import Producer

FLIGHTS_LOOKUP_TOPIC = os.environ.get("FLIGHTS_LOOKUP_TOPIC")
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")


def hhmm_to_hhmm_str(col):
	return F.format_string("%02d:%02d", F.floor(F.col(col) / 100), F.col(col) % 100)


def run_aggregation(spark: SparkSession):
	df = spark.read.parquet("/data/transformed/*.parquet")
	airlines = spark.read.csv(AIRLINES_DB_PATH, header=True, inferSchema=True) \
		.withColumnRenamed("Name", "AirlineName") \
		.filter(F.length(F.trim(F.coalesce(F.col("ICAO"), F.lit("")))) > 0)

	df = df.join(airlines, df.IATA == airlines.IATA, "inner") \
		.withColumn("callsign", F.concat(F.col("ICAO"), F.col("Flight_Number_Operating_Airline").cast("string"))) \
		.drop("IATA", "ICAO") \
		.filter(F.length(F.trim(F.col("callsign"))) > 0)

	arr_counts = df.groupBy("callsign", "CRSArrTime").agg(
		F.count("*").alias("cnt"),
		F.first("AirlineName").alias("AirlineName"),
	)

	dep_counts = df.groupBy("callsign", "CRSDepTime").agg(F.count("*").alias("cnt"))

	w_arr = Window.partitionBy("callsign").orderBy(F.desc("cnt"), F.asc("CRSArrTime"))
	w_dep = Window.partitionBy("callsign").orderBy(F.desc("cnt"), F.asc("CRSDepTime"))

	arr_mode = arr_counts.withColumn("rn", F.row_number().over(w_arr)) \
		.filter(F.col("rn") == 1) \
		.select("callsign", F.col("CRSArrTime").alias("mode_arr"), "AirlineName")

	dep_mode = dep_counts.withColumn("rn", F.row_number().over(w_dep)) \
		.filter(F.col("rn") == 1) \
		.select("callsign", F.col("CRSDepTime").alias("mode_dep"))

	result = arr_mode.join(dep_mode, "callsign") \
		.withColumn("CRSArrTime", hhmm_to_hhmm_str("mode_arr")) \
		.withColumn("CRSDepTime", hhmm_to_hhmm_str("mode_dep")) \
		.select("callsign", "AirlineName", "CRSArrTime", "CRSDepTime") \
		.orderBy("callsign")

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
		key = str(row.callsign)
		payload = {
			"callsign": key,
			"AirlineName": str(row.AirlineName) if row.AirlineName is not None else None,
			"CRSArrTime": str(row.CRSArrTime) if row.CRSArrTime is not None else None,
			"CRSDepTime": str(row.CRSDepTime) if row.CRSDepTime is not None else None,
		}
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
		refresh_topic(FLIGHTS_LOOKUP_TOPIC, KAFKA_BOOTSTRAP_SERVERS)
		send_to_kafka(rows, FLIGHTS_LOOKUP_TOPIC, KAFKA_BOOTSTRAP_SERVERS)
	except Exception as e:
		print(f"✗ Failed: {e}")
		raise
	finally:
		if spark:
			spark.stop()


if __name__ == "__main__":
	main()
