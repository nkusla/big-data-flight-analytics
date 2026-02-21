#!/usr/bin/env python3

import json
import os
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from shared import build_spark_session, refresh_topic
from confluent_kafka import Producer

FLIGHTS_LOOKUP_TOPIC = os.environ.get("FLIGHTS_LOOKUP_TOPIC")
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")


def hhmm_to_minutes(col_name):
	return F.floor(F.col(col_name) / 100) * 60 + (F.col(col_name) % 100)


def minutes_to_hhmm_str(avg_minutes_col):
	rounded = F.round(F.col(avg_minutes_col)).cast("int")
	hour = (rounded / 60).cast("int") % 24
	minute = rounded % 60
	return F.format_string("%02d:%02d", hour, minute)


def run_aggregation(spark: SparkSession):
	df = spark.read.parquet("/data/transformed/*.parquet")
	arr_min = hhmm_to_minutes("CRSArrTime")
	dep_min = hhmm_to_minutes("CRSDepTime")
	agg = (
		df.filter(F.col("Flight_Number_Operating_Airline").isNotNull())
		.groupBy(F.col("Flight_Number_Operating_Airline"))
		.agg(
			F.avg(arr_min).alias("avg_arr_min"),
			F.avg(dep_min).alias("avg_dep_min"),
		)
	)
	result = (
		agg.withColumn("AvgCRSArrTime", minutes_to_hhmm_str("avg_arr_min"))
		.withColumn("AvgCRSDepTime", minutes_to_hhmm_str("avg_dep_min"))
		.select(
			"Flight_Number_Operating_Airline",
			"AvgCRSArrTime",
			"AvgCRSDepTime",
		)
		.orderBy("Flight_Number_Operating_Airline")
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
		key = str(row.Flight_Number_Operating_Airline)
		payload = {
			"Flight_Number_Operating_Airline": key,
			"AvgCRSArrTime": str(row.AvgCRSArrTime) if row.AvgCRSArrTime is not None else None,
			"AvgCRSDepTime": str(row.AvgCRSDepTime) if row.AvgCRSDepTime is not None else None,
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
