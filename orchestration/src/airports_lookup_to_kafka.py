#!/usr/bin/env python3
"""
Script to read busiest airports from curated parquet and write to Kafka (e.g. for Global KTable).

Reads /data/curated/busiest_airports.parquet (written by process.calculate_busiest_airports).
"""

import json
import os
from pyspark.sql import SparkSession
from shared import build_spark_session
from confluent_kafka import Producer

AIRPORTS_LOOKUP_TOPIC = os.environ.get("AIRPORTS_LOOKUP_TOPIC")
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
BUSIEST_AIRPORTS_PARQUET = "/data/curated/busiest_airports.parquet"


def send_to_kafka(rows, topic: str, bootstrap_servers: str):
	conf = {
		"bootstrap.servers": bootstrap_servers,
		"client.id": "airports-lookup-producer",
	}
	producer = Producer(conf)

	def delivery_callback(err, msg):
		if err:
			print(f"Delivery failed: {err}")

	def _to_jsonable(v):
		if v is None:
			return None
		if isinstance(v, (int, str, bool)):
			return v
		try:
			return round(float(v), 4)
		except (TypeError, ValueError):
			return str(v)

	for row in rows:
		row_dict = row.asDict()
		key = str(row_dict.get("AirportCode", ""))
		payload = {k: _to_jsonable(v) for k, v in row_dict.items()}
		value = json.dumps(payload)
		producer.produce(topic, key=key.encode("utf-8"), value=value.encode("utf-8"), callback=delivery_callback)

	producer.flush()
	print(f"✓ Written {len(rows)} records to Kafka topic '{topic}' (for Global KTable)")


def main():
	if not KAFKA_BOOTSTRAP_SERVERS:
		raise ValueError("KAFKA_BOOTSTRAP_SERVERS environment variable is required")

	spark = None
	try:
		spark = build_spark_session("Airports Lookup to Kafka Global KTable")
		df = spark.read.parquet(BUSIEST_AIRPORTS_PARQUET)
		rows = df.collect()
		if not rows:
			print("No rows to send (parquet is empty).")
			return
		send_to_kafka(rows, AIRPORTS_LOOKUP_TOPIC, KAFKA_BOOTSTRAP_SERVERS)
	except Exception as e:
		print(f"✗ Failed: {e}")
		raise
	finally:
		if spark:
			spark.stop()

if __name__ == "__main__":
	main()
