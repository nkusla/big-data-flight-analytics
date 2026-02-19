#!/usr/bin/env python3

import json
import time
import argparse
import os
from pathlib import Path
from dotenv import load_dotenv
from confluent_kafka import Producer
from opensky_api import OpenSkyApi

# Load .env from same directory as this script (e.g. /app/.env in Docker)
load_dotenv(Path(__file__).resolve().parent / ".env")

class FlightDataProducer:
	def __init__(self, bootstrap_servers='localhost:29092', topic='flight-data'):
		conf = {
			'bootstrap.servers': bootstrap_servers,
			'client.id': 'flight-data-producer',
			'acks': 'all',
			'retries': 3
		}
		self.producer = Producer(conf)
		self.topic = topic
		print(f"Producer connected to Kafka at {bootstrap_servers}")
		print(f"Publishing to topic: {topic}")

	def delivery_callback(self, err, msg):
		if err is not None:
			print(f"Message delivery failed: {err}")
		else:
			print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

	def send_to_kafka(self, data):
		try:
			value = json.dumps(data).encode('utf-8')

			self.producer.produce(
				self.topic,
				value=value,
				callback=self.delivery_callback
			)

			self.producer.poll(0)

			print(f"Sent: {data}")
			return True
		except BufferError:
			print(f"Buffer full, waiting for messages to be delivered...")
			self.producer.poll(1)
			return self.send_to_kafka(data)
		except Exception as e:
			print(f"Error sending to Kafka: {e}")
			return False

	def produce_from_opensky(self, interval=10, bbox=None):

		username = os.environ.get("OPENSKY_USERNAME") or None
		password = os.environ.get("OPENSKY_PASSWORD") or None
		api = None
		if username is None or password is None:
			print("No OpenSky credentials found in environment variables")
			api = OpenSkyApi()
		else:
			api = OpenSkyApi(username=username, password=password)

		print(f"Starting OpenSky producer (interval: {interval}s)")

		if bbox:
			print(f"Using bounding box (min_lat, max_lat, min_lon, max_lon): {bbox}")

		try:
			while True:
				try:
					states = api.get_states(bbox=bbox) if bbox else api.get_states()

					if states and states.states:
						print(f"\nFetched {len(states.states)} aircraft states at {states.time}")

						for state in states.states:
							flight_data = {
								'timestamp': states.time,
								'icao24': state.icao24,
								'callsign': state.callsign.strip() if state.callsign else None,
								'origin_country': state.origin_country,
								'longitude': state.longitude,
								'latitude': state.latitude,
								'geo_altitude': state.geo_altitude,
								'velocity': state.velocity,
								'true_track': state.true_track,
								'vertical_rate': state.vertical_rate,
								'on_ground': state.on_ground,
								'baro_altitude': state.baro_altitude
							}
							self.dump_to_disk(flight_data)
							self.send_to_kafka(flight_data)
					else:
						print("No states received (Rate limit exceeded)")

				except Exception as e:
					print(f"Error fetching from OpenSky: {e}")

				# Wait before next fetch
				time.sleep(interval)

		except KeyboardInterrupt:
			print("\nStopping producer...")
			self.close()

	def produce_from_file(self, file_path, interval=1, partition_size=None):
		print(f"Reading from file: {file_path}")

		try:
			with open(file_path, 'r') as f:
				content = f.read().strip()

				if content.startswith('['):
					data = json.loads(content)
				else:
					f.seek(0)
					data = [json.loads(line) for line in f if line.strip()]

			total = len(data)
			print(f"Found {total} records")

			if partition_size is None or partition_size <= 0:
				partitions = [data]
			else:
				partitions = [data[i:i + partition_size] for i in range(0, total, partition_size)]

			print(f"Sending in {len(partitions)} partition(s) (partition_size={partition_size or 'all'})")

			for part_idx, partition in enumerate(partitions):
				print(f"Partition {part_idx + 1}/{len(partitions)}: sending {len(partition)} records")
				for record in partition:
					self.send_to_kafka(record)

				time.sleep(interval)

			print(f"\nFinished sending {total} records")

		except KeyboardInterrupt:
			print("\nStopping producer...")
		finally:
			self.close()

	def close(self):
		remaining = self.producer.flush(timeout=10)
		if remaining > 0:
			print(f"Warning: {remaining} messages were not delivered")
		print("Producer closed")


def main():
	print("Starting Flight Data Producer...")

	parser = argparse.ArgumentParser(description='Flight Data Kafka Producer')
	parser.add_argument('--bootstrap-servers', default='localhost:29092',
						help='Kafka bootstrap servers (default: localhost:29092)')
	parser.add_argument('--topic', default='flight-data',
						help='Kafka topic name (default: flight-data)')
	parser.add_argument('--mode', choices=['api', 'file'], required=True,
						help='Data source: api (OpenSky) or file')
	parser.add_argument('--interval', type=int, default=10,
						help='Seconds between API calls or file records (default: 10 for API, 1 for file)')
	parser.add_argument('--partition-size', type=int, default=None, metavar='N',
						help='When using file mode: send data in batches of N records (partitions). If omitted, send all records.')
	parser.add_argument('--bbox', nargs=4, type=float, metavar=('MIN_LAT', 'MAX_LAT', 'MIN_LON', 'MAX_LON'),
						help='Bounding box for OpenSky API (e.g., 45.8389 47.8229 5.9962 10.5226 for Switzerland)')

	args = parser.parse_args()

	producer = FlightDataProducer(
		bootstrap_servers=args.bootstrap_servers,
		topic=args.topic
	)

	if args.mode == 'api':
		bbox = tuple(args.bbox) if args.bbox else None
		producer.produce_from_opensky(interval=args.interval, bbox=bbox)
	else:
		producer.produce_from_file(
			"data/opensky_states.json",
			interval=args.interval,
			partition_size=args.partition_size
		)


if __name__ == '__main__':
	main()
