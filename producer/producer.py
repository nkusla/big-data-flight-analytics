#!/usr/bin/env python3
"""
Simple Flight Data Producer
Fetches data from OpenSky API or reads from files and sends to Kafka
"""

import json
import time
import argparse
from confluent_kafka import Producer
from opensky_api import OpenSkyApi
import os


class FlightDataProducer:
	def __init__(self, bootstrap_servers='localhost:29092', topic='flight-data'):
		"""Initialize Kafka producer"""
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
		"""Callback for message delivery reports"""
		if err is not None:
			print(f"Message delivery failed: {err}")
		else:
			print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

	def send_to_kafka(self, data):
		"""Send data to Kafka topic"""
		try:
			# Serialize data to JSON string and encode to bytes
			value = json.dumps(data).encode('utf-8')
			
			# Produce message asynchronously
			self.producer.produce(
				self.topic,
				value=value,
				callback=self.delivery_callback
			)
			
			# Trigger delivery reports by polling
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
		"""
		Fetch real-time data from OpenSky API and send to Kafka

		Args:
			interval: seconds between API calls (minimum 10s for anonymous)
			bbox: optional bounding box (min_lat, max_lat, min_lon, max_lon)
		"""
		api = OpenSkyApi()
		print(f"Starting OpenSky producer (interval: {interval}s)")

		if bbox:
			print(f"Using bounding box: {bbox}")

		try:
			while True:
				try:
					# Fetch states from OpenSky
					states = api.get_states(bbox=bbox) if bbox else api.get_states()

					if states and states.states:
						print(f"\nFetched {len(states.states)} aircraft states at {states.time}")

						# Send each state vector to Kafka
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
							self.send_to_kafka(flight_data)
					else:
						print("No states received")

				except Exception as e:
					print(f"Error fetching from OpenSky: {e}")

				# Wait before next fetch
				time.sleep(interval)

		except KeyboardInterrupt:
			print("\nStopping producer...")
			self.close()

	def produce_from_file(self, file_path, interval=1):
		"""
		Read flight data from file and send to Kafka

		Args:
			file_path: path to JSON or JSONL file
			interval: seconds between sending each record
		"""
		print(f"Reading from file: {file_path}")

		try:
			with open(file_path, 'r') as f:
				# Check if it's JSONL (one JSON per line) or single JSON array
				content = f.read().strip()

				if content.startswith('['):
					# JSON array
					data = json.loads(content)
				else:
					# JSONL - one JSON per line
					f.seek(0)
					data = [json.loads(line) for line in f if line.strip()]

				print(f"Found {len(data)} records")

				for record in data:
					self.send_to_kafka(record)
					time.sleep(interval)

				print(f"\nFinished sending {len(data)} records")

		except KeyboardInterrupt:
			print("\nStopping producer...")
		finally:
			self.close()

	def close(self):
		"""Close Kafka producer"""
		# Flush any remaining messages
		remaining = self.producer.flush(timeout=10)
		if remaining > 0:
			print(f"Warning: {remaining} messages were not delivered")
		print("Producer closed")


def main():
	parser = argparse.ArgumentParser(description='Flight Data Kafka Producer')
	parser.add_argument('--bootstrap-servers', default='localhost:29092',
						help='Kafka bootstrap servers (default: localhost:29092)')
	parser.add_argument('--topic', default='flight-data',
						help='Kafka topic name (default: flight-data)')
	parser.add_argument('--mode', choices=['api', 'file'], required=True,
						help='Data source: api (OpenSky) or file')
	parser.add_argument('--interval', type=int, default=10,
						help='Seconds between API calls or file records (default: 10 for API, 1 for file)')
	parser.add_argument('--bbox', nargs=4, type=float, metavar=('MIN_LAT', 'MAX_LAT', 'MIN_LON', 'MAX_LON'),
						help='Bounding box for OpenSky API (e.g., 45.8389 47.8229 5.9962 10.5226 for Switzerland)')

	args = parser.parse_args()

	# Create producer
	producer = FlightDataProducer(
		bootstrap_servers=args.bootstrap_servers,
		topic=args.topic
	)

	# Run based on mode
	if args.mode == 'api':
		bbox = tuple(args.bbox) if args.bbox else None
		producer.produce_from_opensky(interval=args.interval, bbox=bbox)
	else:
		producer.produce_from_file("data/sample_data.json", interval=args.interval)


if __name__ == '__main__':
	main()
