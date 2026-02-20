"""
DAG: Flights and airports lookup data → Kafka (Global KTable sources).

Runs Spark jobs that write lookup data for enrichment of realtime flight data:
- flights-lookup: calculated from transformed data (aircraft delay stats).
- airports-lookup: read from curated busiest_airports.parquet (written by process DAG).

Both tasks run in parallel. Kafka Streams can read these as GlobalKTables.
"""

from datetime import datetime, timedelta

from airflow.sdk import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

@dag(
	dag_id='generate_lookups',
	description='Generate flights lookup data and airports lookup data',
	start_date=datetime(2026, 1, 27),
	max_active_runs=1,
	catchup=False,
	tags=['spark', 'kafka', 'global-ktable', 'flight-analytics'],
	default_args={
		'owner': 'airflow',
		'depends_on_past': False,
		'retries': 2,
		'retry_delay': timedelta(minutes=5),
		'execution_timeout': timedelta(minutes=30),
	},
)

def generate_lookups():
	flights_lookup = SparkSubmitOperator(
		task_id='flighs_lookup_to_kafka',
		application='/opt/airflow/src/flights_lookup_to_kafka.py',
		name='flights_lookup_to_kafka',
		conn_id='SPARK_CONNECTION',
		conf={
			'spark.hadoop.fs.defaultFS': 'hdfs://hdfs-namenode:9000',
		},
		env_vars={
			'FLIGHTS_LOOKUP_TOPIC': 'flights-lookup',
			'KAFKA_BOOTSTRAP_SERVERS': 'kafka-broker-1:9092,kafka-broker-2:9092',
			'AIRCRAFT_DB_PATH': '/data/metadata/Aircrafts.csv',
		},
		verbose=True,
	)

	airports_lookup = SparkSubmitOperator(
		task_id='airports_lookup_to_kafka',
		application='/opt/airflow/src/airports_lookup_to_kafka.py',
		name='airports_lookup_to_kafka',
		conn_id='SPARK_CONNECTION',
		conf={
			'spark.hadoop.fs.defaultFS': 'hdfs://hdfs-namenode:9000',
		},
		env_vars={
			'AIRPORTS_LOOKUP_TOPIC': 'airports-lookup',
			'KAFKA_BOOTSTRAP_SERVERS': 'kafka-broker-1:9092,kafka-broker-2:9092',
		},
		verbose=True,
	)

	# Both tasks run in parallel (no dependency between them)
	[flights_lookup, airports_lookup]


generate_lookups()
