"""
DAG: Flights lookup data → Kafka (Global KTable source).

Runs a Spark job that caclulates data for enrichment of realtime flight data.
The data is written to Kafka topic flights-lookup so Kafka Streams can read it as a GlobalKTable (all workers get the same lookup data).
"""

from datetime import datetime, timedelta

from airflow.sdk import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

@dag(
	dag_id='flights_lookup_to_kafka',
	description='Compute flights lookup data and write to Kafka for Global KTable',
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

def flights_lookup_to_kafka():
	flights_lookup_to_kafka = SparkSubmitOperator(
		task_id='flights_lookup_to_kafka',
		application='/opt/airflow/src/flights_lookup_to_kafka.py',
		name='flights_lookup_to_kafka',
		conn_id='SPARK_CONNECTION',
		conf={
			'spark.hadoop.fs.defaultFS': 'hdfs://hdfs-namenode:9000',
		},
		env_vars={
			'FLIGHTS_LOOKUP_TOPIC': 'flights-lookup',
			'KAFKA_BOOTSTRAP_SERVERS': 'kafka-broker-1:9092,kafka-broker-2:9092',
		},
		verbose=True,
	)
	flights_lookup_to_kafka


flights_lookup_to_kafka()
