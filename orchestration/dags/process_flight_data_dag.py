"""
DAG for processing flight data from HDFS and loading to MongoDB.

This DAG runs the Spark job that:
1. Reads raw CSV data from HDFS
2. Calculates airline On-Time Performance (OTP)
3. Writes results to HDFS (curated zone) and MongoDB
"""

import os
from datetime import datetime, timedelta

from airflow.sdk import dag, task
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

@dag(
	dag_id='process_flight_data',
	description='Process flight data from HDFS and save OTP metrics to MongoDB',
	start_date=datetime(2026, 1, 27),
	max_active_runs=1,
	catchup=False,
	tags=['spark', 'flight-analytics', 'batch-processing'],
	default_args={
		'owner': 'airflow',
		'depends_on_past': False,
		'email_on_failure': False,
		'email_on_retry': False,
		'retries': 2,
		'retry_delay': timedelta(minutes=5),
		'execution_timeout': timedelta(minutes=30),
	},
)
def process_flight_data():
	"""
	Process flight data pipeline:
	1. Upload CSV files to HDFS
	2. Run Spark job to calculate OTP metrics
	3. Store results in MongoDB
	"""

	@task
	def check_transformed_data():
		"""
		Checks for existing parquet data in /data/transformed.
		"""
		hdfs_data_dir = "/data/raw"
		hdfs_transformed_dir = "/data/transformed"

		print("Connecting to HDFS via WebHDFS...")
		hdfs_hook = WebHDFSHook(webhdfs_conn_id='HDFS_CONNECTION')
		conn = hdfs_hook.get_conn()

		transformed_parquet_files = []
		try:
			all_names = conn.list(hdfs_transformed_dir)
			for name in all_names:
				if name.endswith('.parquet'):
					transformed_parquet_files.append(name)
			print(f"✓ Found {len(transformed_parquet_files)} parquet file(s) in {hdfs_transformed_dir}")
		except Exception:
			print(f"✗ No transformed data yet at {hdfs_transformed_dir} (path missing or empty)")
		if not transformed_parquet_files:
			print(f"✗ No parquet files in {hdfs_transformed_dir}")

		try:
			conn.makedirs(hdfs_data_dir)
			print(f"✓ HDFS directory exists: {hdfs_data_dir}")
		except Exception:
			print(f"✗ Failed to create HDFS directory: {hdfs_data_dir}")

	process_data = SparkSubmitOperator(
		task_id='process_flight_data',
		application='/opt/airflow/src/process.py',
		name='process_flight_data',
		conn_id='SPARK_CONNECTION',
		conf={
			'spark.hadoop.fs.defaultFS': 'hdfs://hdfs-namenode:9000',
			"spark.jars": "/opt/spark/jars/mongo-spark-connector_2.12-10.2.0.jar,"
			"/opt/spark/jars/mongodb-driver-sync-4.8.2.jar,"
			"/opt/spark/jars/mongodb-driver-core-4.8.2.jar,"
			"/opt/spark/jars/bson-4.8.2.jar",
			"spark.executor.extraClassPath": "/opt/spark/jars/*",
			"spark.driver.extraClassPath": "/opt/spark/jars/*",
		},
		verbose=True,
	)

	check_transformed_data() >> process_data


process_flight_data()
