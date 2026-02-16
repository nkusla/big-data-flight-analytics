"""
DAG for transforming raw data from HDFS to a transformed parquet file.
"""
import os
from datetime import datetime, timedelta

from airflow.sdk import dag, task
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


@dag(
	dag_id='transform_raw_data',
	description='Transform raw data from HDFS to a transformed parquet file.',
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

def transform_raw_data():
	tranfrom_data = SparkSubmitOperator(
		task_id='tranfrom_data',
		application='/opt/airflow/src/transform.py',
		name='tranfrom_data',
		conn_id='SPARK_CONNECTION',
		conf={
			'spark.hadoop.fs.defaultFS': 'hdfs://hdfs-namenode:9000',
		},
		verbose=True,
	)

	return tranfrom_data

transform_raw_data()