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
    def upload_csv_to_hdfs():
        """
        Upload all CSV files from local data directory to HDFS.
        """
        hdfs_data_dir = "/data/raw"
        local_data_dir = "/opt/airflow/data"

        print("Connecting to HDFS via WebHDFS...")
        hdfs_hook = WebHDFSHook(webhdfs_conn_id='HDFS_CONNECTION')

        # Create HDFS directory if it doesn't exist
        print(f"Ensuring HDFS directory exists: {hdfs_data_dir}")
        try:
            hdfs_hook.get_conn().makedirs(hdfs_data_dir)
            print(f"✓ Created HDFS directory: {hdfs_data_dir}")
        except Exception:
            print(f"✓ HDFS directory already exists: {hdfs_data_dir}")

        # Check if local data directory exists
        if not os.path.exists(local_data_dir):
            print(f"⚠ Warning: Local data directory {local_data_dir} does not exist")
            print("No files to upload")
            return

        # Find all CSV files
        csv_files = []
        for root, dirs, files in os.walk(local_data_dir):
            for file in files:
                if file.endswith('.csv'):
                    csv_files.append(os.path.join(root, file))

        if not csv_files:
            print(f"⚠ No CSV files found in {local_data_dir}")
            return

        print(f"Found {len(csv_files)} CSV file(s) to upload")

        # Upload each CSV file
        uploaded_count = 0
        failed_count = 0

        for local_file in csv_files:
            filename = os.path.basename(local_file)
            hdfs_file_path = f"{hdfs_data_dir}/{filename}"

            try:
                # Read local file and upload to HDFS
                with open(local_file, 'rb') as f:
                    file_data = f.read()

                # Upload to HDFS (overwrite if exists)
                hdfs_hook.get_conn().write(hdfs_file_path, file_data, overwrite=True)
                print(f"✓ Successfully uploaded {filename} ({len(file_data):,} bytes)")
                uploaded_count += 1
            except Exception as e:
                print(f"✗ Failed to upload {filename}: {e}")
                failed_count += 1

        # List files in HDFS
        print(f"\n{'='*60}")
        print(f"Upload Summary:")
        print(f"  Uploaded: {uploaded_count}")
        print(f"  Failed: {failed_count}")
        print(f"{'='*60}\n")

        print(f"Listing files in HDFS {hdfs_data_dir}:")
        try:
            files = hdfs_hook.get_conn().list(hdfs_data_dir)
            for file_name in files:
                file_path = f"{hdfs_data_dir}/{file_name}"
                status = hdfs_hook.get_conn().status(file_path)
                size = status.get('length', 0)
                print(f"  - {file_name} ({size:,} bytes)")
        except Exception as e:
            print(f"⚠ Could not list files: {e}")

        print(f"\n✓ Done! Files are available in HDFS at {hdfs_data_dir}")

        if failed_count > 0:
            raise Exception(f"Failed to upload {failed_count} file(s)")

    # Run the Spark processing job using SparkSubmitOperator
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
        application_args=[],
    )

    # Set task dependencies
    upload_csv_to_hdfs() >> process_data


# Instantiate the DAG
process_flight_data()
