"""
DAG for monitoring HDFS health and connectivity.

This DAG runs independently to verify HDFS availability and can be used
to trigger alerts or prevent other DAGs from running if HDFS is down.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id='hdfs_health_check',
    default_args=default_args,
    description='Monitor HDFS health and connectivity',
    #schedule='*/15 * * * *',  # Run every 15 minutes
    start_date=datetime(2026, 1, 27),
    catchup=False,
    tags=['monitoring', 'health-check', 'hdfs'],
) as dag:

    # Check HDFS connectivity
    check_hdfs = SparkSubmitOperator(
        task_id='check_hdfs_connectivity',
        application='/opt/airflow/src/ping_hdfs.py',
        name='hdfs_connectivity_check',
        conn_id='SPARK_CONNECTION',
        conf={
            'spark.hadoop.fs.defaultFS': 'hdfs://hdfs-namenode:9000',
        },
        verbose=False,
        doc_md="""
        ### Check HDFS Connectivity
        Verifies that HDFS is accessible and responding.
        """
    )

    # Set task dependencies
    check_hdfs
