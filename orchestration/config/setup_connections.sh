#!/bin/bash

# Set up connections
echo ">> Setting up Airflow connections"

airflow connections add 'AIRFLOW_DB_CONNECTION' \
    --conn-json '{
        "conn_type": "postgres",
        "login": "airflow",
        "password": "airflow",
        "host": "postgres",
        "port": 5432,
        "schema": "airflow"
    }'

airflow connections add 'HDFS_CONNECTION' \
    --conn-json '{
        "conn_type": "hdfs",
        "host": "hdfs-namenode",
        "port": 9870,
        "extra": {
            "use_ssl": false
        }
    }'

airflow connections add 'SPARK_CONNECTION' \
  --conn-json '{
    "conn_type": "spark",
    "host": "spark://spark-master",
    "port": 7077
  }'

airflow connections add 'MONGO_CONNECTION' \
  --conn-json '{
    "conn_type": "mongo",
    "host": "mongodb",
    "port": 27017,
    "login": "admin",
    "password": "admin123"
  }'

airflow connections add 'LOCAL_FS_FILES' \
    --conn-json '{
        "conn_type": "fs",
        "extra": "{ \"path\": \"/opt/airflow/files\"}"
    }'

# Set up variables
echo ">> Setting up airflow variables"
airflow variables set HDFS_DEFAULT_FS "hdfs://hdfs-namenode:9000"
airflow variables set MONGO_URI "mongodb://admin:admin123@mongodb:27017"