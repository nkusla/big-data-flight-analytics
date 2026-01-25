#!/bin/bash
docker compose -f ./orchestration/docker-compose.yml up -d
sleep 60
cmd='bash -c "/opt/airflow/config/setup_connections.sh"'
docker exec -it orchestration-airflow-apiserver-1 $cmd