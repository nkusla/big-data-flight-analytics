#!/bin/bash
docker compose -f ./docker-compose.yml up -d
sleep 5
cmd="bash -c \"/opt/airflow/config/setup_connections.sh\""
docker exec orchestration-airflow-apiserver-1 $cmd
