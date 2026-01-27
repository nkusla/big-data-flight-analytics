#!/bin/bash

docker compose -f ./dashboard/docker-compose.yaml up -d
sleep 30

cmd='bash -c "/config/restore.sh"'
docker exec -it metabase-postgres $cmd