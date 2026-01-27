#!/bin/bash

cmd='bash -c "/config/backup.sh"'
docker exec -it metabase-postgres $cmd

docker compose -f ./dashboard/docker-compose.yaml down -v
