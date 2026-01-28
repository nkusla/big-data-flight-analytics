#!/bin/bash

docker compose -f ./docker-compose.yaml up -d
sleep 5
cmd="bash -c \"/config/restore.sh\""
docker exec metabase-postgres $cmd