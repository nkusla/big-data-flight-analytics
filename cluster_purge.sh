#!/bin/bash

# Get all container IDs
CONTAINER_IDS=$(docker ps -a --format {{.ID}})

# Only remove containers if there are any
if [ -n "$CONTAINER_IDS" ]; then
  echo "Removing containers..."
  docker rm -f $CONTAINER_IDS
else
  echo "No containers to remove."
fi

# Remove all volumes
echo "Removing ALL volumes..."
VOLUME_IDS=$(docker volume ls -q)
if [ -n "$VOLUME_IDS" ]; then
  docker volume rm -f $VOLUME_IDS
else
	echo "No volumes to remove."
fi

# Remove network pipeline-network
echo "Removing network pipeline-network..."
if docker network inspect pipeline-network >/dev/null 2>&1; then
  docker network rm pipeline-network
else
	echo "Network pipeline-network not found."
fi
