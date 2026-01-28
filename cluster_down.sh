#!/bin/bash

# Color codes for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${RED}======================================${NC}"
echo -e "${RED}Stopping Flight Analytics Cluster${NC}"
echo -e "${RED}======================================${NC}"

# Function to print status messages
print_status() {
  echo -e "${YELLOW}[$(date '+%H:%M:%S')]${NC} $1"
}

print_error() {
  echo -e "${RED}[ERROR]${NC} $1"
}

print_success() {
  echo -e "${GREEN}[SUCCESS]${NC} $1"
}

# Change to script directory
cd "$(dirname "$0")"

# Step 1: Stop Dashboard (Metabase) - using down.sh
print_status "Stopping Dashboard (Metabase)..."
cd dashboard
bash down.sh
if [ $? -eq 0 ]; then
	print_success "Dashboard stopped"
else
	print_error "Failed to stop Dashboard"
fi
cd ..

# Step 2: Stop Orchestration (Airflow) - using down.sh
print_status "Stopping Orchestration (Airflow)..."
cd orchestration
bash down.sh
if [ $? -eq 0 ]; then
	print_success "Orchestration stopped"
else
	print_error "Failed to stop Orchestration"
fi
cd ..

# Step 3: Stop Batch Processing
print_status "Stopping Batch Processing..."
docker compose -f ./batch-processing/docker-compose.yml down
if [ $? -eq 0 ]; then
  print_success "Batch Processing stopped"
else
  print_error "Failed to stop Batch Processing"
fi

# Step 4: Stop Stream Processing
print_status "Stopping Stream Processing..."
docker compose -f ./stream-processing/docker-compose.yml down
if [ $? -eq 0 ]; then
  print_success "Stream Processing stopped"
else
  print_error "Failed to stop Stream Processing"
fi

# Step 5: Stop Producer
print_status "Stopping Flight Data Producer..."
docker compose -f ./producer/docker-compose.yml down
if [ $? -eq 0 ]; then
  print_success "Producer stopped"
else
  print_error "Failed to stop Producer"
fi

# Step 6: Stop Curated Layer
print_status "Stopping Curated Layer..."
docker compose -f ./curated/docker-compose.yml down
if [ $? -eq 0 ]; then
  print_success "Curated Layer stopped"
else
  print_error "Failed to stop Curated Layer"
fi

# Step 7: Stop Stream Cluster (Kafka)
print_status "Stopping Stream Cluster (Kafka)..."
docker compose -f ./stream-cluster/docker-compose.yml down
if [ $? -eq 0 ]; then
  print_success "Stream Cluster stopped"
else
  print_error "Failed to stop Stream Cluster"
fi

# Step 8: Stop Data Lake (HDFS)
print_status "Stopping Data Lake (HDFS)..."
docker compose -f ./data-lake/docker-compose.yml down
if [ $? -eq 0 ]; then
  print_success "Data Lake stopped"
else
  print_error "Failed to stop Data Lake"
fi

# Final status
echo ""
echo -e "${GREEN}======================================${NC}"
echo -e "${GREEN}Cluster Shutdown Complete!${NC}"
echo -e "${GREEN}======================================${NC}"
echo ""
echo "To remove volumes and clean up completely, run: ./cluster_purge.sh"
echo "To restart the cluster, run: ./cluster_up.sh"
echo ""
