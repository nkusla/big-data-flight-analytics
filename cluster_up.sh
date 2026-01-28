#!/bin/bash

# Color codes for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${GREEN}======================================${NC}"
echo -e "${GREEN}Starting Flight Analytics Cluster${NC}"
echo -e "${GREEN}======================================${NC}"

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

# Step 1: Create the external network if it doesn't exist
print_status "Creating pipeline-network..."
if ! docker network inspect pipeline-network &> /dev/null; then
	docker network create pipeline-network
	print_success "Pipeline network created"
else
	print_status "Pipeline network already exists"
fi

# Step 2: Start Data Lake (HDFS)
print_status "Starting Data Lake (HDFS)..."
docker compose -f ./data-lake/docker-compose.yml up -d
if [ $? -eq 0 ]; then
	print_success "Data Lake started"
else
	print_error "Failed to start Data Lake"
	exit 1
fi
print_status "Waiting for HDFS to stabilize..."

# Step 3: Start Stream Cluster (Kafka)
print_status "Starting Stream Cluster (Kafka)..."
docker compose -f ./stream-cluster/docker-compose.yml up -d
if [ $? -eq 0 ]; then
	print_success "Stream Cluster started"
else
	print_error "Failed to start Stream Cluster"
	exit 1
fi
print_status "Waiting for Kafka brokers to be ready..."

# Step 4: Start Curated Layer
print_status "Starting Curated Layer..."
docker compose -f ./curated/docker-compose.yml up -d
if [ $? -eq 0 ]; then
	print_success "Curated Layer started"
else
	print_error "Failed to start Curated Layer"
	exit 1
fi

# Step 5: Start Producer
print_status "Starting Flight Data Producer..."
docker compose -f ./producer/docker-compose.yml up -d
if [ $? -eq 0 ]; then
	print_success "Producer started"
else
	print_error "Failed to start Producer"
	exit 1
fi

# Step 6: Start Stream Processing
print_status "Starting Stream Processing..."
docker compose -f ./stream-processing/docker-compose.yml up -d
if [ $? -eq 0 ]; then
	print_success "Stream Processing started"
else
	print_error "Failed to start Stream Processing"
	exit 1
fi

# Step 7: Start Batch Processing
print_status "Starting Batch Processing..."
docker compose -f ./batch-processing/docker-compose.yml up -d
if [ $? -eq 0 ]; then
	print_success "Batch Processing started"
else
	print_error "Failed to start Batch Processing"
	exit 1
fi

# Step 8: Start Orchestration (Airflow) - using up.sh
print_status "Starting Orchestration (Airflow)..."
cd orchestration
bash up.sh
if [ $? -eq 0 ]; then
	print_success "Orchestration started"
else
	print_error "Failed to start Orchestration"
fi
cd ..

# Step 9: Start Dashboard (Metabase) - using up.sh
print_status "Starting Dashboard (Metabase)..."
cd dashboard
bash up.sh
if [ $? -eq 0 ]; then
	print_success "Dashboard started"
else
	print_error "Failed to start Dashboard"
fi
cd ..

# Final status
echo ""
echo -e "${GREEN}======================================${NC}"
echo -e "${GREEN}Cluster Startup Complete!${NC}"
echo -e "${GREEN}======================================${NC}"
echo ""
echo "Service URLs:"
echo "  - HDFS NameNode UI:    http://localhost:9870"
echo "  - Spark Master UI:     http://localhost:8090"
echo "  - Kafka UI:            http://localhost:8092"
echo "  - Airflow:             http://localhost:8080"
echo "  - Metabase Dashboard:  http://localhost:3000"
echo ""
echo "To check status: docker ps"
echo "To view logs: docker compose -f <component>/docker-compose.yml logs -f"
echo ""
