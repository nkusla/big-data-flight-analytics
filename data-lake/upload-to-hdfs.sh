#!/bin/bash

# Script to automatically upload CSV files from data directory to HDFS

HDFS_DATA_DIR="/data/raw"
HDFS_METADATA_DIR="/data/metadata"
DATA_DIR="./data"

echo "Checking if hdfs-namenode container is running..."
if ! docker ps | grep -q hdfs-namenode; then
    echo "Error: hdfs-namenode container is not running. Please start it with: docker-compose up -d"
    exit 1
fi

echo "Creating HDFS directory: $HDFS_DATA_DIR"
docker exec hdfs-namenode hdfs dfs -mkdir -p $HDFS_DATA_DIR

echo "Creating HDFS directory: $HDFS_METADATA_DIR"
docker exec hdfs-namenode hdfs dfs -mkdir -p $HDFS_METADATA_DIR


echo "Uploading CSV files from $DATA_DIR to HDFS..."
if [ ! -d "$DATA_DIR" ]; then
    echo "Error: Data directory $DATA_DIR does not exist"
    exit 1
fi

CSV_FILES=$(find "$DATA_DIR" -name "*.csv" -type f)
METADATA_FILES=$(find "$DATA_DIR/metadata" -name "*.csv" -type f)

if [ -z "$CSV_FILES" ]; then
    echo "No CSV files found in $DATA_DIR"
    exit 0
fi

for file in $CSV_FILES; do
    if [[ "$file" == *"/metadata/"* ]]; then
        continue
    fi
    filename=$(basename "$file")
    echo "Uploading $filename to HDFS..."
    docker exec hdfs-namenode hdfs dfs -put "/data/$filename" "$HDFS_DATA_DIR/"
    if [ $? -eq 0 ]; then
        echo "✓ Successfully uploaded $filename"
    else
        echo "✗ Failed to upload $filename"
    fi
done

for file in $METADATA_FILES; do
    filename=$(basename "$file")
    echo "Uploading $filename (metadata) to HDFS..."
    docker exec hdfs-namenode hdfs dfs -put "/data/metadata/$filename" "$HDFS_METADATA_DIR/"
    if [ $? -eq 0 ]; then
        echo "✓ Successfully uploaded $filename"
    else
        echo "✗ Failed to upload $filename"
    fi
done

echo ""
echo "Listing files in HDFS $HDFS_DATA_DIR:"
docker exec hdfs-namenode hdfs dfs -ls $HDFS_DATA_DIR
echo ""
echo "Listing files in HDFS $HDFS_METADATA_DIR:"
docker exec hdfs-namenode hdfs dfs -ls $HDFS_METADATA_DIR
echo ""
echo "Done! Files are available in HDFS at $HDFS_DATA_DIR"
