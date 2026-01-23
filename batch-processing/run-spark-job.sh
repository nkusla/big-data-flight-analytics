#!/bin/bash
# Helper script to run Spark jobs in the cluster

# Check if script file is provided
if [ -z "$1" ]; then
    echo "Usage: $0 src/<script-name> [additional-spark-submit-args]"
    echo ""
    echo "Examples:"
    echo "  $0 src/test_hdfs_connection.py"
    echo ""
    echo "Note: Script must exist in the src/ directory"
    exit 1
fi

INPUT_PATH="$1"
shift  # Remove first argument, rest are additional spark-submit args

# Strip 'src/' prefix if present
if [[ "$INPUT_PATH" == src/* ]]; then
    SCRIPT_NAME="${INPUT_PATH#src/}"
else
    echo "❌ Error: Script must be in src/ directory"
    echo ""
    echo "Available scripts in src/:"
    ls -1 src/*.py 2>/dev/null || echo "  (no Python scripts found)"
    exit 1
fi

# Container path - src/ is mounted to /opt/spark/work-dir/src
CONTAINER_PATH="/opt/spark/work-dir/src/$SCRIPT_NAME"

echo "🚀 Submitting Spark job: $SCRIPT_NAME"
echo "📦 Container path: $CONTAINER_PATH"
echo ""

# Run spark-submit in the master container
docker exec -it spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    "$@" \
    "$CONTAINER_PATH"
