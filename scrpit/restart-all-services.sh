#!/bin/bash
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR/.."

# Get configuration from command line arguments
NUM_WORKERS=${1:-5}
THREAD_POOL_SIZE=${2:-16}
CONCURRENCY=${3:-20}

# Validate that NUM_WORKERS is a positive integer
if ! [[ "$NUM_WORKERS" =~ ^[1-9][0-9]*$ ]]; then
  echo "Error: Number of workers must be a positive integer"
  echo "Usage: bash restart-all-services.sh [num_workers] [thread_pool_size] [concurrency]"
  echo "Example: bash restart-all-services.sh 8 28 35"
  echo "Defaults: 5 workers, 16 threads, 20 concurrency"
  exit 1
fi

# Validate thread pool size
if ! [[ "$THREAD_POOL_SIZE" =~ ^[1-9][0-9]*$ ]]; then
  echo "Error: Thread pool size must be a positive integer"
  exit 1
fi

# Validate concurrency
if ! [[ "$CONCURRENCY" =~ ^[1-9][0-9]*$ ]]; then
  echo "Error: Concurrency must be a positive integer"
  exit 1
fi

echo "============================================================"
echo "Restart All Services"
echo "============================================================"
echo "Configuration:"
echo "  Workers: $NUM_WORKERS"
echo "  Thread Pool Size: $THREAD_POOL_SIZE"
echo "  Concurrency: $CONCURRENCY"
echo "============================================================"
echo ""

# Create logs directory if it doesn't exist
mkdir -p logs

# Detect OS and set appropriate path separator
if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "win32" || "$OSTYPE" == "cygwin" ]]; then
  PATH_SEP=";"
  IS_WINDOWS=true
else
  PATH_SEP=":"
  IS_WINDOWS=false
fi

echo "Killing all existing services..."
# detect OS or windows
if [[ "$IS_WINDOWS" == "true" ]]; then
  # Windows
  taskkill //F //IM java.exe 2>&1 > /dev/null
else
  # macOS/Linux
  pkill -f 'cis5550' 2>&1 > /dev/null || true
fi
sleep 3
echo "  All Java processes killed"

echo ""
echo "Waiting 10 seconds for ports to clear..."
sleep 10

echo ""
echo "Starting KVS Coordinator on port 8000..."
java -Dhttp.keepAlive=false -cp "build${PATH_SEP}lib/*" cis5550.kvs.Coordinator 8000 > logs/kvs-coordinator.log 2>&1 &
echo "  Started (PID: $!)"
sleep 3

echo ""
echo "Starting $NUM_WORKERS KVS Workers..."
for i in $(seq 1 $NUM_WORKERS); do
  PORT=$((8000 + i))
  java -Dhttp.keepAlive=false -cp "build${PATH_SEP}lib/*" cis5550.kvs.Worker $PORT data localhost:8000 > logs/kvs-worker-$PORT.log 2>&1 &
  echo "  KVS Worker #$i on port $PORT (PID: $!)"
done
sleep 3

echo ""
echo "Starting Flame Coordinator on port 9000..."
java -Dhttp.keepAlive=false -Dflame.coordinator.concurrency=$CONCURRENCY -cp "build${PATH_SEP}lib/*" cis5550.flame.Coordinator 9000 localhost:8000 > logs/flame-coordinator.log 2>&1 &
echo "  Started (PID: $!) with concurrency=$CONCURRENCY"
sleep 3

echo ""
echo "Starting $NUM_WORKERS Flame Workers..."
for i in $(seq 1 $NUM_WORKERS); do
  PORT=$((9000 + i))
  java -Dhttp.keepAlive=false -Dflame.worker.threads=$THREAD_POOL_SIZE -cp "build${PATH_SEP}lib/*" cis5550.flame.Worker $PORT localhost:9000 > logs/flame-worker-$PORT.log 2>&1 &
  echo "  Flame Worker #$i on port $PORT (PID: $!) with threads=$THREAD_POOL_SIZE"
  sleep 2
done

echo ""
echo "Waiting 10 seconds for workers to register..."
sleep 10

echo ""
echo "============================================================"
echo "Ready to run crawler!"
echo "============================================================"
echo ""
echo "Usage:"
echo "  bash restart-all-services.sh [num_workers] [thread_pool_size] [concurrency]"
echo ""
echo "Examples:"
echo "  bash restart-all-services.sh                    # 5 workers, 16 threads, 20 concurrency (defaults)"
echo "  bash restart-all-services.sh 8                  # 8 workers, 16 threads, 20 concurrency"
echo "  bash restart-all-services.sh 8 28               # 8 workers, 28 threads, 20 concurrency"
echo "  bash restart-all-services.sh 8 28 35            # 8 workers, 28 threads, 35 concurrency"
echo ""
