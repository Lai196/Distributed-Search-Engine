#!/bin/bash
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR/.."

MODE="${1:-dev}"   # default to dev if no argument

echo "============================================================"
echo "Start KVS Coordinator/Workers + Frontend  (mode: $MODE)"
echo "============================================================"
echo ""

mkdir -p logs

echo "Killing existing Java processes..."
if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "win32" || "$OSTYPE" == "cygwin" ]]; then
  taskkill //F //IM java.exe 2>&1 > /dev/null || true
else
  pkill -f 'java' 2>/dev/null || true
fi
sleep 3
echo "  All Java processes terminated (if any)"
echo ""

echo "Waiting 5 seconds for ports to clear..."
sleep 5
echo ""

echo "Starting KVS Coordinator on port 8000..."
java -Dhttp.keepAlive=false -cp build cis5550.kvs.Coordinator 8000 > logs/kvs-coordinator.log 2>&1 &
echo "  Started (PID: $!)"
sleep 3
echo ""

echo "Starting 5 KVS Workers..."
for i in 1 2 3 4 5; do
  PORT=$((8000 + i))
  java -Dhttp.keepAlive=false -cp build cis5550.kvs.Worker $PORT data localhost:8000 > logs/kvs-worker-$PORT.log 2>&1 &
  echo "  KVS Worker #$i on port $PORT (PID: $!)"
  sleep 1
done
echo ""

if [[ "$MODE" == "prod" ]]; then
  # Require being run as root when using prod mode (for ports 80/443)
  if [[ "$EUID" -ne 0 ]]; then
    echo "ERROR: prod mode must be run as root. Use:"
    echo "  sudo bash scrpit/start-front-end.sh prod"
    exit 1
  fi

  echo "Starting Sauron Frontend in PROD mode on HTTP port 80 and HTTPS port 443..."
  # Needs keystore.jks in the current working directory
  java -cp "build:lib/*" -Dsauron.env=prod cis5550.frontend.SauronFrontend localhost:8000 > logs/frontend-80-443.log 2>&1 &
  echo "  Started (PID: $!)"
  echo ""
  echo "============================================================"
  echo "KVS UI:        http://asksauron.cis5550.net:8000/"
  echo "Frontend UI:   http://asksauron.cis5550.net/    (HTTP)"
  echo "Frontend UI:   https://asksauron.cis5550.net/   (HTTPS)"
  echo "============================================================"
else
  echo "Starting Sauron Frontend in DEV mode on port 8080."
  java -cp "build:lib/*" cis5550.frontend.SauronFrontend localhost:8000 > logs/frontend-8080.log 2>&1 &
  echo "  Started (PID: $!)"
  echo ""
  echo "============================================================"
  echo "KVS UI:        http://localhost:8000"
  echo "Frontend UI:   http://localhost:8080"
  echo "============================================================"
fi
