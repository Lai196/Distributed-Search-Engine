#!/bin/bash
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR/.."

if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "win32" || "$OSTYPE" == "cygwin" ]]; then
  PATH_SEP=";"
else
  PATH_SEP=":"
fi

echo "============================================================"
echo "Running PageRank Job"
echo "============================================================"
echo ""

#default to 0.001 for threshold, 100.0 for percentRequired
THRESHOLD=${1:-0.001}
PERCENT_REQUIRED=${2:-100.0}

java -Dhttp.keepAlive=false -cp "build${PATH_SEP}lib/*" cis5550.flame.FlameSubmit localhost:9000 jar/crawler.jar cis5550.jobs.PageRank $THRESHOLD $PERCENT_REQUIRED

echo ""
echo "============================================================"
echo "PageRank Job Complete (threshold: $THRESHOLD, percentRequired: $PERCENT_REQUIRED%)"
echo "============================================================"
