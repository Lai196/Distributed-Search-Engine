#!/bin/bash
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR/.."

if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "win32" || "$OSTYPE" == "cygwin" ]]; then
  PATH_SEP=";"
else
  PATH_SEP=":"
fi

RESUME_MODE=""
BUCKET_COUNT=""

# Parse arguments
if [[ "$1" == "resume" ]]; then
  RESUME_MODE="resume"
  if [[ -n "$2" ]]; then
    BUCKET_COUNT="$2"
  fi
else
  if [[ -n "$1" ]]; then
    BUCKET_COUNT="$1"
  fi
fi

echo "============================================================"
if [[ -n "$RESUME_MODE" ]]; then
  echo "Running Indexer Job (RESUME MODE)"
else
  echo "Running Indexer Job (FRESH START)"
fi
if [[ -n "$BUCKET_COUNT" ]]; then
  echo "Using $BUCKET_COUNT buckets"
else
  echo "Using default bucket count (100)"
fi
echo "============================================================"
echo ""
echo "Usage: $0 [resume] [bucket_count]"
echo "  - No argument: Start fresh with default 100 buckets"
echo "  - 48: Use 48 buckets"
echo "  - resume: Resume from existing index"
echo "  - resume 96: Resume with 96 buckets"
echo ""

java -Dhttp.keepAlive=false -cp "build${PATH_SEP}lib/*" cis5550.flame.FlameSubmit localhost:9000 jar/crawler.jar cis5550.jobs.Indexer $RESUME_MODE $BUCKET_COUNT
EXIT_CODE=$?

echo ""
echo "============================================================"
if [ $EXIT_CODE -eq 0 ]; then
    echo "Indexer Job Complete"
else
    echo "Indexer Job Stopped (Exit Code: $EXIT_CODE)"
fi
echo "============================================================"
echo ""

exit $EXIT_CODE
