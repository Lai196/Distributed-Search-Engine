#!/bin/bash
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR/.."

# Create necessary directories
mkdir -p jar logs timestemp

echo "============================================================"
echo "Run Crawler"
echo "============================================================"
echo ""

SEEDS=""
if [ $# -eq 0 ]; then
  # missing arguments, print usage instructions
  echo "Usage:"
  echo "  bash run-crawler-production.sh <seed-url> [seed-url2] ..."
  echo "  bash run-crawler-production.sh -f <seeds-file.txt>"
  echo ""
  echo "Examples:"
  echo "  bash run-crawler-production.sh https://www.cis.upenn.edu/"
  echo "  bash run-crawler-production.sh -f seeds.txt"
  echo ""
  echo "Seed file format (seeds.txt):"
  echo "  One URL per line, empty lines and lines starting with # are ignored"
  echo ""
  exit 1
elif [ "$1" = "-f" ] || [ "$1" = "--file" ]; then
  # read from file
  if [ $# -lt 2 ]; then
    echo "Error: File path required after -f flag"
    exit 1
  fi
  SEED_FILE="scrpit/$2"
  if [ ! -f "$SEED_FILE" ]; then
    echo "Error: File not found: $SEED_FILE"
    exit 1
  fi
  echo "Reading seed URLs from file: $SEED_FILE"
  echo ""
  # read URLs from file
  while IFS= read -r line || [ -n "$line" ]; do
    line=$(echo "$line" | xargs)
    if [ -n "$line" ] && [ "${line#\#}" = "$line" ]; then
      SEEDS="$SEEDS $line"
    fi
  done < "$SEED_FILE"
  SEEDS=$(echo "$SEEDS" | xargs)
elif [ "$1" != "--" ]; then
  SEED_FILE="scrpit/$1"
  
  if [ -f "$SEED_FILE" ]; then
    echo "Reading seed URLs from file: $SEED_FILE"
    echo ""
    SEEDS=""
    while IFS= read -r line || [ -n "$line" ]; do
      line=$(echo "$line" | xargs)
      if [ -n "$line" ] && [ "${line#\#}" = "$line" ]; then
        SEEDS="$SEEDS $line"
      fi
    done < "$SEED_FILE"
    SEEDS=$(echo "$SEEDS" | xargs)
    if [ -z "$SEEDS" ]; then
      echo "Error: No valid URLs found in file: $SEED_FILE"
      exit 1
    fi
  else
    SEEDS="$@"
  fi
else
  SEEDS="$@"
fi

if [ -z "$SEEDS" ]; then
  echo "Error: No valid seed URLs found"
  exit 1
fi

SEED_COUNT=$(echo $SEEDS | wc -w)
echo "Seed URLs: $SEED_COUNT"
echo ""

WORKERS=$(curl -s http://localhost:9000 2>/dev/null | grep -o "127.0.0.1:900[1-5]" | wc -l)
echo "Flame Workers registered: $WORKERS"
echo ""

# check if checkpoint exists
CHECKPOINT_SIZE=$(curl -s "http://localhost:8000/count?table=crawler-url-queue" 2>/dev/null)
IS_RESUMING=false
if [[ "$CHECKPOINT_SIZE" =~ ^[0-9]+$ ]] && [ "$CHECKPOINT_SIZE" -gt 0 ]; then
  echo "Previous crawl detected - Resuming from checkpoint ($CHECKPOINT_SIZE URLs in queue)"
  echo ""
  IS_RESUMING=true
fi

echo "============================================================"
echo "Starting Crawler..."
echo "============================================================"
echo ""

# session-based tracking: preserve overall start time, track session separately
BASELINE_PAGES=$(curl -s http://localhost:8000/count?table=pt-crawl 2>/dev/null)
if [[ ! "$BASELINE_PAGES" =~ ^[0-9]+$ ]]; then
  BASELINE_PAGES=0
fi

if [ -f timestemp/.crawler-start-time ]; then
  if [ "$IS_RESUMING" = true ]; then
    echo "Resuming crawl (preserving original start time)"
  else
    echo "Restarting crawl with existing pages (preserving original start time)"
  fi
  echo "$BASELINE_PAGES" > timestemp/.crawler-session-baseline
else
  date +%s > timestemp/.crawler-start-time
  date > timestemp/.crawler-start-time-str
  echo "$BASELINE_PAGES" > timestemp/.crawler-session-baseline
fi

date +%s > timestemp/.crawler-session-start-time
date > timestemp/.crawler-session-start-time-str

START_TIME=$(date +%s)
START_TIME_STR=$(date)
if [ "$IS_RESUMING" = true ]; then
  ORIGINAL_START_STR=$(cat timestemp/.crawler-start-time-str 2>/dev/null || echo "Unknown")
  echo "Overall start: $ORIGINAL_START_STR"
  echo "Session start: $START_TIME_STR"
else
  echo "Start time: $START_TIME_STR"
fi
echo ""

java -Dhttp.keepAlive=false -cp build cis5550.flame.FlameSubmit localhost:9000 jar/crawler.jar cis5550.jobs.Crawler $(echo $SEEDS)

END_TIME=$(date +%s)
END_TIME_STR=$(date)
SESSION_DURATION=$((END_TIME - START_TIME))
SESSION_HOURS=$((SESSION_DURATION / 3600))
SESSION_MINUTES=$(((SESSION_DURATION % 3600) / 60))
SESSION_SECONDS=$((SESSION_DURATION % 60))

echo ""
echo "============================================================"
echo "Crawler Complete!"
echo "============================================================"
echo ""

echo "Timing:"
if [ "$IS_RESUMING" = true ] && [ -f timestemp/.crawler-start-time ]; then
  ORIGINAL_START=$(cat timestemp/.crawler-start-time)
  ORIGINAL_START_STR=$(cat timestemp/.crawler-start-time-str 2>/dev/null || echo "Unknown")
  OVERALL_DURATION=$((END_TIME - ORIGINAL_START))
  OVERALL_HOURS=$((OVERALL_DURATION / 3600))
  OVERALL_MINUTES=$(((OVERALL_DURATION % 3600) / 60))
  OVERALL_SECONDS=$((OVERALL_DURATION % 60))
  
  echo "  Overall start: $ORIGINAL_START_STR"
  printf "  Overall duration: %02d:%02d:%02d (%dh %dm %ds)\n" $OVERALL_HOURS $OVERALL_MINUTES $OVERALL_SECONDS $OVERALL_HOURS $OVERALL_MINUTES $OVERALL_SECONDS
  echo "  Session start: $START_TIME_STR"
  echo "  Session end:   $END_TIME_STR"
  printf "  Session duration: %02d:%02d:%02d (%dh %dm %ds)\n" $SESSION_HOURS $SESSION_MINUTES $SESSION_SECONDS $SESSION_HOURS $SESSION_MINUTES $SESSION_SECONDS
else
  echo "  Start time: $START_TIME_STR"
  echo "  End time:   $END_TIME_STR"
  printf "  Duration:   %02d:%02d:%02d (%dh %dm %ds)\n" $SESSION_HOURS $SESSION_MINUTES $SESSION_SECONDS $SESSION_HOURS $SESSION_MINUTES $SESSION_SECONDS
fi
echo ""

echo "Final Statistics:"
PAGES=$(curl -s http://localhost:8000/count?table=pt-crawl 2>&1)
CONTENT=$(curl -s http://localhost:8000/count?table=pt-content 2>&1)

if [[ "$PAGES" =~ ^[0-9]+$ ]]; then
  echo "  Pages crawled: $PAGES"
else
  echo "  Pages crawled: Error fetching count"
  PAGES=0
fi

if [[ "$CONTENT" =~ ^[0-9]+$ ]]; then
  echo "  Unique content: $CONTENT"
else
  echo "  Unique content: Error fetching count"
fi

# calculate rates
if [ "$IS_RESUMING" = true ] && [ -f timestemp/.crawler-session-baseline ] && [ -f timestemp/.crawler-start-time ]; then
  BASELINE=$(cat timestemp/.crawler-session-baseline)
  if [[ "$BASELINE" =~ ^[0-9]+$ ]] && [[ "$PAGES" =~ ^[0-9]+$ ]]; then
    SESSION_PAGES=$((PAGES - BASELINE))
    if [ $SESSION_DURATION -gt 0 ] && [ "$SESSION_PAGES" -gt 0 ]; then
      SESSION_RATE=$((SESSION_PAGES * 60 / SESSION_DURATION))
      echo "  Session rate: $SESSION_RATE pages/min ($SESSION_PAGES pages in this session)"
    fi
    
    ORIGINAL_START=$(cat timestemp/.crawler-start-time)
    OVERALL_DURATION=$((END_TIME - ORIGINAL_START))
    if [ $OVERALL_DURATION -gt 0 ] && [ "$PAGES" -gt 0 ]; then
      OVERALL_RATE=$((PAGES * 60 / OVERALL_DURATION))
      echo "  Overall rate: $OVERALL_RATE pages/min ($PAGES pages total)"
    fi
  fi
elif [[ "$PAGES" =~ ^[0-9]+$ ]] && [ $SESSION_DURATION -gt 0 ] && [ "$PAGES" -gt 0 ]; then
  RATE=$((PAGES * 60 / SESSION_DURATION))
  echo "  Crawl rate: $RATE pages/min"
fi
