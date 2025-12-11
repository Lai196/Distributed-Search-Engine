#!/bin/bash
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR/.."

echo "============================================================"
echo "Crawler Progress"
echo "============================================================"
echo ""

# overall stats
if [ -f timestemp/.crawler-start-time ]; then
  CURRENT_TIME=$(date +%s)
  START_TIME=$(cat timestemp/.crawler-start-time)
  START_TIME_STR=$(cat timestemp/.crawler-start-time-str 2>/dev/null || echo "Unknown")
  ELAPSED=$((CURRENT_TIME - START_TIME))
  
  HOURS=$((ELAPSED / 3600))
  MINUTES=$(((ELAPSED % 3600) / 60))
  SECONDS=$((ELAPSED % 60))
  
  echo "Overall Stats:"
  echo "  Started: $START_TIME_STR"
  echo "  Running: ${HOURS}h ${MINUTES}m ${SECONDS}s"
else
  echo "Crawler not started yet"
  ELAPSED=0
fi

# session stats
if [ -f timestemp/.crawler-session-start-time ]; then
  if [ -z "$CURRENT_TIME" ]; then
    CURRENT_TIME=$(date +%s)
  fi
  SESSION_START_TIME=$(cat timestemp/.crawler-session-start-time)
  SESSION_START_TIME_STR=$(cat timestemp/.crawler-session-start-time-str 2>/dev/null || echo "Unknown")
  SESSION_ELAPSED=$((CURRENT_TIME - SESSION_START_TIME))
  
  SESSION_HOURS=$((SESSION_ELAPSED / 3600))
  SESSION_MINUTES=$(((SESSION_ELAPSED % 3600) / 60))
  SESSION_SECONDS=$((SESSION_ELAPSED % 60))
  
  echo ""
  echo "Session Stats:"
  echo "  Started: $SESSION_START_TIME_STR"
  echo "  Running: ${SESSION_HOURS}h ${SESSION_MINUTES}m ${SESSION_SECONDS}s"
fi

echo ""
# Get pages crawled from KVS table
PAGES=$(curl -s http://localhost:8000/count?table=pt-crawl 2>&1)
if [[ "$PAGES" =~ ^[0-9]+$ ]]; then
  echo "Pages crawled: $PAGES"
else
  echo "Pages crawled: Error fetching count"
  PAGES=0
fi

# Get unique content from KVS table
CONTENT=$(curl -s http://localhost:8000/count?table=pt-content 2>&1)
if [[ "$CONTENT" =~ ^[0-9]+$ ]]; then
  echo "Unique content: $CONTENT"
else
  echo "Unique content: Error fetching count"
fi

echo ""
CURRENT_TIME=$(date +%s)

# calculate crawl rates
if [ -f timestemp/.crawler-start-time ] && [ -n "$ELAPSED" ] && [ $ELAPSED -gt 0 ] && [[ "$PAGES" =~ ^[0-9]+$ ]] && [ "$PAGES" -gt 0 ]; then
  START_TIME=$(cat timestemp/.crawler-start-time)
  ELAPSED=$((CURRENT_TIME - START_TIME))
  OVERALL_RATE=$((PAGES * 60 / ELAPSED))
  echo "Overall rate: $OVERALL_RATE pages/min"
fi

# session rate
if [ -f timestemp/.crawler-session-start-time ] && [ -f timestemp/.crawler-session-baseline ]; then
  SESSION_START_TIME=$(cat timestemp/.crawler-session-start-time)
  SESSION_ELAPSED=$((CURRENT_TIME - SESSION_START_TIME))
  BASELINE=$(cat timestemp/.crawler-session-baseline)
  if [[ "$BASELINE" =~ ^[0-9]+$ ]] && [[ "$PAGES" =~ ^[0-9]+$ ]]; then
    SESSION_PAGES=$((PAGES - BASELINE))
    if [ "$SESSION_PAGES" -lt 0 ]; then
      SESSION_PAGES=0
    fi
    if [ $SESSION_ELAPSED -gt 0 ]; then
      if [ "$SESSION_PAGES" -gt 0 ]; then
        SESSION_RATE=$((SESSION_PAGES * 60 / SESSION_ELAPSED))
        echo "Session rate: $SESSION_RATE pages/min ($SESSION_PAGES pages in this session)"
      else
        echo "Session rate: 0 pages/min (no new pages in this session)"
      fi
    fi
  fi
fi

echo ""
echo "============================================================"
