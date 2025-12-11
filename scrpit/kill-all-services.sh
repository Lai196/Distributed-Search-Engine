#!/bin/bash
# Kill all Java processes (KVS, Flame coordinators, and workers)

echo "============================================================"
echo "Killing All Services"
echo "============================================================"
echo ""

# detect OS or windows
if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "win32" || "$OSTYPE" == "cygwin" ]]; then
  # Windows
  BEFORE=$(tasklist | findstr java.exe | wc -l)
  echo "Current Java processes: $BEFORE"
  echo ""
  
  if [ $BEFORE -eq 0 ]; then
    echo "No Java processes running"
    exit 0
  fi
  
  echo "Killing all Java processes..."
  taskkill //F //IM java.exe 2>&1
else
  # macOS/Linux
  BEFORE=$(ps aux | grep java | grep cis5550 | grep -v grep | wc -l)
  echo "Current Java processes (cis5550): $BEFORE"
  echo ""
  
  if [ $BEFORE -eq 0 ]; then
    echo "No Java processes running"
    exit 0
  fi
  
  echo "Killing all Java processes (cis5550)..."
  pkill -f 'cis5550' 2>&1 || true
fi

# Detect operating system
OS=$(uname -s)

echo "Detected OS: $OS"
echo "Killing all Java processes..."

if [[ "$OS" == "Darwin" ]]; then
  # macOS
  pkill -f "java" 2>/dev/null
elif [[ "$OS" == "Linux" ]]; then
  # Linux
  pkill -f "java" 2>/dev/null
elif [[ "$OS" == *"MINGW"* || "$OS" == *"CYGWIN"* || "$OS" == *"MSYS"* ]]; then
  # Windows (Git Bash or Cygwin)
  taskkill //F //IM java.exe 2>&1
else
  echo "Unsupported OS: $OS — please kill Java processes manually."
  exit 1
fi

# Wait for processes to die
echo ""
echo "Waiting 5 seconds for processes to terminate..."
sleep 5

# check remaining processes
if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "win32" || "$OSTYPE" == "cygwin" ]]; then
  AFTER=$(tasklist | findstr java.exe | wc -l)
else
  AFTER=$(ps aux | grep java | grep cis5550 | grep -v grep | wc -l)
fi

echo ""
echo "============================================================"
if [ $AFTER -eq 0 ]; then
  echo "All Java processes killed successfully"
  echo "Killed: $BEFORE processes"
else
  echo "Warning: $AFTER processes still running"
  echo "Remaining processes:"
  if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "win32" || "$OSTYPE" == "cygwin" ]]; then
    tasklist | findstr java.exe
  else
    ps aux | grep java | grep cis5550 | grep -v grep
  fi
fi
echo "============================================================"
echo ""
