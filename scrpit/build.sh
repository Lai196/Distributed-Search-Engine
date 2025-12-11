#!/bin/bash
set -e  # stop on first error

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR/.."

echo "Cleaning old build..."
rm -rf build
mkdir build

# check and remove BOM from Java files
if command -v powershell.exe &> /dev/null && [ -f "scrpit/remove-bom-from-java.ps1" ]; then
    echo "Checking for BOM characters in Java files..."
    powershell.exe -NoProfile -ExecutionPolicy Bypass -File scrpit/remove-bom-from-java.ps1 2> /dev/null || true
fi

echo "Compiling all sources from scratch..."
find src -name "*.java" > sources.txt

# copy resource files (e.g. stopwords.txt) into build directory
if [ -f "src/cis5550/resources/stopwords.txt" ]; then
  mkdir -p build/cis5550/resources
  cp src/cis5550/resources/stopwords.txt build/cis5550/resources/
fi

# count Java files for verification
if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "win32" || "$OSTYPE" == "cygwin" ]]; then
  # Windows
  if command -v powershell.exe &> /dev/null; then
    JAVA_COUNT=$(powershell.exe -Command "(Get-Content sources.txt | Measure-Object -Line).Lines")
  else
    echo "Compiling all Java files found in src/..."
  fi
else
  # macOS/Linux
  JAVA_COUNT=$(wc -l < sources.txt | tr -d ' ')
fi

# compile all Java files
javac --release 21 -cp "lib/*" -d build @sources.txt

if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "win32" || "$OSTYPE" == "cygwin" ]]; then
  if command -v powershell.exe &> /dev/null; then
    CLASS_COUNT=$(powershell.exe -Command "(Get-ChildItem -Path build -Recurse -Filter *.class | Measure-Object).Count")
  fi
else
  CLASS_COUNT=$(find build -name "*.class" 2>/dev/null | wc -l | tr -d ' ')
fi
rm sources.txt

echo "Creating jar directory..."
mkdir -p jar

echo "Packaging crawler.jar..."
jar cf jar/crawler.jar -C build cis5550/jobs/Crawler.class -C build cis5550/jobs/Crawler\$Link.class -C build cis5550/jobs/Crawler\$Rule.class -C build cis5550/jobs/Crawler\$Robots.class -C build cis5550/jobs/Crawler\$CrawlOperation.class

echo "Adding Indexer, PageRank, and ClearTables jobs to crawler.jar..."
jar uf jar/crawler.jar \
  -C build cis5550/jobs/Indexer.class \
  -C build cis5550/jobs/PageRank.class \
  -C build cis5550/jobs/ClearTables.class

# include stopwords.txt to crawler.jar if it exists
if [ -f "build/cis5550/resources/stopwords.txt" ]; then
  echo "Including stopwords.txt in crawler.jar..."
  jar uf jar/crawler.jar -C build cis5550/resources
fi

echo "Build complete!"
echo "   - All classes compiled from src/ to build/"
echo "   - crawler.jar created in jar/"
echo ""