#!/bin/bash

# Script to split the URL dataset into chunks and run parallel resolution pipelines
# Usage: ./run_parallel_resolution.sh [NUM_CHUNKS]

set -e

# Configuration
NUM_CHUNKS="${1:-4}"
BASE_PATH="/work/Datadonationer/urlextraction_scraping/data/new_data_251126"
INPUT_CSV="$BASE_PATH/browser_urlextract/extracted_urls_optimized.csv"
SPLIT_DIR="$BASE_PATH/browser_urlextract/splits"
LOG_DIR="$BASE_PATH/logs/parallel"

# Ensure directories exist
mkdir -p "$SPLIT_DIR"
mkdir -p "$LOG_DIR"

echo "=== Parallel URL Resolution Setup ==="
echo "Input: $INPUT_CSV"
echo "Chunks: $NUM_CHUNKS"

# 1. Split the CSV file
if [ -z "$(ls -A $SPLIT_DIR)" ]; then
    echo "Splitting CSV into $NUM_CHUNKS parts..."
    
    # Get total lines (minus header)
    TOTAL_LINES=$(tail -n +2 "$INPUT_CSV" | wc -l)
    LINES_PER_CHUNK=$(( (TOTAL_LINES + NUM_CHUNKS - 1) / NUM_CHUNKS ))
    
    echo "Total URLs: $TOTAL_LINES"
    echo "URLs per chunk: $LINES_PER_CHUNK"
    
    # Save header
    head -n 1 "$INPUT_CSV" > "$SPLIT_DIR/header.csv"
    
    # Split body
    tail -n +2 "$INPUT_CSV" | split -l "$LINES_PER_CHUNK" - "$SPLIT_DIR/chunk_"
    
    # Add header to each chunk and rename
    count=0
    for file in "$SPLIT_DIR"/chunk_*; do
        mv "$file" "${file}.tmp"
        cat "$SPLIT_DIR/header.csv" "${file}.tmp" > "$SPLIT_DIR/part_${count}.csv"
        rm "${file}.tmp"
        echo "Created chunk: part_${count}.csv"
        count=$((count + 1))
    done
    rm "$SPLIT_DIR/header.csv"
else
    echo "Chunks already exist in $SPLIT_DIR. Skipping split."
fi

# 2. Launch Parallel Processes
echo "=== Launching $NUM_CHUNKS Parallel Resolvers ==="

# Activate venv
source "/work/Datadonationer/urlextraction_scraping/venv/bin/activate"

# Loop to start background processes
for ((i=0; i<NUM_CHUNKS; i++)); do
    CHUNK_FILE="$SPLIT_DIR/part_${i}.csv"
    # Each chunk gets its own cache file to avoid SQLite locking issues
    CACHE_FILE="$BASE_PATH/complete_resolved/cache_part_${i}.db"
    # We need to trick the pipeline script to use our chunk file
    # The script normally looks for 'extracted_urls_optimized.csv' in the input dir.
    # So we'll pass a special argument or modify the script.
    # Better approach: We'll use a small python wrapper here to call the resolver directly.
    
    LOG_FILE="$LOG_DIR/worker_${i}.log"
    
    echo "Starting Worker $i on $CHUNK_FILE..."
    
    nohup python3 -c "
import sys
import pandas as pd
import logging
from pathlib import Path

# Add pipeline dir to path
sys.path.append('/work/Datadonationer/urlextraction_scraping/pipelines/url_resolution')
from enhanced_resolver import EnhancedURLResolver

# Setup logging
logging.basicConfig(
    filename='$LOG_FILE',
    level=logging.INFO,
    format='%(asctime)s - Worker $i - %(levelname)s - %(message)s'
)

# Config
chunk_file = '$CHUNK_FILE'
cache_file = '$CACHE_FILE'
output_dir = Path('$BASE_PATH/complete_resolved/worker_$i')
output_dir.mkdir(parents=True, exist_ok=True)

# Load URLs
try:
    # Try to read with default settings first, skip bad lines
    df = pd.read_csv(chunk_file, on_bad_lines='skip')
    
    # Clean up column names (strip whitespace)
    df.columns = df.columns.str.strip()
    
    # Identify the URL column
    url_col = None
    for col in ['url', 'original_url', 'Visit URL']:
        if col in df.columns:
            url_col = col
            break
    
    if not url_col:
        # Fallback: Assume first column if no known header found
        url_col = df.columns[0]
        
    # Clean URLs: remove whitespace, handle potential CSV weirdness
    urls = df[url_col].dropna().astype(str).str.strip().unique().tolist()
    
    # Filter out obvious garbage (e.g., overly long strings that are likely CSV parsing errors)
    # URLs shouldn't normally be > 2000 chars, but let's be generous. 
    # The issue '0/0/0/...' suggests a binary file read as text or severe corruption.
    urls = [u for u in urls if len(u) < 2048 and not u.startswith('http://0/0/0')]
    
    logging.info(f'Loaded {len(urls)} unique URLs from {chunk_file} (Column: {url_col})')

except Exception as e:
    logging.error(f'Failed to load CSV {chunk_file}: {e}')
    sys.exit(1)

# Initialize Resolver
resolver = EnhancedURLResolver(
    cache_file=cache_file,
    timeout=3,
    max_workers=40,
    dead_links_file='$BASE_PATH/complete_resolved/dead_links_global.txt' # Shared dead links
)

# Process
results = []
batch_size = 1000
for idx, url in enumerate(urls):
    try:
        res = resolver.resolve_single_url(url)
        results.append(res)
        
        if (idx + 1) % batch_size == 0:
            # Save batch
            batch_num = (idx + 1) // batch_size
            batch_df = pd.DataFrame(results)
            batch_file = output_dir / f'resolved_batch_{batch_num:04d}.csv'
            batch_df.to_csv(batch_file, index=False)
            results = [] # Clear memory
            logging.info(f'Saved batch {batch_num}')
            
    except Exception as e:
        logging.error(f'Error processing {url}: {e}')

# Save remaining
if results:
    batch_df = pd.DataFrame(results)
    batch_file = output_dir / f'resolved_final.csv'
    batch_df.to_csv(batch_file, index=False)

logging.info('Worker $i finished.')
" > "$LOG_DIR/worker_${i}.out" 2>&1 &
    
    PID=$!
    echo "Worker $i running (PID: $PID). Logs: $LOG_FILE"
done

echo "=== All workers started ==="
echo "Monitor progress with: tail -f $LOG_DIR/worker_*.log"

