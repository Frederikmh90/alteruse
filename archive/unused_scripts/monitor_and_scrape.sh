#!/bin/bash

# Script to monitor parallel resolution workers and start scraping when done
# Usage: ./monitor_and_scrape.sh

set -e

BASE_PATH="/work/Datadonationer/urlextraction_scraping/data/new_data_251126"
LOG_DIR="$BASE_PATH/logs/parallel"
PROJECT_DIR="/work/Datadonationer/urlextraction_scraping"

echo "=== Monitor & Auto-Scrape ==="
echo "Watching for resolution workers to complete..."

# Function to count running python workers
count_workers() {
    ps aux | grep "python3 -c" | grep -v grep | wc -l
}

# Wait for all workers to finish
while true; do
    WORKERS=$(count_workers)
    
    if [ "$WORKERS" -eq 0 ]; then
        echo "[$(date)] All workers finished!"
        break
    else
        echo "[$(date)] $WORKERS workers still running. Checking again in 60 seconds..."
        sleep 60
    fi
done

echo ""
echo "=== RESOLUTION COMPLETE ==="
echo ""

# Activate virtual environment
source "$PROJECT_DIR/venv/bin/activate"

# Step 1: Combine all resolved results
echo "[$(date)] Combining resolved URL results..."

COMBINED_FILE="$BASE_PATH/combined_resolved_urls.csv"

python3 -c "
import pandas as pd
import glob
from pathlib import Path

base_dir = Path('$BASE_PATH/complete_resolved')
all_csvs = list(base_dir.glob('worker_*/resolved_*.csv'))

print(f'Found {len(all_csvs)} result files')

if all_csvs:
    df_list = []
    for f in all_csvs:
        try:
            df_list.append(pd.read_csv(f))
        except Exception as e:
            print(f'Error reading {f}: {e}')
    
    if df_list:
        combined_df = pd.concat(df_list, ignore_index=True)
        
        # Filter for successful resolutions only
        if 'success' in combined_df.columns:
            successful = combined_df[combined_df['success'] == True]
            print(f'Total resolved: {len(combined_df)}, Successful: {len(successful)}')
        else:
            successful = combined_df
        
        # Get URLs for scraping
        if 'resolved_url' in successful.columns:
            urls_df = successful[['resolved_url']].rename(columns={'resolved_url': 'url'})
        else:
            urls_df = successful[['original_url']].rename(columns={'original_url': 'url'})
        
        urls_df = urls_df.dropna().drop_duplicates()
        urls_df.to_csv('$COMBINED_FILE', index=False)
        print(f'Saved {len(urls_df)} unique URLs for scraping to $COMBINED_FILE')
else:
    print('No result files found!')
    exit(1)
"

echo "[$(date)] Combined URLs saved."
echo ""

# Step 2: Start scraping with ExpressVPN proxy
echo "=== STARTING CONTENT SCRAPING ==="
echo "[$(date)] Launching ExpressVPN proxy scraper..."

cd "$PROJECT_DIR"

# Check if credentials exist
if [ ! -f "config/credentials.yaml" ]; then
    echo "Warning: ExpressVPN credentials not found at config/credentials.yaml"
    echo "Scraping will run without proxy..."
fi

# Run the scraper
python3 scrapers/expressvpn_proxy_scraper.py "combined_dataset" \
    --urls-file "$COMBINED_FILE" \
    --workers 40 \
    --timeout 15

echo ""
echo "=== SCRAPING COMPLETE ==="
echo "[$(date)] Pipeline finished!"
echo "Results saved to: $PROJECT_DIR/data/combined_dataset/"

