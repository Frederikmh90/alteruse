#!/bin/bash

# Script to combine parallel results and run the rest of the pipeline (Facebook + Scraping)
# Usage: ./finish_pipeline.sh

set -e

BASE_PATH="/work/Datadonationer/urlextraction_scraping/data/new_data_251126"
RESOLVED_DIR="$BASE_PATH/complete_resolved"
COMBINED_FILE="$BASE_PATH/combined_resolved_urls.csv"

echo "=== Pipeline Finisher ==="

# 1. Combine Parallel Results
echo "Checking for parallel worker output..."
WORKER_DIRS=$(find "$RESOLVED_DIR" -name "worker_*" -type d)

if [ -n "$WORKER_DIRS" ]; then
    echo "Combining results from workers..."
    
    python3 -c "
import pandas as pd
import glob
import os
from pathlib import Path

base_dir = Path('$RESOLVED_DIR')
worker_dirs = base_dir.glob('worker_*')
all_csvs = []

for d in worker_dirs:
    csvs = list(d.glob('*.csv'))
    print(f'Found {len(csvs)} files in {d.name}')
    all_csvs.extend(csvs)

if all_csvs:
    print(f'combining {len(all_csvs)} CSV files...')
    # Read and concatenate
    df_list = []
    for f in all_csvs:
        try:
            df_list.append(pd.read_csv(f))
        except Exception as e:
            print(f'Error reading {f}: {e}')
            
    if df_list:
        combined_df = pd.concat(df_list, ignore_index=True)
        
        # Prioritize resolved_url, fallback to original
        if 'resolved_url' in combined_df.columns:
            final_df = combined_df[['resolved_url']].rename(columns={'resolved_url': 'url'})
        else:
            final_df = combined_df[['original_url']].rename(columns={'original_url': 'url'})
            
        final_df = final_df.dropna().drop_duplicates()
        final_df.to_csv('$COMBINED_FILE', index=False)
        print(f'Successfully created $COMBINED_FILE with {len(final_df)} URLs')
    else:
        print('No valid data found to combine.')
else:
    print('No CSV files found.')
"
else
    echo "No worker directories found. Checking for standard output..."
    # Fallback to standard pipeline output if parallel wasn't used
    # ... (existing logic)
fi

# 2. Run Facebook Pipeline (If not done)
echo "=== STEP: Facebook Data ==="
# (Reusing logic from original script)
FACEBOOK_INPUT="$BASE_PATH/Facebook"
FACEBOOK_OUTPUT="$BASE_PATH/facebook_urlextract"

if [ -d "$FACEBOOK_INPUT" ]; then
    echo "Starting Facebook URL Extraction..."
    python3 notebooks/url_extraction_facebook/step1_extract_urls_facebook.py \
        --input-dir "$FACEBOOK_INPUT" \
        --output-dir "$FACEBOOK_OUTPUT"
    
    # Note: Facebook URLs usually don't need the heavy resolution process 
    # as they are internal or simple. We can add them to the combined file directly.
    
    if [ -f "$FACEBOOK_OUTPUT/extracted_urls_facebook.csv" ]; then
        echo "Appending Facebook URLs to scraping list..."
        python3 -c "
import pandas as pd
fb_file = '$FACEBOOK_OUTPUT/extracted_urls_facebook.csv'
main_file = '$COMBINED_FILE'

try:
    fb_df = pd.read_csv(fb_file)
    # Assuming column is 'url'
    if 'url' in fb_df.columns:
        fb_urls = fb_df[['url']]
        
        if os.path.exists(main_file):
            main_df = pd.read_csv(main_file)
            combined = pd.concat([main_df, fb_urls]).drop_duplicates()
        else:
            combined = fb_urls.drop_duplicates()
            
        combined.to_csv(main_file, index=False)
        print(f'Added {len(fb_urls)} Facebook URLs. Total: {len(combined)}')
except Exception as e:
    print(f'Error merging Facebook data: {e}')
"
    fi
fi

# 3. Run Scraping (with Proxy)
echo "=== STEP: Content Scraping ==="
if [ -f "$COMBINED_FILE" ]; then
    echo "Starting ExpressVPN Proxy Scraper..."
    python3 scrapers/expressvpn_proxy_scraper.py "combined_dataset" \
        --urls-file "$COMBINED_FILE" \
        --workers 40 \
        --timeout 15
else
    echo "Error: No combined URLs file found. Skipping scraping."
fi

