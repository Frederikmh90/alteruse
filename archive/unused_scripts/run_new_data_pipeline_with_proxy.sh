#!/bin/bash

# Script to run the URL extraction, resolution, and scraping pipeline on new data
# WITH ExpressVPN Proxy Integration (No Root Required)
# Usage: ./run_new_data_pipeline_with_proxy.sh [BASE_PATH]

set -e  # Exit on any error

# Configuration
# Default base path for the new data on the remote server
REMOTE_BASE_PATH="${1:-/work/Datadonationer/urlextraction_scraping/data/new_data_251126}"

PROJECT_DIR="/work/Datadonationer/urlextraction_scraping"
VENV_ACTIVATE="$PROJECT_DIR/venv/bin/activate"

# Input subdirectories
BROWSER_INPUT="$REMOTE_BASE_PATH/Browser"
FACEBOOK_INPUT="$REMOTE_BASE_PATH/Facebook"

# Output subdirectories
BROWSER_OUTPUT="$REMOTE_BASE_PATH/browser_urlextract"
FACEBOOK_OUTPUT="$REMOTE_BASE_PATH/facebook_urlextract"
RESOLVED_OUTPUT="$REMOTE_BASE_PATH/complete_resolved"
SCRAPED_OUTPUT="$REMOTE_BASE_PATH/scraped_content"
LOG_DIR="$REMOTE_BASE_PATH/logs"

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m'

log() {
    echo -e "${BLUE}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} $1"
}

success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 1. Setup Environment
log "Setting up environment..."
cd "$PROJECT_DIR"

if [ -f "$VENV_ACTIVATE" ]; then
    source "$VENV_ACTIVATE"
    log "Virtual environment activated."
else
    error "Virtual environment not found at $VENV_ACTIVATE"
    exit 1
fi

# Install dependencies if missing (polars required for proxy scraper)
pip install polars trafilatura beautifulsoup4 requests >/dev/null 2>&1 || true

# Create output directories
mkdir -p "$BROWSER_OUTPUT"
mkdir -p "$FACEBOOK_OUTPUT"
mkdir -p "$RESOLVED_OUTPUT"
mkdir -p "$SCRAPED_OUTPUT"
mkdir -p "$LOG_DIR"

log "Base path: $REMOTE_BASE_PATH"

# 2. Browser URL Extraction
log "=== STEP 1: Browser URL Extraction ==="
# Check if output file already exists to skip
if [ -f "$BROWSER_OUTPUT/extracted_urls_optimized.csv" ]; then
    log "Browser extraction already complete. Skipping."
else
    if [ -d "$BROWSER_INPUT" ]; then
        python3 notebooks/url_extraction/step1_extract_urls_optimized.py \
            --browser-dir "$BROWSER_INPUT" \
            --output-dir "$BROWSER_OUTPUT"
        success "Browser extraction complete."
    else
        error "Browser input directory not found: $BROWSER_INPUT"
    fi
fi

# 3. Facebook URL Extraction
log "=== STEP 2: Facebook URL Extraction ==="
if [ -f "$FACEBOOK_OUTPUT/extracted_urls_facebook.csv" ]; then
    log "Facebook extraction already complete. Skipping."
else
    if [ -d "$FACEBOOK_INPUT" ]; then
        python3 notebooks/url_extraction_facebook/step1_extract_urls_facebook.py \
            --input-dir "$FACEBOOK_INPUT" \
            --output-dir "$FACEBOOK_OUTPUT"
        success "Facebook extraction complete."
    else
        error "Facebook input directory not found: $FACEBOOK_INPUT"
    fi
fi

# 4. URL Resolution (Batched)
# This step cleans and resolves redirects before scraping.
log "=== STEP 3: URL Resolution ==="
python3 pipelines/url_resolution/complete_pipeline.py \
    --base-dir "$REMOTE_BASE_PATH" \
    --batch-size 1000

success "URL Resolution complete."

# 5. Content Scraping with ExpressVPN Proxy
log "=== STEP 4: Content Scraping (with ExpressVPN Proxy) ==="

# We need to prepare a combined CSV for the proxy scraper to consume
# The resolved output is split into batches. Let's combine them or feed one by one.
# The proxy scraper expects a --urls-file argument with a 'url' column.

# Combine all resolved batches into one file for scraping
COMBINED_URLS_FILE="$REMOTE_BASE_PATH/combined_resolved_urls.csv"
log "Combining resolved batches into $COMBINED_URLS_FILE..."

# Header
echo "url" > "$COMBINED_URLS_FILE"

# Concatenate resolved URLs (taking 'resolved_url' column, skipping header)
# Assuming the resolved CSVs have a header and 'resolved_url' is the 2nd column (index 2)
# Actually, let's use python to safely combine them to avoid CSV parsing issues
python3 -c "
import pandas as pd
import glob
import os

input_path = '$RESOLVED_OUTPUT'
output_file = '$COMBINED_URLS_FILE'
all_files = glob.glob(os.path.join(input_path, '*.csv'))

if all_files:
    combined_df = pd.concat((pd.read_csv(f) for f in all_files), ignore_index=True)
    # Use resolved_url if available, else original_url
    if 'resolved_url' in combined_df.columns:
        # Filter for successful resolutions only? Or try all?
        # Let's keep it simple: scrape the resolved URL
        urls_df = combined_df[['resolved_url']].rename(columns={'resolved_url': 'url'})
    else:
        urls_df = combined_df[['original_url']].rename(columns={'original_url': 'url'})
    
    # Drop duplicates and empty
    urls_df = urls_df.dropna().drop_duplicates()
    urls_df.to_csv(output_file, index=False)
    print(f'Combined {len(urls_df)} URLs.')
else:
    print('No resolved files found.')
"

if [ -f "$COMBINED_URLS_FILE" ]; then
    # Run the PROXY scraper
    # Credentials are loaded from config/credentials.yaml by the script
    # Increased workers to 40 and reduced timeout to 15 for speed
    python3 scrapers/expressvpn_proxy_scraper.py "combined_dataset" \
        --urls-file "$COMBINED_URLS_FILE" \
        --workers 40 \
        --timeout 15
    
    # Move results to expected output
    # The proxy scraper saves to data/combined_dataset/batch_*.json
    # We might want to move or convert these later, but for now the data is saved there.
    success "Content Scraping with Proxy complete."
else
    error "Failed to create combined URLs file. Skipping scraping."
fi

log "=== Pipeline Finished Successfully ==="
log "Outputs are in: $REMOTE_BASE_PATH"
log "Scraped data in: data/combined_dataset/"
