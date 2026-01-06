#!/bin/bash

# Script to run the URL extraction, resolution, and scraping pipeline on new data
# WITH ExpressVPN Integration
# Usage: ./run_new_data_pipeline_remote.sh [BASE_PATH]

set -e  # Exit on error

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

# Create output directories
mkdir -p "$BROWSER_OUTPUT"
mkdir -p "$FACEBOOK_OUTPUT"
mkdir -p "$RESOLVED_OUTPUT"
mkdir -p "$SCRAPED_OUTPUT"
mkdir -p "$LOG_DIR"

# ==========================================
# VPN SETUP
# ==========================================
log "🔌 Setting up ExpressVPN connection..."

# Generate auth file from config
python3 -c "
import yaml
try:
    with open('config/credentials.yaml', 'r') as f:
        creds = yaml.safe_load(f)['expressvpn']
        with open('expressvpn_auth.txt', 'w') as out:
            out.write(f\"{creds['username']}\n{creds['password']}\")
    print('Auth file created.')
except Exception as e:
    print(f'Error creating auth file: {e}')
"

# Connect to VPN (Defaulting to Denmark)
# We run this in background or direct? The python script handles daemon mode.
if python3 scripts/vm_expressvpn_connector.py 1; then
    success "VPN Connected!"
else
    error "Failed to connect to VPN. Proceeding without it (or exit?)"
    # Uncomment to exit on VPN failure
    # exit 1
fi

# ==========================================
# PIPELINE EXECUTION
# ==========================================

log "Base path: $REMOTE_BASE_PATH"

# 2. Browser URL Extraction
log "=== STEP 1: Browser URL Extraction ==="
if [ -d "$BROWSER_INPUT" ]; then
    python3 notebooks/url_extraction/step1_extract_urls_optimized.py \
        --browser-dir "$BROWSER_INPUT" \
        --output-dir "$BROWSER_OUTPUT"
    success "Browser extraction complete."
else
    error "Browser input directory not found: $BROWSER_INPUT"
fi

# 3. Facebook URL Extraction
log "=== STEP 2: Facebook URL Extraction ==="
if [ -d "$FACEBOOK_INPUT" ]; then
    python3 notebooks/url_extraction_facebook/step1_extract_urls_facebook.py \
        --input-dir "$FACEBOOK_INPUT" \
        --output-dir "$FACEBOOK_OUTPUT"
    success "Facebook extraction complete."
else
    error "Facebook input directory not found: $FACEBOOK_INPUT"
fi

# 4. URL Resolution (Batched)
log "=== STEP 3: URL Resolution ==="
python3 pipelines/url_resolution/complete_pipeline.py \
    --base-dir "$REMOTE_BASE_PATH" \
    --batch-size 1000

success "URL Resolution complete."

# 5. Content Scraping
log "=== STEP 4: Content Scraping ==="
python3 scrapers/browser_scraper.py \
    --data-dir "$REMOTE_BASE_PATH" \
    --output-dir "$SCRAPED_OUTPUT" \
    --log-dir "$LOG_DIR" \
    --max-workers 20 \
    --timeout 15

success "Content Scraping complete."

# ==========================================
# TEARDOWN
# ==========================================
log "🔌 Disconnecting VPN..."
python3 scripts/vm_expressvpn_connector.py disconnect

log "=== Pipeline Finished Successfully ==="
log "Outputs are in: $REMOTE_BASE_PATH"

