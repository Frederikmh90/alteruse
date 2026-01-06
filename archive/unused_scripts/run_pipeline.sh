#!/bin/bash

# URL Extraction and Scraping Pipeline Runner
# Run this script to execute the complete pipeline

set -e  # Exit on any error

# Configuration
PROJECT_DIR="$HOME/alteruse"
LOG_DIR="$PROJECT_DIR/logs"
OUTPUT_DIR="$PROJECT_DIR/data/url_extract"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Create log directory
mkdir -p "$LOG_DIR"

# Logging function
log() {
    echo -e "${BLUE}[$(date '+%Y-%m-%d %H:%M:%S')]${NC} $1" | tee -a "$LOG_DIR/pipeline.log"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1" | tee -a "$LOG_DIR/pipeline.log"
    exit 1
}

success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1" | tee -a "$LOG_DIR/pipeline.log"
}

warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1" | tee -a "$LOG_DIR/pipeline.log"
}

# Check if we're in the right directory
if [ ! -d "$PROJECT_DIR" ]; then
    error "Project directory not found: $PROJECT_DIR"
fi

cd "$PROJECT_DIR"

# Check if virtual environment is activated
if [[ "$VIRTUAL_ENV" == "" ]]; then
    warning "Virtual environment not detected. Please activate it first:"
    echo "source venv/bin/activate"
    echo "Then run this script again."
    exit 1
fi

# Check if browser data exists
if [ ! -d "data/Samlet_06112025/Browser" ]; then
    error "Browser data directory not found: data/Samlet_06112025/Browser"
fi

# Check if pipeline scripts exist
for script in step1_extract_urls_optimized.py step2_analyze_urls.py step3_prioritize_domains.py step4_scrape_content.py; do
    if [ ! -f "notebooks/url_extraction/$script" ]; then
        error "Pipeline script not found: notebooks/url_extraction/$script"
    fi
done

# Create output directory
mkdir -p "$OUTPUT_DIR"

log "Starting URL Extraction and Scraping Pipeline"
log "Project directory: $PROJECT_DIR"
log "Output directory: $OUTPUT_DIR"
log "Log file: $LOG_DIR/pipeline.log"

# Step 1: Extract URLs
log "=== STEP 1: Extracting URLs from browser data ==="
if [ -f "$OUTPUT_DIR/extracted_urls_optimized.csv" ]; then
    warning "Step 1 output already exists. Skipping..."
    log "Found existing: $OUTPUT_DIR/extracted_urls_optimized.csv"
else
    log "Running step 1..."
    python3 notebooks/url_extraction/step1_extract_urls_optimized.py
    if [ $? -eq 0 ]; then
        success "Step 1 completed successfully"
    else
        error "Step 1 failed"
    fi
fi

# Step 2: Analyze URLs
log "=== STEP 2: Analyzing and resolving URLs ==="
if [ -f "$OUTPUT_DIR/analyzed_urls.csv" ]; then
    warning "Step 2 output already exists. Skipping..."
    log "Found existing: $OUTPUT_DIR/analyzed_urls.csv"
else
    log "Running step 2..."
    python3 notebooks/url_extraction/step2_analyze_urls.py
    if [ $? -eq 0 ]; then
        success "Step 2 completed successfully"
    else
        error "Step 2 failed"
    fi
fi

# Step 3: Prioritize domains
log "=== STEP 3: Prioritizing domains and creating batches ==="
if [ -d "$OUTPUT_DIR/prioritized_batches" ] && [ "$(ls -A $OUTPUT_DIR/prioritized_batches)" ]; then
    warning "Step 3 output already exists. Skipping..."
    log "Found existing batches in: $OUTPUT_DIR/prioritized_batches"
else
    log "Running step 3..."
    python3 notebooks/url_extraction/step3_prioritize_domains.py
    if [ $? -eq 0 ]; then
        success "Step 3 completed successfully"
    else
        error "Step 3 failed"
    fi
fi

# Step 4: Scrape content
log "=== STEP 4: Scraping content from URLs ==="
log "This step will take several hours and runs automatically with progress tracking"
log "You can safely interrupt and restart - it will resume from where it left off"
log "Starting step 4..."

python3 notebooks/url_extraction/step4_scrape_content.py
if [ $? -eq 0 ]; then
    success "Step 4 completed successfully"
else
    error "Step 4 failed"
fi

# Final summary
log "=== PIPELINE COMPLETED ==="
log "Check the following output files:"
log "  - $OUTPUT_DIR/extracted_urls_optimized.csv"
log "  - $OUTPUT_DIR/analyzed_urls.csv"
log "  - $OUTPUT_DIR/prioritized_batches/"
log "  - $OUTPUT_DIR/scraped_content/"
log "  - $LOG_DIR/pipeline.log"

success "Pipeline completed successfully!" 