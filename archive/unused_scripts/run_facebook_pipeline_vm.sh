#!/bin/bash
# Facebook URL Extraction Pipeline for VM
# ======================================

set -e

# Configuration
DATA_DIR="/work/Datadonationer/data/Kantar_download_398_unzipped_new"
OUTPUT_DIR="/work/Datadonationer/data/url_extract_facebook"
PIPELINE_DIR="/work/Datadonationer/facebook_pipeline"
LOG_DIR="/work/Datadonationer/logs"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging function
log() {
    echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $1"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

# Check if running in tmux
if [ -z "$TMUX" ]; then
    error "This script should be run inside a tmux session"
    echo "Please run: tmux new-session -d -s facebook_pipeline"
    echo "Then: tmux attach-session -t facebook_pipeline"
    echo "Then run this script again"
    exit 1
fi

# Create directories
log "Creating directories..."
mkdir -p "$OUTPUT_DIR"
mkdir -p "$PIPELINE_DIR"
mkdir -p "$LOG_DIR"

# Check if data directory exists
if [ ! -d "$DATA_DIR" ]; then
    error "Data directory not found: $DATA_DIR"
    echo "Please ensure the Facebook data is uploaded to the correct location"
    exit 1
fi

# Check if pipeline files exist
if [ ! -f "$PIPELINE_DIR/step1_extract_urls_facebook.py" ]; then
    error "Pipeline files not found in $PIPELINE_DIR"
    echo "Please ensure the pipeline files are uploaded"
    exit 1
fi

# Function to run a step with logging
run_step() {
    local step_name="$1"
    local script_name="$2"
    local log_file="$LOG_DIR/facebook_${step_name}_$(date +%Y%m%d_%H%M%S).log"
    
    log "Starting Step $step_name: $script_name"
    info "Log file: $log_file"
    
    cd "$PIPELINE_DIR"
    
    # Run the step and capture output
    if python "$script_name" 2>&1 | tee "$log_file"; then
        log "Step $step_name completed successfully"
        return 0
    else
        error "Step $step_name failed"
        error "Check log file: $log_file"
        return 1
    fi
}

# Main pipeline execution
main() {
    log "Starting Facebook URL Extraction Pipeline"
    log "Data directory: $DATA_DIR"
    log "Output directory: $OUTPUT_DIR"
    log "Pipeline directory: $PIPELINE_DIR"
    
    # Check Python environment
    if ! command -v python &> /dev/null; then
        error "Python not found"
        exit 1
    fi
    
    # Check if virtual environment exists
    if [ ! -d "/work/Datadonationer/venv" ]; then
        warning "Virtual environment not found, creating one..."
        python -m venv /work/Datadonationer/venv
        source /work/Datadonationer/venv/bin/activate
        pip install -r "$PIPELINE_DIR/requirements.txt"
    else
        source /work/Datadonationer/venv/bin/activate
    fi
    
    # Step 1: Extract URLs
    if ! run_step "1" "step1_extract_urls_facebook.py"; then
        error "Pipeline failed at Step 1"
        exit 1
    fi
    
    # Step 2: Analyze URLs
    if ! run_step "2" "step2_analyze_urls_facebook.py"; then
        error "Pipeline failed at Step 2"
        exit 1
    fi
    
    # Step 3: Prioritize domains
    if ! run_step "3" "step3_prioritize_domains_facebook.py"; then
        error "Pipeline failed at Step 3"
        exit 1
    fi
    
    # Step 4: Scrape content (with user input)
    log "Step 4: Content scraping requires user input"
    info "To start scraping, run:"
    echo "cd $PIPELINE_DIR"
    echo "python step4_scrape_content_facebook_enhanced.py"
    echo ""
    info "Or to run the original version:"
    echo "python step4_scrape_content_facebook.py"
    
    log "Pipeline Steps 1-3 completed successfully!"
    log "Ready for Step 4 (content scraping)"
    
    # Show summary
    echo ""
    log "Pipeline Summary:"
    echo "================="
    echo "Data processed: $DATA_DIR"
    echo "Output location: $OUTPUT_DIR"
    echo "Logs location: $LOG_DIR"
    echo ""
    echo "Next steps:"
    echo "1. Review the analysis results in $OUTPUT_DIR"
    echo "2. Start content scraping with Step 4"
    echo "3. Monitor progress in tmux session"
}

# Run main function
main "$@" 