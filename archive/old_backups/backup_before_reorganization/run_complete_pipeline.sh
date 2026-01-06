#!/bin/bash

# Complete URL Resolution Pipeline Runner
# ======================================
# This script runs the complete pipeline that integrates all extraction steps
# and creates a proper plan with resolved URLs.

set -e  # Exit on any error

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$SCRIPT_DIR"
DATA_DIR="$BASE_DIR/data"
LOG_DIR="$BASE_DIR/logs"
OUTPUT_DIR="$DATA_DIR/complete_resolved"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging function
log() {
    echo -e "${BLUE}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $1"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1" >&2
}

success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

# Create necessary directories
create_directories() {
    log "Creating necessary directories..."
    mkdir -p "$DATA_DIR"
    mkdir -p "$LOG_DIR"
    mkdir -p "$OUTPUT_DIR"
    success "Directories created"
}

# Check Python environment
check_python() {
    log "Checking Python environment..."
    
    if ! command -v python3 &> /dev/null; then
        error "Python3 is not installed"
        exit 1
    fi
    
    python_version=$(python3 --version 2>&1)
    log "Python version: $python_version"
    
    # Check if virtual environment exists
    if [ -d "venv" ]; then
        log "Virtual environment found, activating..."
        source venv/bin/activate
    else
        warning "No virtual environment found, creating one..."
        python3 -m venv venv
        source venv/bin/activate
        pip install --upgrade pip
    fi
    
    success "Python environment ready"
}

# Install dependencies
install_dependencies() {
    log "Installing dependencies..."
    
    # Install required packages
    pip install pandas numpy requests beautifulsoup4 lxml urllib3
    
    # Install additional packages for URL resolution
    pip install aiohttp asyncio tqdm
    
    success "Dependencies installed"
}

# Check data files
check_data_files() {
    log "Checking data files..."
    
    browser_file="$DATA_DIR/browser_urlextract/extracted_urls_optimized.csv"
    facebook_file="$DATA_DIR/facebook_urlextract/extracted_urls_facebook.csv"
    
    if [ ! -f "$browser_file" ]; then
        error "Browser URLs file not found: $browser_file"
        exit 1
    fi
    
    if [ ! -f "$facebook_file" ]; then
        error "Facebook URLs file not found: $facebook_file"
        exit 1
    fi
    
    browser_count=$(wc -l < "$browser_file")
    facebook_count=$(wc -l < "$facebook_file")
    
    log "Found $browser_count browser URLs"
    log "Found $facebook_count Facebook URLs"
    
    success "Data files verified"
}

# Run the complete pipeline
run_pipeline() {
    log "Starting complete URL resolution pipeline..."
    
    # Set environment variables
    export PYTHONPATH="$BASE_DIR:$PYTHONPATH"
    export PYTHONUNBUFFERED=1
    
    # Run the pipeline
    python3 complete_url_resolution_pipeline.py \
        --base-dir "$DATA_DIR" \
        2>&1 | tee "$LOG_DIR/complete_pipeline_$(date +%Y%m%d_%H%M%S).log"
    
    if [ $? -eq 0 ]; then
        success "Pipeline completed successfully"
    else
        error "Pipeline failed"
        exit 1
    fi
}

# Generate summary report
generate_summary() {
    log "Generating summary report..."
    
    summary_file="$OUTPUT_DIR/pipeline_summary.json"
    if [ -f "$summary_file" ]; then
        echo "=== PIPELINE SUMMARY ==="
        python3 -c "
import json
with open('$summary_file', 'r') as f:
    summary = json.load(f)
print(f\"Duration: {summary['pipeline_summary']['duration']}\")
print(f\"URLs loaded: {summary['data_loading']['total_urls_loaded']}\")
print(f\"Unique URLs: {summary['url_resolution']['total_unique_urls']}\")
print(f\"Success rate: {summary['url_resolution']['resolution_success_rate']:.2%}\")
"
    else
        warning "No summary file found"
    fi
}

# List output files
list_outputs() {
    log "Pipeline outputs:"
    
    if [ -d "$OUTPUT_DIR" ]; then
        echo "Files in $OUTPUT_DIR:"
        ls -la "$OUTPUT_DIR"
    else
        warning "Output directory not found"
    fi
}

# Main execution
main() {
    echo "=========================================="
    echo "Complete URL Resolution Pipeline"
    echo "=========================================="
    echo
    
    # Parse command line arguments
    while [[ $# -gt 0 ]]; do
        case $1 in
            --test)
                log "Running in test mode..."
                python3 complete_url_resolution_pipeline.py --test-only
                exit 0
                ;;
            --check-only)
                log "Running checks only..."
                create_directories
                check_python
                check_data_files
                success "All checks passed"
                exit 0
                ;;
            --help)
                echo "Usage: $0 [OPTIONS]"
                echo "Options:"
                echo "  --test       Run URL resolution test only"
                echo "  --check-only Run environment checks only"
                echo "  --help       Show this help message"
                exit 0
                ;;
            *)
                error "Unknown option: $1"
                exit 1
                ;;
        esac
    done
    
    # Run full pipeline
    create_directories
    check_python
    install_dependencies
    check_data_files
    run_pipeline
    generate_summary
    list_outputs
    
    echo
    success "Complete URL Resolution Pipeline finished!"
    echo "Check the logs in $LOG_DIR for detailed information"
}

# Run main function
main "$@" 