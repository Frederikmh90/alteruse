#!/bin/bash

# Content Scraping Pipeline Runner for tmux
# ========================================
# Simple script to run the browser content scraping pipeline in tmux
# with proper logging and monitoring capabilities.

set -e  # Exit on any error

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VM_DATA_DIR="/work/Datadonationer/urlextraction_scraping/data"
VM_OUTPUT_DIR="/work/Datadonationer/urlextraction_scraping/data/browser_scraped_final"
VM_LOG_DIR="/work/Datadonationer/urlextraction_scraping/logs"
TMUX_SESSION="content_scraping"

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

# Function to copy script to VM
copy_script_to_vm() {
    log "Copying scraping script to VM..."
    
    # Copy the Python script
    scp -P 2089 "$SCRIPT_DIR/browser_content_scraper.py" ucloud@ssh.cloud.sdu.dk:/work/Datadonationer/urlextraction_scraping/
    
    if [ $? -eq 0 ]; then
        success "Script copied successfully"
    else
        error "Failed to copy script to VM"
        exit 1
    fi
}

# Function to check VM connectivity
check_vm_connection() {
    log "Checking VM connectivity..."
    
    ssh -p 2089 ucloud@ssh.cloud.sdu.dk "echo 'VM connection successful'" > /dev/null 2>&1
    
    if [ $? -eq 0 ]; then
        success "VM connection verified"
    else
        error "Cannot connect to VM"
        exit 1
    fi
}

# Function to setup VM environment
setup_vm_environment() {
    log "Setting up VM environment..."
    
    ssh -p 2089 ucloud@ssh.cloud.sdu.dk "
        cd /work/Datadonationer/urlextraction_scraping
        
        # Create necessary directories
        mkdir -p '$VM_OUTPUT_DIR'
        mkdir -p '$VM_LOG_DIR'
        
        # Check Python environment
        if [ -d 'venv' ]; then
            echo 'Virtual environment found'
        else
            echo 'Creating virtual environment...'
            python3 -m venv venv
        fi
        
        # Activate and install dependencies
        source venv/bin/activate
        
        echo 'Installing/updating dependencies...'
        pip install pandas requests trafilatura numpy pathlib-enhanced || echo 'Some packages may already be installed'
        
        echo 'VM environment setup completed'
    "
    
    if [ $? -eq 0 ]; then
        success "VM environment setup completed"
    else
        error "Failed to setup VM environment"
        exit 1
    fi
}

# Function to check data availability
check_data_availability() {
    log "Checking data availability on VM..."
    
    ssh -p 2089 ucloud@ssh.cloud.sdu.dk "
        cd '$VM_DATA_DIR'
        
        echo '=== Data Directory Check ==='
        pwd
        
        echo 'Resolved batch files:'
        ls complete_resolved/resolved_browser_batch_*.csv | wc -l
        
        echo 'Sample batch file:'
        ls complete_resolved/resolved_browser_batch_0001.csv 2>/dev/null || echo 'Batch 001 not found'
        
        echo 'Existing scraped content:'
        if [ -f 'browser_urlextract/scraped_content/scraped_content.csv' ]; then
            echo 'Found existing scraped content:'
            wc -l browser_urlextract/scraped_content/scraped_content.csv
        else
            echo 'No existing scraped content found'
        fi
    "
}

# Function to run test scraping
run_test_scraping() {
    log "Running test scraping (5 batches)..."
    
    ssh -p 2089 ucloud@ssh.cloud.sdu.dk "
        cd /work/Datadonationer/urlextraction_scraping
        source venv/bin/activate
        
        echo 'Starting test run with 5 batches...'
        python3 browser_content_scraper.py \\
            --data-dir '$VM_DATA_DIR' \\
            --output-dir '$VM_OUTPUT_DIR' \\
            --log-dir '$VM_LOG_DIR' \\
            --max-batches 5
    "
}

# Function to start full scraping in tmux
start_full_scraping() {
    log "Starting full content scraping in tmux session: $TMUX_SESSION"
    
    # Create tmux session on VM
    ssh -p 2089 ucloud@ssh.cloud.sdu.dk "
        # Kill existing session if it exists
        tmux kill-session -t '$TMUX_SESSION' 2>/dev/null || true
        
        # Create new tmux session
        tmux new-session -d -s '$TMUX_SESSION' -c '/work/Datadonationer/urlextraction_scraping'
        
        # Send commands to the tmux session
        tmux send-keys -t '$TMUX_SESSION' 'source venv/bin/activate' Enter
        tmux send-keys -t '$TMUX_SESSION' 'echo \"=== Starting Browser Content Scraping Pipeline ===\"' Enter
        tmux send-keys -t '$TMUX_SESSION' 'python3 browser_content_scraper.py --data-dir \"$VM_DATA_DIR\" --output-dir \"$VM_OUTPUT_DIR\" --log-dir \"$VM_LOG_DIR\"' Enter
        
        echo 'Tmux session \"$TMUX_SESSION\" started successfully'
        echo 'Use the following command to attach to the session:'
        echo 'ssh -p 2089 ucloud@ssh.cloud.sdu.dk -t \"tmux attach-session -t $TMUX_SESSION\"'
    "
}

# Function to check tmux status
check_tmux_status() {
    log "Checking tmux session status..."
    
    ssh -p 2089 ucloud@ssh.cloud.sdu.dk "
        echo '=== Tmux Sessions ==='
        tmux list-sessions 2>/dev/null || echo 'No tmux sessions found'
        
        echo
        echo '=== Content Scraping Session Status ==='
        if tmux has-session -t '$TMUX_SESSION' 2>/dev/null; then
            echo 'Session \"$TMUX_SESSION\" is running'
            echo 'Windows in session:'
            tmux list-windows -t '$TMUX_SESSION'
        else
            echo 'Session \"$TMUX_SESSION\" is not running'
        fi
    "
}

# Function to show logs
show_logs() {
    log "Showing recent scraping logs..."
    
    ssh -p 2089 ucloud@ssh.cloud.sdu.dk "
        cd '$VM_LOG_DIR'
        
        echo '=== Recent Log Files ==='
        ls -lat browser_content_scraping_*.log 2>/dev/null | head -3 || echo 'No log files found'
        
        echo
        echo '=== Latest Log Tail ==='
        latest_log=\$(ls -t browser_content_scraping_*.log 2>/dev/null | head -1)
        if [ -n \"\$latest_log\" ]; then
            echo \"Showing last 20 lines of: \$latest_log\"
            tail -20 \"\$latest_log\"
        else
            echo 'No log files found'
        fi
    "
}

# Function to show progress
show_progress() {
    log "Showing scraping progress..."
    
    ssh -p 2089 ucloud@ssh.cloud.sdu.dk "
        cd '$VM_OUTPUT_DIR'
        
        echo '=== Scraping Progress ==='
        if [ -f 'scraping_progress.json' ]; then
            echo 'Progress file found:'
            cat scraping_progress.json | python3 -m json.tool 2>/dev/null || cat scraping_progress.json
        else
            echo 'No progress file found'
        fi
        
        echo
        echo '=== Output Files ==='
        if [ -f 'scraped_content_from_resolved.csv' ]; then
            echo 'Scraped content file found:'
            ls -la scraped_content_from_resolved.csv
            echo 'Number of scraped URLs:'
            tail -n +2 scraped_content_from_resolved.csv | wc -l
        else
            echo 'No scraped content file found yet'
        fi
    "
}

# Function to attach to tmux session
attach_tmux() {
    log "Attaching to tmux session..."
    echo "Connecting to VM and attaching to session '$TMUX_SESSION'..."
    ssh -p 2089 ucloud@ssh.cloud.sdu.dk -t "tmux attach-session -t '$TMUX_SESSION'"
}

# Main function
main() {
    echo "=========================================="
    echo "Browser Content Scraping Pipeline Runner"
    echo "=========================================="
    echo
    
    # Parse command line arguments
    case "${1:-}" in
        "setup")
            log "Setting up scraping environment..."
            check_vm_connection
            copy_script_to_vm
            setup_vm_environment
            check_data_availability
            success "Setup completed! Ready to run scraping."
            echo
            echo "Next steps:"
            echo "  Test run:  $0 test"
            echo "  Full run:  $0 start"
            ;;
        "test")
            log "Running test scraping..."
            check_vm_connection
            run_test_scraping
            success "Test scraping completed!"
            ;;
        "start")
            log "Starting full scraping pipeline..."
            check_vm_connection
            start_full_scraping
            success "Scraping pipeline started in tmux!"
            echo
            echo "To monitor progress:"
            echo "  Attach:     $0 attach"
            echo "  Status:     $0 status"
            echo "  Logs:       $0 logs"
            echo "  Progress:   $0 progress"
            ;;
        "status")
            check_tmux_status
            ;;
        "logs")
            show_logs
            ;;
        "progress")
            show_progress
            ;;
        "attach")
            attach_tmux
            ;;
        "stop")
            log "Stopping content scraping..."
            ssh -p 2089 ucloud@ssh.cloud.sdu.dk "tmux kill-session -t '$TMUX_SESSION' 2>/dev/null || echo 'Session not running'"
            success "Scraping session stopped"
            ;;
        "help"|"--help"|"")
            echo "Usage: $0 [COMMAND]"
            echo
            echo "Commands:"
            echo "  setup     - Setup environment and copy scripts to VM"
            echo "  test      - Run test scraping (5 batches)"
            echo "  start     - Start full scraping pipeline in tmux"
            echo "  status    - Check tmux session status"
            echo "  logs      - Show recent scraping logs"
            echo "  progress  - Show scraping progress"
            echo "  attach    - Attach to tmux session"
            echo "  stop      - Stop scraping session"
            echo "  help      - Show this help message"
            echo
            echo "Example workflow:"
            echo "  $0 setup     # First time setup"
            echo "  $0 test      # Test with 5 batches"
            echo "  $0 start     # Start full pipeline"
            echo "  $0 attach    # Monitor progress"
            ;;
        *)
            error "Unknown command: $1"
            echo "Use '$0 help' for usage information"
            exit 1
            ;;
    esac
}

# Run main function with all arguments
main "$@" 