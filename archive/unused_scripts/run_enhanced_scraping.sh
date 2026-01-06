#!/bin/bash

# Enhanced Content Scraping Pipeline Runner
# =========================================
# Runner for the enhanced scraper with improved unique URL handling
# and better stop/restart functionality.

set -e  # Exit on any error

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VM_DATA_DIR="/work/Datadonationer/urlextraction_scraping/data"
VM_OUTPUT_DIR="/work/Datadonationer/urlextraction_scraping/data/browser_scraped_final"
VM_LOG_DIR="/work/Datadonationer/urlextraction_scraping/logs"
TMUX_SESSION="enhanced_scraping"
ENHANCED_SCRIPT="scripts/content_scraping/browser_content_scraper_v2.py"

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

# Function to copy enhanced script to VM
copy_enhanced_script() {
    log "Copying enhanced scraping script to VM..."
    
    # Copy the enhanced Python script to the correct organized location
    scp -P 2089 "$SCRIPT_DIR/browser_content_scraper_v2.py" ucloud@ssh.cloud.sdu.dk:/work/Datadonationer/urlextraction_scraping/scripts/content_scraping/
    
    if [ $? -eq 0 ]; then
        success "Enhanced script copied successfully"
    else
        error "Failed to copy enhanced script to VM"
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

# Function to test enhanced scraper
test_enhanced_scraper() {
    log "Testing enhanced scraper (2 batches)..."
    
    ssh -p 2089 ucloud@ssh.cloud.sdu.dk "
        cd /work/Datadonationer/urlextraction_scraping
        source venv/bin/activate
        
        echo 'Testing enhanced scraper with 2 batches...'
        python3 $ENHANCED_SCRIPT \\
            --data-dir '$VM_DATA_DIR' \\
            --output-dir '$VM_OUTPUT_DIR' \\
            --log-dir '$VM_LOG_DIR' \\
            --max-batches 2
    "
}

# Function to start enhanced scraping in tmux
start_enhanced_scraping() {
    log "Starting enhanced content scraping in tmux session: $TMUX_SESSION"
    
    # Create tmux session on VM
    ssh -p 2089 ucloud@ssh.cloud.sdu.dk "
        # Kill existing session if it exists
        tmux kill-session -t '$TMUX_SESSION' 2>/dev/null || true
        
        # Create new tmux session
        tmux new-session -d -s '$TMUX_SESSION' -c '/work/Datadonationer/urlextraction_scraping'
        
        # Send commands to the tmux session
        tmux send-keys -t '$TMUX_SESSION' 'source venv/bin/activate' Enter
        tmux send-keys -t '$TMUX_SESSION' 'echo \"=== Starting Enhanced Browser Content Scraping Pipeline ===\"' Enter
        tmux send-keys -t '$TMUX_SESSION' 'python3 $ENHANCED_SCRIPT --data-dir \"$VM_DATA_DIR\" --output-dir \"$VM_OUTPUT_DIR\" --log-dir \"$VM_LOG_DIR\"' Enter
        
        echo 'Enhanced tmux session \"$TMUX_SESSION\" started successfully'
        echo 'The enhanced scraper includes:'
        echo '  ✓ Better unique URL detection'
        echo '  ✓ Signal handling for clean shutdown (Ctrl+C)'
        echo '  ✓ Enhanced progress tracking'
        echo '  ✓ Automatic resume functionality'
        echo ''
        echo 'Use the following command to attach to the session:'
        echo 'ssh -p 2089 ucloud@ssh.cloud.sdu.dk -t \"tmux attach-session -t $TMUX_SESSION\"'
    "
}

# Function to gracefully stop scraping
stop_enhanced_scraping() {
    log "Gracefully stopping enhanced scraping..."
    
    ssh -p 2089 ucloud@ssh.cloud.sdu.dk "
        if tmux has-session -t '$TMUX_SESSION' 2>/dev/null; then
            echo 'Sending graceful shutdown signal to scraper...'
            
            # Send Ctrl+C to the session for graceful shutdown
            tmux send-keys -t '$TMUX_SESSION' C-c
            
            echo 'Waiting for clean shutdown (up to 30 seconds)...'
            sleep 5
            
            # Check if still running after graceful shutdown attempt
            if tmux has-session -t '$TMUX_SESSION' 2>/dev/null; then
                echo 'Session still running after 5 seconds, waiting...'
                sleep 10
                
                if tmux has-session -t '$TMUX_SESSION' 2>/dev/null; then
                    echo 'Forcing session termination...'
                    tmux kill-session -t '$TMUX_SESSION'
                fi
            fi
            
            echo 'Enhanced scraping session stopped'
        else
            echo 'No enhanced scraping session found'
        fi
    "
}

# Function to check tmux status
check_enhanced_status() {
    log "Checking enhanced tmux session status..."
    
    ssh -p 2089 ucloud@ssh.cloud.sdu.dk "
        echo '=== Tmux Sessions ==='
        tmux list-sessions 2>/dev/null || echo 'No tmux sessions found'
        
        echo
        echo '=== Enhanced Scraping Session Status ==='
        if tmux has-session -t '$TMUX_SESSION' 2>/dev/null; then
            echo 'Session \"$TMUX_SESSION\" is running'
            echo 'Windows in session:'
            tmux list-windows -t '$TMUX_SESSION'
        else
            echo 'Session \"$TMUX_SESSION\" is not running'
        fi
    "
}

# Function to show enhanced logs
show_enhanced_logs() {
    log "Showing enhanced scraping logs..."
    
    ssh -p 2089 ucloud@ssh.cloud.sdu.dk "
        cd '$VM_LOG_DIR'
        
        echo '=== Recent Enhanced Log Files ==='
        ls -lat enhanced_browser_scraping_*.log 2>/dev/null | head -3 || echo 'No enhanced log files found'
        
        echo
        echo '=== Latest Enhanced Log Tail ==='
        latest_log=\$(ls -t enhanced_browser_scraping_*.log 2>/dev/null | head -1)
        if [ -n \"\$latest_log\" ]; then
            echo \"Showing last 25 lines of: \$latest_log\"
            tail -25 \"\$latest_log\"
        else
            echo 'No enhanced log files found'
        fi
    "
}

# Function to show enhanced progress
show_enhanced_progress() {
    log "Showing enhanced scraping progress..."
    
    ssh -p 2089 ucloud@ssh.cloud.sdu.dk "
        cd '$VM_OUTPUT_DIR'
        
        echo '=== Enhanced Scraping Progress ==='
        if [ -f 'scraping_progress.json' ]; then
            echo 'Enhanced progress file found:'
            python3 -c \"
import json
try:
    with open('scraping_progress.json', 'r') as f:
        data = json.load(f)
    print(f'Current batch: {data.get(\\\"current_batch\\\", \\\"N/A\\\")}')
    print(f'Total batches: {data.get(\\\"total_batches\\\", \\\"N/A\\\")}')
    print(f'Completion: {data.get(\\\"completion_percentage\\\", 0):.1f}%')
    print(f'Clean shutdown: {data.get(\\\"shutdown_clean\\\", False)}')
    
    stats = data.get('stats', {})
    print(f'Total processed: {stats.get(\\\"total_processed\\\", 0):,}')
    print(f'Successful: {stats.get(\\\"successful_scrapes\\\", 0):,}')
    print(f'Failed: {stats.get(\\\"failed_scrapes\\\", 0):,}')
    print(f'Duplicates skipped: {stats.get(\\\"duplicates_skipped\\\", 0):,}')
    
except Exception as e:
    print(f'Error reading progress: {e}')
\"
        else
            echo 'No enhanced progress file found'
        fi
        
        echo
        echo '=== URL Tracker ==='
        if [ -f 'processed_urls.txt' ]; then
            echo 'URL tracker found:'
            wc -l processed_urls.txt
        else
            echo 'No URL tracker found'
        fi
        
        echo
        echo '=== Output Files ==='
        if [ -f 'scraped_content_from_resolved.csv' ]; then
            echo 'Scraped content file found:'
            ls -la scraped_content_from_resolved.csv
            echo 'Number of scraped URLs:'
            tail -n +2 scraped_content_from_resolved.csv | wc -l 2>/dev/null || echo 'Could not count URLs'
        else
            echo 'No scraped content file found yet'
        fi
    "
}

# Function to attach to enhanced tmux session
attach_enhanced_tmux() {
    log "Attaching to enhanced tmux session..."
    echo "Connecting to VM and attaching to enhanced session '$TMUX_SESSION'..."
    echo "To detach: Ctrl+b then d"
    echo "To gracefully stop scraper: Ctrl+c"
    ssh -p 2089 ucloud@ssh.cloud.sdu.dk -t "tmux attach-session -t '$TMUX_SESSION'"
}

# Main function
main() {
    echo "============================================"
    echo "Enhanced Browser Content Scraping Pipeline"
    echo "============================================"
    echo
    
    # Parse command line arguments
    case "${1:-}" in
        "setup")
            log "Setting up enhanced scraping environment..."
            check_vm_connection
            copy_enhanced_script
            success "Enhanced setup completed!"
            echo
            echo "Enhancements include:"
            echo "  ✓ Better unique URL detection with normalization"
            echo "  ✓ URL tracker file for perfect resume capability"
            echo "  ✓ Signal handling for graceful shutdown"
            echo "  ✓ Enhanced progress tracking"
            echo "  ✓ Improved duplicate detection"
            echo
            echo "Next steps:"
            echo "  Test run:  $0 test"
            echo "  Full run:  $0 start"
            ;;
        "test")
            log "Running enhanced scraping test..."
            check_vm_connection
            test_enhanced_scraper
            success "Enhanced test completed!"
            ;;
        "start")
            log "Starting enhanced scraping pipeline..."
            check_vm_connection
            start_enhanced_scraping
            success "Enhanced scraping pipeline started!"
            echo
            echo "To monitor progress:"
            echo "  Attach:     $0 attach"
            echo "  Status:     $0 status"  
            echo "  Logs:       $0 logs"
            echo "  Progress:   $0 progress"
            echo "  Stop:       $0 stop (graceful)"
            ;;
        "status")
            check_enhanced_status
            ;;
        "logs")
            show_enhanced_logs
            ;;
        "progress")
            show_enhanced_progress
            ;;
        "attach")
            attach_enhanced_tmux
            ;;
        "stop")
            stop_enhanced_scraping
            ;;
        "help"|"--help"|"")
            echo "Usage: $0 [COMMAND]"
            echo
            echo "Enhanced Browser Content Scraping Pipeline Commands:"
            echo "  setup     - Setup enhanced environment and copy scripts"
            echo "  test      - Run enhanced test scraping (2 batches)"
            echo "  start     - Start enhanced scraping pipeline in tmux"
            echo "  status    - Check enhanced tmux session status"
            echo "  logs      - Show enhanced scraping logs"
            echo "  progress  - Show detailed scraping progress"
            echo "  attach    - Attach to enhanced tmux session"
            echo "  stop      - Gracefully stop enhanced scraping"
            echo "  help      - Show this help message"
            echo
            echo "Enhanced Features:"
            echo "  ✓ Better unique URL detection"
            echo "  ✓ Signal handling for clean shutdown"
            echo "  ✓ URL tracker for perfect resume"
            echo "  ✓ Enhanced progress tracking"
            echo "  ✓ Improved duplicate detection"
            echo
            echo "Example workflow:"
            echo "  $0 setup     # Setup enhanced scraper"
            echo "  $0 test      # Test with 2 batches"  
            echo "  $0 start     # Start full pipeline"
            echo "  $0 attach    # Monitor progress"
            echo "  $0 stop      # Graceful shutdown"
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