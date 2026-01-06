#!/bin/bash

# Turbo Content Scraping Pipeline Runner
# =====================================
# Ultra-fast scraper with parallel processing and speed optimization

set -e  # Exit on any error

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VM_DATA_DIR="/work/Datadonationer/urlextraction_scraping/data"
VM_OUTPUT_DIR="/work/Datadonationer/urlextraction_scraping/data/browser_scraped_turbo"
VM_LOG_DIR="/work/Datadonationer/urlextraction_scraping/logs"
TMUX_SESSION="turbo_scraping"
TURBO_SCRIPT="scripts/content_scraping/browser_content_scraper_turbo.py"

# Performance settings - these can be tuned!
MAX_WORKERS=20        # Parallel threads (increase for more speed)
TIMEOUT=15           # Request timeout (reduced for speed)
MAX_BATCHES=         # Empty = process all batches

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
PURPLE='\033[0;35m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Logging function
log() {
    echo -e "${BLUE}[$(date +'%Y-%m-%d %H:%M:%S')] $1${NC}"
}

success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

turbo() {
    echo -e "${PURPLE}[TURBO]${NC} $1"
}

# Function to print header
print_header() {
    echo -e "${CYAN}============================================${NC}"
    echo -e "${CYAN}🚀 TURBO Browser Content Scraping Pipeline${NC}"
    echo -e "${CYAN}============================================${NC}"
    echo
}

# Function to check VM connectivity
check_vm_connectivity() {
    if ssh -p 2089 -o ConnectTimeout=5 ucloud@ssh.cloud.sdu.dk "echo 'VM connection test'" >/dev/null 2>&1; then
        return 0
    else
        return 1
    fi
}

# Function to copy turbo script to VM
copy_turbo_script() {
    log "Copying turbo scraping script to VM..."
    
    # Copy the turbo Python script to the correct organized location
    scp -P 2089 "$SCRIPT_DIR/browser_content_scraper_turbo.py" ucloud@ssh.cloud.sdu.dk:/work/Datadonationer/urlextraction_scraping/scripts/content_scraping/
    
    if [ $? -eq 0 ]; then
        success "Turbo script copied successfully"
    else
        error "Failed to copy turbo script to VM"
        exit 1
    fi
}

# Function to setup environment
setup_environment() {
    print_header
    log "Setting up turbo scraping environment..."
    
    # Check VM connectivity
    log "Checking VM connectivity..."
    if check_vm_connectivity; then
        success "VM connection verified"
    else
        error "Cannot connect to VM. Please check your connection."
        exit 1
    fi
    
    # Copy turbo script
    copy_turbo_script
    
    # Setup directories and environment on VM
    ssh -p 2089 ucloud@ssh.cloud.sdu.dk << 'EOF'
        cd /work/Datadonationer/urlextraction_scraping
        
        # Create turbo output directory
        mkdir -p data/browser_scraped_turbo
        mkdir -p logs
        
        # Activate virtual environment
        source venv/bin/activate
        
        # Check if all required packages are installed
        python3 -c "import pandas, requests, trafilatura; print('All packages available')" || {
            echo "Installing missing packages..."
            pip install pandas requests trafilatura numpy lxml beautifulsoup4
        }
        
        echo "Turbo environment setup complete!"
        
        # Show current directory structure
        echo "=== Data Directory Status ==="
        find data -name "resolved_browser_batch_*.csv" | wc -l | xargs echo "Resolved browser batches:"
        ls -la data/browser_scraped_turbo/ 2>/dev/null || echo "Turbo output directory created"
EOF
    
    if [ $? -eq 0 ]; then
        success "Turbo environment setup complete"
    else
        error "Failed to setup turbo environment"
        exit 1
    fi
}

# Function to start turbo scraping
start_turbo_scraping() {
    print_header
    log "Starting turbo scraping pipeline..."
    
    # Check VM connectivity first
    log "Checking VM connectivity..."
    if ! check_vm_connectivity; then
        error "Cannot connect to VM. Please check your connection."
        exit 1
    fi
    
    success "VM connection verified"
    
    copy_turbo_script
    
    log "Starting turbo content scraping in tmux session: $TMUX_SESSION"
    
    # Build command with performance parameters
    TURBO_CMD="cd /work/Datadonationer/urlextraction_scraping && source venv/bin/activate && python3 $TURBO_SCRIPT --data-dir '$VM_DATA_DIR' --output-dir '$VM_OUTPUT_DIR' --log-dir '$VM_LOG_DIR' --max-workers $MAX_WORKERS --timeout $TIMEOUT"
    
    if [ -n "$MAX_BATCHES" ]; then
        TURBO_CMD="$TURBO_CMD --max-batches $MAX_BATCHES"
    fi
    
    # Start in tmux
    ssh -p 2089 ucloud@ssh.cloud.sdu.dk "tmux new-session -d -s '$TMUX_SESSION' '$TURBO_CMD'"
    
    if [ $? -eq 0 ]; then
        success "Turbo scraping pipeline started!"
        echo
        turbo "Performance Configuration:"
        turbo "  🔥 Max Workers: $MAX_WORKERS parallel threads"
        turbo "  ⚡ Timeout: ${TIMEOUT}s per request"
        turbo "  🚀 Expected Speed: 10-50x faster than sequential!"
        echo
        echo "The turbo scraper includes:"
        echo "  ✓ Parallel processing with $MAX_WORKERS workers"
        echo "  ✓ Optimized timeouts and session pooling"
        echo "  ✓ Enhanced duplicate detection"
        echo "  ✓ Real-time performance monitoring"
        echo "  ✓ Thread-safe progress tracking"
        echo
        echo "Use the following command to attach to the session:"
        echo "ssh -p 2089 ucloud@ssh.cloud.sdu.dk -t \"tmux attach-session -t $TMUX_SESSION\""
        success "Turbo scraping pipeline started!"
        echo
        echo "To monitor progress:"
        echo "  Attach:     $0 attach"
        echo "  Status:     $0 status"
        echo "  Logs:       $0 logs"
        echo "  Progress:   $0 progress"
        echo "  Stop:       $0 stop"
    else
        error "Failed to start turbo scraping pipeline"
        exit 1
    fi
}

# Function to check status
check_status() {
    print_header
    log "Checking turbo tmux session status..."
    ssh -p 2089 ucloud@ssh.cloud.sdu.dk << EOF
        echo "=== Tmux Sessions ==="
        tmux list-sessions 2>/dev/null || echo "No tmux sessions found"
        echo
        echo "=== Turbo Scraping Session Status ==="
        if tmux has-session -t '$TMUX_SESSION' 2>/dev/null; then
            echo "Session \"$TMUX_SESSION\" is running"
            echo "Windows in session:"
            tmux list-windows -t '$TMUX_SESSION'
        else
            echo "Session \"$TMUX_SESSION\" is not running"
        fi
EOF
}

# Function to attach to session
attach_session() {
    print_header
    log "Attaching to turbo scraping session..."
    ssh -p 2089 ucloud@ssh.cloud.sdu.dk -t "tmux attach-session -t $TMUX_SESSION"
}

# Function to show logs
show_logs() {
    print_header
    log "Showing turbo scraping logs..."
    ssh -p 2089 ucloud@ssh.cloud.sdu.dk << EOF
        cd /work/Datadonationer/urlextraction_scraping/logs
        echo "=== Recent Turbo Log Files ==="
        ls -la turbo_browser_scraping_*.log 2>/dev/null | tail -5
        echo
        echo "=== Latest Turbo Log Tail ==="
        LATEST_LOG=\$(ls -t turbo_browser_scraping_*.log 2>/dev/null | head -1)
        if [ -n "\$LATEST_LOG" ]; then
            echo "Showing last 25 lines of: \$LATEST_LOG"
            tail -25 "\$LATEST_LOG"
        else
            echo "No turbo log files found"
        fi
EOF
}

# Function to show progress
show_progress() {
    print_header
    log "Showing turbo scraping progress..."
    ssh -p 2089 ucloud@ssh.cloud.sdu.dk << EOF
        cd /work/Datadonationer/urlextraction_scraping
        echo "=== Turbo Performance Metrics ==="
        
        # Check log for performance data
        LATEST_LOG=\$(ls -t logs/turbo_browser_scraping_*.log 2>/dev/null | head -1)
        if [ -n "\$LATEST_LOG" ]; then
            echo "Performance from latest log:"
            grep -E "URLs/sec|Performance|Speed|Processed.*batch" "logs/\$LATEST_LOG" | tail -10
        fi
        
        echo
        echo "=== Output Files ==="
        ls -la data/browser_scraped_turbo/ 2>/dev/null || echo "No turbo output files yet"
        
        echo
        echo "=== Session Activity ==="
        if tmux has-session -t '$TMUX_SESSION' 2>/dev/null; then
            tmux capture-pane -t '$TMUX_SESSION' -p | tail -10
        else
            echo "Turbo session not running"
        fi
EOF
}

# Function to stop scraping
stop_scraping() {
    print_header
    log "Stopping turbo scraping pipeline..."
    ssh -p 2089 ucloud@ssh.cloud.sdu.dk << EOF
        if tmux has-session -t '$TMUX_SESSION' 2>/dev/null; then
            echo "Sending graceful shutdown signal to turbo scraper..."
            tmux send-keys -t '$TMUX_SESSION' C-c
            sleep 3
            echo "Killing tmux session..."
            tmux kill-session -t '$TMUX_SESSION'
            echo "Turbo scraping pipeline stopped"
        else
            echo "Turbo session '$TMUX_SESSION' is not running"
        fi
EOF
}

# Function to test turbo scraper
test_turbo() {
    print_header
    log "Testing turbo scraper with small batch..."
    
    copy_turbo_script
    
    # Test with just 2 batches
    ssh -p 2089 ucloud@ssh.cloud.sdu.dk << EOF
        cd /work/Datadonationer/urlextraction_scraping
        source venv/bin/activate
        mkdir -p data/browser_scraped_turbo
        
        echo "Running turbo test with 2 batches..."
        python3 $TURBO_SCRIPT \\
            --data-dir '$VM_DATA_DIR' \\
            --output-dir '$VM_OUTPUT_DIR' \\
            --log-dir '$VM_LOG_DIR' \\
            --max-workers 10 \\
            --timeout 10 \\
            --max-batches 2
EOF
}

# Main script logic
case "${1:-help}" in
    setup)
        setup_environment
        ;;
    start)
        start_turbo_scraping
        ;;
    status)
        check_status
        ;;
    attach)
        attach_session
        ;;
    logs)
        show_logs
        ;;
    progress)
        show_progress
        ;;
    stop)
        stop_scraping
        ;;
    test)
        test_turbo
        ;;
    help|*)
        print_header
        echo "🚀 Turbo Browser Content Scraping Pipeline"
        echo
        echo "Usage: $0 [COMMAND]"
        echo
        echo "Commands:"
        echo "  setup      Setup turbo scraping environment"
        echo "  start      Start turbo scraping pipeline in tmux"
        echo "  status     Check if turbo scraping session is running"
        echo "  attach     Attach to turbo scraping tmux session"  
        echo "  logs       Show recent turbo scraping logs"
        echo "  progress   Show detailed turbo progress and performance"
        echo "  stop       Stop turbo scraping pipeline (graceful)"
        echo "  test       Test turbo scraper with small batch"
        echo "  help       Show this help message"
        echo
        turbo "Performance Features:"
        turbo "  🔥 $MAX_WORKERS parallel workers"
        turbo "  ⚡ ${TIMEOUT}s optimized timeouts"
        turbo "  🚀 10-50x faster than sequential processing"
        turbo "  📊 Real-time performance monitoring"
        ;;
esac 