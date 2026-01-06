#!/bin/bash
# Facebook Pipeline Monitor
# ========================

set -e

# Configuration
PIPELINE_DIR="/work/Datadonationer/facebook_pipeline"
OUTPUT_DIR="/work/Datadonationer/data/url_extract_facebook"
LOG_DIR="/work/Datadonationer/logs"
TMUX_SESSION="facebook_pipeline"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

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

# Check if tmux session exists
check_tmux_session() {
    if tmux has-session -t "$TMUX_SESSION" 2>/dev/null; then
        return 0
    else
        return 1
    fi
}

# Check if pipeline is running
check_pipeline_running() {
    if check_tmux_session; then
        # Check if there are any Python processes running the pipeline
        if pgrep -f "step[1-4].*facebook" > /dev/null; then
            return 0
        fi
    fi
    return 1
}

# Show status
status() {
    echo "Facebook Pipeline Status"
    echo "======================="
    
    if check_tmux_session; then
        echo -e "${GREEN}✓${NC} Tmux session '$TMUX_SESSION' is active"
    else
        echo -e "${RED}✗${NC} Tmux session '$TMUX_SESSION' not found"
    fi
    
    if check_pipeline_running; then
        echo -e "${GREEN}✓${NC} Pipeline is running"
    else
        echo -e "${YELLOW}○${NC} Pipeline is not running"
    fi
    
    echo ""
    echo "Output files:"
    if [ -f "$OUTPUT_DIR/extracted_urls_facebook.csv" ]; then
        local count=$(wc -l < "$OUTPUT_DIR/extracted_urls_facebook.csv")
        echo -e "${GREEN}✓${NC} Extracted URLs: $((count - 1)) URLs"
    else
        echo -e "${RED}✗${NC} No extracted URLs file found"
    fi
    
    if [ -f "$OUTPUT_DIR/domain_analysis_facebook.csv" ]; then
        local count=$(wc -l < "$OUTPUT_DIR/domain_analysis_facebook.csv")
        echo -e "${GREEN}✓${NC} Domain analysis: $((count - 1)) domains"
    else
        echo -e "${RED}✗${NC} No domain analysis file found"
    fi
    
    if [ -f "$OUTPUT_DIR/scraping_plan_facebook.csv" ]; then
        local count=$(wc -l < "$OUTPUT_DIR/scraping_plan_facebook.csv")
        echo -e "${GREEN}✓${NC} Scraping plan: $((count - 1)) domains"
    else
        echo -e "${RED}✗${NC} No scraping plan file found"
    fi
    
    if [ -d "$OUTPUT_DIR/scraping_batches" ]; then
        local batch_count=$(ls "$OUTPUT_DIR/scraping_batches"/*.csv 2>/dev/null | wc -l)
        echo -e "${GREEN}✓${NC} Scraping batches: $batch_count batches"
    else
        echo -e "${RED}✗${NC} No scraping batches directory found"
    fi
    
    if [ -f "$OUTPUT_DIR/scraped_content_facebook.csv" ]; then
        local count=$(wc -l < "$OUTPUT_DIR/scraped_content_facebook.csv")
        echo -e "${GREEN}✓${NC} Scraped content: $((count - 1)) URLs"
    else
        echo -e "${YELLOW}○${NC} No scraped content file found"
    fi
}

# Show logs
logs() {
    echo "Recent Facebook Pipeline Logs"
    echo "============================"
    
    if [ -d "$LOG_DIR" ]; then
        local log_files=$(ls -t "$LOG_DIR"/facebook_*.log 2>/dev/null | head -5)
        if [ -n "$log_files" ]; then
            for log_file in $log_files; do
                echo ""
                echo "=== $(basename "$log_file") ==="
                tail -20 "$log_file"
            done
        else
            echo "No log files found"
        fi
    else
        echo "Log directory not found"
    fi
}

# Show progress
progress() {
    echo "Facebook Pipeline Progress"
    echo "========================="
    
    if [ -f "$OUTPUT_DIR/scraped_content_facebook.csv" ]; then
        local total_scraped=$(wc -l < "$OUTPUT_DIR/scraped_content_facebook.csv")
        total_scraped=$((total_scraped - 1))  # Subtract header
        
        if [ -f "$OUTPUT_DIR/scraping_estimates.json" ]; then
            local total_urls=$(jq -r '.total_urls' "$OUTPUT_DIR/scraping_estimates.json" 2>/dev/null || echo "0")
            if [ "$total_urls" -gt 0 ]; then
                local percentage=$((total_scraped * 100 / total_urls))
                echo "Scraping progress: $total_scraped / $total_urls URLs ($percentage%)"
            fi
        fi
        
        # Show recent scraping activity
        echo ""
        echo "Recent scraping activity:"
        tail -10 "$OUTPUT_DIR/scraped_content_facebook.csv" | cut -d',' -f1,2,8,9 | head -5
    else
        echo "No scraping progress data available"
    fi
}

# Attach to tmux session
attach() {
    if check_tmux_session; then
        log "Attaching to tmux session '$TMUX_SESSION'"
        tmux attach-session -t "$TMUX_SESSION"
    else
        error "Tmux session '$TMUX_SESSION' not found"
        echo "To create a new session: tmux new-session -d -s $TMUX_SESSION"
    fi
}

# Stop pipeline
stop() {
    if check_tmux_session; then
        log "Stopping Facebook pipeline..."
        tmux kill-session -t "$TMUX_SESSION"
        echo "Pipeline stopped"
    else
        echo "No active pipeline session found"
    fi
}

# Start pipeline
start() {
    if check_tmux_session; then
        warning "Tmux session '$TMUX_SESSION' already exists"
        echo "Use 'attach' to connect to existing session"
        echo "Use 'stop' to stop existing session first"
        return 1
    fi
    
    log "Starting Facebook pipeline in tmux session..."
    tmux new-session -d -s "$TMUX_SESSION" -c "$PIPELINE_DIR"
    tmux send-keys -t "$TMUX_SESSION" "bash run_facebook_pipeline_vm.sh" Enter
    
    echo "Pipeline started in tmux session '$TMUX_SESSION'"
    echo "Use 'attach' to view the session"
    echo "Use 'status' to check progress"
}

# Show help
help() {
    echo "Facebook Pipeline Monitor"
    echo "========================"
    echo ""
    echo "Usage: $0 [command]"
    echo ""
    echo "Commands:"
    echo "  status    - Show pipeline status and file counts"
    echo "  logs      - Show recent log files"
    echo "  progress  - Show scraping progress"
    echo "  start     - Start pipeline in new tmux session"
    echo "  stop      - Stop pipeline and tmux session"
    echo "  attach    - Attach to existing tmux session"
    echo "  help      - Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 status    # Check current status"
    echo "  $0 logs      # View recent logs"
    echo "  $0 attach    # Connect to running session"
}

# Main command handling
case "${1:-help}" in
    status)
        status
        ;;
    logs)
        logs
        ;;
    progress)
        progress
        ;;
    start)
        start
        ;;
    stop)
        stop
        ;;
    attach)
        attach
        ;;
    help|*)
        help
        ;;
esac 