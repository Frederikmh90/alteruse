 #!/bin/bash

# Pipeline Monitoring Script
# Usage: ./monitor_pipeline.sh [status|logs|output|stop|start|progress]

VM_ADDRESS="ucloud@ssh.cloud.sdu.dk"
VM_PORT="2033"
SESSION_NAME="scraping_pipeline"

case "$1" in
    "status")
        echo "🔍 Checking pipeline status..."
        
        # Check for tmux session
        TMUX_STATUS=$(ssh -p $VM_PORT $VM_ADDRESS "tmux list-sessions 2>/dev/null | grep $SESSION_NAME || echo ''")
        
        # Check for direct processes
        PROCESS_STATUS=$(ssh -p $VM_PORT $VM_ADDRESS "ps aux | grep -E '(step4_scrape_content|run_pipeline_vm)' | grep -v grep || echo ''")
        
        if [ ! -z "$TMUX_STATUS" ]; then
            echo "✅ Pipeline running in tmux session: $TMUX_STATUS"
        elif [ ! -z "$PROCESS_STATUS" ]; then
            echo "✅ Pipeline running directly:"
            echo "$PROCESS_STATUS"
        else
            echo "❌ Pipeline not running"
        fi
        ;;
    
    "progress")
        echo "📊 Checking scraping progress..."
        
        # Count scraped JSON files
        JSON_COUNT=$(ssh -p $VM_PORT $VM_ADDRESS "cd /work/Datadonationer/data/url_extract/scraped_content && ls -1 *.json 2>/dev/null | wc -l || echo '0'")
        echo "JSON files scraped: $JSON_COUNT"
        
        # Count log entries
        LOG_COUNT=$(ssh -p $VM_PORT $VM_ADDRESS "wc -l /work/Datadonationer/data/url_extract/scraped_content/scraping_log_*.log 2>/dev/null | tail -1 | awk '{print \$1}' || echo '0'")
        echo "Log entries: $LOG_COUNT"
        
        # Count successful scrapes
        SUCCESS_COUNT=$(ssh -p $VM_PORT $VM_ADDRESS "grep -c 'Successfully scraped' /work/Datadonationer/data/url_extract/scraped_content/scraping_log_*.log 2>/dev/null || echo '0'")
        echo "Successfully scraped: $SUCCESS_COUNT"
        
        # Count errors
        ERROR_COUNT=$(ssh -p $VM_PORT $VM_ADDRESS "grep -c 'Error\|Failed' /work/Datadonationer/data/url_extract/scraped_content/scraping_log_*.log 2>/dev/null || echo '0'")
        echo "Errors: $ERROR_COUNT"
        
        # Calculate success rate
        if [ "$LOG_COUNT" -gt 0 ]; then
            SUCCESS_RATE=$(echo "scale=1; $SUCCESS_COUNT * 100 / $LOG_COUNT" | bc 2>/dev/null || echo "0")
            echo "Success rate: ${SUCCESS_RATE}%"
        fi
        
        # Check current batch being processed
        CURRENT_BATCH=$(ssh -p $VM_PORT $VM_ADDRESS "ps aux | grep step4_scrape_content | grep -o 'batch_[0-9]*' | head -1 || echo 'unknown'")
        if [ "$CURRENT_BATCH" != "unknown" ]; then
            echo "Currently processing: $CURRENT_BATCH"
        fi
        
        # Show recent scraping activity
        echo ""
        echo "Recent scraping activity:"
        ssh -p $VM_PORT $VM_ADDRESS "tail -5 /work/Datadonationer/data/url_extract/scraped_content/scraping_log_*.log 2>/dev/null || echo 'No log activity yet'"
        ;;
    
    "logs")
        echo "📋 Showing recent log entries..."
        ssh -p $VM_PORT $VM_ADDRESS "tail -20 /work/Datadonationer/logs/pipeline.log 2>/dev/null || echo 'No log file found yet'"
        ;;
    
    "output")
        echo "📁 Checking output files..."
        ssh -p $VM_PORT $VM_ADDRESS "ls -la /work/Datadonationer/data/url_extract/ 2>/dev/null || echo 'Output directory not created yet'"
        ;;
    
    "start")
        echo "🚀 Starting pipeline in tmux session..."
        ssh -p $VM_PORT $VM_ADDRESS "tmux new-session -d -s $SESSION_NAME 'cd /work/Datadonationer && source venv/bin/activate && ./run_pipeline_vm.sh'"
        echo "Pipeline started! Use './monitor_pipeline.sh status' to check progress"
        ;;
    
    "stop")
        echo "🛑 Stopping pipeline..."
        ssh -p $VM_PORT $VM_ADDRESS "tmux kill-session -t $SESSION_NAME 2>/dev/null || echo 'No tmux session to kill'"
        ssh -p $VM_PORT $VM_ADDRESS "pkill -f 'step4_scrape_content' 2>/dev/null || echo 'No direct process to kill'"
        echo "Pipeline stopped"
        ;;
    
    "attach")
        echo "🔗 Attaching to pipeline session..."
        echo "Press Ctrl+B, then D to detach"
        ssh -p $VM_PORT $VM_ADDRESS "tmux attach-session -t $SESSION_NAME"
        ;;
    
    "disk")
        echo "💾 Checking disk space..."
        ssh -p $VM_PORT $VM_ADDRESS "df -h /work/Datadonationer/"
        ;;
    
    *)
        echo "Pipeline Monitoring Script"
        echo "Usage: $0 [status|logs|output|start|stop|attach|disk|progress]"
        echo ""
        echo "Commands:"
        echo "  status   - Check if pipeline is running (tmux or direct)"
        echo "  progress - Show scraping progress and current batch"
        echo "  logs     - Show recent log entries"
        echo "  output   - List output files"
        echo "  start    - Start the pipeline"
        echo "  stop     - Stop the pipeline"
        echo "  attach   - Attach to running session"
        echo "  disk     - Check disk space"
        ;;
esac 