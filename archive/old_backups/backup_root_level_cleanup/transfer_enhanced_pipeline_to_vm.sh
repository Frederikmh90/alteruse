#!/bin/bash

# Transfer Enhanced URL Resolution Pipeline to VM
# =============================================

VM_HOST="ucloud@ssh.cloud.sdu.dk"
VM_PORT="2285"
VM_PROJECT_PATH="/work/alteruse"

echo "=== Transferring Enhanced URL Resolution Pipeline to VM ==="

# Create directories on VM first
echo "Creating directories on VM..."
ssh -p $VM_PORT $VM_HOST "mkdir -p $VM_PROJECT_PATH/notebooks/url_extraction"
ssh -p $VM_PORT $VM_HOST "mkdir -p $VM_PROJECT_PATH/notebooks/url_extraction_facebook"

# Transfer Step 1: Enhanced Content Scrapers
echo "Transferring Step 1: Enhanced Content Scrapers..."
scp -P $VM_PORT notebooks/url_extraction/step4_scrape_content_with_resolution.py $VM_HOST:$VM_PROJECT_PATH/notebooks/url_extraction/
scp -P $VM_PORT notebooks/url_extraction_facebook/step4_scrape_content_facebook_with_resolution.py $VM_HOST:$VM_PROJECT_PATH/notebooks/url_extraction_facebook/

# Transfer Step 2: Combined URL Resolution
echo "Transferring Step 2: Combined URL Resolution..."
scp -P $VM_PORT combined_url_resolution_enhanced.py $VM_HOST:$VM_PROJECT_PATH/

# Transfer Step 3: Unified Re-scraping Pipeline
echo "Transferring Step 3: Unified Re-scraping Pipeline..."
scp -P $VM_PORT unified_rescraping_pipeline.py $VM_HOST:$VM_PROJECT_PATH/

# Transfer core URL resolver (dependency)
echo "Transferring RobustURLResolver dependency..."
scp -P $VM_PORT robust_url_resolver.py $VM_HOST:$VM_PROJECT_PATH/

# Transfer documentation
echo "Transferring documentation..."
scp -P $VM_PORT README_URL_Resolution_Pipeline.md $VM_HOST:$VM_PROJECT_PATH/

# Create a VM execution script
echo "Creating VM execution script..."
cat > vm_run_enhanced_pipeline.sh << 'EOF'
#!/bin/bash

# Enhanced URL Resolution Pipeline Execution Script for VM
# ========================================================

cd /work/alteruse

echo "=== Enhanced URL Resolution Pipeline on VM ==="
echo "Current directory: $(pwd)"
echo "Python version: $(python3 --version)"
echo "Date: $(date)"

# Activate virtual environment if it exists
if [ -d "venv" ]; then
    echo "Activating virtual environment..."
    source venv/bin/activate
fi

# Function to run command in tmux session
run_in_tmux() {
    local session_name=$1
    local command=$2
    local log_file=$3
    
    echo "Starting tmux session: $session_name"
    echo "Command: $command"
    echo "Log file: $log_file"
    
    tmux new-session -d -s "$session_name" "$command | tee $log_file"
    echo "Started tmux session: $session_name"
    echo "To attach: tmux attach-session -t $session_name"
    echo "To view logs: tail -f $log_file"
}

# Step 1: Test URL resolution first
echo ""
echo "=== Testing URL Resolution ==="
echo "Testing browser scraper URL resolution..."
python3 notebooks/url_extraction/step4_scrape_content_with_resolution.py --test-only

echo "Testing Facebook scraper URL resolution..."
python3 notebooks/url_extraction_facebook/step4_scrape_content_facebook_with_resolution.py --test-only

echo "Testing combined URL resolver..."
python3 combined_url_resolution_enhanced.py --test-only

# Step 2: Run Combined URL Resolution
echo ""
echo "=== Step 2: Combined URL Resolution ==="
echo "This will merge Facebook and browser data and resolve URLs..."
read -p "Do you want to run the combined URL resolution? (y/n): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    mkdir -p logs
    run_in_tmux "url_resolution" \
        "python3 combined_url_resolution_enhanced.py" \
        "logs/combined_url_resolution_$(date +%Y%m%d_%H%M%S).log"
    
    echo "Combined URL resolution started in tmux session 'url_resolution'"
    echo "Monitor progress with: tmux attach-session -t url_resolution"
fi

# Step 3: Run Unified Re-scraping (after Step 2 completes)
echo ""
echo "=== Step 3: Unified Re-scraping Pipeline ==="
echo "This will re-scrape content using resolved URLs..."
echo "Note: Run this AFTER Step 2 completes successfully"
read -p "Do you want to run the unified re-scraping pipeline? (y/n): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    # Check if input file exists
    if [ -f "data/combined_resolved_enhanced/urls_for_rescraping.csv" ]; then
        run_in_tmux "unified_rescraping" \
            "python3 unified_rescraping_pipeline.py --max-urls 1000 --delay 2.0" \
            "logs/unified_rescraping_$(date +%Y%m%d_%H%M%S).log"
        
        echo "Unified re-scraping started in tmux session 'unified_rescraping'"
        echo "Monitor progress with: tmux attach-session -t unified_rescraping"
    else
        echo "ERROR: Input file not found. Please run Step 2 first."
        echo "Expected file: data/combined_resolved_enhanced/urls_for_rescraping.csv"
    fi
fi

echo ""
echo "=== Pipeline Status ==="
echo "Active tmux sessions:"
tmux list-sessions 2>/dev/null || echo "No active tmux sessions"

echo ""
echo "=== Monitoring Commands ==="
echo "To attach to URL resolution session: tmux attach-session -t url_resolution"
echo "To attach to re-scraping session: tmux attach-session -t unified_rescraping"
echo "To list all sessions: tmux list-sessions"
echo "To kill a session: tmux kill-session -t <session_name>"

echo ""
echo "=== Log Locations ==="
echo "Combined URL resolution logs: logs/combined_url_resolution_*.log"
echo "Unified re-scraping logs: logs/unified_rescraping_*.log"
echo "Processing reports: data/combined_resolved_enhanced/processing_report.json"
echo "Re-scraping stats: data/unified_rescraped/rescraping_stats_*.json"

EOF

# Transfer the VM execution script
scp -P $VM_PORT vm_run_enhanced_pipeline.sh $VM_HOST:$VM_PROJECT_PATH/

# Make the script executable on VM
ssh -p $VM_PORT $VM_HOST "chmod +x $VM_PROJECT_PATH/vm_run_enhanced_pipeline.sh"

echo ""
echo "=== Transfer Complete ==="
echo "All enhanced pipeline files transferred to VM at: $VM_PROJECT_PATH"
echo ""
echo "Next steps:"
echo "1. SSH to VM: ssh -p $VM_PORT $VM_HOST"
echo "2. Navigate to project: cd $VM_PROJECT_PATH"
echo "3. Run the pipeline: ./vm_run_enhanced_pipeline.sh"
echo ""
echo "Files transferred:"
echo "- notebooks/url_extraction/step4_scrape_content_with_resolution.py"
echo "- notebooks/url_extraction_facebook/step4_scrape_content_facebook_with_resolution.py"
echo "- combined_url_resolution_enhanced.py"
echo "- unified_rescraping_pipeline.py"
echo "- robust_url_resolver.py"
echo "- README_URL_Resolution_Pipeline.md"
echo "- vm_run_enhanced_pipeline.sh" 