# VM Commands Guide - Enhanced URL Resolution Pipeline

## Step 1: SSH to VM and Navigate to Project

```bash
ssh ucloud@ssh.cloud.sdu.dk -p 2285
cd /work/alteruse
pwd
ls -la
```

## Step 2: Check Transferred Files

```bash
# Check that all files were transferred
ls -la *.py
ls -la notebooks/url_extraction/step4_scrape_content_with_resolution.py
ls -la notebooks/url_extraction_facebook/step4_scrape_content_facebook_with_resolution.py
ls -la vm_run_enhanced_pipeline.sh
```

## Step 3: Activate Virtual Environment (if exists)

```bash
# Check if virtual environment exists and activate it
if [ -d "venv" ]; then
    source venv/bin/activate
    echo "Virtual environment activated"
    python3 --version
    pip list | grep -E "(pandas|requests|trafilatura)"
else
    echo "No virtual environment found, using system Python"
    python3 --version
fi
```

## Step 4: Test URL Resolution Capabilities

```bash
# Test browser scraper URL resolution
echo "=== Testing Browser Scraper URL Resolution ==="
python3 notebooks/url_extraction/step4_scrape_content_with_resolution.py --test-only

# Test Facebook scraper URL resolution  
echo "=== Testing Facebook Scraper URL Resolution ==="
python3 notebooks/url_extraction_facebook/step4_scrape_content_facebook_with_resolution.py --test-only

# Test combined URL resolver
echo "=== Testing Combined URL Resolver ==="
python3 combined_url_resolution_enhanced.py --test-only
```

## Step 5: Check Available Data

```bash
# Check what data files are available
echo "=== Checking Available Data ==="
find data -name "*.csv" | head -10
ls -la data/url_extract*/scraped_content/*.csv 2>/dev/null || echo "No scraped content files found"
```

## Step 6: Run Step 2 - Combined URL Resolution (in tmux)

```bash
# Create logs directory
mkdir -p logs

# Start tmux session for combined URL resolution
tmux new-session -d -s url_resolution "python3 combined_url_resolution_enhanced.py | tee logs/combined_url_resolution_$(date +%Y%m%d_%H%M%S).log"

# Check tmux session started
tmux list-sessions

# Attach to monitor progress (optional)
# tmux attach-session -t url_resolution
# (Press Ctrl+B then D to detach from tmux session)
```

## Step 7: Monitor Progress

```bash
# Check if tmux session is still running
tmux list-sessions

# View recent log output
tail -f logs/combined_url_resolution_*.log

# Check output files being created
ls -la data/combined_resolved_enhanced/
```

## Step 8: Run Step 3 - Unified Re-scraping (after Step 2 completes)

```bash
# First check that Step 2 completed successfully
ls -la data/combined_resolved_enhanced/urls_for_rescraping.csv

# If file exists, start re-scraping pipeline
tmux new-session -d -s unified_rescraping "python3 unified_rescraping_pipeline.py --max-urls 1000 --delay 2.0 | tee logs/unified_rescraping_$(date +%Y%m%d_%H%M%S).log"

# Monitor progress
tmux list-sessions
tail -f logs/unified_rescraping_*.log
```

## Step 9: Check Results

```bash
# Check processing reports
cat data/combined_resolved_enhanced/processing_report.json | python3 -m json.tool

# Check re-scraping statistics
ls -la data/unified_rescraped/rescraping_stats_*.json
cat data/unified_rescraped/rescraping_stats_*.json | python3 -m json.tool

# Check output files
ls -la data/unified_rescraped/
wc -l data/unified_rescraped/unified_rescraped_content.csv
```

## Useful tmux Commands

```bash
# List all tmux sessions
tmux list-sessions

# Attach to a session
tmux attach-session -t url_resolution
tmux attach-session -t unified_rescraping

# Detach from session (while inside tmux)
# Press: Ctrl+B then D

# Kill a session
tmux kill-session -t url_resolution
tmux kill-session -t unified_rescraping

# Kill all sessions
tmux kill-server
```

## Troubleshooting

```bash
# Check Python dependencies
python3 -c "import pandas, requests, trafilatura; print('All dependencies available')"

# Check disk space
df -h

# Check memory usage
free -h

# Check processes
ps aux | grep python

# If scripts fail, check error logs
tail -100 logs/combined_url_resolution_*.log
tail -100 logs/unified_rescraping_*.log
```

## Quick Start Commands (Copy & Paste)

```bash
# Navigate and activate environment
cd /work/alteruse && source venv/bin/activate 2>/dev/null || echo "No venv"

# Test everything first
python3 combined_url_resolution_enhanced.py --test-only

# If tests pass, run the full pipeline
mkdir -p logs
tmux new-session -d -s url_resolution "python3 combined_url_resolution_enhanced.py | tee logs/combined_url_resolution_$(date +%Y%m%d_%H%M%S).log"

# Check progress
tmux list-sessions
echo "Monitor with: tmux attach-session -t url_resolution"
echo "View logs with: tail -f logs/combined_url_resolution_*.log"
``` 