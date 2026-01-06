# VM Setup Guide - URL Extraction & Scraping Pipeline

## 📦 Files to Transfer to VM

### 1. Core Pipeline Scripts
```bash
notebooks/url_extraction/
├── step1_extract_urls_optimized.py    # URL extraction (REQUIRED)
├── step2_analyze_urls.py              # URL analysis  
├── step3_prioritize_domains.py        # Domain prioritization
├── step4_scrape_content.py            # Content scraping (REQUIRED)
└── archive/                           # Keep for reference (optional)
```

### 2. Configuration & Requirements
```bash
vm_requirements.txt                    # Dependencies for VM
vm_setup_guide.md                     # This guide
```

### 3. Browser Data (1.6GB)
```bash
data/Samlet_06112025/Browser/          # All browser history files
# Contains: 16 SQLite .db files + 135 JSON files
```

### 4. Output Directory Structure
```bash
data/url_extract/                     # Will be created automatically
├── extracted_urls_optimized.csv      # Step 1 output (if already generated)
├── analyzed_urls.csv                 # Step 2 output  
├── prioritized_batches/              # Step 3 output
└── scraped_content/                  # Step 4 output
```

## 🚀 VM Setup Instructions

### Step 1: Environment Setup
```bash
# Create project directory
mkdir -p ~/alteruse/data/Samlet_06112025
cd ~/alteruse

# Create Python virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r vm_requirements.txt
```

### Step 2: Transfer Files
```bash
# Transfer using scp, rsync, or your preferred method
# Example with scp:
scp -r notebooks/ user@vm:/home/user/alteruse/
scp -r data/Samlet_06112025/Browser/ user@vm:/home/user/alteruse/data/Samlet_06112025/
scp vm_requirements.txt user@vm:/home/user/alteruse/
```

### Step 3: Verify Setup
```bash
# Check Python environment
python3 --version  # Should be 3.8+
pip list           # Verify packages installed

# Check data structure
ls -la data/Samlet_06112025/Browser/
# Should show: 16 .db files + 135 .json files

# Test imports
python3 -c "import pandas, requests, trafilatura; print('Dependencies OK')"
```

## 🏃‍♂️ Running the Pipeline

### Full Pipeline (Recommended)
```bash
cd ~/alteruse

# Step 1: Extract URLs from browser data (15-20 minutes)
python3 notebooks/url_extraction/step1_extract_urls_optimized.py

# Step 2: Analyze and resolve URLs (30-60 minutes) 
python3 notebooks/url_extraction/step2_analyze_urls.py

# Step 3: Prioritize domains and create batches (5-10 minutes)
python3 notebooks/url_extraction/step3_prioritize_domains.py

# Step 4: Scrape content (several hours, runs in batches)
python3 notebooks/url_extraction/step4_scrape_content.py
```

### Quick Start (If you already have extracted URLs)
```bash
# If you already ran step 1 locally and have extracted_urls_optimized.csv:
# Transfer the CSV file and start from step 2

scp data/url_extract/extracted_urls_optimized.csv user@vm:/home/user/alteruse/data/url_extract/

# Then run steps 2-4 as above
```

## 📊 Expected Outputs

### Step 1 Output
- `data/url_extract/extracted_urls_optimized.csv` 
- ~633k URLs from 35k domains
- Processing time: 15-20 minutes

### Step 2 Output  
- `data/url_extract/analyzed_urls.csv`
- URL resolution and analysis results
- Processing time: 30-60 minutes

### Step 3 Output
- `data/url_extract/prioritized_batches/batch_001.csv` to `batch_050.csv`
- ~9,754 high-priority URLs in 50 batches
- Processing time: 5-10 minutes

### Step 4 Output
- `data/url_extract/scraped_content/batch_001_content.csv` etc.
- Scraped article content and metadata
- Processing time: Several hours (runs automatically with progress tracking)

## 🔧 Troubleshooting

### Memory Issues
```bash
# Monitor memory usage
htop
free -h

# For large datasets, consider:
# - Using a VM with at least 8GB RAM
# - Running steps individually rather than chaining
# - Processing smaller batches in step 4
```

### Network Issues
```bash
# Check internet connectivity for scraping
curl -I https://httpbin.org/status/200

# Adjust timeout settings in step4_scrape_content.py if needed
```

### File Paths
```bash
# Ensure all paths are relative to ~/alteruse/
# Scripts expect this directory structure:
# ~/alteruse/
# ├── notebooks/url_extraction/
# ├── data/Samlet_06112025/Browser/
# └── data/url_extract/
```

## ⚡ Performance Tips

1. **Use SSD storage** for better I/O performance
2. **At least 8GB RAM** recommended for full dataset
3. **Good internet connection** for step 4 (content scraping)
4. **Run in screen/tmux** for long-running processes:
   ```bash
   screen -S scraping
   python3 notebooks/url_extraction/step4_scrape_content.py
   # Ctrl+A, D to detach
   screen -r scraping  # to reattach
   ```

## 📋 File Transfer Checklist

- [ ] Pipeline scripts (`notebooks/url_extraction/`)
- [ ] Requirements file (`vm_requirements.txt`)
- [ ] Browser data (`data/Samlet_06112025/Browser/` - 1.6GB)
- [ ] Setup guide (`vm_setup_guide.md`)
- [ ] Python 3.8+ installed on VM
- [ ] Sufficient disk space (5GB+ recommended)
- [ ] Internet access for package installation and scraping 