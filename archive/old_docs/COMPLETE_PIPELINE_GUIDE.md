# Complete Data Pipeline Guide

## Overview

This guide covers the complete end-to-end data processing pipeline that handles URL extraction from browser and Facebook data, VM-based processing, and final analysis. The pipeline is designed to work efficiently with the separation between local extraction and remote processing.

## Pipeline Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   LOCAL EXTRACT │    │   VM PROCESSING │    │  LOCAL ANALYZE  │
│                 │    │                 │    │                 │
│ • Browser Data  │───▶│ • URL Resolution│───▶│ • News Analysis │
│ • Facebook Data │    │ • Content Scrape│    │ • Results Save  │
│ • URL Extraction│    │ • Batch Process │    │ • Final Reports │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## Components

### 1. Local URL Extraction
- **Browser Data**: `notebooks/url_extraction/step1_extract_urls_optimized.py`
- **Facebook Data**: `notebooks/url_extraction_facebook/step1_extract_urls_facebook.py`
- **Output**: CSV files with extracted URLs and metadata

### 2. VM Processing Pipeline
- **URL Resolution**: `scripts/url_resolution/complete_url_resolution_pipeline.py`
- **Content Scraping**: `scripts/content_scraping/browser_content_scraper_turbo.py`
- **Unified Interface**: `pipelines/unified_data_pipeline.py`

### 3. Local Analysis
- **News Analysis**: `core/news_analysis.py`
- **Results Processing**: `pipelines/complete_data_pipeline.py`

## Quick Start

### Prerequisites
1. **Local Setup**:
   ```bash
   # Install dependencies
   pip install -r requirements.txt
   
   # Setup data directories
   mkdir -p data/{browser_data,facebook_data,extracted_urls,extracted_urls_facebook}
   ```

2. **VM Access**:
   ```bash
   # Test SSH connection
   ssh ucloud@ssh.cloud.sdu.dk -p 2390
   ```

### Basic Usage

#### 1. Complete End-to-End Pipeline
```bash
# Run everything from extraction to analysis
python pipelines/complete_data_pipeline.py --mode full --data-type both
```

#### 2. Step-by-Step Execution
```bash
# Step 1: Extract URLs locally
python pipelines/complete_data_pipeline.py --mode extract --data-type both

# Step 2: Process on VM
python pipelines/complete_data_pipeline.py --mode process

# Step 3: Analyze results
python pipelines/complete_data_pipeline.py --mode analyze
```

#### 3. Data Type Specific Processing
```bash
# Process only browser data
python pipelines/complete_data_pipeline.py --mode full --data-type browser

# Process only Facebook data
python pipelines/complete_data_pipeline.py --mode full --data-type facebook
```

## Detailed Usage

### Local URL Extraction

#### Browser Data Extraction
```bash
# Direct script usage
python notebooks/url_extraction/step1_extract_urls_optimized.py \
    --browser-dir data/browser_data \
    --output-dir data/extracted_urls
```

**Expected Input Structure**:
```
data/browser_data/
├── Chrome/
│   ├── History
│   └── ...
├── Safari/
│   ├── History.db
│   └── ...
└── Firefox/
    ├── places.sqlite
    └── ...
```

**Output Structure**:
```
data/extracted_urls/
├── extracted_urls_optimized.csv
├── extraction_log.txt
└── summary_report.json
```

#### Facebook Data Extraction
```bash
# Direct script usage
python notebooks/url_extraction_facebook/step1_extract_urls_facebook.py \
    --data-dir data/facebook_data \
    --output-dir data/extracted_urls_facebook
```

**Expected Input Structure**:
```
data/facebook_data/
├── Kantar_download_398_unzipped_new/
│   ├── 474-4477-c-146271_2025-05-08T07__4477g1746690538960sn2dqgvrdtNju5517u5517uhistory-5MIbsIS.json
│   └── ...
└── ...
```

**Output Structure**:
```
data/extracted_urls_facebook/
├── extracted_urls_facebook.csv
├── facebook_url_extraction_YYYYMMDD_HHMMSS.log
└── extraction_summary.json
```

### VM Processing

#### Using the Unified Pipeline
```bash
# SSH to VM and run unified pipeline
ssh ucloud@ssh.cloud.sdu.dk -p 2390
cd /work/Datadonationer/urlextraction_scraping

# Run complete processing
./run_pipeline.sh full --input data/extracted_urls_optimized.csv --output data/processed_results

# Or run individual steps
./run_pipeline.sh resolve --input data/extracted_urls_optimized.csv --output data/resolved_urls
./run_pipeline.sh scrape --input data/resolved_urls --output data/scraped_content
```

#### Manual VM Processing
```bash
# URL Resolution
python3 scripts/url_resolution/complete_url_resolution_pipeline.py \
    --base-dir /work/Datadonationer/urlextraction_scraping/data \
    --batch-size 1000

# Content Scraping
python3 scripts/content_scraping/browser_content_scraper_turbo.py \
    --data-dir /work/Datadonationer/urlextraction_scraping/data \
    --output-dir /work/Datadonationer/urlextraction_scraping/data/scraped_content \
    --log-dir /work/Datadonationer/urlextraction_scraping/logs \
    --max-workers 20
```

### Local Analysis

#### News Source Analysis
```bash
# Analyze processed results
python -c "
from core.news_analysis import analyze_news_sources, save_analysis_results
import pandas as pd

# Load processed data
df = pd.read_csv('data/vm_download/scraped_content.csv')

# Run analysis
results = analyze_news_sources(df)

# Save results
save_analysis_results(results, 'data/final_results/news_analysis.json')
"
```

## Data Flow and File Structure

### Complete Data Flow
```
Raw Data (Browser/Facebook)
    ↓
Local Extraction (URLs + Metadata)
    ↓
CSV Files (extracted_urls_optimized.csv, extracted_urls_facebook.csv)
    ↓
VM Upload
    ↓
VM Processing (Resolution + Scraping)
    ↓
VM Download
    ↓
Local Analysis (News Classification)
    ↓
Final Results (JSON Reports)
```

### File Structure
```
data/
├── browser_data/                    # Raw browser data
├── facebook_data/                   # Raw Facebook data
├── extracted_urls/                  # Local extraction results
│   ├── extracted_urls_optimized.csv
│   └── extraction_log.txt
├── extracted_urls_facebook/         # Facebook extraction results
│   ├── extracted_urls_facebook.csv
│   └── facebook_url_extraction_*.log
├── vm_upload/                       # Files ready for VM
│   ├── extracted_urls_optimized.csv
│   └── extracted_urls_facebook.csv
├── vm_download/                     # Results from VM
│   ├── resolved_urls.csv
│   ├── scraped_content.csv
│   └── processing_logs/
├── final_results/                   # Analysis results
│   ├── analysis_resolved_urls.json
│   ├── analysis_scraped_content.json
│   └── summary_report.json
└── logs/                           # Pipeline logs
    └── complete_pipeline_*.log
```

## Configuration

### Pipeline Configuration
The pipeline uses a configuration dictionary in `CompleteDataPipeline`:

```python
config = {
    "browser_extraction": {
        "input_dir": "browser_data",
        "output_dir": "extracted_urls",
        "script": "notebooks/url_extraction/step1_extract_urls_optimized.py"
    },
    "facebook_extraction": {
        "input_dir": "facebook_data", 
        "output_dir": "extracted_urls_facebook",
        "script": "notebooks/url_extraction_facebook/step1_extract_urls_facebook.py"
    },
    "vm_processing": {
        "batch_size": 1000,
        "max_workers": 20,
        "timeout": 3600  # 1 hour per step
    }
}
```

### VM Configuration
```python
vm_host = "ucloud@ssh.cloud.sdu.dk"
vm_port = "2390"
vm_project_dir = "/work/Datadonationer/urlextraction_scraping"
```

## Monitoring and Troubleshooting

### Pipeline Status
```bash
# Check pipeline status
python pipelines/complete_data_pipeline.py --status
```

**Status Output**:
```json
{
  "local_extraction": {
    "browser_data_exists": true,
    "facebook_data_exists": true,
    "browser_urls_extracted": false,
    "facebook_urls_extracted": false
  },
  "vm_processing": {
    "upload_ready": false,
    "results_downloaded": false
  },
  "final_results": {
    "analysis_complete": false
  }
}
```

### Log Files
- **Local Extraction**: `data/logs/extraction_*.log`
- **VM Processing**: `data/logs/vm_processing_*.log`
- **Complete Pipeline**: `data/logs/complete_pipeline_*.log`

### Common Issues

#### 1. SSH Connection Issues
```bash
# Test SSH connection
ssh ucloud@ssh.cloud.sdu.dk -p 2390 "echo 'Connection successful'"

# Check if VM project directory exists
ssh ucloud@ssh.cloud.sdu.dk -p 2390 "ls -la /work/Datadonationer/urlextraction_scraping"
```

#### 2. Missing Dependencies
```bash
# On VM, activate conda and install requirements
ssh ucloud@ssh.cloud.sdu.dk -p 2390
source /opt/conda/bin/activate
cd /work/Datadonationer/urlextraction_scraping
pip install -r requirements.txt
```

#### 3. Data Structure Issues
```bash
# Verify data structure
python pipelines/complete_data_pipeline.py --status

# Check specific directories
ls -la data/browser_data/
ls -la data/facebook_data/
```

## Performance Optimization

### Local Extraction
- **Batch Processing**: Configure batch size in extraction scripts
- **Memory Management**: Use optimized extraction scripts
- **Parallel Processing**: Run browser and Facebook extraction simultaneously

### VM Processing
- **Batch Size**: Adjust `--batch-size` parameter (default: 1000)
- **Workers**: Adjust `--max-workers` parameter (default: 20)
- **Timeout**: Increase timeout for large datasets

### Data Transfer
- **Compression**: Use compressed files for large datasets
- **Incremental**: Only upload changed files
- **Resume**: Support for resuming interrupted transfers

## Advanced Usage

### Custom Data Sources
```python
# Modify pipeline configuration for custom data sources
pipeline = CompleteDataPipeline()
pipeline.config["custom_extraction"] = {
    "input_dir": "custom_data",
    "output_dir": "custom_extracted",
    "script": "custom_extraction_script.py"
}
```

### Parallel Processing
```bash
# Run browser and Facebook extraction in parallel
python pipelines/complete_data_pipeline.py --mode extract --data-type browser &
python pipelines/complete_data_pipeline.py --mode extract --data-type facebook &
wait
```

### Incremental Processing
```bash
# Skip local extraction if already done
python pipelines/complete_data_pipeline.py --mode full --skip-local

# Skip VM processing if already done
python pipelines/complete_data_pipeline.py --mode full --skip-vm
```

## Integration with Existing Workflows

### Jupyter Notebooks
```python
# Use pipeline components in notebooks
from pipelines.complete_data_pipeline import CompleteDataPipeline

pipeline = CompleteDataPipeline()
status = pipeline.get_pipeline_status()
print(status)
```

### Automated Scripts
```bash
#!/bin/bash
# Automated pipeline execution
set -e

echo "Starting complete data pipeline..."
python pipelines/complete_data_pipeline.py --mode full --data-type both

if [ $? -eq 0 ]; then
    echo "Pipeline completed successfully!"
    # Send notification or trigger next step
else
    echo "Pipeline failed!"
    exit 1
fi
```

## Summary

This complete pipeline provides:

1. **🔍 URL Extraction**: Local extraction from browser and Facebook data
2. **🛠️ VM Processing**: Remote URL resolution and content scraping
3. **📋 Analysis**: Local news source classification and reporting
4. **🔄 Automation**: End-to-end orchestration with proper error handling
5. **📊 Monitoring**: Status tracking and comprehensive logging

The pipeline maintains clean separation between local and remote processing while providing a unified interface for the complete workflow.
