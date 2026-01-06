#!/bin/bash
"""
Deploy Complete Data Pipeline to VM
===================================
This script deploys the complete end-to-end pipeline including:
1. Local URL extraction scripts
2. VM processing pipeline
3. Complete orchestration tools
"""

set -e  # Exit on any error

# Configuration
VM_HOST="ucloud@ssh.cloud.sdu.dk"
VM_PORT="2390"
VM_PROJECT_DIR="/work/Datadonationer/urlextraction_scraping"
LOCAL_PROJECT_DIR="$(pwd)"

echo "🚀 Deploying Complete Data Pipeline to VM..."

# Create backup of existing scripts
echo "📦 Creating backup of existing scripts..."
ssh -p $VM_PORT $VM_HOST "cd $VM_PROJECT_DIR && tar -czf complete_pipeline_backup_$(date +%Y%m%d_%H%M%S).tar.gz scripts/"

# Create pipeline directories
echo "📁 Creating pipeline directories..."
ssh -p $VM_PORT $VM_HOST "mkdir -p $VM_PROJECT_DIR/scripts/unified_pipeline"
ssh -p $VM_PORT $VM_HOST "mkdir -p $VM_PROJECT_DIR/scripts/url_extraction"
ssh -p $VM_PORT $VM_HOST "mkdir -p $VM_PROJECT_DIR/scripts/url_extraction_facebook"

# Copy the unified pipeline tool
echo "📋 Copying unified pipeline tool..."
scp -P $VM_PORT pipelines/unified_data_pipeline.py $VM_HOST:$VM_PROJECT_DIR/scripts/unified_pipeline/

# Copy URL extraction scripts
echo "📋 Copying URL extraction scripts..."
scp -P $VM_PORT notebooks/url_extraction/step1_extract_urls_optimized.py $VM_HOST:$VM_PROJECT_DIR/scripts/url_extraction/
scp -P $VM_PORT notebooks/url_extraction_facebook/step1_extract_urls_facebook.py $VM_HOST:$VM_PROJECT_DIR/scripts/url_extraction_facebook/

# Make scripts executable
ssh -p $VM_PORT $VM_HOST "chmod +x $VM_PROJECT_DIR/scripts/unified_pipeline/unified_data_pipeline.py"
ssh -p $VM_PORT $VM_HOST "chmod +x $VM_PROJECT_DIR/scripts/url_extraction/step1_extract_urls_optimized.py"
ssh -p $VM_PORT $VM_HOST "chmod +x $VM_PROJECT_DIR/scripts/url_extraction_facebook/step1_extract_urls_facebook.py"

# Create wrapper script for unified pipeline
echo "🔧 Creating unified pipeline wrapper script..."
cat > /tmp/run_pipeline.sh << 'EOF'
#!/bin/bash
source /opt/conda/bin/activate
cd /work/Datadonationer/urlextraction_scraping
python3 scripts/unified_pipeline/unified_data_pipeline.py "$@"
EOF

scp -P $VM_PORT /tmp/run_pipeline.sh $VM_HOST:$VM_PROJECT_DIR/
ssh -p $VM_PORT $VM_HOST "chmod +x $VM_PROJECT_DIR/run_pipeline.sh"

# Create wrapper script for URL extraction
echo "🔧 Creating URL extraction wrapper script..."
cat > /tmp/extract_urls.sh << 'EOF'
#!/bin/bash
source /opt/conda/bin/activate
cd /work/Datadonationer/urlextraction_scraping

# Extract URLs from browser data
echo "Extracting URLs from browser data..."
python3 scripts/url_extraction/step1_extract_urls_optimized.py \
    --browser-dir data/browser_data \
    --output-dir data/extracted_urls

# Extract URLs from Facebook data
echo "Extracting URLs from Facebook data..."
python3 scripts/url_extraction_facebook/step1_extract_urls_facebook.py \
    --data-dir data/facebook_data \
    --output-dir data/extracted_urls_facebook

echo "URL extraction completed!"
EOF

scp -P $VM_PORT /tmp/extract_urls.sh $VM_HOST:$VM_PROJECT_DIR/
ssh -p $VM_PORT $VM_HOST "chmod +x $VM_PROJECT_DIR/extract_urls.sh"

# Create complete pipeline wrapper script
echo "🔧 Creating complete pipeline wrapper script..."
cat > /tmp/run_complete_pipeline.sh << 'EOF'
#!/bin/bash
source /opt/conda/bin/activate
cd /work/Datadonationer/urlextraction_scraping

echo "🚀 Starting Complete Data Pipeline..."

# Step 1: Extract URLs
echo "📋 Step 1: Extracting URLs..."
./extract_urls.sh

# Step 2: Process on VM
echo "🛠️ Step 2: Processing URLs and scraping content..."
./run_pipeline.sh full --input data/extracted_urls/extracted_urls_optimized.csv --output data/processed_results

echo "✅ Complete pipeline finished!"
EOF

scp -P $VM_PORT /tmp/run_complete_pipeline.sh $VM_HOST:$VM_PROJECT_DIR/
ssh -p $VM_PORT $VM_HOST "chmod +x $VM_PROJECT_DIR/run_complete_pipeline.sh"

# Create comprehensive usage documentation
echo "📚 Creating comprehensive usage documentation..."
cat > /tmp/COMPLETE_PIPELINE_USAGE.md << 'EOF'
# Complete Data Pipeline - Usage Guide

## Overview

This pipeline provides end-to-end processing from raw browser/Facebook data to analyzed results:

1. **URL Extraction**: Extract URLs from browser and Facebook data
2. **VM Processing**: URL resolution and content scraping
3. **Analysis**: News source classification and reporting

## Quick Start

### Run Complete Pipeline (All Steps)
```bash
./run_complete_pipeline.sh
```

### Run Individual Components

#### 1. URL Extraction Only
```bash
./extract_urls.sh
```

#### 2. VM Processing Only
```bash
./run_pipeline.sh full --input data/extracted_urls/extracted_urls_optimized.csv --output data/processed_results
```

#### 3. Individual VM Steps
```bash
# URL Resolution only
./run_pipeline.sh resolve --input data/extracted_urls/extracted_urls_optimized.csv --output data/resolved_urls

# Content Scraping only
./run_pipeline.sh scrape --input data/resolved_urls --output data/scraped_content
```

## Data Structure Requirements

### Input Data Structure
```
data/
├── browser_data/           # Raw browser data (Chrome, Safari, Firefox)
│   ├── Chrome/
│   ├── Safari/
│   └── Firefox/
└── facebook_data/          # Raw Facebook data
    └── Kantar_download_398_unzipped_new/
        └── *.json files
```

### Output Data Structure
```
data/
├── extracted_urls/         # Browser URL extraction results
│   └── extracted_urls_optimized.csv
├── extracted_urls_facebook/ # Facebook URL extraction results
│   └── extracted_urls_facebook.csv
└── processed_results/      # Final processed results
    ├── resolved_urls/
    ├── scraped_content/
    └── pipeline_report_*.json
```

## Examples

### Process Browser Data Only
```bash
# Extract URLs from browser data
python3 scripts/url_extraction/step1_extract_urls_optimized.py \
    --browser-dir data/browser_data \
    --output-dir data/extracted_urls

# Process on VM
./run_pipeline.sh full --input data/extracted_urls/extracted_urls_optimized.csv --output data/browser_results
```

### Process Facebook Data Only
```bash
# Extract URLs from Facebook data
python3 scripts/url_extraction_facebook/step1_extract_urls_facebook.py \
    --data-dir data/facebook_data \
    --output-dir data/extracted_urls_facebook

# Process on VM
./run_pipeline.sh full --input data/extracted_urls_facebook/extracted_urls_facebook.csv --output data/facebook_results
```

### High-Performance Processing
```bash
./run_pipeline.sh full \
    --input data/extracted_urls/extracted_urls_optimized.csv \
    --output data/results \
    --batch-size 2000 \
    --max-workers 40
```

## Parameters

### URL Extraction Parameters
- `--browser-dir`: Directory containing browser data
- `--data-dir`: Directory containing Facebook data
- `--output-dir`: Output directory for extracted URLs

### VM Processing Parameters
- `--input`: Input CSV file with URLs
- `--output`: Output directory for results
- `--batch-size`: Batch size for URL resolution (default: 1000)
- `--max-workers`: Max workers for scraping (default: 20)

## Monitoring and Logs

### Check Processing Status
```bash
# Check if extraction completed
ls -la data/extracted_urls/
ls -la data/extracted_urls_facebook/

# Check if VM processing completed
ls -la data/processed_results/
```

### View Logs
```bash
# URL extraction logs
tail -f data/extracted_urls/extraction_log.txt
tail -f data/extracted_urls_facebook/facebook_url_extraction_*.log

# VM processing logs
tail -f data/processed_results/pipeline_report_*.json
```

## Troubleshooting

### Common Issues

1. **Missing Dependencies**
   ```bash
   source /opt/conda/bin/activate
   pip install -r requirements.txt
   ```

2. **Permission Issues**
   ```bash
   chmod +x *.sh
   chmod +x scripts/*/*.py
   ```

3. **Data Structure Issues**
   ```bash
   # Verify data structure
   ls -la data/browser_data/
   ls -la data/facebook_data/
   ```

4. **VM Connection Issues**
   ```bash
   # Test SSH connection
   ssh ucloud@ssh.cloud.sdu.dk -p 2390 "echo 'Connection successful'"
   ```

### Performance Optimization

- **Large Datasets**: Increase batch size and workers
- **Memory Issues**: Reduce batch size
- **Network Issues**: Use smaller files or compress data

## Integration with Local Pipeline

This VM pipeline can be integrated with the local complete pipeline:

```bash
# On local machine
python pipelines/complete_data_pipeline.py --mode full --data-type both
```

This will:
1. Extract URLs locally
2. Upload to VM
3. Process on VM
4. Download results
5. Analyze locally
EOF

scp -P $VM_PORT /tmp/COMPLETE_PIPELINE_USAGE.md $VM_HOST:$VM_PROJECT_DIR/

# Clean up temporary files
rm -f /tmp/run_pipeline.sh /tmp/extract_urls.sh /tmp/run_complete_pipeline.sh /tmp/COMPLETE_PIPELINE_USAGE.md

echo "✅ Complete pipeline deployment finished!"
echo ""
echo "📋 Usage:"
echo "  SSH to VM: ssh $VM_HOST -p $VM_PORT"
echo "  Run complete pipeline: cd $VM_PROJECT_DIR && ./run_complete_pipeline.sh"
echo "  Extract URLs only: cd $VM_PROJECT_DIR && ./extract_urls.sh"
echo "  Process on VM only: cd $VM_PROJECT_DIR && ./run_pipeline.sh full --input data/input.csv --output data/output"
echo "  View usage: cat $VM_PROJECT_DIR/COMPLETE_PIPELINE_USAGE.md"
