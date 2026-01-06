#!/bin/bash
"""
Deploy Unified Data Pipeline to VM
==================================
This script deploys the unified pipeline tool and optimized scripts to the VM.
"""

set -e  # Exit on any error

# Configuration
VM_HOST="ucloud@ssh.cloud.sdu.dk"
VM_PORT="2390"
VM_PROJECT_DIR="/work/Datadonationer/urlextraction_scraping"
LOCAL_PROJECT_DIR="$(pwd)"

echo "🚀 Deploying Unified Data Pipeline to VM..."

# Create backup of existing scripts
echo "📦 Creating backup of existing scripts..."
ssh -p $VM_PORT $VM_HOST "cd $VM_PROJECT_DIR && tar -czf scripts_backup_$(date +%Y%m%d_%H%M%S).tar.gz scripts/"

# Create unified pipeline directory
echo "📁 Creating unified pipeline directory..."
ssh -p $VM_PORT $VM_HOST "mkdir -p $VM_PROJECT_DIR/scripts/unified_pipeline"

# Copy the unified pipeline tool
echo "📋 Copying unified pipeline tool..."
scp -P $VM_PORT pipelines/unified_data_pipeline.py $VM_HOST:$VM_PROJECT_DIR/scripts/unified_pipeline/

# Make it executable
ssh -p $VM_PORT $VM_HOST "chmod +x $VM_PROJECT_DIR/scripts/unified_pipeline/unified_data_pipeline.py"

# Create a simple wrapper script for easy access
echo "🔧 Creating wrapper script..."
cat > /tmp/run_pipeline.sh << 'EOF'
#!/bin/bash
# Wrapper script for easy pipeline execution
cd /work/Datadonationer/urlextraction_scraping
python3 scripts/unified_pipeline/unified_data_pipeline.py "$@"
EOF

scp -P $VM_PORT /tmp/run_pipeline.sh $VM_HOST:$VM_PROJECT_DIR/
ssh -p $VM_PORT $VM_HOST "chmod +x $VM_PROJECT_DIR/run_pipeline.sh"

# Create usage documentation
echo "📚 Creating usage documentation..."
cat > /tmp/PIPELINE_USAGE.md << 'EOF'
# Unified Data Pipeline - Usage Guide

## Quick Start

### Run Full Pipeline (URL Resolution + Content Scraping)
```bash
./run_pipeline.sh --mode full --input data/your_input.csv --output results/
```

### Run Only URL Resolution
```bash
./run_pipeline.sh --mode resolve --input data/your_input.csv --output resolved/
```

### Run Only Content Scraping
```bash
./run_pipeline.sh --mode scrape --input resolved_urls.csv --output scraped/
```

## Parameters

- `--mode` / `-m`: Pipeline mode (`resolve`, `scrape`, or `full`)
- `--input` / `-i`: Input CSV file with URLs
- `--output` / `-o`: Output directory
- `--batch-size` / `-b`: Batch size for URL resolution (default: 1000)
- `--max-workers` / `-w`: Max workers for scraping (default: 20)

## Examples

1. **Process browser data**:
   ```bash
   ./run_pipeline.sh --mode full --input data/extracted_urls_optimized.csv --output results/browser_processing/
   ```

2. **Process Facebook data**:
   ```bash
   ./run_pipeline.sh --mode full --input data/extracted_urls_facebook.csv --output results/facebook_processing/
   ```

3. **Resume interrupted processing**:
   ```bash
   ./run_pipeline.sh --mode scrape --input data/complete_resolved/resolved_browser_batch_0364.csv --output results/scraped_content/
   ```

## Output Structure

```
results/
├── resolved_urls/          # Resolved URLs from step 1
├── scraped_content/        # Scraped content from step 2
└── pipeline_report_*.json  # Execution report
```

## Monitoring

- Check logs in `logs/unified_pipeline_*.log`
- Monitor progress in real-time output
- Review execution reports for detailed statistics

## Troubleshooting

1. **Timeout errors**: Increase timeout values in the script
2. **Memory issues**: Reduce batch size or max workers
3. **Network errors**: Check VM connectivity and retry
4. **Permission errors**: Ensure proper file permissions

## Performance Tips

- Use `--batch-size 500` for large datasets
- Use `--max-workers 10` on limited resources
- Monitor system resources during execution
- Use SSD storage for better I/O performance
EOF

scp -P $VM_PORT /tmp/PIPELINE_USAGE.md $VM_HOST:$VM_PROJECT_DIR/

# Clean up temporary files
rm -f /tmp/run_pipeline.sh /tmp/PIPELINE_USAGE.md

echo "✅ Deployment completed successfully!"
echo ""
echo "📋 Next steps:"
echo "1. SSH to VM: ssh -p $VM_PORT $VM_HOST"
echo "2. Navigate to: cd $VM_PROJECT_DIR"
echo "3. Read usage guide: cat PIPELINE_USAGE.md"
echo "4. Run pipeline: ./run_pipeline.sh --help"
echo ""
echo "🎯 Example command:"
echo "./run_pipeline.sh --mode full --input data/extracted_urls_optimized.csv --output results/full_processing/"
