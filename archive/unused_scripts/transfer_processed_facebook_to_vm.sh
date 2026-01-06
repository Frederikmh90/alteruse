#!/bin/bash

# Transfer processed Facebook data to VM
# This script transfers the already processed Facebook URL extraction data

set -e

echo "=== Transferring Processed Facebook Data to VM ==="
echo "Source: data/url_extract_facebook"
echo "Destination: /work/Datadonationer/data/url_extract_facebook"
echo ""

# Check if source directory exists
if [ ! -d "data/url_extract_facebook" ]; then
    echo "[ERROR] Source directory data/url_extract_facebook not found"
    exit 1
fi

# Create destination directory on VM
echo "[INFO] Creating destination directory on VM..."
ssh -p 2285 ucloud@ssh.cloud.sdu.dk "mkdir -p /work/Datadonationer/data/url_extract_facebook"

# Transfer all files and directories
echo "[INFO] Transferring processed Facebook data..."
rsync -avz -e "ssh -p 2285" \
    data/url_extract_facebook/ \
    ucloud@ssh.cloud.sdu.dk:/work/Datadonationer/data/url_extract_facebook/

echo ""
echo "[SUCCESS] Processed Facebook data transferred successfully!"
echo ""
echo "Files transferred:"
ssh -p 2285 ucloud@ssh.cloud.sdu.dk "ls -la /work/Datadonationer/data/url_extract_facebook/"

echo ""
echo "Next steps:"
echo "1. The Facebook pipeline Steps 1-3 are now complete"
echo "2. You can start Step 4 (content scraping) directly"
echo "3. Run: ssh -p 2285 ucloud@ssh.cloud.sdu.dk"
echo "4. Then: cd /work/Datadonationer/facebook_pipeline"
echo "5. And: python step4_scrape_content_facebook_enhanced.py" 