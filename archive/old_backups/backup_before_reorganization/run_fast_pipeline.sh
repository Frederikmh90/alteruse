#!/bin/bash
# Fast URL Resolution Pipeline Runner
# Correct workflow: Raw URLs → Resolution → Plan → Scraping

export PATH="$HOME/.local/bin:$PATH"

echo "🚀 Starting Correct URL Resolution Pipeline"
echo "==========================================="

# Step 1: Enhanced Browser URL Resolution (from raw URLs)
echo "📊 Step 1: Running Enhanced Browser URL Resolution"
echo "Processing raw browser URLs: ../data/browser_urlextract/extracted_urls_optimized.csv"
python3 notebooks/url_extraction/step4_scrape_content_with_resolution.py \
    --data-dir ../data/browser_urlextract \
    --output-dir ../data/browser_urlextract/scraped_content_resolved \
    --delay 1.5 \
    --max-batches 50

# Step 2: Enhanced Facebook URL Resolution (from raw URLs)
echo "📘 Step 2: Running Enhanced Facebook URL Resolution"
echo "Processing raw Facebook URLs: ../data/facebook_urlextract/extracted_urls_facebook.csv"
python3 notebooks/url_extraction_facebook/step4_scrape_content_facebook_with_resolution.py \
    --data-dir ../data/facebook_urlextract \
    --output-dir ../data/facebook_urlextract/scraped_content_resolved \
    --delay 1.5 \
    --max-batches 50

# Step 3: Combined URL Resolution and Merging
echo "🔗 Step 3: Running Combined URL Resolution and Data Merging"
python3 combined_url_resolution_enhanced.py \
    --facebook-data ../data/facebook_urlextract/scraped_content_resolved/scraped_content_facebook.csv \
    --browser-data ../data/browser_urlextract/scraped_content_resolved/scraped_content.csv \
    --output-dir ../data/combined_resolved_enhanced \
    --batch-size 1000

# Step 4: Unified Re-scraping Pipeline (final content scraping)
echo "🔄 Step 4: Running Unified Re-scraping Pipeline"
python3 unified_rescraping_pipeline.py \
    --input-file ../data/combined_resolved_enhanced/urls_for_rescraping.csv \
    --output-dir ../data/unified_rescraped \
    --batch-size 1000 \
    --delay 1.0

echo "✅ Pipeline completed successfully!"
echo "📁 Results available in: ../data/unified_rescraped/"
echo ""
echo "📊 Summary:"
echo "- Step 1: Resolved browser URLs from raw data"
echo "- Step 2: Resolved Facebook URLs from raw data"
echo "- Step 3: Merged and prioritized all resolved data"
echo "- Step 4: Final content scraping with resolved URLs" 