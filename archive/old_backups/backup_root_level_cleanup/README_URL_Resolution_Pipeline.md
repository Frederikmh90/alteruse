# Enhanced URL Resolution and Scraping Pipeline

This document describes the comprehensive 3-step pipeline for robust URL resolution and content scraping for both Facebook and browser data.

## Overview

The pipeline addresses issues with URL shorteners (like t.co, bit.ly, fb.me) and improves content scraping quality through proper URL resolution before content extraction.

## Pipeline Architecture

### Step 1: Enhanced Content Scrapers with URL Resolution
- **Browser Data**: `notebooks/url_extraction/step4_scrape_content_with_resolution.py`
- **Facebook Data**: `notebooks/url_extraction_facebook/step4_scrape_content_facebook_with_resolution.py`

### Step 2: Combined URL Resolution
- **Main Script**: `combined_url_resolution_enhanced.py`

### Step 3: Unified Re-scraping Pipeline
- **Main Script**: `unified_rescraping_pipeline.py`

## Step 1: Enhanced Content Scrapers

### Features
- **Robust URL Resolution**: Resolves redirects from t.co, bit.ly, fb.me, etc. before scraping
- **URL Resolution Testing**: Tests resolution capabilities on known redirect URLs
- **Comprehensive Statistics**: Tracks resolution success rates and content improvements
- **Caching**: Uses SQLite cache to avoid re-resolving the same URLs
- **Enhanced Paywall Detection**: Improved detection patterns for subscription content

### Browser Data Scraper
```bash
cd notebooks/url_extraction
python step4_scrape_content_with_resolution.py \
    --data-dir ../../data/url_extract \
    --output-dir ../../data/url_extract/scraped_content \
    --delay 2.0 \
    --max-batches 5
```

### Facebook Data Scraper
```bash
cd notebooks/url_extraction_facebook
python step4_scrape_content_facebook_with_resolution.py \
    --data-dir ../../data/url_extract_facebook \
    --output-dir ../../data/url_extract_facebook/scraped_content \
    --delay 2.0 \
    --max-batches 5
```

### Key Improvements
- **URL Resolution Rate**: Significantly higher success rate for shortened URLs
- **Content Quality**: Better content extraction from resolved URLs
- **Error Handling**: Robust handling of SSL errors and timeouts
- **Progress Tracking**: Detailed logging and statistics

## Step 2: Combined URL Resolution

### Purpose
Merges Facebook and browser datasets, identifies URLs needing resolution, and creates a unified dataset with properly resolved URLs.

### Features
- **Data Standardization**: Unifies Facebook and browser data schemas
- **Smart Deduplication**: Removes duplicate URLs while preserving best quality entries
- **Resolution Analysis**: Identifies URLs likely to be redirects or shortened links
- **Batch Processing**: Efficient batch URL resolution
- **Quality Assessment**: Determines which URLs need re-scraping

### Usage
```bash
python combined_url_resolution_enhanced.py \
    --facebook-data data/url_extract_facebook/scraped_content/scraped_content_facebook.csv \
    --browser-data data/url_extract/scraped_content/scraped_content.csv \
    --output-dir data/combined_resolved_enhanced \
    --batch-size 500
```

### Output Files
- `combined_urls_resolved.csv`: Complete merged dataset with resolution results
- `urls_for_rescraping.csv`: URLs that need re-scraping (input for Step 3)
- `successfully_resolved_urls.csv`: URLs where resolution worked
- `high_quality_existing_content.csv`: URLs with good existing content
- `processing_report.json`: Comprehensive processing statistics

## Step 3: Unified Re-scraping Pipeline

### Purpose
Re-scrapes content from URLs that need updated scraping, using the best available resolved URLs from Step 2.

### Features
- **Smart URL Selection**: Chooses best URL (resolved vs original) for scraping
- **Content Improvement Tracking**: Compares new vs old content quality
- **Priority Processing**: Prioritizes successfully resolved URLs
- **Incremental Processing**: Supports resuming from where it left off
- **Performance Metrics**: Tracks improvement rates and success statistics

### Usage
```bash
python unified_rescraping_pipeline.py \
    --input-file data/combined_resolved_enhanced/urls_for_rescraping.csv \
    --output-dir data/unified_rescraped \
    --batch-size 500 \
    --delay 2.0
```

### Output Files
- `unified_rescraped_content.csv`: Complete re-scraped content
- `rescraping_stats_*.json`: Processing statistics
- `unified_rescraping_*.log`: Detailed processing logs

## Key Components

### RobustURLResolver Class
Core URL resolution engine with:
- **Multi-redirect handling**: Follows redirect chains properly
- **SSL error recovery**: Handles SSL certificate issues
- **Caching system**: SQLite-based caching for efficiency
- **Batch processing**: Concurrent URL resolution
- **Comprehensive logging**: Detailed resolution tracking

### Enhanced Paywall Detection
Improved patterns for detecting:
- **Subscription walls**: Premium content indicators
- **Metered access**: Article limit notifications
- **Login requirements**: Access restriction patterns
- **Domain-specific rules**: Known paywall sites

### Content Quality Assessment
Metrics for determining content improvement:
- **Word count increase**: Significant content expansion
- **Error reduction**: Fewer scraping errors
- **Status code improvement**: Better HTTP responses
- **Content hash changes**: Actual content differences

## Statistics and Monitoring

### Key Metrics Tracked
- **URL Resolution Success Rate**: % of URLs successfully resolved
- **Content Improvement Rate**: % of re-scraped content that improved
- **Paywall Detection Rate**: % of paywalled content identified
- **Processing Speed**: URLs processed per minute
- **Error Rates**: Types and frequency of errors

### Example Statistics Output
```json
{
  "url_resolution": {
    "urls_needing_resolution": 5432,
    "urls_successfully_resolved": 4891,
    "resolution_success_rate": 0.90
  },
  "scraping_needs": {
    "urls_needing_rescraping": 8765,
    "rescraping_rate": 0.65
  },
  "content_quality": {
    "improved_content": 3456,
    "improvement_rate": 0.78
  }
}
```

## Testing URL Resolution

Both enhanced scrapers include URL resolution testing:

```bash
# Test browser scraper resolution
python step4_scrape_content_with_resolution.py --test-only

# Test Facebook scraper resolution
python step4_scrape_content_facebook_with_resolution.py --test-only

# Test combined resolver
python combined_url_resolution_enhanced.py --test-only
```

## VM Deployment

### Transfer Files to VM
```bash
# Copy enhanced scripts to VM
scp step4_scrape_content_with_resolution.py user@vm:/path/to/project/notebooks/url_extraction/
scp step4_scrape_content_facebook_with_resolution.py user@vm:/path/to/project/notebooks/url_extraction_facebook/
scp combined_url_resolution_enhanced.py user@vm:/path/to/project/
scp unified_rescraping_pipeline.py user@vm:/path/to/project/
```

### Run on VM
```bash
# Step 1: Enhanced scraping (if needed)
python step4_scrape_content_with_resolution.py --max-batches 10

# Step 2: Combined resolution
python combined_url_resolution_enhanced.py

# Step 3: Unified re-scraping
python unified_rescraping_pipeline.py --max-urls 1000  # Start with smaller test
```

## Performance Recommendations

### For Large Datasets
- **Batch Size**: Use 500-1000 for URL resolution
- **Delay**: Use 1-2 seconds between requests
- **Max Workers**: 10-15 for URL resolution
- **Memory**: Monitor memory usage for large datasets

### For Testing
- **Max URLs**: Use `--max-urls 100` for initial testing
- **Max Batches**: Use `--max-batches 5` for testing
- **Test Mode**: Use `--test-only` to verify URL resolution

## Error Handling

### Common Issues
1. **SSL Certificate Errors**: Automatically retried without verification
2. **Timeout Errors**: Configurable timeout settings
3. **Rate Limiting**: Built-in delays and backoff
4. **Memory Issues**: Batch processing to limit memory usage

### Recovery
- **Incremental Processing**: Resume from where stopped
- **Error Logging**: Detailed error tracking
- **Skip Duplicates**: Automatic duplicate detection

## Expected Improvements

### URL Resolution
- **20-40% improvement** in successful URL resolution
- **Significantly better** content from resolved URLs
- **Reduced errors** from broken redirects

### Content Quality
- **30-50% improvement** in content word counts
- **Better domain diversity** from resolved URLs
- **Higher quality** news article extraction

### Processing Efficiency
- **Faster processing** through better error handling
- **Reduced re-work** through comprehensive caching
- **Better monitoring** through detailed statistics

## Next Steps

1. **Deploy to VM**: Transfer and test the enhanced pipeline
2. **Monitor Performance**: Track resolution and improvement rates
3. **Optimize Settings**: Adjust delays and batch sizes based on performance
4. **Scale Processing**: Increase batch sizes for production runs
5. **Analyze Results**: Compare content quality before and after pipeline

## Files Created

### Enhanced Scrapers
- `notebooks/url_extraction/step4_scrape_content_with_resolution.py`
- `notebooks/url_extraction_facebook/step4_scrape_content_facebook_with_resolution.py`

### Combined Processing
- `combined_url_resolution_enhanced.py`
- `unified_rescraping_pipeline.py`

### Documentation
- `README_URL_Resolution_Pipeline.md` (this file)

This pipeline provides a comprehensive solution for handling URL resolution issues and improving content scraping quality across both Facebook and browser data sources. 