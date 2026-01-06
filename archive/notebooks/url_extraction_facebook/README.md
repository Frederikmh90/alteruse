# Facebook URL Extraction and Scraping Pipeline

This pipeline extracts URLs from Facebook data files and scrapes content from those URLs, similar to the browser data pipeline but specifically designed for Facebook data structures.

## Overview

The pipeline processes Facebook data files (JSON and HTML) to:
1. Extract URLs from posts, comments, and other Facebook activities
2. Analyze and categorize domains
3. Create prioritized batches for efficient scraping
4. Scrape content with deduplication and error handling

## Pipeline Steps

### Step 1: URL Extraction (`step1_extract_urls_facebook.py`)
- **Input**: Facebook data files in `data/Samlet_06112025/Facebook/`
- **Output**: `data/url_extract_facebook/extracted_urls_facebook.csv`
- **Features**:
  - Scans all JSON and HTML files in Facebook directories
  - Extracts URLs using regex patterns
  - Filters out Facebook-related URLs (facebook.com, fb.com, facebook.dk)
  - Keeps Instagram URLs as requested
  - Adds metadata: source directory, source file, timestamp
  - URL-based deduplication

### Step 2: URL Analysis (`step2_analyze_urls_facebook.py`)
- **Input**: `extracted_urls_facebook.csv`
- **Output**: Domain analysis, resolution test results, scraping plan
- **Features**:
  - Analyzes domain patterns and categorizes them
  - Tests URL resolution for sample URLs
  - Creates scraping plan with priority scores
  - Categorizes domains (news, social media, shopping, etc.)

### Step 3: Domain Prioritization (`step3_prioritize_domains_facebook.py`)
- **Input**: Domain analysis and scraping plan
- **Output**: Prioritized batches in `scraping_batches/`
- **Features**:
  - Creates priority tiers (high, medium, low)
  - Organizes URLs into manageable batches (200 URLs each)
  - Creates category-based batches
  - Generates metadata and estimates

### Step 4: Content Scraping (`step4_scrape_content_facebook.py`)
- **Input**: Scraping batches
- **Output**: `scraped_content_facebook.csv`
- **Features**:
  - Scrapes content using requests + trafilatura
  - Fallback extraction using BeautifulSoup
  - URL-based and content-hash-based deduplication
  - Incremental saving (every batch)
  - Comprehensive error handling and logging

## Data Structure

### Input Data
Facebook data is organized in directories like:
```
data/Samlet_06112025/Facebook/
├── [account_directory]/
│   ├── your_facebook_activity/
│   │   ├── posts/
│   │   ├── comments_and_reactions/
│   │   ├── groups/
│   │   ├── pages/
│   │   └── events/
│   └── logged_information/
```

### Output Data
```
data/url_extract_facebook/
├── extracted_urls_facebook.csv          # Step 1 output
├── domain_analysis_facebook.csv         # Step 2 output
├── scraping_plan_facebook.csv           # Step 2 output
├── scraping_batches/                    # Step 3 output
│   ├── batch_001_tier_1_high_priority.csv
│   ├── batch_002_tier_1_high_priority.csv
│   └── ...
└── scraped_content/                     # Step 4 output
    ├── scraped_content_facebook.csv
    └── scraping_log_*.log
```

## Usage

### Run Complete Pipeline
```bash
cd notebooks/url_extraction_facebook
python run_facebook_pipeline.py
```

### Run Individual Steps
```bash
# Step 1: Extract URLs
python step1_extract_urls_facebook.py

# Step 2: Analyze URLs
python step2_analyze_urls_facebook.py

# Step 3: Create batches
python step3_prioritize_domains_facebook.py

# Step 4: Scrape content
python step4_scrape_content_facebook.py
```

## Key Features

### URL Filtering
- **Excluded**: facebook.com, fb.com, facebook.dk, fbcdn.net, messenger.com, workplace.com
- **Included**: instagram.com (as requested)
- **Excluded**: Generic search URLs (google.com/search, bing.com/search)

### Domain Prioritization
- **Tier 1 (High Priority)**: News sites, international news, blogs
- **Tier 2 (Medium Priority)**: Danish public institutions, tech sites
- **Tier 3 (Low Priority)**: Shopping, real estate, other sites

### Deduplication
- **URL-based**: Same URL = skip
- **Content-hash-based**: Same content = skip
- **Running checks**: Maintains sets of processed URLs and content hashes

### Error Handling
- Timeout handling (15 seconds)
- Connection error handling
- Content type validation
- Graceful fallback extraction methods

## Configuration

### Batch Size
- Default: 200 URLs per batch
- Configurable in `step3_prioritize_domains_facebook.py`

### Scraping Delay
- Default: 2 seconds between requests
- Configurable in `step4_scrape_content_facebook.py`

### Priority Thresholds
- Minimum priority for scraping: 6/10
- Minimum URLs per domain: 5
- Configurable in `step2_analyze_urls_facebook.py`

## Monitoring

### Logs
- Extraction logs: `facebook_url_extraction_*.log`
- Scraping logs: `scraping_log_*.log`
- Statistics: `scraping_stats_*.json`

### Progress Tracking
- Real-time progress updates
- ETA calculations
- Success rate monitoring
- Batch completion tracking

## Dependencies

Required Python packages:
- pandas
- numpy
- requests
- trafilatura
- beautifulsoup4
- pathlib
- logging
- json
- hashlib
- datetime
- urllib.parse
- re

## Notes

- The pipeline is designed to be resumable - it can be stopped and restarted
- Content is saved incrementally to prevent data loss
- The pipeline respects rate limits and is polite to servers
- Instagram URLs are preserved as requested (not filtered out)
- Facebook-related URLs are filtered out to focus on external content 