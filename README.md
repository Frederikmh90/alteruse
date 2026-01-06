# AlterUse - Data Donation Processing Pipeline

A comprehensive data pipeline for extracting, resolving, and scraping URLs from browser history and Facebook data donations. This project processes data donations to analyze news consumption patterns and information sources.

**Note:** This repository contains code only. `config/` (credentials) and `data/` (data donations) folders are excluded for security and privacy.

## Project Overview

This pipeline processes data donations from browser history (Safari, Chrome) and Facebook exports to:
1. **Extract** URLs from various data sources
2. **Resolve** shortened URLs and redirects
3. **Scrape** content from final destinations
4. **Analyze** news sources and information consumption patterns

### Key Features
- Handles multiple browser formats (Safari SQLite, Chrome JSON)
- Recursively processes complex Facebook export structures
- Robust URL resolution with shortlink handling
- Content scraping with progress tracking and resume capability
- Comprehensive source tracking for traceability

## Repository Structure

```
alteruse/
├── pipeline/                      # Main processing pipeline
│   ├── 1_extract_urls/           # Extract URLs from donations
│   │   ├── browser.py            # Browser history extraction
│   │   └── facebook.py           # Facebook data extraction
│   ├── 2_resolve_urls/           # Resolve URLs and shortlinks
│   │   ├── resolver.py           # URL resolution engine
│   │   └── run_resolution.py     # Resolution runner
│   └── 3_scrape_content/         # Scrape final content
│       ├── scraper.py            # Main scraper
│       └── scraper_direct.py     # Direct scraper (VM-based)
├── core/                         # Core processing modules
│   ├── browser_processing.py     # Browser data processing
│   ├── facebook_processing.py    # Facebook data processing
│   └── news_analysis.py          # News source classification
├── scripts/                      # Data processing utilities
│   ├── combine_scraped_data.py   # Merge scraped batches
│   ├── compare_datasets.py       # Dataset comparison
│   └── isolate_new_data.py       # Extract new donations
├── docs/                         # Documentation
│   ├── TECHNICAL_PIPELINE_REPORT.md  # Complete technical docs
│   └── PIPELINE_DOCUMENTATION.md     # Pipeline guide
├── data_samples/                 # Sample data (for reference)
├── archive/                      # Archived code/docs
├── requirements.txt              # Python dependencies
└── README.md                     # This file

# Excluded folders (not in repository):
# ├── config/                     # Credentials and API keys
# └── data/                       # Raw and processed data
```

## Pipeline Overview

### Stage 1: Extract URLs

**Browser History:**
```bash
python3 pipeline/1_extract_urls/browser.py \
    --browser-dir /path/to/browser/data \
    --output-dir /path/to/output
```
Supports: Safari (SQLite), Chrome (JSON), and other Chromium-based browsers.

**Facebook Data:**
```bash
python3 pipeline/1_extract_urls/facebook.py \
    --input-dir /path/to/Facebook \
    --output-dir /path/to/output
```
Recursively scans all JSON and HTML files in Facebook export folders, tracking source directories and files for complete traceability.

### Stage 2: Resolve URLs

```bash
python3 pipeline/2_resolve_urls/run_resolution.py \
    --input-file extracted_urls.csv \
    --output-file resolved_urls.csv
```

- Follows HTTP redirects and meta-refresh tags
- Resolves shortened URLs (t.co, bit.ly, etc.)
- Handles JavaScript-based redirects with browser automation
- Extracts clean final destination URLs

### Stage 3: Scrape Content

```bash
python3 pipeline/3_scrape_content/scraper_direct.py \
    --urls-file resolved_urls.csv \
    --output-dir scraped_content \
    --workers 20
```

**Features:**
- Automatic progress tracking and resume capability
- Batch saving every 100 URLs
- Extracts: title, author, date, content, metadata
- Configurable worker count and timeouts
- Can run with or without proxy

## Data Processing Utilities

### Combine Scraped Data
```bash
python3 scripts/combine_scraped_data.py \
    --input-dir scraped_content \
    --output-file combined_data.csv
```
Merges individual batch files into a single dataset.

### Compare Datasets
```bash
python3 scripts/compare_datasets.py \
    --old-data old_dataset.csv \
    --new-data new_dataset.csv
```
Identifies differences between data donation batches.

## Key Datasets

The pipeline produces several unified datasets:

1. **Extracted URLs** - Raw URLs with source tracking
   - Columns: `url`, `source_directory`, `source_file`, `timestamp`

2. **Resolved URLs** - Final destination URLs
   - Columns: `url`, `resolved_url`, `domain`, `redirect_chain`

3. **Scraped Content** - Full content with metadata
   - Columns: `url`, `resolved_url`, `domain`, `content`, `title`, `author`, `date`, `source`, `word_count`, `status_code`, `response_time`

4. **Merged Dataset** - Combined old + new data
   - Source tracking: `old_browser`, `old_facebook`, `new_browser`, `new_facebook`
   - **Statistics:** 1,108,858 total URLs | 692,684 with content | 36,813 unique domains

## Documentation

Comprehensive documentation available in the `docs/` folder:

- **[TECHNICAL_PIPELINE_REPORT.md](docs/TECHNICAL_PIPELINE_REPORT.md)** - Complete technical documentation
  - Detailed folder structures for Facebook data
  - URL extraction methodology with examples
  - Resolution strategies and handling
  - Source tracking and traceability
  - Data quality reports and statistics

- **[PIPELINE_DOCUMENTATION.md](docs/PIPELINE_DOCUMENTATION.md)** - Pipeline implementation guide

- **[EXPRESSVPN_SSH_TUNNEL_GUIDE.md](docs/EXPRESSVPN_SSH_TUNNEL_GUIDE.md)** - VPN setup (if needed)

## Requirements

### Python Dependencies
```bash
pip install -r requirements.txt
```

Main dependencies:
- `pandas` - Data processing
- `aiohttp` - Async HTTP requests
- `beautifulsoup4` - HTML parsing
- `playwright` - Browser automation (for JavaScript-heavy sites)
- `pyarrow` or `fastparquet` - Parquet file support

### VM Setup (UCloud)

The main processing typically runs on a UCloud VM for better network reliability and resources.

**SSH Configuration** (`~/.ssh/config`):
```
Host ucloud
    HostName ssh.cloud.sdu.dk
    User ucloud
    Port 2885
    IdentityFile ~/.ssh/id_rsa
```

**VM Project Path:**
```
/work/Datadonationer/urlextraction_scraping/
```

## Security & Privacy

⚠️ **Important:** This repository does NOT include:
- `config/` - Contains credentials, API keys, and VPN configurations
- `data/` - Contains raw data donations and processed datasets

These folders are permanently excluded via `.gitignore` to protect sensitive information and participant privacy.

## Sample Data

Sample datasets are available in `data_samples/` for reference:
- `sample_old_browser.csv`
- `sample_old_facebook.csv`
- `sample_new_browser.csv`
- `sample_new_facebook.csv`

These samples demonstrate the data structure without exposing participant information.

## Project Status

**Current Status:** Production pipeline with complete documentation

**Key Achievements:**
- ✅ Complete URL extraction from browser and Facebook data
- ✅ Robust URL resolution with 93%+ success rate on shortlinks
- ✅ Content scraping with automatic resume
- ✅ Unified dataset: 1.1M URLs from 4 sources
- ✅ Comprehensive technical documentation
- ✅ Clean, organized repository structure

## Contributing

This is a research project. For questions or collaboration inquiries, please refer to the documentation in `docs/`.

## License

[Add appropriate license information]

## Citation

If you use this pipeline or methodology in your research, please cite appropriately.

---

**Repository:** https://github.com/Frederikmh90/alteruse.git
