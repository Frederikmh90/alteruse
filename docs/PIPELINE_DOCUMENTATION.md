# URL Extraction, Resolution, and Scraping Pipeline Documentation

## Overview

This pipeline is designed to process data donations containing browser history and Facebook activity. It extracts URLs, resolves them to their final destination (handling redirects and shorteners), and scrapes content from relevant pages. The pipeline is modular and designed to run on a remote server (UCloud) to handle large datasets efficiently.

## Pipeline Architecture

The pipeline consists of four main stages executed sequentially:

1.  **Browser URL Extraction**
2.  **Facebook URL Extraction**
3.  **URL Resolution**
4.  **Content Scraping**

### Directory Structure

The pipeline operates on a specific data directory structure. For the "New Data" run (e.g., `new_data_251126`), the structure is:

```text
/work/Datadonationer/urlextraction_scraping/data/new_data_251126/
├── Browser/                   # [INPUT] Raw browser history files (.json, .db)
├── Facebook/                  # [INPUT] Raw Facebook export folders
├── browser_urlextract/        # [OUTPUT] Extracted URLs from browser history
├── facebook_urlextract/       # [OUTPUT] Extracted URLs from Facebook data
├── complete_resolved/         # [OUTPUT] Batch files of resolved URLs
├── scraped_content/           # [OUTPUT] Final scraped content
└── logs/                      # [OUTPUT] Execution logs
```

---

## Detailed Pipeline Steps

### Step 1: Browser URL Extraction
*   **Script:** `notebooks/url_extraction/step1_extract_urls_optimized.py`
*   **Input:** `.db` (SQLite Safari history) and `.json` (Chrome/Other history) files in the `Browser/` directory.
*   **Process:**
    *   Parses SQLite databases and JSON files.
    *   Handles specific timestamp formats (WebKit vs. Chrome/Unix).
    *   Cleans URLs (removes tracking parameters like `utm_source`, `fbclid`).
    *   Filters out generic domains (e.g., `google.com`, `localhost`) to reduce noise.
    *   **Error Handling:** Skips malformed database files (e.g., "database disk image is malformed") and logs errors without stopping the pipeline.
*   **Output:** `browser_urlextract/extracted_urls_optimized.csv`

### Step 2: Facebook URL Extraction
*   **Script:** `notebooks/url_extraction_facebook/step1_extract_urls_facebook.py`
*   **Input:** Folders containing Facebook data exports (JSON and HTML files) in the `Facebook/` directory.
*   **Process:**
    *   Recursively scans directories for relevant files.
    *   Extracts links from posts, comments, and shared content using regex.
    *   Filters out internal Facebook links (`facebook.com`, `messenger.com`) to focus on external shared content.
*   **Output:** `facebook_urlextract/extracted_urls_facebook.csv`

### Step 3: URL Resolution
*   **Script:** `pipelines/url_resolution/complete_pipeline.py`
*   **Input:** The CSV files generated in Steps 1 and 2.
*   **Process:**
    *   takes raw URLs and follows redirects to find the "final" URL (e.g., resolving `bit.ly` links or news redirects).
    *   Checks if the link is still valid (returns status 200).
    *   Operates in batches (default: 1000 URLs) to save progress incrementally.
    *   Uses a local cache (`url_resolution_cache.db`) to avoid re-resolving known URLs.
*   **Output:** Batched CSV files in `complete_resolved/` (e.g., `resolved_browser_batch_0001.csv`).

### Step 4: Content Scraping (Turbo Scraper)
*   **Script:** `scrapers/browser_scraper.py`
*   **Input:** The resolved URL batches from Step 3.
*   **Process:**
    *   Uses parallel processing (`max_workers=20`) for high speed.
    *   Fetches the HTML content of the resolved URLs.
    *   Extracts main article text, titles, and metadata using `trafilatura`.
    *   Skips duplicates (URLs already scraped).
*   **Output:** `scraped_content/scraped_content_turbo.csv` containing the full text and metadata.

---

## How to Run

### Prerequisites
*   SSH access to UCloud.
*   `tmux` (recommended for keeping the process running).

### Command
Run the following command on the remote server:

```bash
bash /work/Datadonationer/urlextraction_scraping/scripts/run_new_data_pipeline_remote.sh
```

### Monitoring
*   **Logs:** Check the `logs/` directory in your data folder.
*   **Console:** The script prints real-time progress (e.g., "Processed 121098 records").

## Error Handling & Paths Verification
*   **Malformed DBs:** The pipeline detects corrupted SQLite files (as seen in your output: `database disk image is malformed`) and skips them safely, continuing with the next file.
*   **Paths:** All paths in the runner script `run_new_data_pipeline_remote.sh` are dynamic. They are based on the `REMOTE_BASE_PATH` (defaulting to `/work/Datadonationer/urlextraction_scraping/data/new_data_251126`), ensuring that inputs and outputs are correctly isolated to the specific dataset run.


