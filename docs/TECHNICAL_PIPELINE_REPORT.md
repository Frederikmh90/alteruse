# Technical Pipeline Report: Data Donation Verification and Processing

## Executive Summary

This document provides a comprehensive technical overview of the data donation processing pipeline, focusing on URL extraction, resolution, and content scraping for both browser history and Facebook activity data. The pipeline processes raw data donations to extract URLs, resolve redirects and shortlinks, classify news sources (mainstream vs. alternative), and analyze activity spans.

---

## 1. Pipeline Architecture Overview

The pipeline consists of three main stages executed sequentially:

1. **Extraction** (`src/pipeline/extract/`): Extract URLs from raw data files
2. **Resolution** (`src/pipeline/resolve/`): Resolve URLs through redirects and shortlinks
3. **Processing** (`src/pipeline/process/`): Merge, enrich, and analyze extracted data

---

## 2. Facebook URL Extraction

### 2.1 Overview

The Facebook URL extraction process (`pipeline/1_extract_urls/facebook.py`) scans Facebook data export folders to extract URLs embedded in posts, comments, and other activities.

### 2.2 Source Data Structure

**CRITICAL:** The Facebook URL extraction process uses **recursive directory traversal** (`os.walk()`) to scan **ALL** folders and subfolders within the input directory. **Every** JSON and HTML file found is processed, regardless of its location in the folder hierarchy.

#### Input Directory Paths

The extraction script accepts an `--input-dir` parameter. Common input paths include:

**On VM (UCloud) - Verified Actual Paths:**
- `/work/Datadonationer/urlextraction_scraping/data/new_data_251126/Facebook/` ✅ (32 account folders)
- `/work/Datadonationer/urlextraction_scraping/data/Kantar_download_398_unzipped_new/Facebook/`
- `/work/Datadonationer/urlextraction_scraping/data/Samlet_06112025/Facebook/`

**Note:** SSH connection uses port **2885** (not 2046).

**Default (if not specified):**
- `../../data/Kantar_download_398_unzipped_new/` (relative to script location)

#### Recursive Folder Structure

Facebook data donations are organized in a hierarchical folder structure. The extractor processes **ALL** folders recursively:

```
[Input Directory]/Facebook/                    # Base input directory
│                                               # Example: /work/.../data/new_data_251126/Facebook/
│
├── [Account Folder 1]/                       # ACTUAL: Long encoded names like:
│                                               # "4477g1748422889834sOEA05YM50hju5966uu5966ufacebookdinesan1990280520257p1hm4nc-xx8Lxda"
│                                               # Contains patterns: 4477g, ju5, uu5, facebook
│   │
│   ├── your_facebook_activity/               # ✅ SCANNED RECURSIVELY (VERIFIED)
│   │   │
│   │   ├── posts/                             # ✅ SCANNED RECURSIVELY (VERIFIED)
│   │   │   ├── your_posts__check_ins__photos_and_videos_1.json  # ✅ PROCESSED (VERIFIED)
│   │   │   ├── edits_you_made_to_posts.json  # ✅ PROCESSED (VERIFIED)
│   │   │   ├── posts_on_other_pages_and_profiles.json  # ✅ PROCESSED (VERIFIED)
│   │   │   ├── media/                         # ✅ SCANNED RECURSIVELY (VERIFIED)
│   │   │   │   └── [photo/video folders]/     # ✅ SCANNED RECURSIVELY
│   │   │   └── album/                         # ✅ SCANNED RECURSIVELY
│   │   │
│   │   ├── groups/                            # ✅ SCANNED RECURSIVELY (VERIFIED)
│   │   │   ├── your_comments_in_groups.json   # ✅ PROCESSED (VERIFIED)
│   │   │   ├── group_posts_and_comments.json  # ✅ PROCESSED (VERIFIED)
│   │   │   ├── your_pending_posts_in_groups.json  # ✅ PROCESSED (VERIFIED)
│   │   │   ├── your_group_messages/           # ✅ SCANNED RECURSIVELY
│   │   │   └── [any subfolder]/              # ✅ SCANNED RECURSIVELY
│   │   │       └── *.json, *.html            # ✅ PROCESSED
│   │   │
│   │   ├── comments_and_reactions/           # ✅ SCANNED RECURSIVELY (VERIFIED)
│   │   │   ├── comments.json                 # ✅ PROCESSED (VERIFIED)
│   │   │   ├── your_comment_edits.json        # ✅ PROCESSED (VERIFIED)
│   │   │   └── [any subfolder]/              # ✅ SCANNED RECURSIVELY
│   │   │       └── *.json, *.html            # ✅ PROCESSED
│   │   │
│   │   ├── pages/                             # ✅ SCANNED RECURSIVELY (VERIFIED)
│   │   │   ├── pages_and_profiles_you_follow.json  # ✅ PROCESSED (VERIFIED)
│   │   │   ├── pages_you've_liked.json        # ✅ PROCESSED (VERIFIED)
│   │   │   └── [any subfolder]/              # ✅ SCANNED RECURSIVELY
│   │   │       └── *.json, *.html            # ✅ PROCESSED
│   │   │
│   │   ├── events/                            # ✅ SCANNED RECURSIVELY (VERIFIED)
│   │   │   ├── your_events.json              # ✅ PROCESSED (VERIFIED)
│   │   │   ├── event_invitations.json        # ✅ PROCESSED (VERIFIED)
│   │   │   └── [any subfolder]/              # ✅ SCANNED RECURSIVELY
│   │   │       └── *.json, *.html            # ✅ PROCESSED
│   │   │
│   │   └── [ANY OTHER FOLDER]/               # ✅ SCANNED RECURSIVELY
│   │       ├── *.json                         # ✅ PROCESSED
│   │       ├── *.html                         # ✅ PROCESSED
│   │       ├── *.htm                          # ✅ PROCESSED
│   │       └── [any nested subfolder]/       # ✅ SCANNED RECURSIVELY (unlimited depth)
│   │           └── *.json, *.html, *.htm     # ✅ PROCESSED
│   │
│   ├── logged_information/                    # ✅ SCANNED RECURSIVELY (VERIFIED)
│   │   ├── interactions/                      # ✅ SCANNED RECURSIVELY (VERIFIED)
│   │   │   ├── recently_visited.json         # ✅ PROCESSED (VERIFIED)
│   │   │   └── recently_viewed.json          # ✅ PROCESSED (VERIFIED)
│   │   └── [any subfolder]/                  # ✅ SCANNED RECURSIVELY
│   │       └── *.json, *.html, *.htm         # ✅ PROCESSED
│   │
│   ├── [ANY OTHER FOLDER NAME]/              # ✅ SCANNED RECURSIVELY
│   │   ├── *.json                             # ✅ PROCESSED
│   │   ├── *.html                             # ✅ PROCESSED
│   │   ├── *.htm                              # ✅ PROCESSED
│   │   └── [any nested structure]/           # ✅ SCANNED RECURSIVELY (unlimited depth)
│   │       └── *.json, *.html, *.htm         # ✅ PROCESSED
│   │
│   └── [NESTED FOLDERS AT ANY DEPTH]/        # ✅ SCANNED RECURSIVELY
│       └── *.json, *.html, *.htm             # ✅ PROCESSED
│
├── [Account Folder 2]/                       # ✅ SCANNED RECURSIVELY
│   └── [Same recursive structure]            # ✅ ALL FILES PROCESSED
│
└── [Account Folder N]/                       # ✅ SCANNED RECURSIVELY
    └── [Same recursive structure]            # ✅ ALL FILES PROCESSED
```

**Key Points:**
- **Recursive scanning**: Uses `os.walk()` to traverse ALL subdirectories at ANY depth
- **No folder exclusion**: All folders are scanned except hidden directories (starting with `.`)
- **File type filtering**: Only processes files ending in `.json`, `.html`, or `.htm`
- **No path depth limit**: Processes files at any nesting level
- **Complete coverage**: Every JSON/HTML file found is processed, regardless of folder name or location

### 2.3 Files Processed

The extractor processes the following file types:

#### JSON Files
- **`comments_v2.json`**: Comments made by the user
- **`group_posts_v2.json`**: Posts in Facebook groups
- **`group_comments_v2.json`**: Comments in Facebook groups
- **`posts.json`**: User's posts
- **`recently_viewed.json`**: Recently viewed content
- **`pages.json`**: Pages followed
- **`groups.json`**: Groups joined
- **`events.json`**: Events attended
- **`comments_and_reactions.json`**: Comments and reactions

#### HTML Files
- **`*.html` / `*.htm`**: HTML-formatted Facebook data exports
  - Parsed using BeautifulSoup
  - Extracts URLs from text content
  - Extracts timestamps from `<time>` elements

### 2.4 Extraction Process

#### Step 1: File Discovery
```python
def find_facebook_files(self) -> List[Tuple[Path, str]]:
    """Find all Facebook data files (JSON and HTML) in the data directory."""
    for root, dirs, files in os.walk(self.data_dir):
        # Recursively walks through ALL subdirectories
        for file in files:
            if file.endswith((".json", ".html", ".htm")):
                # Processes EVERY matching file found
```

**Recursive Traversal Details:**
- Uses `os.walk(self.data_dir)` to traverse the entire directory tree
- **No depth limit**: Processes files at any nesting level (1 level, 10 levels, 100 levels deep)
- **No folder filtering**: All folders are scanned except hidden directories (starting with `.`)
- **File type matching**: Identifies all files ending in `.json`, `.html`, or `.htm`
- **Complete coverage**: Every matching file found anywhere in the directory tree is processed
- Maps each file to its source directory (account folder) using `get_source_directory()`

**Example:**
If a file exists at:
```
Facebook/account_123/nested/deeply/in/many/folders/file.json
```
It **WILL** be found and processed, regardless of how deeply nested it is.

#### Step 2: JSON Processing
```python
def process_json_file(self, file_path: Path, source_dir: str) -> List[Dict]:
```

**Handles multiple JSON structures:**

1. **V2 Structures** (modern Facebook exports):
   - `comments_v2`
   - `group_posts_v2`
   - `group_comments_v2`

2. **Legacy Structures**:
   - `posts`
   - `recently_viewed`
   - `pages`
   - `groups`
   - `events`
   - `comments_and_reactions`

**Content Extraction Logic:**
- Extracts text from `post`, `comment`, `text` fields
- Handles nested `data` arrays
- Extracts from `attachments` → `data` → `text`
- Extracts from `media` → `description` / `title`
- Extracts timestamps from `timestamp` field

#### Step 3: HTML Processing
```python
def process_html_file(self, file_path: Path, source_dir: str) -> List[Dict]:
```

- Parses HTML using BeautifulSoup
- Extracts all text content
- Uses regex pattern to find URLs: `r'https?://[^\s<>"{}|\\^`\[\]]+'`
- Extracts timestamps from `<time datetime="...">` elements

#### Step 4: URL Filtering

**Skipped Domains:**
- `facebook.com`
- `fb.com`
- `facebook.dk`
- `fbcdn.net`
- `messenger.com`
- `workplace.com`
- `instagram.com` (kept as requested)
- Generic search URLs (`google.com/search`, `bing.com/search`)

#### Step 5: Source Tracking

Each extracted URL includes metadata:
- **`source_directory`**: The Facebook account folder name (e.g., `facebook_4477g`)
- **`source_file`**: The specific file name (e.g., `comments.json`)
- **`timestamp`**: Original Facebook timestamp (if available)
- **`extracted_at`**: Extraction timestamp

### 2.5 Output Format

**Output File:** `extracted_urls_facebook.csv`

**Columns:**
- `url`: Original URL extracted
- `resolved_url`: Empty initially (filled in resolution step)
- `domain`: Extracted domain (www. prefix removed)
- `content`: Empty initially (filled in scraping step)
- `source_directory`: Facebook account folder
- `source_file`: Source file name
- `timestamp`: Facebook activity timestamp
- `extracted_at`: Extraction timestamp

### 2.6 Deduplication

- URLs are deduplicated at extraction time using a set (`self.extracted_urls`)
- Final DataFrame removes duplicates based on `url` column
- Results sorted by `domain` and `url`

---

## 3. Browser URL Extraction

### 3.1 Overview

The browser URL extraction process (`pipeline/1_extract_urls/browser.py`) extracts URLs from browser history files (Safari SQLite databases and Chrome/other JSON exports).

### 3.2 Source Data Structure

Browser data donations contain:

```
data/Samlet_06112025/Browser/
├── *.db                    # Safari history databases (SQLite)
├── *.json                  # Chrome/other browser history exports
└── ...
```

### 3.3 Files Processed

#### SQLite Files (`.db`)
- **Safari History Databases**
  - Tables: `history_items`, `history_visits`
  - Extracts: URL, title, visit time, visit count
  - Converts WebKit timestamps (seconds since 2001-01-01) to datetime

#### JSON Files (`.json`)
- **Chrome History**
  - Structure: `{"Browser History": [...]}` or `{"history": [...]}`
  - Extracts: URL, title, visit time
  - Converts Chrome timestamps (milliseconds since 1970-01-01) to datetime

- **Other Browser Formats**
  - Handles various JSON structures
  - Processes in chunks (5000 records) for large files

### 3.4 Extraction Process

#### Step 1: File Discovery
```python
sqlite_files = list(self.browser_dir.glob("*.db"))
json_files = list(self.browser_dir.glob("*.json"))
```

#### Step 2: SQLite Processing
```python
def process_sqlite_file(self, file_path: Path) -> int:
```

**Query:**
```sql
SELECT 
    history_items.url,
    history_visits.title,
    history_visits.visit_time,
    history_items.visit_count
FROM history_items
JOIN history_visits ON history_items.id = history_visits.history_item
ORDER BY history_visits.visit_time DESC
```

- Validates database structure
- Handles empty databases
- Converts WebKit timestamps to datetime

#### Step 3: JSON Processing
```python
def process_json_file(self, file_path: Path) -> int:
```

- Handles multiple JSON structures
- Processes in chunks (5000 records) for memory efficiency
- Converts timestamps (Chrome/WebKit)
- Performs garbage collection after each chunk

#### Step 4: URL Cleaning

**URL Normalization:**
- Decodes URL encoding (`unquote`)
- Removes fragments (`#...`)
- Removes tracking parameters:
  - `utm_source`, `utm_medium`, `utm_campaign`, `utm_content`, `utm_term`
  - `fbclid`, `gclid`, `_ga`, `_gl`
  - `mc_cid`, `mc_eid`, `ref`, `referrer`, `source`

**Domain Extraction:**
- Parses URL using `urlparse`
- Removes `www.` prefix
- Converts to lowercase

#### Step 5: Generic URL Filtering

**Skipped Domains:**
- `google.com`, `google.dk`, `google.co.uk`
- `bing.com`, `yahoo.com`, `duckduckgo.com`
- `facebook.com`, `instagram.com`, `twitter.com`, `linkedin.com`
- `youtube.com`, `youtu.be`
- `login.microsoftonline.com`, `accounts.google.com`
- `localhost`, `127.0.0.1`, `file://`

**Skipped Patterns:**
- `/search?`, `/search/`
- `q=`, `query=`
- `login`, `signin`, `signup`, `auth`, `logout`, `signout`
- `redirect`, `callback`, `oauth`

#### Step 6: Metadata Aggregation

For each unique URL:
- **`first_seen`**: Earliest visit timestamp
- **`last_seen`**: Latest visit timestamp
- **`visit_count`**: Total number of visits
- **`title`**: Page title (best available)
- **`source_files`**: List of source files containing this URL

### 3.5 Output Format

**Output File:** `extracted_urls_optimized.csv`

**Columns:**
- `url`: Cleaned, normalized URL
- `resolved_url`: Empty initially
- `domain`: Extracted domain
- `content`: Empty initially
- `title`: Page title
- `first_seen`: Earliest visit datetime
- `last_seen`: Latest visit datetime
- `visit_count`: Number of visits
- `source_files`: Semicolon-separated list of source files

### 3.6 Memory Optimization

- **Batch Processing**: JSON files processed in batches (default: 15 files)
- **Garbage Collection**: Forced after each batch and SQLite file
- **Chunked Processing**: Large JSON files processed in 5000-record chunks
- **Memory Monitoring**: Tracks memory usage during processing

---

## 4. URL Resolution

### 4.1 Overview

The URL resolution process (`pipeline/2_resolve_urls/resolver.py`) follows redirects and resolves shortlinks to their final destinations.

### 4.2 Resolution Process

#### Step 1: Cache Check
- SQLite cache database (`url_resolution_cache.db`)
- Stores: original_url, resolved_url, status_code, redirect_count, success, error, response_time
- Prevents re-resolving known URLs

#### Step 2: Dead Links Cache
- Text file (`dead_links_cache.txt`)
- Skips URLs that previously failed
- Reduces unnecessary network requests

#### Step 3: HTTP Redirect Resolution
```python
def resolve_single_url(self, url: str) -> Dict:
```

**Handles:**
- HTTP redirects: `301`, `302`, `303`, `307`, `308`
- Meta refresh redirects: `<meta http-equiv="refresh" content="URL=...">`
- Maximum redirects: 10 (configurable)
- Timeout: 3-15 seconds (configurable)

**Process:**
1. Check dead links cache
2. Check SQLite cache
3. Make HTTP request with `allow_redirects=False`
4. Follow Location header for HTTP redirects
5. Parse HTML for meta refresh redirects
6. Repeat until no more redirects or max redirects reached
7. Save result to cache

#### Step 4: SSL Error Handling
- Retries without SSL verification for problematic sites
- Logs SSL errors
- Adds to dead links if retry fails

### 4.3 Output Format

**Output Files:** `resolved_browser_batch_*.csv`, `resolved_facebook_batch_*.csv`

**Columns:**
- `original_url`: Original URL from extraction
- `resolved_url`: Final URL after following redirects
- `resolution_success`: Boolean (HTTP request succeeded)
- `actually_resolved`: Boolean (URL changed during resolution)
- `redirect_count`: Number of redirects followed
- `source`: `browser` or `facebook`
- `domain`: Domain of resolved URL
- `error`: Error message (if any)
- `response_time`: Time taken to resolve (seconds)

### 4.4 Batch Processing

- Processes URLs in batches (default: 1000 URLs per batch)
- Saves results incrementally to disk
- Parallel processing with ThreadPoolExecutor (default: 40 workers)
- Progress logging every 100 URLs

---

## 5. News Source Classification

### 5.1 Overview

The news classification system (`core/news_analysis.py`) categorizes domains as **mainstream**, **alternative**, or **other** based on predefined domain lists.

### 5.2 Classification Lists

#### Alternative News Sources

**Danish:**
- `180grader.dk`, `24nyt.dk`, `arbejderen.dk`, `denkorteavis.dk`
- `dkdox.tv`, `document.dk`, `folkets.dk`, `frihedensstemme.dk`
- `indblik.dk`, `konfront.dk`, `kontrast.dk`, `newspeek.info`
- `nordfront.dk`, `piopio.dk`, `redox.dk`, `sameksistens.com`
- `solidaritet.dk`, `psst-nyt.dk`, `dagensblaeser.net`
- `danmarksfriefjernsyn.dk`, `denuafhaengige.dk`, `freeobserver.org`
- `tv.frihedensstemme.dk`, `frihedsbrevet.dk`, `indblik.net`, `responsmedie.dk`

**Swedish:**
- `aktuelltfokus.se`, `arbetaren.se`, `bubb.la`, `bulletin.nu`
- `detgodasamhallet.com`, `direktaktion.nu`, `epochtimes.se`
- `exakt24.se`, `feministisktperspektiv.se`, `flamman.se`
- `folkungen.se`, `friasidor.is`, `friatider.se`, `ledarsidorna.se`
- `nationalisten.se`, `newsvoice.se`, `nordfront.se`, `nyadagbladet.se`
- `nyatider.nu`, `nyheteridag.se`, `nyhetsbyran.org`, `proletaren.se`
- `svegot.se`, `riks.se`, `samnytt.se`, `samtiden.nu`
- `tidningensyre.se`, `vaken.se`

**Austrian:**
- `addendum.org`, `allesroger.at`, `alpenschau.com`, `anschlaege.at`
- `auf1.tv`, `contra-magazin.com`, `info-direkt.eu`, `kontrast.at`
- `moment.at`, `mosaik-blog.at`, `neuezeit.at`, `report24.news`
- `tagesstimme.com`, `unser-mitteleuropa.com`, `unsere-zeitung.at`
- `unzensuriert.at`, `volksstimme.at`, `wochenblick.at`, `zackzack.at`, `zurzeit.at`

**German:**
- Extensive list including `achgut.com`, `afd-kompakt.de`, `alternative-presse.de`
- And many more German alternative sources

#### Mainstream News Sources

**Danish:**
- `dr.dk`, `tv2.dk`, `politiken.dk`, `berlingske.dk`
- `jyllands-posten.dk`, `information.dk`, `ekstrabladet.dk`
- `bt.dk`, `b.dk`, `altinget.dk`, `finans.dk`
- `weekendavisen.dk`, `kristeligt-dagblad.dk`, `jv.dk`
- `fyens.dk`, `nordjyske.dk`, `jv.dk`, `sn.dk`
- `amtsavisen.dk`, `lokavisen.dk`, `fagbladet3f.dk`
- `ing.dk`, `ing.dk`, `ing.dk`, `ing.dk`

**Swedish:**
- `svt.se`, `tv4.se`, `aftonbladet.se`, `expressen.se`
- `dn.se`, `svd.se`, `gp.se`, `sydsvenskan.se`
- `hd.se`, `nt.se`, `corren.se`, `barometern.se`
- `folkbladet.se`, `helahalsingland.se`, `kuriren.nu`
- `lt.se`, `vlt.se`, `nsk.se`, `op.se`
- `ostrasmaland.se`, `smp.se`, `vlt.se`, `vlt.se`

**International:**
- `bbc.com`, `bbc.co.uk`, `cnn.com`, `reuters.com`
- `theguardian.com`, `nytimes.com`, `washingtonpost.com`
- `lemonde.fr`, `spiegel.de`, `zeit.de`, `faz.net`
- `welt.de`, `sueddeutsche.de`, `tagesspiegel.de`
- And many more international mainstream sources

### 5.3 Classification Function

```python
def classify_news_source(domain: str) -> str:
    """Classify a domain as alternative, mainstream, or other."""
    if domain in alternative_news_sources:
        return "alternative"
    if domain in mainstream_news_sources:
        return "mainstream"
    return "other"
```

### 5.4 Analysis Function

```python
def analyze_news_sources(df: pd.DataFrame) -> Dict[str, Any]:
    """Analyze news sources in the DataFrame."""
```

**Returns:**
- `classification_summary`: Counts of alternative, mainstream, other
- `alternative_sources`: Dictionary of alternative domains and counts
- `mainstream_sources`: Dictionary of mainstream domains and counts

---

## 6. Activity Span Analysis

### 6.1 Browser Activity Span

**Metrics Calculated:**
- `earliest_visit`: Minimum `first_seen` timestamp
- `latest_visit`: Maximum `last_seen` timestamp
- `activity_span_days`: Difference in days between earliest and latest visit
- `total_visits`: Sum of `visit_count` for all URLs
- `unique_domains`: Number of unique domains visited
- `mainstream_news_visits`: Count of visits to mainstream news domains
- `alternative_news_visits`: Count of visits to alternative news domains

### 6.2 Facebook Activity Span

**Metrics Calculated:**
- `earliest_activity`: Minimum timestamp from posts/comments
- `latest_activity`: Maximum timestamp from posts/comments
- `activity_span_days`: Difference in days
- `total_activities`: Total number of posts/comments
- `unique_domains`: Number of unique domains in shared URLs
- `mainstream_news_shares`: Count of URLs from mainstream news domains
- `alternative_news_shares`: Count of URLs from alternative news domains

### 6.3 Implementation

**Browser:**
```python
def analyze_browser_data(df: pd.DataFrame) -> Dict:
    metrics = {
        "earliest_visit": df["visit_datetime"].min(),
        "latest_visit": df["visit_datetime"].max(),
        "activity_span_days": (df["visit_datetime"].max() - df["visit_datetime"].min()).days,
        ...
    }
```

**Facebook:**
```python
def analyze_account_activity(folder_path: str) -> Optional[Dict[str, Any]]:
    metrics = {
        "earliest_activity": min(timestamps),
        "latest_activity": max(timestamps),
        "activity_span_days": (max(timestamps) - min(timestamps)).days,
        ...
    }
```

---

## 7. Data Processing Pipeline

### 7.1 Merge Process

**Script:** `src/pipeline/process/merge.py`

**Steps:**
1. Load old browser data
2. Load old Facebook data
3. Load new browser data
4. Load new Facebook data
5. Unify schema (add missing columns, rename columns)
6. Add `source` column (`old_browser`, `old_facebook`, `new_browser`, `new_facebook`)
7. Merge all datasets
8. Remove duplicates based on URL
9. Save to `all_scraped_content_merged_final.csv` and `.parquet`

### 7.2 Enrichment Process

**Script:** `src/pipeline/process/enrich.py`

**Steps:**
1. Load scraped content
2. Load resolved URLs
3. Merge resolved URLs into scraped content
4. Add `resolved_url` and `domain` columns
5. Handle parsing errors in resolution files

### 7.3 Combine Process

**Script:** `src/pipeline/process/combine.py`

**Steps:**
1. Load batch files (`scraped_batch_*.csv`)
2. Combine into single DataFrame
3. Add `source` column (`browser` or `facebook`)
4. Save to Parquet and CSV

---

## 8. Technical Specifications

### 8.1 Dependencies

**Python Libraries:**
- `pandas`: Data manipulation and analysis
- `numpy`: Numerical operations
- `requests`: HTTP requests for URL resolution
- `beautifulsoup4`: HTML parsing
- `sqlite3`: Database operations (built-in)
- `concurrent.futures`: Parallel processing
- `pathlib`: File path handling
- `urllib.parse`: URL parsing
- `re`: Regular expressions
- `json`: JSON parsing
- `logging`: Logging framework
- `datetime`: Date/time handling

### 8.2 Performance Characteristics

**Facebook Extraction:**
- Processes ~100-1000 files per account folder
- Memory-efficient: Processes one file at a time
- Time complexity: O(n) where n = number of files

**Browser Extraction:**
- Batch processing: 15 files per batch
- Chunked JSON processing: 5000 records per chunk
- Memory monitoring and garbage collection
- Time complexity: O(n) where n = number of records

**URL Resolution:**
- Parallel processing: 40 workers (configurable)
- Caching: SQLite database + dead links file
- Batch saving: 1000 URLs per batch
- Time complexity: O(n) where n = number of URLs (with caching)

### 8.3 Error Handling

**Facebook Extraction:**
- Skips invalid JSON files (logs error)
- Handles missing fields gracefully
- Continues processing on individual file errors

**Browser Extraction:**
- Validates SQLite database structure
- Handles empty databases
- Handles malformed JSON (logs error, continues)
- Memory error handling with garbage collection

**URL Resolution:**
- Dead links cache prevents retries
- SSL error handling with retry without verification
- Timeout handling (3-15 seconds)
- Error logging for failed resolutions

---

## 9. Output Files and Data Flow

### 9.1 Extraction Outputs

**Facebook:**
- `data/url_extract_facebook/extracted_urls_facebook.csv`
- `data/url_extract_facebook/extraction_summary.txt`
- `data/url_extract_facebook/facebook_url_extraction_*.log`

**Browser:**
- `data/url_extract/extracted_urls_optimized.csv`

### 9.2 Resolution Outputs

- `data/complete_resolved/resolved_browser_batch_*.csv`
- `data/complete_resolved/resolved_facebook_batch_*.csv`
- `data/complete_resolved/url_resolution_cache.db`
- `data/complete_resolved/dead_links_cache.txt`
- `data/complete_resolved/complete_pipeline_*.log`

### 9.3 Final Merged Outputs

- `data/merged_all/all_scraped_content_merged_final.csv`
- `data/merged_all/all_scraped_content_merged_final.parquet`

**Schema:**
- `url`: Original URL
- `resolved_url`: Resolved URL (after redirects)
- `domain`: Domain name
- `content`: Scraped content text
- `content_hash`: SHA256 hash of content
- `word_count`: Number of words in content
- `status_code`: HTTP status code
- `response_time`: Response time in seconds
- `error`: Error message (if any)
- `scraped_at`: Scraping timestamp
- `title`: Page title
- `author`: Article author (if extracted)
- `date`: Publication date (if extracted)
- `source`: Data source (`old_browser`, `old_facebook`, `new_browser`, `new_facebook`)

---

## 10. Verification and Quality Assurance

### 10.1 Data Verification Steps

1. **File Count Verification**: Counts processed files vs. total files
2. **URL Deduplication**: Ensures unique URLs in final dataset
3. **Schema Validation**: Ensures consistent columns across datasets
4. **Domain Extraction**: Validates domain parsing
5. **Timestamp Validation**: Validates date/time conversions
6. **Content Hash Verification**: Ensures content integrity

### 10.2 Quality Metrics

- **Extraction Success Rate**: URLs extracted / Total URLs found
- **Resolution Success Rate**: Successfully resolved / Total URLs
- **Scraping Success Rate**: URLs with content / Total URLs scraped
- **Deduplication Rate**: Unique URLs / Total URLs before deduplication

### 10.3 Logging and Monitoring

- All stages log to timestamped log files
- Progress logging every 100 files/URLs
- Error logging with full tracebacks
- Summary statistics at completion

---

## 11. Conclusion

This pipeline provides a robust, scalable solution for processing data donations containing browser history and Facebook activity. The modular design allows for independent execution of each stage, with comprehensive error handling and logging throughout. The classification system enables analysis of news consumption patterns, distinguishing between mainstream and alternative news sources, while activity span analysis provides temporal insights into user behavior.

---

## Appendix A: Facebook Data Source Folders and Files

### A.1 Complete Folder Structure (Verified from Actual VM Data)

**IMPORTANT:** The Facebook URL extraction process uses **`os.walk()`** to perform **recursive directory traversal**. This means it scans **ALL** JSON and HTML files at **ANY depth** within the input directory, regardless of folder names or nesting levels.

**Verified Structure from `/work/Datadonationer/urlextraction_scraping/data/new_data_251126/Facebook/`:**
- **32 account folders** (as of December 2025)
- Each account folder contains nested subfolders with JSON files
- All files are processed recursively

**Recursive Scanning Behavior:**
- **No folder exclusion**: All folders are scanned except hidden directories (starting with `.`)
- **Unlimited depth**: Files at any nesting level are found and processed
- **Complete coverage**: Every `.json`, `.html`, and `.htm` file found anywhere in the directory tree is processed
- **No path filtering**: Folder names are not filtered - any folder containing matching files is processed

Below is the complete structure of folders and files that are processed:

```
data/Kantar_download_398_unzipped_new/Facebook/
│
├── [Account Folder 1]/                    # e.g., "facebook_4477g", "facebook_ju5", "facebook_uu5"
│   │
│   ├── your_facebook_activity/           # Main activity folder
│   │   │
│   │   ├── posts_and_comments/           # Posts and comments data
│   │   │   ├── comments.json             # ✅ PROCESSED
│   │   │   ├── posts.json                # ✅ PROCESSED
│   │   │   └── comments_v2.json          # ✅ PROCESSED (if exists)
│   │   │
│   │   ├── recently_viewed/              # Recently viewed content
│   │   │   ├── recently_viewed.json    # ✅ PROCESSED
│   │   │   └── ...
│   │   │
│   │   ├── pages_you_follow/             # Followed pages
│   │   │   ├── pages.json                # ✅ PROCESSED
│   │   │   └── ...
│   │   │
│   │   ├── groups/                       # Group memberships
│   │   │   ├── group_posts_v2.json       # ✅ PROCESSED
│   │   │   ├── group_comments_v2.json    # ✅ PROCESSED
│   │   │   └── groups.json                # ✅ PROCESSED
│   │   │
│   │   ├── events/                       # Events data
│   │   │   ├── events.json               # ✅ PROCESSED
│   │   │   └── ...
│   │   │
│   │   ├── comments_and_reactions/       # Comments and reactions
│   │   │   ├── comments_and_reactions.json  # ✅ PROCESSED
│   │   │   └── ...
│   │   │
│   │   └── [other activity folders]/     # Any other folders
│   │       └── *.json                    # ✅ PROCESSED (all JSON files)
│   │
│   ├── logged_information/                # Login/logout information
│   │   ├── *.json                        # ✅ PROCESSED
│   │   └── *.html                        # ✅ PROCESSED
│   │
│   ├── [Other Facebook folders]/         # Any other folders
│   │   ├── *.json                        # ✅ PROCESSED
│   │   ├── *.html                        # ✅ PROCESSED
│   │   └── *.htm                         # ✅ PROCESSED
│   │
│   └── [Nested subdirectories]/          # Any nested structure
│       └── *.json, *.html, *.htm         # ✅ PROCESSED (recursively)
│
└── [Account Folder 2]/
    └── [Same structure as above]
```

### A.2 Specific JSON File Types Processed

The extractor handles the following JSON file structures:

#### V2 Structures (Modern Facebook Exports)
1. **`comments_v2.json`**
   - Structure: `{"comments_v2": [...]}`
   - Content: User's comments on posts
   - Fields extracted: `post`, `comment`, `text`, `timestamp`, `attachments`

2. **`group_posts_v2.json`**
   - Structure: `{"group_posts_v2": [...]}`
   - Content: Posts made in Facebook groups
   - Fields extracted: `post`, `text`, `timestamp`, `attachments`

3. **`group_comments_v2.json`**
   - Structure: `{"group_comments_v2": [...]}`
   - Content: Comments made in Facebook groups
   - Fields extracted: `comment`, `text`, `timestamp`, `attachments`

#### Legacy Structures
4. **`posts.json`**
   - Structure: `{"posts": [...]}` or `[...]`
   - Content: User's own posts
   - Fields extracted: `post`, `text`, `title`, `timestamp`

5. **`comments.json`**
   - Structure: `{"comments": [...]}` or `[...]`
   - Content: User's comments
   - Fields extracted: `comment`, `text`, `timestamp`

6. **`recently_viewed.json`**
   - Structure: `{"recently_viewed": [...]}` or `[...]`
   - Content: Recently viewed content
   - Fields extracted: `title`, `text`, `timestamp`

7. **`pages.json`**
   - Structure: `{"pages": [...]}` or `[...]`
   - Content: Pages followed by user
   - Fields extracted: `title`, `text`, `timestamp`

8. **`groups.json`**
   - Structure: `{"groups": [...]}` or `[...]`
   - Content: Groups joined by user
   - Fields extracted: `title`, `text`, `timestamp`

9. **`events.json`**
   - Structure: `{"events": [...]}` or `[...]`
   - Content: Events attended/interested
   - Fields extracted: `title`, `text`, `timestamp`

10. **`comments_and_reactions.json`**
    - Structure: `{"comments_and_reactions": [...]}` or `[...]`
    - Content: Comments and reactions
    - Fields extracted: `comment`, `text`, `timestamp`, `attachments`

### A.3 HTML File Processing

All HTML files (`.html`, `.htm`) are processed:
- **Location**: Any folder within the Facebook data directory
- **Processing**: Parsed using BeautifulSoup
- **Extraction**: 
  - All text content scanned for URLs using regex
  - Timestamps extracted from `<time datetime="...">` elements
  - Account name extracted from `<aside role="contentinfo">` elements

### A.4 Source Directory Identification

The `get_source_directory()` function identifies the Facebook account folder by walking up the directory tree from each file's location.

**Algorithm:**
```python
def get_source_directory(self, file_path: Path) -> str:
    current = file_path.parent
    while current != self.data_dir and current != current.parent:
        dir_name = current.name
        # Check if this looks like a Facebook account directory
        if "facebook" in dir_name.lower() or any(
            pattern in dir_name for pattern in ["4477g", "ju5", "uu5"]
        ):
            return dir_name
        current = current.parent
    # If not found, return the immediate parent directory
    return file_path.parent.name
```

**Identification Rules:**
1. Starts at the file's immediate parent directory
2. Walks up the directory tree toward the input directory root
3. Looks for directory names containing:
   - `"facebook"` (case-insensitive match)
   - Patterns: `"4477g"`, `"ju5"`, `"uu5"` (common account identifiers)
4. Returns the **first matching directory name** found
5. If no match found, returns the immediate parent directory name as fallback

**Example Source Directory Names (Actual from VM):**
- `4477g1748422889834sOEA05YM50hju5966uu5966ufacebookdinesan1990280520257p1hm4nc-xx8Lxda` (full account folder name)
- `4477g1748444409852s9Ex8qp5Rmtju6033uu6033ufacebookbirthetangpetersen280520253nI4V1Nd-8v0kupR`
- `your_facebook_activity` (subfolder name, used as fallback when account folder not detected)
- Any folder name containing "facebook" (case-insensitive) or patterns "4477g", "ju5", "uu5"

**Note:** Account folders have very long encoded names containing patterns like:
- `4477g...` (account identifier)
- `ju5...` or `ju6033...` (user identifier)
- `uu5...` or `uu6033...` (user identifier)
- `facebook...` (Facebook identifier)

**Example Paths and Source Directory Assignment (Actual from VM):**

| File Path | Source Directory Assigned |
|-----------|---------------------------|
| `Facebook/4477g1748422889834s.../your_facebook_activity/posts/your_posts__check_ins__photos_and_videos_1.json` | `your_facebook_activity` (if account folder not detected) or account folder name |
| `Facebook/4477g1748444409852s.../your_facebook_activity/groups/your_comments_in_groups.json` | Account folder name or `your_facebook_activity` |
| `Facebook/4477g1748862082907s.../your_facebook_activity/comments_and_reactions/comments.json` | Account folder name or `your_facebook_activity` |
| `Facebook/4477g1748893035872s.../logged_information/interactions/recently_viewed.json` | Account folder name or `logged_information` |

**Actual Example from Extracted Data:**
```
url: http://www.101greatgoals.com/videodisplay/...
source_directory: your_facebook_activity
source_file: your_posts__check_ins__photos_and_videos_1.json
```

**Note:** The source directory detection walks up the tree looking for folders containing "facebook", "4477g", "ju5", or "uu5". If found, that folder name is used. Otherwise, the immediate parent folder name is used as fallback.

**Important:** The `source_directory` field in the output CSV preserves the account folder name, allowing traceability of which Facebook account each URL originated from.

### A.5 Files Excluded from Processing

**Skipped:**
- Hidden directories (starting with `.`)
- Non-JSON/HTML files
- System files

**Filtered URLs (after extraction):**
- Facebook domains: `facebook.com`, `fb.com`, `facebook.dk`, `fbcdn.net`, `messenger.com`, `workplace.com`
- Generic search URLs: `google.com/search`, `bing.com/search`
- Instagram URLs are **kept** (as requested)

### A.6 Extraction Metadata Preserved

For each extracted URL, the following source information is preserved:

**`source_directory`**: The Facebook account folder name
- Examples: `facebook_4477g`, `facebook_ju5`, `your_facebook_activity`

**`source_file`**: The specific file name
- Examples: `comments.json`, `posts.json`, `group_posts_v2.json`, `index.html`

**`timestamp`**: Original Facebook activity timestamp (if available)
- Format: Unix timestamp or ISO datetime string
- Source: `timestamp` field in JSON or `<time datetime>` in HTML

**`extracted_at`**: Extraction timestamp
- Format: ISO datetime string
- Example: `2025-12-12T10:30:45.123456`

### A.7 Processing Statistics

The extraction process logs comprehensive statistics including:

**File-Level Statistics:**
- Total files found (all `.json`, `.html`, `.htm` files recursively)
- Files processed successfully
- Files with errors (logged with error details)
- Progress logging every 100 files

**URL-Level Statistics:**
- Total URLs extracted (before deduplication)
- Unique URLs (after deduplication)
- Unique domains
- URLs per source directory
- URLs per source file

**Source Tracking Statistics:**
- Number of unique source directories processed
- Number of unique source files processed
- Top source directories by URL count
- Top source files by URL count

**Example Log Output:**
```
Starting Facebook URL extraction...
Found 1,234 files to process
Processing: comments.json from facebook_4477g
Processing: posts.json from facebook_4477g
Processing: group_posts_v2.json from facebook_ju5
...
Processed 100 files, extracted 3,456 URLs
Processed 200 files, extracted 7,890 URLs
...
Processed 1,234 files, extracted 45,678 URLs
Extraction completed!
Total files processed: 1,234
Total unique URLs extracted: 32,456
Unique domains: 1,234
Source directories: 398
Source files: 1,234
```

**Summary File Output:**
The extraction also generates `extraction_summary.txt` containing:
- Extraction date and time
- Total URLs extracted
- Unique domains count
- Number of source directories
- Number of source files
- Top 10 domains by URL count

### A.8 Traceability and Verification

**Complete Traceability:**
Every extracted URL in the output CSV (`extracted_urls_facebook.csv`) includes:
1. **`source_directory`**: The Facebook account folder name (e.g., `facebook_4477g`)
2. **`source_file`**: The exact file name where the URL was found (e.g., `comments.json`)
3. **`timestamp`**: The original Facebook activity timestamp (if available)
4. **`extracted_at`**: The exact time when the URL was extracted

This allows complete traceability:
- **Which account** the URL came from (`source_directory`)
- **Which file** the URL was found in (`source_file`)
- **When** the activity occurred (`timestamp`)
- **When** it was extracted (`extracted_at`)

**Verification Example:**
To verify where a specific URL came from:
```python
import pandas as pd
df = pd.read_csv('extracted_urls_facebook.csv')
url_info = df[df['url'] == 'https://example.com/article']
print(url_info[['url', 'source_directory', 'source_file', 'timestamp']])
```

This will show exactly which Facebook account folder and which file contained that URL.

---

## Appendix B: File Locations on VM

**Extraction Scripts:**
- `/work/Datadonationer/urlextraction_scraping/src/pipeline/extract/facebook.py`
- `/work/Datadonationer/urlextraction_scraping/src/pipeline/extract/browser.py`

**Resolution Scripts:**
- `/work/Datadonationer/urlextraction_scraping/src/pipeline/resolve/resolver.py`
- `/work/Datadonationer/urlextraction_scraping/src/pipeline/resolve/run_resolution.py`

**Processing Scripts:**
- `/work/Datadonationer/urlextraction_scraping/src/pipeline/process/merge.py`
- `/work/Datadonationer/urlextraction_scraping/src/pipeline/process/enrich.py`
- `/work/Datadonationer/urlextraction_scraping/src/pipeline/process/combine.py`

**Core Modules:**
- `/work/Datadonationer/urlextraction_scraping/core/news_analysis.py`
- `/work/Datadonationer/urlextraction_scraping/core/facebook_processing.py`
- `/work/Datadonationer/urlextraction_scraping/core/browser_processing.py`

---

*Report generated: 2025-12-12*
*Pipeline version: 1.0*

