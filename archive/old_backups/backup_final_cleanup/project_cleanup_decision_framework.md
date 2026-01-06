# Project Cleanup Decision Framework

## Executive Summary

Based on the automated analysis, this project contains:
- **96 total Python files**
- **15 active files** (confirmed in use)
- **83 potentially inactive files** (need review)
- **Multiple redundant file groups** (version conflicts)

## Decision Categories

### ✅ KEEP (Confirmed Active)
These files are confirmed to be actively used and should be preserved:

#### Core Processing Scripts
- `notebooks/Processing_browser.py` - Main browser data processing
- `notebooks/Processing_facebook_batch_analysis.py` - Facebook batch processing
- `notebooks/Processing_facebook_news_analysis.py` - Facebook news analysis

#### URL Extraction Pipeline
- `notebooks/url_extraction/step1_extract_urls_optimized.py`
- `notebooks/url_extraction/step2_analyze_urls.py`
- `notebooks/url_extraction/step3_prioritize_domains.py`
- `notebooks/url_extraction/step4_scrape_content.py`
- `notebooks/url_extraction/step4_scrape_content_with_resolution.py`

#### Facebook URL Extraction
- `notebooks/url_extraction_facebook/step4_scrape_content_facebook_with_resolution.py`

#### Root Level Active Scripts
- `browser_content_scraper.py` - Content scraping functionality
- `combined_url_resolution_enhanced.py` - Enhanced URL resolution
- `complete_url_resolution_pipeline.py` - Complete pipeline
- `unified_rescraping_pipeline.py` - Unified scraping pipeline

### 🗑️ SAFE TO DELETE (High Confidence)

#### Redundant Version Files
These are clearly older versions with newer alternatives:

**Language Field Patches (Keep only the latest)**
- ❌ `language_field_patch.py` (base version)
- ❌ `language_field_patch_v2.py` 
- ❌ `language_field_patch_v4.py`
- ❌ `language_field_patch_v5.py`
- ✅ `language_field_patch_rework.py` (keep - appears to be latest)

**URL Resolution Pipelines (Keep only the most recent)**
- ❌ `parallel_url_resolution_pipeline_resume_181.py`
- ❌ `parallel_url_resolution_pipeline_resume_fixed.py`
- ✅ `parallel_url_resolution_pipeline.py` (keep - base version)
- ✅ `parallel_url_resolution_pipeline_resume.py` (keep - main resume version)

**Content Scrapers (Keep only the base versions)**
- ❌ `browser_content_scraper_v2.py`
- ❌ `browser_content_scraper_turbo.py`
- ✅ `browser_content_scraper.py` (keep - base version)

**Combined URL Resolution (Keep only the enhanced version)**
- ❌ `combined_url_resolution.py` (base version)
- ✅ `combined_url_resolution_enhanced.py` (keep - enhanced version)

#### Archive Files (Already archived)
- ❌ `notebooks/archive/Processing_browser copy.py`
- ❌ `notebooks/archive/Processing_facebook_batch_analysis copy.py`
- ❌ `notebooks/archive/Processing_facebook_news_analysis copy.py`
- ❌ `notebooks/archive/quick_chrome_check.py`

#### Test and Debug Files
- ❌ `test_*.py` files (multiple test files)
- ❌ `debug_*.py` files (debug scripts)
- ❌ `*_test.py` files (test scripts)

### 🔍 REVIEW REQUIRED (Medium Confidence)

#### Language Collection Scripts
These appear to be related to a different project/analysis:

**High Priority Review:**
- `actor_language_filtered_collection_optimized.py`
- `actor_language_filtered_collection_v2.py`
- `all_platforms_fixed_fast.py`
- `percentage_based_language_sampler.py`
- `technocracy_datacollection_080825.py`
- `technocracy_datacollection_080825_with_language_filter.py`

**Medium Priority Review:**
- `find_actor_language_metadata.py`
- `find_actor_language_metadata_with_logging.py`
- `fixed_language_collection.py`
- `truly_optimized_language_collection.py`

#### Instagram/Social Media Scripts
These appear to be for a different platform:

- `instagram_debug_minimal.py`
- `instagram_efficient_sampler.py`
- `instagram_test_simple.py`

#### VM and Pipeline Scripts
These need verification of current usage:

- `vm_url_resolver.py`
- `split_data_for_multiple_vms.py`
- `enhanced_mongo_sampler_with_checkpoints.py`
- `checkpoint_manager.py`

### ⚠️ POTENTIALLY USEFUL (Low Confidence)

#### Enhanced Versions
These might be improvements but need verification:

- `enhanced_content_scraper.py`
- `enhanced_url_resolver.py`
- `robust_url_resolver.py`
- `final_integration_pipeline.py`

#### Facebook Analysis Extensions
These might be useful extensions:

- `facebook_batch_analysis_with_html.py`
- `facebook_html_parser.py`
- `validate_html_facebook_data.py`
- `analyze_news_detection.py`

## Recommended Action Plan

### Phase 1: Safe Deletions (Immediate)
1. Delete redundant version files (language_field_patch_v*.py, etc.)
2. Delete archive files (already archived)
3. Delete test/debug files
4. Delete clearly outdated versions

### Phase 2: Review and Decision (Next)
1. Review language collection scripts
2. Review Instagram/social media scripts
3. Review VM-related scripts
4. Review enhanced versions

### Phase 3: Reorganization (Final)
1. Move remaining active scripts to appropriate directories
2. Update import statements and references
3. Create clear documentation
4. Establish new project structure

## Project Structure Recommendation

```
alteruse/
├── core/                          # Core processing scripts
│   ├── browser_processing.py      # Processing_browser.py
│   ├── facebook_processing.py     # Facebook processing scripts
│   └── news_analysis.py          # News analysis functionality
├── pipelines/                     # URL extraction pipelines
│   ├── browser_extraction/        # Browser URL extraction
│   ├── facebook_extraction/       # Facebook URL extraction
│   └── url_resolution/           # URL resolution scripts
├── scrapers/                      # Content scraping
│   ├── browser_scraper.py        # Browser content scraping
│   └── facebook_scraper.py       # Facebook content scraping
├── utils/                         # Utility scripts
│   ├── url_resolvers/            # URL resolution utilities
│   └── data_processing/          # Data processing utilities
├── data/                          # Data directory (keep as is)
├── scripts/                       # Shell scripts (keep as is)
├── logs/                          # Logs (keep as is)
└── reports/                       # Reports (keep as is)
```

## Risk Assessment

### Low Risk Deletions
- Redundant version files (clear newer versions exist)
- Archive files (already archived)
- Test/debug files (not part of main functionality)

### Medium Risk Deletions
- Language collection scripts (different project scope)
- Instagram scripts (different platform)
- Enhanced versions (need verification)

### High Risk Deletions
- VM-related scripts (need VM access verification)
- Pipeline scripts (need usage verification)

## Next Steps

1. **Confirm VM access** to verify VM-based scripts
2. **Review language collection scripts** to confirm they're not needed
3. **Test enhanced versions** to ensure they work correctly
4. **Execute Phase 1 deletions** (safe deletions)
5. **Plan Phase 2 review** for remaining files
6. **Implement new project structure** after cleanup
