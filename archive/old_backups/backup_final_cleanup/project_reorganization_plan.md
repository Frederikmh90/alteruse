# Project Reorganization Plan

## Current State Analysis

After the safe deletions, the project will have a cleaner structure with:
- **Core processing scripts** (3 main notebooks)
- **URL extraction pipelines** (browser and Facebook)
- **Content scraping scripts** (various versions)
- **Utility scripts** (URL resolution, etc.)
- **Data directories** (Kantar datasets and processed outputs)

## Proposed New Structure

```
alteruse/
├── core/                          # Core data processing
│   ├── __init__.py
│   ├── browser_processing.py      # Processing_browser.py
│   ├── facebook_processing.py     # Processing_facebook_batch_analysis.py
│   └── news_analysis.py          # Processing_facebook_news_analysis.py
├── pipelines/                     # URL extraction and resolution
│   ├── __init__.py
│   ├── browser_extraction/        # Browser URL extraction
│   │   ├── __init__.py
│   │   ├── step1_extract_urls.py
│   │   ├── step2_analyze_urls.py
│   │   ├── step3_prioritize_domains.py
│   │   └── step4_scrape_content.py
│   ├── facebook_extraction/       # Facebook URL extraction
│   │   ├── __init__.py
│   │   ├── step1_extract_urls.py
│   │   ├── step2_analyze_urls.py
│   │   ├── step3_prioritize_domains.py
│   │   └── step4_scrape_content.py
│   └── url_resolution/           # URL resolution scripts
│       ├── __init__.py
│       ├── enhanced_resolver.py
│       ├── complete_pipeline.py
│       └── unified_pipeline.py
├── scrapers/                      # Content scraping
│   ├── __init__.py
│   ├── browser_scraper.py        # browser_content_scraper.py
│   └── facebook_scraper.py       # Facebook-specific scraping
├── utils/                         # Utility scripts
│   ├── __init__.py
│   ├── url_resolvers/            # URL resolution utilities
│   │   ├── __init__.py
│   │   ├── robust_resolver.py
│   │   └── vm_resolver.py
│   └── data_processing/          # Data processing utilities
│       ├── __init__.py
│       └── checkpoint_manager.py
├── data/                          # Data directory (keep as is)
│   ├── Kantar_download_*/         # Raw Kantar data
│   ├── Analyzed_Kantar_*/         # Processed Kantar data
│   ├── browser_urlextract/        # Browser URL extraction results
│   ├── facebook_urlextract/       # Facebook URL extraction results
│   ├── url_extract/               # URL extraction results
│   ├── url_extract_facebook/      # Facebook URL extraction results
│   └── processed/                 # Final processed data
├── scripts/                       # Shell scripts (keep as is)
│   ├── run_pipeline.sh
│   ├── run_facebook_pipeline.sh
│   ├── monitor_pipeline.sh
│   └── vm_setup_guide.md
├── logs/                          # Logs (keep as is)
├── reports/                       # Reports (keep as is)
├── requirements.txt               # Dependencies
├── README.md                      # Project documentation
└── setup.py                       # Package setup (new)
```

## Migration Steps

### Step 1: Create New Directory Structure
```bash
mkdir -p core pipelines/browser_extraction pipelines/facebook_extraction pipelines/url_resolution scrapers utils/url_resolvers utils/data_processing
```

### Step 2: Move Core Processing Scripts
```bash
# Move core processing scripts
mv notebooks/Processing_browser.py core/browser_processing.py
mv notebooks/Processing_facebook_batch_analysis.py core/facebook_processing.py
mv notebooks/Processing_facebook_news_analysis.py core/news_analysis.py
```

### Step 3: Move URL Extraction Pipelines
```bash
# Move browser extraction scripts
mv notebooks/url_extraction/step1_extract_urls_optimized.py pipelines/browser_extraction/step1_extract_urls.py
mv notebooks/url_extraction/step2_analyze_urls.py pipelines/browser_extraction/step2_analyze_urls.py
mv notebooks/url_extraction/step3_prioritize_domains.py pipelines/browser_extraction/step3_prioritize_domains.py
mv notebooks/url_extraction/step4_scrape_content.py pipelines/browser_extraction/step4_scrape_content.py
mv notebooks/url_extraction/step4_scrape_content_with_resolution.py pipelines/browser_extraction/step4_scrape_content_with_resolution.py

# Move Facebook extraction scripts
mv notebooks/url_extraction_facebook/step1_extract_urls_facebook.py pipelines/facebook_extraction/step1_extract_urls.py
mv notebooks/url_extraction_facebook/step2_analyze_urls_facebook.py pipelines/facebook_extraction/step2_analyze_urls.py
mv notebooks/url_extraction_facebook/step3_prioritize_domains_facebook.py pipelines/facebook_extraction/step3_prioritize_domains.py
mv notebooks/url_extraction_facebook/step4_scrape_content_facebook.py pipelines/facebook_extraction/step4_scrape_content.py
mv notebooks/url_extraction_facebook/step4_scrape_content_facebook_with_resolution.py pipelines/facebook_extraction/step4_scrape_content_with_resolution.py
```

### Step 4: Move URL Resolution Scripts
```bash
# Move URL resolution scripts
mv enhanced_url_resolver.py pipelines/url_resolution/enhanced_resolver.py
mv complete_url_resolution_pipeline.py pipelines/url_resolution/complete_pipeline.py
mv unified_rescraping_pipeline.py pipelines/url_resolution/unified_pipeline.py
```

### Step 5: Move Content Scrapers
```bash
# Move content scraping scripts
mv browser_content_scraper.py scrapers/browser_scraper.py
```

### Step 6: Move Utility Scripts
```bash
# Move utility scripts
mv robust_url_resolver.py utils/url_resolvers/robust_resolver.py
mv vm_url_resolver.py utils/url_resolvers/vm_resolver.py
mv checkpoint_manager.py utils/data_processing/checkpoint_manager.py
```

### Step 7: Create __init__.py Files
```bash
# Create __init__.py files for Python packages
touch core/__init__.py
touch pipelines/__init__.py
touch pipelines/browser_extraction/__init__.py
touch pipelines/facebook_extraction/__init__.py
touch pipelines/url_resolution/__init__.py
touch scrapers/__init__.py
touch utils/__init__.py
touch utils/url_resolvers/__init__.py
touch utils/data_processing/__init__.py
```

### Step 8: Update Import Statements
After moving files, update all import statements to reflect the new structure.

### Step 9: Update Shell Scripts
Update shell scripts to use the new file paths.

### Step 10: Create setup.py
Create a proper Python package setup for easier installation and distribution.

## Benefits of New Structure

### 1. Clear Separation of Concerns
- **Core**: Main data processing logic
- **Pipelines**: URL extraction and resolution workflows
- **Scrapers**: Content scraping functionality
- **Utils**: Reusable utility functions

### 2. Better Maintainability
- Related functionality grouped together
- Clear import paths
- Easier to find specific functionality

### 3. Improved Scalability
- Easy to add new extraction pipelines
- Modular design allows independent development
- Clear interfaces between components

### 4. Professional Structure
- Follows Python package conventions
- Proper __init__.py files
- Clear documentation structure

## Implementation Timeline

### Phase 1: Preparation (Day 1)
- [ ] Create new directory structure
- [ ] Backup current files
- [ ] Create migration script

### Phase 2: Migration (Day 2)
- [ ] Move files to new locations
- [ ] Update import statements
- [ ] Update shell scripts
- [ ] Test basic functionality

### Phase 3: Testing (Day 3)
- [ ] Run full pipeline tests
- [ ] Verify all functionality works
- [ ] Fix any import issues
- [ ] Update documentation

### Phase 4: Cleanup (Day 4)
- [ ] Remove old directories
- [ ] Update README
- [ ] Create new documentation
- [ ] Final testing

## Risk Mitigation

### Backup Strategy
- Create full backup before migration
- Keep original files until testing is complete
- Use version control for all changes

### Testing Strategy
- Test each component individually
- Run full pipeline end-to-end
- Verify data outputs are identical
- Check all shell scripts work

### Rollback Plan
- Keep original structure as backup
- Document all changes for easy rollback
- Test rollback procedure

## Post-Migration Tasks

### 1. Update Documentation
- Update README.md with new structure
- Create component-specific documentation
- Update usage examples

### 2. Create Package Setup
- Create setup.py for easy installation
- Add requirements.txt with specific versions
- Create installation instructions

### 3. Improve Testing
- Add unit tests for core components
- Create integration tests for pipelines
- Add automated testing workflow

### 4. Performance Optimization
- Profile performance bottlenecks
- Optimize slow components
- Add caching where appropriate

## Success Criteria

### Functional Requirements
- [ ] All existing functionality preserved
- [ ] All shell scripts work correctly
- [ ] All import statements resolved
- [ ] No broken dependencies

### Quality Requirements
- [ ] Clear, logical structure
- [ ] Easy to navigate and understand
- [ ] Professional appearance
- [ ] Well-documented

### Performance Requirements
- [ ] No performance degradation
- [ ] Faster development workflow
- [ ] Easier maintenance
- [ ] Better scalability
