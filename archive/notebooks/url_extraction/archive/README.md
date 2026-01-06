# URL Extraction Archive
*Archived files from URL extraction development*

## Test Files
- `test_bulk_processing.py` - Test script for isolating bulk JSON processing issues
- `test_json_processing.py` - Test script for debugging individual JSON file processing  
- `validate_url_extraction.py` - Validation script comparing extraction results
- `test_browser.ipynb` - Jupyter notebook for browser data exploration

## Outdated Versions (Working but Superseded)
- `step1_extract_urls_standalone.py` - Original working standalone version (135 JSON files working, 16 .db files failing)
- `step1_extract_urls_fixed.py` - Fixed SQLite query version (all .db files working, but JSON memory issues)

## Notes
- Current production version: `../step1_extract_urls_optimized.py` (100% success rate, 633k URLs)
- Archived versions kept for reference and debugging history
- Test files may be useful for future troubleshooting

## Timeline
1. **Original**: step1_extract_urls.py (had .db processing issues) - DELETED
2. **Standalone**: step1_extract_urls_standalone.py (JSON working, .db failing) - ARCHIVED
3. **Fixed**: step1_extract_urls_fixed.py (.db fixed, but JSON memory issues) - ARCHIVED  
4. **Optimized**: step1_extract_urls_optimized.py (100% working with batch processing) - CURRENT
