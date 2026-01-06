#!/usr/bin/env python3
"""
Combine Scraped Data
====================
Concatenates all scraped batch files from browser and Facebook scraping
into unified datasets.

Usage:
    python combine_scraped_data.py --data-dir /path/to/new_data_251126
"""

import pandas as pd
from pathlib import Path
import argparse
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def combine_batch_files(batch_dir: Path, pattern: str = "scraped_batch_*.csv") -> pd.DataFrame:
    """Combine all batch CSV files from a directory."""
    batch_files = sorted(batch_dir.glob(pattern))
    
    if not batch_files:
        logger.warning(f"No batch files found in {batch_dir}")
        return pd.DataFrame()
    
    logger.info(f"Found {len(batch_files)} batch files in {batch_dir}")
    
    dfs = []
    for i, f in enumerate(batch_files):
        try:
            df = pd.read_csv(f)
            dfs.append(df)
            if (i + 1) % 100 == 0:
                logger.info(f"Loaded {i + 1}/{len(batch_files)} files...")
        except Exception as e:
            logger.error(f"Error reading {f}: {e}")
            continue
    
    if not dfs:
        return pd.DataFrame()
    
    combined = pd.concat(dfs, ignore_index=True)
    logger.info(f"Combined {len(combined):,} rows from {len(dfs)} files")
    
    return combined


def main():
    parser = argparse.ArgumentParser(description="Combine scraped data batches")
    parser.add_argument(
        "--data-dir",
        default="/work/Datadonationer/urlextraction_scraping/data/new_data_251126",
        help="Base data directory"
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Output directory (defaults to data-dir)"
    )
    args = parser.parse_args()
    
    data_dir = Path(args.data_dir)
    output_dir = Path(args.output_dir) if args.output_dir else data_dir
    output_dir.mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # === 1. Combine Browser Scraped Data ===
    browser_dir = data_dir / "scraped_content"
    if browser_dir.exists():
        logger.info("=" * 50)
        logger.info("Combining BROWSER scraped data...")
        browser_df = combine_batch_files(browser_dir)
        
        if not browser_df.empty:
            # Add source column
            browser_df["source"] = "browser"
            
            # Save as parquet (efficient)
            browser_output = output_dir / "browser_scraped_combined.parquet"
            browser_df.to_parquet(browser_output, index=False)
            logger.info(f"✅ Saved browser data: {browser_output}")
            logger.info(f"   Rows: {len(browser_df):,}")
            logger.info(f"   Columns: {list(browser_df.columns)}")
            
            # Also save CSV for convenience
            browser_csv = output_dir / "browser_scraped_combined.csv"
            browser_df.to_csv(browser_csv, index=False)
            logger.info(f"   Also saved as CSV: {browser_csv}")
    else:
        logger.warning(f"Browser directory not found: {browser_dir}")
        browser_df = pd.DataFrame()
    
    # === 2. Combine Facebook Scraped Data ===
    facebook_dir = data_dir / "facebook_scraped"
    if facebook_dir.exists():
        logger.info("=" * 50)
        logger.info("Combining FACEBOOK scraped data...")
        facebook_df = combine_batch_files(facebook_dir)
        
        if not facebook_df.empty:
            # Add source column
            facebook_df["source"] = "facebook"
            
            # Save as parquet
            facebook_output = output_dir / "facebook_scraped_combined.parquet"
            facebook_df.to_parquet(facebook_output, index=False)
            logger.info(f"✅ Saved Facebook data: {facebook_output}")
            logger.info(f"   Rows: {len(facebook_df):,}")
            
            # Also save CSV
            facebook_csv = output_dir / "facebook_scraped_combined.csv"
            facebook_df.to_csv(facebook_csv, index=False)
            logger.info(f"   Also saved as CSV: {facebook_csv}")
    else:
        logger.warning(f"Facebook directory not found: {facebook_dir}")
        facebook_df = pd.DataFrame()
    
    # === 3. Combine Everything ===
    logger.info("=" * 50)
    logger.info("Creating UNIFIED dataset...")
    
    all_dfs = []
    if not browser_df.empty:
        all_dfs.append(browser_df)
    if not facebook_df.empty:
        all_dfs.append(facebook_df)
    
    if all_dfs:
        unified_df = pd.concat(all_dfs, ignore_index=True)
        
        # Save unified dataset
        unified_output = output_dir / "all_scraped_content.parquet"
        unified_df.to_parquet(unified_output, index=False)
        logger.info(f"✅ Saved UNIFIED data: {unified_output}")
        logger.info(f"   Total rows: {len(unified_df):,}")
        logger.info(f"   Browser: {len(browser_df):,}")
        logger.info(f"   Facebook: {len(facebook_df):,}")
        
        # Summary statistics
        logger.info("=" * 50)
        logger.info("SUMMARY")
        logger.info("=" * 50)
        
        if "status" in unified_df.columns:
            success = (unified_df["status"] == "success").sum()
            failed = (unified_df["status"] != "success").sum()
            logger.info(f"Success: {success:,} ({100*success/len(unified_df):.1f}%)")
            logger.info(f"Failed: {failed:,} ({100*failed/len(unified_df):.1f}%)")
        
        # Content stats
        if "content" in unified_df.columns:
            has_content = unified_df["content"].notna() & (unified_df["content"] != "")
            logger.info(f"With content: {has_content.sum():,}")
        
        logger.info(f"\nOutput files in: {output_dir}")
        logger.info("  - browser_scraped_combined.parquet")
        logger.info("  - facebook_scraped_combined.parquet") 
        logger.info("  - all_scraped_content.parquet (unified)")
    else:
        logger.warning("No data to combine!")
    
    logger.info("\n✅ Done!")


if __name__ == "__main__":
    main()





