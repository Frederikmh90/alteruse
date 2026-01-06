#!/usr/bin/env python3
"""
Enhanced Combined URL Resolution Pipeline
========================================
Merges Facebook and browser datasets, resolves URLs properly with robust resolution,
and prepares unified dataset for re-scraping.
"""

import pandas as pd
import numpy as np
from robust_url_resolver import RobustURLResolver
import sqlite3
import os
import logging
from datetime import datetime
from typing import List, Dict, Set, Tuple
import hashlib
from urllib.parse import urlparse
import time
import json


class EnhancedCombinedURLProcessor:
    """
    Enhanced processor that combines Facebook and browser datasets with robust URL resolution.
    """

    def __init__(
        self,
        facebook_data_path: str = "data/url_extract_facebook/scraped_content/scraped_content_facebook.csv",
        browser_data_path: str = "data/url_extract/scraped_content/scraped_content.csv",
        output_dir: str = "data/combined_resolved_enhanced",
        cache_file: str = "combined_enhanced_url_resolution_cache.db",
    ):
        self.facebook_data_path = facebook_data_path
        self.browser_data_path = browser_data_path
        self.output_dir = output_dir
        self.cache_file = cache_file

        # Setup logging
        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
        )
        self.logger = logging.getLogger(__name__)

        # Create output directory
        os.makedirs(output_dir, exist_ok=True)

        # Initialize enhanced URL resolver
        self.resolver = RobustURLResolver(
            cache_file=cache_file,
            max_workers=15,
            delay_between_requests=0.1,
            timeout=30,
        )

        # Statistics
        self.stats = {
            "facebook_urls_loaded": 0,
            "browser_urls_loaded": 0,
            "total_combined_urls": 0,
            "unique_urls_after_dedup": 0,
            "urls_needing_resolution": 0,
            "urls_successfully_resolved": 0,
            "urls_resolution_failed": 0,
            "urls_needing_rescraping": 0,
            "duplicate_content_removed": 0,
            "start_time": None,
            "end_time": None,
            "processing_time": None,
        }

    def test_url_resolution(self):
        """Test URL resolution with various types of URLs."""
        test_urls = [
            "https://t.co/testurl123",
            "https://bit.ly/testurl123",
            "https://fb.me/testurl123",
            "https://l.facebook.com/l.php?u=https%3A%2F%2Fexample.com%2Ftest",
            "https://tinyurl.com/testurl123",
            "https://www.google.com/url?q=https%3A%2F%2Fexample.com",
            "https://example.com/direct-url",
        ]

        self.logger.info("Testing enhanced URL resolution capabilities...")

        for url in test_urls:
            try:
                result = self.resolver.resolve_single_url(url)
                self.logger.info(
                    f"Test: {url[:50]}... -> {result['resolved_url'][:50]}... (Success: {result['success']}, Redirects: {result.get('redirect_count', 0)})"
                )
            except Exception as e:
                self.logger.error(f"Test failed for {url}: {e}")

        self.logger.info("URL resolution testing completed")

    def load_datasets(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Load and validate Facebook and browser datasets."""
        self.logger.info("Loading Facebook and browser datasets...")

        # Load Facebook data
        facebook_df = pd.DataFrame()
        if os.path.exists(self.facebook_data_path):
            try:
                facebook_df = pd.read_csv(self.facebook_data_path)
                self.stats["facebook_urls_loaded"] = len(facebook_df)
                self.logger.info(f"Loaded Facebook data: {len(facebook_df)} rows")
            except Exception as e:
                self.logger.error(f"Error loading Facebook data: {e}")
        else:
            self.logger.warning(
                f"Facebook data file not found: {self.facebook_data_path}"
            )

        # Load browser data
        browser_df = pd.DataFrame()
        if os.path.exists(self.browser_data_path):
            try:
                browser_df = pd.read_csv(self.browser_data_path)
                self.stats["browser_urls_loaded"] = len(browser_df)
                self.logger.info(f"Loaded browser data: {len(browser_df)} rows")
            except Exception as e:
                self.logger.error(f"Error loading browser data: {e}")
        else:
            self.logger.warning(
                f"Browser data file not found: {self.browser_data_path}"
            )

        return facebook_df, browser_df

    def standardize_datasets(
        self, facebook_df: pd.DataFrame, browser_df: pd.DataFrame
    ) -> pd.DataFrame:
        """Standardize and combine the datasets into a unified format."""
        self.logger.info("Standardizing and combining datasets...")

        combined_rows = []

        # Process Facebook data
        if not facebook_df.empty:
            self.logger.info("Processing Facebook URLs...")
            for _, row in facebook_df.iterrows():
                standardized_row = {
                    "original_url": row.get("url", ""),
                    "old_resolved_url": row.get("resolved_url", row.get("url", "")),
                    "source": "facebook",
                    "domain": self._extract_domain(
                        row.get("resolved_url", row.get("url", ""))
                    ),
                    "old_content": row.get("content", ""),
                    "old_content_hash": row.get("content_hash", ""),
                    "old_word_count": row.get("word_count", 0),
                    "old_char_count": row.get("char_count", 0),
                    "old_status_code": row.get("status_code", None),
                    "old_response_time": row.get("response_time", 0),
                    "old_error": row.get("error", ""),
                    "old_paywall_detected": row.get("paywall_detected", False),
                    "old_paywall_type": row.get("paywall_type", ""),
                    "old_scraped_at": row.get("scraped_at", ""),
                    "old_redirect_count": row.get("redirect_count", 0),
                    # Will be populated during resolution
                    "new_resolved_url": "",
                    "resolution_needed": True,
                    "resolution_worked": False,
                    "new_redirect_count": 0,
                    "needs_rescraping": False,
                }
                combined_rows.append(standardized_row)

        # Process browser data
        if not browser_df.empty:
            self.logger.info("Processing browser URLs...")
            for _, row in browser_df.iterrows():
                standardized_row = {
                    "original_url": row.get("url", ""),
                    "old_resolved_url": row.get("resolved_url", row.get("url", "")),
                    "source": "browser",
                    "domain": self._extract_domain(
                        row.get("resolved_url", row.get("url", ""))
                    ),
                    "old_content": row.get("content", ""),
                    "old_content_hash": row.get("content_hash", ""),
                    "old_word_count": row.get("word_count", 0),
                    "old_char_count": row.get("char_count", 0),
                    "old_status_code": row.get("status_code", None),
                    "old_response_time": row.get(
                        "response_time", row.get("scraping_time", 0)
                    ),
                    "old_error": row.get("error", ""),
                    "old_paywall_detected": row.get("paywall_detected", False),
                    "old_paywall_type": row.get("paywall_type", ""),
                    "old_scraped_at": row.get("scraped_at", row.get("scrape_time", "")),
                    "old_redirect_count": row.get("redirect_count", 0),
                    # Will be populated during resolution
                    "new_resolved_url": "",
                    "resolution_needed": True,
                    "resolution_worked": False,
                    "new_redirect_count": 0,
                    "needs_rescraping": False,
                }
                combined_rows.append(standardized_row)

        combined_df = pd.DataFrame(combined_rows)
        self.stats["total_combined_urls"] = len(combined_df)

        self.logger.info(f"Combined dataset: {len(combined_df)} rows")
        self.logger.info(
            f"Facebook URLs: {len(combined_df[combined_df['source'] == 'facebook'])}"
        )
        self.logger.info(
            f"Browser URLs: {len(combined_df[combined_df['source'] == 'browser'])}"
        )

        return combined_df

    def _extract_domain(self, url: str) -> str:
        """Extract domain from URL safely."""
        try:
            if url:
                return urlparse(url).netloc.lower()
        except:
            pass
        return ""

    def deduplicate_urls(self, combined_df: pd.DataFrame) -> pd.DataFrame:
        """Remove duplicate URLs, keeping the most recent or highest quality entry."""
        self.logger.info("Deduplicating URLs...")

        initial_count = len(combined_df)

        # Sort by source priority (facebook first) and then by data quality indicators
        combined_df["source_priority"] = combined_df["source"].map(
            {"facebook": 1, "browser": 2}
        )
        combined_df["quality_score"] = (
            combined_df["old_word_count"].fillna(0) * 0.4
            + (combined_df["old_status_code"] == 200).astype(int) * 0.3
            + combined_df["old_error"].isna().astype(int) * 0.3
        )

        # Keep the best entry for each original URL
        combined_df = combined_df.sort_values(
            ["original_url", "source_priority", "quality_score"],
            ascending=[True, True, False],
        )
        combined_df = combined_df.drop_duplicates(subset=["original_url"], keep="first")

        # Clean up temporary columns
        combined_df = combined_df.drop(["source_priority", "quality_score"], axis=1)

        final_count = len(combined_df)
        self.stats["unique_urls_after_dedup"] = final_count

        self.logger.info(f"Removed {initial_count - final_count} duplicate URLs")
        self.logger.info(f"Unique URLs after deduplication: {final_count}")

        return combined_df.reset_index(drop=True)

    def identify_resolution_needs(self, combined_df: pd.DataFrame) -> pd.DataFrame:
        """Identify which URLs need resolution and which need re-scraping."""
        self.logger.info("Analyzing URL resolution and re-scraping needs...")

        # URLs that likely need resolution (various indicators)
        likely_redirected = (
            # Short URLs that are likely redirects
            combined_df["original_url"].str.contains(
                r"(t\.co|bit\.ly|tinyurl|goo\.gl|fb\.me|l\.facebook\.com)",
                case=False,
                na=False,
            )
            |
            # Original URL same as resolved URL (no resolution attempted)
            (combined_df["original_url"] == combined_df["old_resolved_url"])
            |
            # Very short domains (likely URL shorteners)
            (combined_df["original_url"].str.len() < 30)
        )

        # URLs that have poor quality content (need re-scraping)
        poor_quality_content = (
            # No content or very short content
            (combined_df["old_content"].isna() | (combined_df["old_content"] == ""))
            | (combined_df["old_word_count"].fillna(0) < 50)
            |
            # Error status codes
            (
                combined_df["old_status_code"].isna()
                | (combined_df["old_status_code"] >= 400)
            )
            |
            # Had errors during scraping
            (combined_df["old_error"].notna() & (combined_df["old_error"] != ""))
            |
            # Paywall detected but content is poor (might be resolved with better approach)
            (
                combined_df["old_paywall_detected"]
                & (combined_df["old_word_count"].fillna(0) < 100)
            )
        )

        # Set flags
        combined_df["resolution_needed"] = likely_redirected
        combined_df["needs_rescraping"] = likely_redirected | poor_quality_content

        resolution_count = combined_df["resolution_needed"].sum()
        rescraping_count = combined_df["needs_rescraping"].sum()

        self.stats["urls_needing_resolution"] = resolution_count
        self.stats["urls_needing_rescraping"] = rescraping_count

        self.logger.info(f"URLs needing resolution: {resolution_count}")
        self.logger.info(f"URLs needing re-scraping: {rescraping_count}")

        return combined_df

    def resolve_urls_enhanced(
        self, combined_df: pd.DataFrame, batch_size: int = 500
    ) -> pd.DataFrame:
        """Resolve URLs that need resolution using enhanced resolver."""
        self.logger.info("Starting enhanced URL resolution process...")

        # Get URLs that need resolution
        urls_to_resolve = (
            combined_df[combined_df["resolution_needed"]]["original_url"]
            .unique()
            .tolist()
        )

        if not urls_to_resolve:
            self.logger.info("No URLs need resolution")
            return combined_df

        self.logger.info(f"Resolving {len(urls_to_resolve)} unique URLs...")

        # Resolve URLs in batches
        resolution_results = self.resolver.resolve_urls_batch(
            urls_to_resolve, batch_size=batch_size
        )

        # Create lookup dictionary for resolved URLs
        resolution_lookup = {
            result["original_url"]: result for result in resolution_results
        }

        # Apply resolution results
        def apply_resolution(row):
            if row["resolution_needed"] and row["original_url"] in resolution_lookup:
                result = resolution_lookup[row["original_url"]]
                row["new_resolved_url"] = result["resolved_url"]
                row["resolution_worked"] = result["resolution_worked"]
                row["new_redirect_count"] = result["redirect_count"]

                # Update domain based on resolved URL
                if result["resolution_worked"]:
                    row["domain"] = self._extract_domain(result["resolved_url"])

                # Mark for re-scraping if resolution worked (new URL to scrape)
                if result["resolution_worked"]:
                    row["needs_rescraping"] = True

            else:
                # No resolution needed or failed
                row["new_resolved_url"] = row["old_resolved_url"]
                row["resolution_worked"] = False
                row["new_redirect_count"] = row["old_redirect_count"]

            return row

        combined_df = combined_df.apply(apply_resolution, axis=1)

        # Count successful resolutions
        successfully_resolved = combined_df[
            combined_df["resolution_needed"] & combined_df["resolution_worked"]
        ]

        self.stats["urls_successfully_resolved"] = len(successfully_resolved)
        self.stats["urls_resolution_failed"] = (
            self.stats["urls_needing_resolution"]
            - self.stats["urls_successfully_resolved"]
        )

        self.logger.info(
            f"Successfully resolved: {self.stats['urls_successfully_resolved']} URLs"
        )
        self.logger.info(
            f"Resolution failed: {self.stats['urls_resolution_failed']} URLs"
        )

        return combined_df

    def remove_duplicate_content(self, combined_df: pd.DataFrame) -> pd.DataFrame:
        """Remove entries with duplicate content based on content hash."""
        self.logger.info("Removing duplicate content...")

        initial_count = len(combined_df)

        # Filter out entries with no content hash
        valid_content = combined_df[
            (combined_df["old_content_hash"].notna())
            & (combined_df["old_content_hash"] != "")
        ]

        if not valid_content.empty:
            # Remove duplicates based on content hash, keeping the first occurrence
            combined_df = combined_df.drop_duplicates(
                subset=["old_content_hash"], keep="first"
            )

        final_count = len(combined_df)
        duplicates_removed = initial_count - final_count
        self.stats["duplicate_content_removed"] = duplicates_removed

        self.logger.info(f"Removed {duplicates_removed} entries with duplicate content")

        return combined_df.reset_index(drop=True)

    def save_results(self, combined_df: pd.DataFrame):
        """Save all results to files."""
        self.logger.info("Saving results...")

        # Save complete dataset
        complete_file = os.path.join(self.output_dir, "combined_urls_resolved.csv")
        combined_df.to_csv(complete_file, index=False)
        self.logger.info(f"Saved complete dataset: {complete_file}")

        # Save URLs that need re-scraping
        rescraping_needed = combined_df[combined_df["needs_rescraping"]].copy()
        rescraping_file = os.path.join(self.output_dir, "urls_for_rescraping.csv")
        rescraping_needed.to_csv(rescraping_file, index=False)
        self.logger.info(
            f"Saved URLs for re-scraping: {rescraping_file} ({len(rescraping_needed)} URLs)"
        )

        # Save URLs with successful resolution
        successfully_resolved = combined_df[
            combined_df["resolution_needed"] & combined_df["resolution_worked"]
        ].copy()
        resolved_file = os.path.join(self.output_dir, "successfully_resolved_urls.csv")
        successfully_resolved.to_csv(resolved_file, index=False)
        self.logger.info(
            f"Saved successfully resolved URLs: {resolved_file} ({len(successfully_resolved)} URLs)"
        )

        # Save high-quality existing content (for reference)
        high_quality = combined_df[
            ~combined_df["needs_rescraping"]
            & (combined_df["old_word_count"] >= 100)
            & (combined_df["old_status_code"] == 200)
        ].copy()
        quality_file = os.path.join(
            self.output_dir, "high_quality_existing_content.csv"
        )
        high_quality.to_csv(quality_file, index=False)
        self.logger.info(
            f"Saved high-quality existing content: {quality_file} ({len(high_quality)} URLs)"
        )

    def generate_summary_report(self, combined_df: pd.DataFrame):
        """Generate a comprehensive summary report."""
        self.logger.info("Generating summary report...")

        report = {
            "processing_summary": {
                "start_time": self.stats["start_time"],
                "end_time": self.stats["end_time"],
                "processing_time_seconds": self.stats["processing_time"],
                "processing_time_formatted": str(
                    datetime.now() - self.stats["start_time"]
                )
                if self.stats["start_time"]
                else "N/A",
            },
            "data_loading": {
                "facebook_urls_loaded": self.stats["facebook_urls_loaded"],
                "browser_urls_loaded": self.stats["browser_urls_loaded"],
                "total_urls_loaded": self.stats["facebook_urls_loaded"]
                + self.stats["browser_urls_loaded"],
            },
            "data_processing": {
                "total_combined_urls": self.stats["total_combined_urls"],
                "unique_urls_after_dedup": self.stats["unique_urls_after_dedup"],
                "duplicate_urls_removed": self.stats["total_combined_urls"]
                - self.stats["unique_urls_after_dedup"],
                "duplicate_content_removed": self.stats["duplicate_content_removed"],
            },
            "url_resolution": {
                "urls_needing_resolution": self.stats["urls_needing_resolution"],
                "urls_successfully_resolved": self.stats["urls_successfully_resolved"],
                "urls_resolution_failed": self.stats["urls_resolution_failed"],
                "resolution_success_rate": (
                    self.stats["urls_successfully_resolved"]
                    / self.stats["urls_needing_resolution"]
                    if self.stats["urls_needing_resolution"] > 0
                    else 0
                ),
            },
            "scraping_needs": {
                "urls_needing_rescraping": self.stats["urls_needing_rescraping"],
                "high_quality_existing": len(
                    combined_df[
                        ~combined_df["needs_rescraping"]
                        & (combined_df["old_word_count"] >= 100)
                        & (combined_df["old_status_code"] == 200)
                    ]
                ),
                "rescraping_rate": (
                    self.stats["urls_needing_rescraping"] / len(combined_df)
                    if len(combined_df) > 0
                    else 0
                ),
            },
            "data_quality": {
                "urls_with_content": len(
                    combined_df[
                        (combined_df["old_content"].notna())
                        & (combined_df["old_content"] != "")
                    ]
                ),
                "urls_with_good_content": len(
                    combined_df[combined_df["old_word_count"] >= 100]
                ),
                "urls_with_paywall": len(
                    combined_df[combined_df["old_paywall_detected"] == True]
                ),
                "urls_with_errors": len(
                    combined_df[
                        (combined_df["old_error"].notna())
                        & (combined_df["old_error"] != "")
                    ]
                ),
            },
            "source_breakdown": {
                "facebook_final": len(combined_df[combined_df["source"] == "facebook"]),
                "browser_final": len(combined_df[combined_df["source"] == "browser"]),
            },
        }

        # Save report
        report_file = os.path.join(self.output_dir, "processing_report.json")
        with open(report_file, "w") as f:
            json.dump(report, f, indent=2, default=str)

        self.logger.info(f"Saved processing report: {report_file}")

        # Log key metrics
        self.logger.info("\n=== PROCESSING SUMMARY ===")
        self.logger.info(f"URLs loaded: {report['data_loading']['total_urls_loaded']}")
        self.logger.info(
            f"Unique URLs after dedup: {report['data_processing']['unique_urls_after_dedup']}"
        )
        self.logger.info(
            f"URLs successfully resolved: {report['url_resolution']['urls_successfully_resolved']}"
        )
        self.logger.info(
            f"URLs needing re-scraping: {report['scraping_needs']['urls_needing_rescraping']}"
        )
        self.logger.info(
            f"Resolution success rate: {report['url_resolution']['resolution_success_rate']:.2%}"
        )
        self.logger.info(
            f"High-quality existing content: {report['scraping_needs']['high_quality_existing']}"
        )

    def run_full_pipeline(self):
        """Run the complete enhanced URL processing pipeline."""
        self.stats["start_time"] = datetime.now()

        self.logger.info("=== Starting Enhanced Combined URL Resolution Pipeline ===")

        # Test URL resolution capabilities
        self.test_url_resolution()

        # Step 1: Load datasets
        facebook_df, browser_df = self.load_datasets()

        if facebook_df.empty and browser_df.empty:
            self.logger.error("No data loaded. Exiting.")
            return

        # Step 2: Standardize and combine
        combined_df = self.standardize_datasets(facebook_df, browser_df)

        # Step 3: Deduplicate URLs
        combined_df = self.deduplicate_urls(combined_df)

        # Step 4: Identify resolution needs
        combined_df = self.identify_resolution_needs(combined_df)

        # Step 5: Resolve URLs
        combined_df = self.resolve_urls_enhanced(combined_df)

        # Step 6: Remove duplicate content
        combined_df = self.remove_duplicate_content(combined_df)

        # Step 7: Save results
        self.save_results(combined_df)

        # Step 8: Generate report
        self.stats["end_time"] = datetime.now()
        self.stats["processing_time"] = (
            self.stats["end_time"] - self.stats["start_time"]
        ).total_seconds()
        self.generate_summary_report(combined_df)

        self.logger.info("=== Enhanced Combined URL Resolution Pipeline Completed ===")


def main():
    """Main function to run the enhanced combined URL processor."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Enhanced Combined URL Resolution Pipeline"
    )
    parser.add_argument(
        "--facebook-data",
        default="data/url_extract_facebook/scraped_content/scraped_content_facebook.csv",
        help="Facebook data CSV file",
    )
    parser.add_argument(
        "--browser-data",
        default="data/url_extract/scraped_content/scraped_content.csv",
        help="Browser data CSV file",
    )
    parser.add_argument(
        "--output-dir",
        default="data/combined_resolved_enhanced",
        help="Output directory",
    )
    parser.add_argument(
        "--batch-size", type=int, default=500, help="URL resolution batch size"
    )
    parser.add_argument(
        "--test-only", action="store_true", help="Only test URL resolution"
    )

    args = parser.parse_args()

    processor = EnhancedCombinedURLProcessor(
        facebook_data_path=args.facebook_data,
        browser_data_path=args.browser_data,
        output_dir=args.output_dir,
    )

    if args.test_only:
        processor.test_url_resolution()
    else:
        processor.run_full_pipeline()


if __name__ == "__main__":
    main()
