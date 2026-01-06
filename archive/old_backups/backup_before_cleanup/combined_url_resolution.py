#!/usr/bin/env python3
"""
Combined URL Resolution Pipeline
Merges Facebook and browser datasets, resolves URLs properly, and prepares for re-scraping.
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


class CombinedURLProcessor:
    """
    Processes Facebook and browser datasets to create a unified, properly resolved URL dataset.
    """

    def __init__(
        self,
        facebook_data_path: str = "data/facebook_urlextract/scraped_content_facebook.csv",
        browser_data_path: str = "data/browser_urlextract/scraped_content/scraped_content.csv",
        output_dir: str = "data/combined_resolved",
        cache_file: str = "combined_url_resolution_cache.db",
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

        # Initialize URL resolver
        self.resolver = RobustURLResolver(
            cache_file=cache_file,
            max_workers=10,
            delay_between_requests=0.1,
            timeout=30,
        )

    def load_datasets(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Load and standardize Facebook and browser datasets."""
        self.logger.info("Loading datasets...")

        # Load Facebook data
        try:
            facebook_df = pd.read_csv(self.facebook_data_path)
            self.logger.info(f"Loaded Facebook data: {len(facebook_df)} rows")
        except Exception as e:
            self.logger.error(f"Error loading Facebook data: {e}")
            facebook_df = pd.DataFrame()

        # Load browser data
        try:
            browser_df = pd.read_csv(self.browser_data_path)
            self.logger.info(f"Loaded browser data: {len(browser_df)} rows")
        except Exception as e:
            self.logger.error(f"Error loading browser data: {e}")
            browser_df = pd.DataFrame()

        return facebook_df, browser_df

    def standardize_datasets(
        self, facebook_df: pd.DataFrame, browser_df: pd.DataFrame
    ) -> pd.DataFrame:
        """Standardize and combine the datasets into a unified format."""
        self.logger.info("Standardizing datasets...")

        combined_rows = []

        # Process Facebook data
        if not facebook_df.empty:
            for _, row in facebook_df.iterrows():
                standardized_row = {
                    "original_url": row.get("url", ""),
                    "old_resolved_url": row.get("resolved_url", ""),
                    "source": "facebook",
                    "domain": row.get("domain", ""),
                    "old_content": row.get("content", ""),
                    "old_content_hash": row.get("content_hash", ""),
                    "old_word_count": row.get("word_count", 0),
                    "old_status_code": row.get("status_code", None),
                    "old_scraping_time": row.get("scraping_time", 0),
                    "old_error": row.get("error", ""),
                    "old_paywall_detected": row.get("paywall_detected", False),
                    "old_scraped_at": row.get("scraped_at", ""),
                    "needs_resolution": row.get("url", "")
                    == row.get("resolved_url", ""),
                    "needs_rescraping": False,  # Will determine this later
                }
                combined_rows.append(standardized_row)

        # Process browser data
        if not browser_df.empty:
            for _, row in browser_df.iterrows():
                standardized_row = {
                    "original_url": row.get("url", ""),
                    "old_resolved_url": row.get("resolved_url", ""),
                    "source": "browser",
                    "domain": urlparse(row.get("url", "")).netloc
                    if row.get("url")
                    else "",
                    "old_content": row.get("content", ""),
                    "old_content_hash": row.get("content_hash", ""),
                    "old_word_count": row.get("word_count", 0),
                    "old_status_code": row.get("status_code", None),
                    "old_scraping_time": row.get("response_time", 0),
                    "old_error": row.get("error", ""),
                    "old_paywall_detected": row.get("paywall_detected", False),
                    "old_scraped_at": row.get("scrape_time", ""),
                    "needs_resolution": row.get("url", "")
                    == row.get("resolved_url", ""),
                    "needs_rescraping": False,  # Will determine this later
                }
                combined_rows.append(standardized_row)

        combined_df = pd.DataFrame(combined_rows)

        self.logger.info(f"Combined dataset: {len(combined_df)} rows")
        self.logger.info(
            f"Facebook URLs: {len(combined_df[combined_df['source'] == 'facebook'])}"
        )
        self.logger.info(
            f"Browser URLs: {len(combined_df[combined_df['source'] == 'browser'])}"
        )

        return combined_df

    def identify_resolution_needs(self, combined_df: pd.DataFrame) -> pd.DataFrame:
        """Identify which URLs need resolution and which need re-scraping."""
        self.logger.info("Analyzing URL resolution needs...")

        # URLs that need resolution (original == resolved, indicating failed redirect)
        failed_resolution = (
            combined_df["original_url"] == combined_df["old_resolved_url"]
        )

        # URLs that might have invalid content (empty content, very short content, error status)
        invalid_content = (
            (combined_df["old_content"].isna() | (combined_df["old_content"] == ""))
            | (combined_df["old_word_count"] < 10)
            | (
                combined_df["old_status_code"].isna()
                | (combined_df["old_status_code"] >= 400)
            )
            | (combined_df["old_error"].notna() & (combined_df["old_error"] != ""))
        )

        # Set resolution and re-scraping flags
        combined_df["needs_resolution"] = failed_resolution
        combined_df["needs_rescraping"] = failed_resolution | invalid_content

        resolution_count = combined_df["needs_resolution"].sum()
        rescraping_count = combined_df["needs_rescraping"].sum()

        self.logger.info(f"URLs needing resolution: {resolution_count}")
        self.logger.info(f"URLs needing re-scraping: {rescraping_count}")

        return combined_df

    def resolve_urls(
        self, combined_df: pd.DataFrame, batch_size: int = 500
    ) -> pd.DataFrame:
        """Resolve URLs that need resolution."""
        self.logger.info("Starting URL resolution process...")

        # Get unique URLs that need resolution
        urls_to_resolve = (
            combined_df[combined_df["needs_resolution"]]["original_url"]
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

        # Apply resolved URLs to the dataset
        def apply_resolution(row):
            if row["needs_resolution"]:
                resolution = resolution_lookup.get(row["original_url"])
                if resolution:
                    row["new_resolved_url"] = resolution["resolved_url"]
                    row["resolution_status_code"] = resolution["status_code"]
                    row["resolution_success"] = resolution["success"]
                    row["resolution_error"] = resolution["error"]
                    row["redirect_count"] = resolution["redirect_count"]
                    row["resolution_worked"] = resolution["resolution_worked"]
                else:
                    row["new_resolved_url"] = row["original_url"]
                    row["resolution_success"] = False
                    row["resolution_error"] = "not_found_in_resolution"
            else:
                # Use old resolved URL if it was already correct
                row["new_resolved_url"] = row["old_resolved_url"]
                row["resolution_success"] = True
                row["resolution_error"] = None
                row["redirect_count"] = 0
                row["resolution_worked"] = (
                    row["original_url"] != row["old_resolved_url"]
                )

            return row

        combined_df = combined_df.apply(apply_resolution, axis=1)

        # Generate resolution report
        report = self.resolver.generate_resolution_report(resolution_results)
        self.logger.info(
            f"Resolution complete. Success rate: {report['success_rate']:.1f}%"
        )

        return combined_df

    def remove_duplicates(self, combined_df: pd.DataFrame) -> pd.DataFrame:
        """Remove duplicate URLs based on resolved URLs."""
        self.logger.info("Removing duplicates based on resolved URLs...")

        initial_count = len(combined_df)

        # Sort by source priority (keep Facebook over browser if same resolved URL)
        # and by content quality (longer content, successful scraping)
        combined_df["priority_score"] = (
            (combined_df["source"] == "facebook").astype(int) * 100  # Facebook priority
            + combined_df["old_word_count"].fillna(0)  # Content length
            + (combined_df["old_status_code"] == 200).astype(int)
            * 50  # Successful status
            + (~combined_df["old_error"].fillna("").astype(bool)).astype(int)
            * 25  # No errors
        )

        # Sort and keep the best version of each resolved URL
        combined_df = combined_df.sort_values("priority_score", ascending=False)
        combined_df = combined_df.drop_duplicates(
            subset=["new_resolved_url"], keep="first"
        )

        final_count = len(combined_df)
        removed_count = initial_count - final_count

        self.logger.info(f"Removed {removed_count} duplicate URLs")
        self.logger.info(f"Final dataset: {final_count} unique resolved URLs")

        return combined_df.drop(columns=["priority_score"])

    def save_results(self, combined_df: pd.DataFrame):
        """Save the processed dataset and generate reports."""
        self.logger.info("Saving results...")

        # Save complete dataset
        output_file = os.path.join(self.output_dir, "combined_resolved_urls.csv")
        combined_df.to_csv(output_file, index=False)
        self.logger.info(f"Saved combined dataset: {output_file}")

        # Save URLs that need re-scraping
        rescrape_df = combined_df[combined_df["needs_rescraping"]].copy()
        rescrape_file = os.path.join(self.output_dir, "urls_for_rescraping.csv")
        rescrape_df.to_csv(rescrape_file, index=False)
        self.logger.info(
            f"Saved re-scraping list: {rescrape_file} ({len(rescrape_df)} URLs)"
        )

        # Save URLs with good existing content (no re-scraping needed)
        good_content_df = combined_df[~combined_df["needs_rescraping"]].copy()
        good_content_file = os.path.join(self.output_dir, "urls_with_good_content.csv")
        good_content_df.to_csv(good_content_file, index=False)
        self.logger.info(
            f"Saved good content URLs: {good_content_file} ({len(good_content_df)} URLs)"
        )

        # Generate summary report
        self.generate_summary_report(combined_df)

    def generate_summary_report(self, combined_df: pd.DataFrame):
        """Generate a comprehensive summary report."""
        report_file = os.path.join(self.output_dir, "processing_report.txt")

        with open(report_file, "w") as f:
            f.write("Combined URL Resolution Report\n")
            f.write("=" * 50 + "\n")
            f.write(f"Generated: {datetime.now().isoformat()}\n\n")

            # Dataset overview
            f.write("Dataset Overview:\n")
            f.write(f"Total URLs processed: {len(combined_df)}\n")
            f.write(
                f"Facebook URLs: {len(combined_df[combined_df['source'] == 'facebook'])}\n"
            )
            f.write(
                f"Browser URLs: {len(combined_df[combined_df['source'] == 'browser'])}\n\n"
            )

            # Resolution statistics
            resolution_needed = combined_df["needs_resolution"].sum()
            resolution_success = combined_df[combined_df["needs_resolution"]][
                "resolution_success"
            ].sum()
            f.write("URL Resolution:\n")
            f.write(f"URLs needing resolution: {resolution_needed}\n")
            f.write(f"Successfully resolved: {resolution_success}\n")
            f.write(
                f"Resolution success rate: {(resolution_success / resolution_needed * 100):.1f}%\n\n"
            )

            # Re-scraping needs
            rescrape_needed = combined_df["needs_rescraping"].sum()
            good_content = len(combined_df) - rescrape_needed
            f.write("Content Quality:\n")
            f.write(f"URLs needing re-scraping: {rescrape_needed}\n")
            f.write(f"URLs with good content: {good_content}\n")
            f.write(
                f"Content retention rate: {(good_content / len(combined_df) * 100):.1f}%\n\n"
            )

            # Top domains
            f.write("Top 10 Domains:\n")
            domain_counts = combined_df["domain"].value_counts().head(10)
            for domain, count in domain_counts.items():
                f.write(f"  {domain}: {count}\n")

        self.logger.info(f"Saved processing report: {report_file}")

    def run_full_pipeline(self):
        """Run the complete URL resolution pipeline."""
        self.logger.info("Starting Combined URL Resolution Pipeline")

        # Step 1: Load datasets
        facebook_df, browser_df = self.load_datasets()

        # Step 2: Standardize and combine
        combined_df = self.standardize_datasets(facebook_df, browser_df)

        # Step 3: Identify resolution needs
        combined_df = self.identify_resolution_needs(combined_df)

        # Step 4: Resolve URLs
        combined_df = self.resolve_urls(combined_df, batch_size=500)

        # Step 5: Remove duplicates
        combined_df = self.remove_duplicates(combined_df)

        # Step 6: Save results
        self.save_results(combined_df)

        self.logger.info("Pipeline completed successfully!")

        return combined_df


def main():
    """Main execution function."""
    processor = CombinedURLProcessor()
    result_df = processor.run_full_pipeline()

    print("\n" + "=" * 60)
    print("COMBINED URL RESOLUTION PIPELINE COMPLETED")
    print("=" * 60)
    print(f"Total URLs processed: {len(result_df)}")
    print(f"URLs needing re-scraping: {result_df['needs_rescraping'].sum()}")
    print(f"URLs with good content: {(~result_df['needs_rescraping']).sum()}")
    print("\nOutput files saved to: data/combined_resolved/")
    print("  - combined_resolved_urls.csv (complete dataset)")
    print("  - urls_for_rescraping.csv (URLs to re-scrape)")
    print("  - urls_with_good_content.csv (URLs with existing good content)")
    print("  - processing_report.txt (detailed report)")


if __name__ == "__main__":
    main()
