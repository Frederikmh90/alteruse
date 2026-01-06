#!/usr/bin/env python3
"""
Final Integration Pipeline
Integrates resolved URLs with enhanced scraping, avoiding duplicates and producing final dataset.
"""

import pandas as pd
import numpy as np
import os
import logging
from datetime import datetime
from typing import List, Dict, Tuple
import sqlite3
from enhanced_content_scraper import EnhancedContentScraper
from urllib.parse import urlparse
import time


class FinalIntegrationPipeline:
    """
    Final pipeline that processes resolved URLs and creates the complete dataset.
    """

    def __init__(
        self,
        resolved_data_dir: str = "data/combined_resolved",
        output_dir: str = "data/final_dataset",
        scraper_cache_file: str = "final_scraping_cache.db",
    ):
        self.resolved_data_dir = resolved_data_dir
        self.output_dir = output_dir
        self.scraper_cache_file = scraper_cache_file

        # Setup logging
        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
        )
        self.logger = logging.getLogger(__name__)

        # Create output directory
        os.makedirs(output_dir, exist_ok=True)

        # Initialize enhanced scraper
        self.scraper = EnhancedContentScraper(
            cache_file=scraper_cache_file,
            max_workers=10,
            delay_between_requests=0.15,
            timeout=30,
            batch_size=200,
        )

    def wait_for_resolution(self, max_wait_minutes: int = 60) -> bool:
        """Wait for URL resolution to complete."""
        self.logger.info("Waiting for URL resolution to complete...")

        required_files = [
            os.path.join(self.resolved_data_dir, "combined_resolved_urls.csv"),
            os.path.join(self.resolved_data_dir, "urls_for_rescraping.csv"),
            os.path.join(self.resolved_data_dir, "processing_report.txt"),
        ]

        start_time = time.time()
        max_wait_seconds = max_wait_minutes * 60

        while time.time() - start_time < max_wait_seconds:
            all_files_exist = all(os.path.exists(f) for f in required_files)

            if all_files_exist:
                self.logger.info("URL resolution completed!")
                return True

            # Check progress by looking at cache
            try:
                cache_file = "combined_url_resolution_cache.db"
                if os.path.exists(cache_file):
                    conn = sqlite3.connect(cache_file)
                    cursor = conn.cursor()
                    cursor.execute("SELECT COUNT(*) FROM url_cache")
                    count = cursor.fetchone()[0]
                    conn.close()
                    self.logger.info(f"Resolution progress: {count} URLs processed...")
            except Exception as e:
                self.logger.debug(f"Error checking progress: {e}")

            time.sleep(30)  # Check every 30 seconds

        self.logger.error(
            f"URL resolution did not complete within {max_wait_minutes} minutes"
        )
        return False

    def load_resolved_data(self) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Load the resolved URL data."""
        self.logger.info("Loading resolved URL data...")

        # Load complete resolved dataset
        resolved_file = os.path.join(
            self.resolved_data_dir, "combined_resolved_urls.csv"
        )
        resolved_df = pd.read_csv(resolved_file)
        self.logger.info(f"Loaded resolved dataset: {len(resolved_df)} URLs")

        # Load URLs needing re-scraping
        rescrape_file = os.path.join(self.resolved_data_dir, "urls_for_rescraping.csv")
        rescrape_df = pd.read_csv(rescrape_file)
        self.logger.info(f"URLs needing re-scraping: {len(rescrape_df)}")

        # Load URLs with good existing content
        good_content_file = os.path.join(
            self.resolved_data_dir, "urls_with_good_content.csv"
        )
        good_content_df = pd.read_csv(good_content_file)
        self.logger.info(f"URLs with good content: {len(good_content_df)}")

        return resolved_df, rescrape_df, good_content_df

    def analyze_scraping_priorities(self, rescrape_df: pd.DataFrame) -> Dict:
        """Analyze and prioritize URLs for scraping."""
        self.logger.info("Analyzing scraping priorities...")

        # Analyze by domain
        domain_counts = rescrape_df["domain"].value_counts()

        # Categorize domains
        news_domains = [
            "politiken.dk",
            "berlingske.dk",
            "information.dk",
            "dr.dk",
            "tv2.dk",
            "ekstrabladet.dk",
            "bt.dk",
            "jyllands-posten.dk",
            "weekendavisen.dk",
        ]

        # Priority categories
        high_priority = rescrape_df[rescrape_df["domain"].isin(news_domains)]
        medium_priority = rescrape_df[
            (~rescrape_df["domain"].isin(news_domains))
            & (rescrape_df["domain"].str.contains("\.dk$", regex=True))
        ]
        low_priority = rescrape_df[
            (~rescrape_df["domain"].isin(news_domains))
            & (~rescrape_df["domain"].str.contains("\.dk$", regex=True))
        ]

        priority_analysis = {
            "total_to_scrape": len(rescrape_df),
            "high_priority": len(high_priority),
            "medium_priority": len(medium_priority),
            "low_priority": len(low_priority),
            "top_domains": domain_counts.head(10).to_dict(),
            "priority_breakdown": {
                "high": high_priority["domain"].value_counts().head(5).to_dict(),
                "medium": medium_priority["domain"].value_counts().head(5).to_dict(),
                "low": low_priority["domain"].value_counts().head(5).to_dict(),
            },
        }

        self.logger.info(
            f"Scraping priorities: High={priority_analysis['high_priority']}, "
            f"Medium={priority_analysis['medium_priority']}, "
            f"Low={priority_analysis['low_priority']}"
        )

        return priority_analysis

    def prepare_scraping_batches(
        self, rescrape_df: pd.DataFrame, batch_size: int = 500
    ) -> List[pd.DataFrame]:
        """Prepare scraping batches with priority ordering."""
        self.logger.info("Preparing scraping batches...")

        # Sort by priority
        news_domains = [
            "politiken.dk",
            "berlingske.dk",
            "information.dk",
            "dr.dk",
            "tv2.dk",
            "ekstrabladet.dk",
            "bt.dk",
            "jyllands-posten.dk",
            "weekendavisen.dk",
        ]

        # Add priority score
        rescrape_df["priority_score"] = (
            rescrape_df["domain"].isin(news_domains).astype(int)
            * 1000  # News sites first
            + rescrape_df["domain"].str.contains("\.dk$", regex=True).astype(int)
            * 100  # Danish sites
            + (rescrape_df["old_word_count"].fillna(0) > 0).astype(int)
            * 10  # Had some content before
        )

        # Sort by priority
        rescrape_df = rescrape_df.sort_values("priority_score", ascending=False)

        # Create batches
        batches = []
        for i in range(0, len(rescrape_df), batch_size):
            batch = rescrape_df.iloc[i : i + batch_size].copy()
            batches.append(batch)

        self.logger.info(
            f"Created {len(batches)} scraping batches of max {batch_size} URLs each"
        )
        return batches

    def scrape_batch(self, batch_df: pd.DataFrame, batch_num: int) -> pd.DataFrame:
        """Scrape a single batch of URLs."""
        self.logger.info(f"Scraping batch {batch_num} ({len(batch_df)} URLs)")

        # Prepare URL pairs (original, resolved)
        url_pairs = [
            (row["original_url"], row["new_resolved_url"])
            for _, row in batch_df.iterrows()
        ]

        # Scrape the batch
        scraping_results = self.scraper.scrape_urls_batch(url_pairs)

        # Convert results to DataFrame
        results_df = pd.DataFrame(scraping_results)

        # Merge with original batch data to preserve metadata
        merged_df = batch_df.merge(
            results_df,
            left_on="new_resolved_url",
            right_on="resolved_url",
            how="left",
            suffixes=("_old", "_new"),
        )

        # Clean up column names
        merged_df = merged_df.rename(
            columns={"original_url_new": "original_url", "resolved_url": "resolved_url"}
        )

        return merged_df

    def run_progressive_scraping(
        self, rescrape_df: pd.DataFrame, max_batches: int = None
    ) -> List[pd.DataFrame]:
        """Run progressive scraping with batch-by-batch processing."""
        self.logger.info("Starting progressive scraping...")

        batches = self.prepare_scraping_batches(rescrape_df)

        if max_batches:
            batches = batches[:max_batches]
            self.logger.info(f"Limited to first {max_batches} batches for testing")

        scraped_batches = []

        for i, batch_df in enumerate(batches, 1):
            try:
                scraped_batch = self.scrape_batch(batch_df, i)
                scraped_batches.append(scraped_batch)

                # Save intermediate results
                batch_file = os.path.join(self.output_dir, f"scraped_batch_{i:03d}.csv")
                scraped_batch.to_csv(batch_file, index=False)
                self.logger.info(f"Saved batch {i} to {batch_file}")

                # Generate batch report
                report = self.scraper.generate_scraping_report(
                    scraped_batch[
                        [
                            "content",
                            "error",
                            "word_count",
                            "paywall_detected",
                            "extraction_method",
                            "scraping_time",
                        ]
                    ].to_dict("records")
                )
                self.logger.info(
                    f"Batch {i} results: {report['success_rate']:.1f}% success, "
                    f"avg {report['avg_word_count']:.0f} words"
                )

            except Exception as e:
                self.logger.error(f"Error scraping batch {i}: {e}")
                continue

        return scraped_batches

    def combine_all_data(
        self, good_content_df: pd.DataFrame, scraped_batches: List[pd.DataFrame]
    ) -> pd.DataFrame:
        """Combine existing good content with newly scraped content."""
        self.logger.info("Combining all data...")

        # Combine all scraped batches
        if scraped_batches:
            new_scraped_df = pd.concat(scraped_batches, ignore_index=True)
            self.logger.info(
                f"Combined {len(scraped_batches)} scraped batches: {len(new_scraped_df)} URLs"
            )
        else:
            new_scraped_df = pd.DataFrame()

        # Standardize columns for good content data
        good_content_standardized = good_content_df.copy()
        good_content_standardized["content"] = good_content_standardized["old_content"]
        good_content_standardized["resolved_url"] = good_content_standardized[
            "new_resolved_url"
        ]
        good_content_standardized["word_count"] = good_content_standardized[
            "old_word_count"
        ]
        good_content_standardized["status_code"] = good_content_standardized[
            "old_status_code"
        ]
        good_content_standardized["paywall_detected"] = good_content_standardized[
            "old_paywall_detected"
        ]
        good_content_standardized["scraped_source"] = "existing_good_content"

        # Standardize columns for newly scraped data
        if not new_scraped_df.empty:
            new_scraped_df["scraped_source"] = "newly_scraped"

        # Combine datasets
        if not new_scraped_df.empty:
            final_df = pd.concat(
                [good_content_standardized, new_scraped_df], ignore_index=True
            )
        else:
            final_df = good_content_standardized

        self.logger.info(f"Final combined dataset: {len(final_df)} URLs")
        return final_df

    def generate_final_report(
        self, final_df: pd.DataFrame, priority_analysis: Dict
    ) -> str:
        """Generate comprehensive final report."""
        report_file = os.path.join(self.output_dir, "final_integration_report.txt")

        with open(report_file, "w") as f:
            f.write("Final Integration Pipeline Report\n")
            f.write("=" * 50 + "\n")
            f.write(f"Generated: {datetime.now().isoformat()}\n\n")

            # Dataset overview
            f.write("Final Dataset Overview:\n")
            f.write(f"Total URLs: {len(final_df)}\n")
            f.write(
                f"Existing good content: {len(final_df[final_df['scraped_source'] == 'existing_good_content'])}\n"
            )
            f.write(
                f"Newly scraped: {len(final_df[final_df['scraped_source'] == 'newly_scraped'])}\n\n"
            )

            # Content quality metrics
            total_words = final_df["word_count"].sum()
            avg_words = final_df["word_count"].mean()
            urls_with_content = len(final_df[final_df["word_count"] > 10])

            f.write("Content Quality:\n")
            f.write(f"Total words extracted: {total_words:,}\n")
            f.write(f"Average words per URL: {avg_words:.1f}\n")
            f.write(f"URLs with substantial content (>10 words): {urls_with_content}\n")
            f.write(
                f"Content coverage: {(urls_with_content / len(final_df) * 100):.1f}%\n\n"
            )

            # Paywall analysis
            paywall_detected = final_df["paywall_detected"].sum()
            f.write("Paywall Analysis:\n")
            f.write(f"URLs with paywall detected: {paywall_detected}\n")
            f.write(
                f"Paywall rate: {(paywall_detected / len(final_df) * 100):.1f}%\n\n"
            )

            # Domain breakdown
            f.write("Top 15 Domains by URL Count:\n")
            domain_counts = final_df["domain"].value_counts().head(15)
            for domain, count in domain_counts.items():
                avg_words_domain = final_df[final_df["domain"] == domain][
                    "word_count"
                ].mean()
                f.write(
                    f"  {domain}: {count} URLs (avg {avg_words_domain:.0f} words)\n"
                )

            # Priority analysis
            f.write(f"\nScraping Priority Analysis:\n")
            for key, value in priority_analysis.items():
                if isinstance(value, dict):
                    f.write(f"{key}:\n")
                    for k, v in value.items():
                        f.write(f"  {k}: {v}\n")
                else:
                    f.write(f"{key}: {value}\n")

        self.logger.info(f"Saved final report: {report_file}")
        return report_file

    def run_full_pipeline(self, max_wait_minutes: int = 60, max_batches: int = None):
        """Run the complete integration pipeline."""
        self.logger.info("Starting Final Integration Pipeline")

        # Step 1: Wait for URL resolution to complete
        if not self.wait_for_resolution(max_wait_minutes):
            self.logger.error("URL resolution did not complete. Exiting.")
            return None

        # Step 2: Load resolved data
        resolved_df, rescrape_df, good_content_df = self.load_resolved_data()

        # Step 3: Analyze scraping priorities
        priority_analysis = self.analyze_scraping_priorities(rescrape_df)

        # Step 4: Run progressive scraping
        scraped_batches = self.run_progressive_scraping(rescrape_df, max_batches)

        # Step 5: Combine all data
        final_df = self.combine_all_data(good_content_df, scraped_batches)

        # Step 6: Save final dataset
        final_file = os.path.join(self.output_dir, "final_complete_dataset.csv")
        final_df.to_csv(final_file, index=False)
        self.logger.info(f"Saved final dataset: {final_file}")

        # Step 7: Generate final report
        report_file = self.generate_final_report(final_df, priority_analysis)

        self.logger.info("Final Integration Pipeline completed successfully!")

        return final_df, report_file


def main():
    """Main execution function."""
    pipeline = FinalIntegrationPipeline()

    print("=" * 60)
    print("FINAL INTEGRATION PIPELINE")
    print("=" * 60)
    print("This pipeline will:")
    print("1. Wait for URL resolution to complete")
    print("2. Load resolved URL data")
    print("3. Analyze scraping priorities")
    print("4. Run progressive scraping on URLs needing re-scraping")
    print("5. Combine with existing good content")
    print("6. Generate final dataset and report")
    print("=" * 60)

    # Run with limited batches for testing
    result = pipeline.run_full_pipeline(max_wait_minutes=60, max_batches=5)

    if result:
        final_df, report_file = result
        print(f"\n✅ Pipeline completed successfully!")
        print(f"📊 Final dataset: {len(final_df)} URLs")
        print(f"📄 Report saved to: {report_file}")
        print(f"📁 Output directory: data/final_dataset/")
    else:
        print("❌ Pipeline failed")


if __name__ == "__main__":
    main()
