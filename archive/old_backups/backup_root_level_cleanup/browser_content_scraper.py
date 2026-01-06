#!/usr/bin/env python3
"""
Browser Content Scraping Pipeline
=================================
Processes resolved URL batches and scrapes content with duplicate checking,
proper logging, and resumable batch processing.
"""

import pandas as pd
import numpy as np
from pathlib import Path
import requests
import trafilatura
import time
import json
import hashlib
from datetime import datetime
from typing import Dict, List, Set, Optional, Tuple
import re
from urllib.parse import urlparse
import logging
import sys
import os
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed


class BrowserContentScraper:
    """
    Simple, robust content scraper for resolved URL batches.
    """

    def __init__(self, data_dir: str, output_dir: str, log_dir: str = "logs"):
        self.data_dir = Path(data_dir)
        self.output_dir = Path(output_dir)
        self.log_dir = Path(log_dir)

        # Create directories
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.log_dir.mkdir(parents=True, exist_ok=True)

        # Setup logging
        self.setup_logging()

        # Output files
        self.scraped_content_file = (
            self.output_dir / "scraped_content_from_resolved.csv"
        )
        self.progress_file = self.output_dir / "scraping_progress.json"

        # Load existing content to avoid duplicates
        self.existing_urls = set()
        self.load_existing_content()

        # Session for requests
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "da,en-US;q=0.7,en;q=0.3",
                "Accept-Encoding": "gzip, deflate",
                "DNT": "1",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
            }
        )

        # Statistics
        self.stats = {
            "total_processed": 0,
            "successful_scrapes": 0,
            "failed_scrapes": 0,
            "duplicates_skipped": 0,
            "start_time": None,
            "current_batch": 0,
        }

        self.logger.info(f"Browser Content Scraper initialized")
        self.logger.info(f"Data directory: {self.data_dir}")
        self.logger.info(f"Output directory: {self.output_dir}")
        self.logger.info(f"Existing URLs loaded: {len(self.existing_urls):,}")

    def setup_logging(self):
        """Setup comprehensive logging."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = self.log_dir / f"browser_content_scraping_{timestamp}.log"

        # Setup logging to both file and console
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[logging.FileHandler(log_file), logging.StreamHandler(sys.stdout)],
        )
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"Logging initialized - log file: {log_file}")

    def load_existing_content(self):
        """Load existing scraped content to avoid duplicates."""
        # Check multiple possible locations for existing content
        existing_files = [
            self.scraped_content_file,
            self.data_dir
            / "browser_urlextract"
            / "scraped_content"
            / "scraped_content.csv",
            self.data_dir
            / "browser_urlextract"
            / "scraped_content_enhanced"
            / "scraped_content.csv",
        ]

        for file_path in existing_files:
            if file_path.exists():
                try:
                    df = pd.read_csv(file_path)
                    if "url" in df.columns:
                        urls = set(df["url"].dropna().tolist())
                        self.existing_urls.update(urls)
                        self.logger.info(
                            f"Loaded {len(urls):,} existing URLs from {file_path}"
                        )
                    if "resolved_url" in df.columns:
                        resolved_urls = set(df["resolved_url"].dropna().tolist())
                        self.existing_urls.update(resolved_urls)
                        self.logger.info(
                            f"Loaded {len(resolved_urls):,} resolved URLs from {file_path}"
                        )
                except Exception as e:
                    self.logger.warning(f"Error loading {file_path}: {e}")

        self.logger.info(f"Total unique existing URLs: {len(self.existing_urls):,}")

    def get_resolved_batch_files(self) -> List[Path]:
        """Get all resolved browser batch files in order."""
        pattern = "resolved_browser_batch_*.csv"
        batch_files = list(self.data_dir.glob(f"complete_resolved/{pattern}"))

        # Sort by batch number
        def extract_batch_num(path):
            try:
                return int(path.stem.split("_")[-1])
            except:
                return 0

        batch_files.sort(key=extract_batch_num)
        self.logger.info(f"Found {len(batch_files)} batch files to process")
        return batch_files

    def scrape_url_content(self, url: str, resolved_url: str) -> Dict:
        """Scrape content from a single URL."""
        start_time = time.time()

        # Use resolved URL for scraping if available, fallback to original
        scrape_url = resolved_url if resolved_url and resolved_url != url else url

        try:
            # Make request
            response = self.session.get(scrape_url, timeout=30, allow_redirects=True)
            response_time = time.time() - start_time

            if response.status_code == 200:
                # Extract content using trafilatura
                content = trafilatura.extract(
                    response.text,
                    include_comments=False,
                    include_tables=True,
                    favor_precision=True,
                )

                # Extract metadata
                metadata = trafilatura.extract_metadata(response.text)

                # Generate content hash for deduplication
                content_hash = hashlib.md5(
                    (content or "").encode("utf-8", errors="ignore")
                ).hexdigest()

                return {
                    "url": url,
                    "resolved_url": scrape_url,
                    "status_code": response.status_code,
                    "content": content or "",
                    "title": metadata.title if metadata else "",
                    "author": metadata.author if metadata else "",
                    "date": metadata.date if metadata else "",
                    "description": metadata.description if metadata else "",
                    "language": metadata.language if metadata else "",
                    "word_count": len((content or "").split()) if content else 0,
                    "content_hash": content_hash,
                    "extraction_method": "trafilatura",
                    "scrape_time": datetime.now().isoformat(),
                    "response_time": response_time,
                    "error": "",
                    "paywall_detected": False,
                    "paywall_type": "",
                    "paywall_confidence": 0.0,
                    "paywall_indicators": "",
                    "paywall_snippet": "",
                }
            else:
                return {
                    "url": url,
                    "resolved_url": scrape_url,
                    "status_code": response.status_code,
                    "content": "",
                    "title": "",
                    "author": "",
                    "date": "",
                    "description": "",
                    "language": "",
                    "word_count": 0,
                    "content_hash": "",
                    "extraction_method": "",
                    "scrape_time": datetime.now().isoformat(),
                    "response_time": response_time,
                    "error": f"HTTP {response.status_code}",
                    "paywall_detected": False,
                    "paywall_type": "",
                    "paywall_confidence": 0.0,
                    "paywall_indicators": "",
                    "paywall_snippet": "",
                }

        except Exception as e:
            response_time = time.time() - start_time
            return {
                "url": url,
                "resolved_url": scrape_url,
                "status_code": 0,
                "content": "",
                "title": "",
                "author": "",
                "date": "",
                "description": "",
                "language": "",
                "word_count": 0,
                "content_hash": "",
                "extraction_method": "",
                "scrape_time": datetime.now().isoformat(),
                "response_time": response_time,
                "error": str(e),
                "paywall_detected": False,
                "paywall_type": "",
                "paywall_confidence": 0.0,
                "paywall_indicators": "",
                "paywall_snippet": "",
            }

    def process_batch(self, batch_file: Path, batch_num: int) -> int:
        """Process a single batch file."""
        self.logger.info(f"Processing batch {batch_num}: {batch_file.name}")

        try:
            # Load batch data
            df = pd.read_csv(batch_file)
            self.logger.info(f"Loaded {len(df)} URLs from batch {batch_num}")

            # Filter out already processed URLs and unsuccessful resolutions
            if "resolution_success" in df.columns:
                df = df[df["resolution_success"] == True]
                self.logger.info(f"Filtered to {len(df)} successfully resolved URLs")

            # Remove duplicates based on resolved_url
            urls_to_process = []
            for _, row in df.iterrows():
                original_url = row["original_url"]
                resolved_url = row.get("resolved_url", original_url)

                # Check if already processed
                if (
                    original_url not in self.existing_urls
                    and resolved_url not in self.existing_urls
                ):
                    urls_to_process.append((original_url, resolved_url))
                else:
                    self.stats["duplicates_skipped"] += 1

            self.logger.info(
                f"Processing {len(urls_to_process)} new URLs from batch {batch_num}"
            )

            if not urls_to_process:
                self.logger.info(f"No new URLs to process in batch {batch_num}")
                return 0

            # Process URLs with rate limiting
            scraped_results = []
            for i, (original_url, resolved_url) in enumerate(urls_to_process):
                try:
                    result = self.scrape_url_content(original_url, resolved_url)
                    scraped_results.append(result)

                    # Update stats
                    self.stats["total_processed"] += 1
                    if result["status_code"] == 200 and result["content"]:
                        self.stats["successful_scrapes"] += 1
                    else:
                        self.stats["failed_scrapes"] += 1

                    # Add to existing URLs to prevent re-processing
                    self.existing_urls.add(original_url)
                    self.existing_urls.add(resolved_url)

                    # Progress logging
                    if (i + 1) % 100 == 0:
                        self.logger.info(
                            f"  Processed {i + 1}/{len(urls_to_process)} URLs in batch {batch_num}"
                        )

                    # Rate limiting
                    time.sleep(0.5)  # 0.5 second delay between requests

                except Exception as e:
                    self.logger.error(f"Error processing URL {original_url}: {e}")
                    self.stats["failed_scrapes"] += 1
                    continue

            # Save batch results
            if scraped_results:
                batch_df = pd.DataFrame(scraped_results)

                # Append to main file or create if doesn't exist
                if self.scraped_content_file.exists():
                    batch_df.to_csv(
                        self.scraped_content_file, mode="a", index=False, header=False
                    )
                else:
                    batch_df.to_csv(self.scraped_content_file, index=False)

                self.logger.info(
                    f"Saved {len(scraped_results)} results from batch {batch_num}"
                )

            return len(scraped_results)

        except Exception as e:
            self.logger.error(f"Error processing batch {batch_file}: {e}")
            return 0

    def save_progress(self, current_batch: int, total_batches: int):
        """Save current progress."""
        progress_data = {
            "current_batch": current_batch,
            "total_batches": total_batches,
            "stats": self.stats,
            "timestamp": datetime.now().isoformat(),
        }

        with open(self.progress_file, "w") as f:
            json.dump(progress_data, f, indent=2)

    def load_progress(self) -> int:
        """Load previous progress if exists."""
        if self.progress_file.exists():
            try:
                with open(self.progress_file, "r") as f:
                    progress_data = json.load(f)
                return progress_data.get("current_batch", 0)
            except Exception as e:
                self.logger.warning(f"Could not load progress: {e}")
        return 0

    def run_scraping_pipeline(self, start_batch: int = 0, max_batches: int = None):
        """Run the complete scraping pipeline."""
        self.stats["start_time"] = datetime.now().isoformat()
        self.logger.info("=== Starting Browser Content Scraping Pipeline ===")

        # Get batch files
        batch_files = self.get_resolved_batch_files()

        if not batch_files:
            self.logger.error("No batch files found!")
            return

        # Resume from previous run if requested
        if start_batch == 0:
            start_batch = self.load_progress()
            if start_batch > 0:
                self.logger.info(f"Resuming from batch {start_batch}")

        # Limit batches if specified
        if max_batches:
            batch_files = batch_files[start_batch : start_batch + max_batches]
            self.logger.info(
                f"Processing {len(batch_files)} batches (limited by max_batches={max_batches})"
            )
        else:
            batch_files = batch_files[start_batch:]
            self.logger.info(
                f"Processing {len(batch_files)} batches from batch {start_batch}"
            )

        # Process batches
        for i, batch_file in enumerate(batch_files):
            batch_num = start_batch + i + 1
            self.stats["current_batch"] = batch_num

            self.logger.info(f"\n{'=' * 50}")
            self.logger.info(
                f"BATCH {batch_num}/{len(self.get_resolved_batch_files())}"
            )
            self.logger.info(f"{'=' * 50}")

            # Process batch
            results_count = self.process_batch(batch_file, batch_num)

            # Save progress
            self.save_progress(batch_num, len(self.get_resolved_batch_files()))

            # Log current statistics
            self.logger.info(
                f"Batch {batch_num} completed: {results_count} URLs processed"
            )
            self.logger.info(
                f"Overall progress: {self.stats['total_processed']:,} processed, "
                f"{self.stats['successful_scrapes']:,} successful, "
                f"{self.stats['failed_scrapes']:,} failed, "
                f"{self.stats['duplicates_skipped']:,} duplicates skipped"
            )

        # Final summary
        self.logger.info("\n" + "=" * 50)
        self.logger.info("SCRAPING PIPELINE COMPLETED")
        self.logger.info("=" * 50)
        self.logger.info(f"Total URLs processed: {self.stats['total_processed']:,}")
        self.logger.info(f"Successful scrapes: {self.stats['successful_scrapes']:,}")
        self.logger.info(f"Failed scrapes: {self.stats['failed_scrapes']:,}")
        self.logger.info(f"Duplicates skipped: {self.stats['duplicates_skipped']:,}")
        self.logger.info(f"Output file: {self.scraped_content_file}")


def main():
    parser = argparse.ArgumentParser(description="Browser Content Scraping Pipeline")
    parser.add_argument(
        "--data-dir",
        default="/work/Datadonationer/urlextraction_scraping/data",
        help="Data directory containing resolved batches",
    )
    parser.add_argument(
        "--output-dir",
        default="/work/Datadonationer/urlextraction_scraping/data/browser_scraped_final",
        help="Output directory for scraped content",
    )
    parser.add_argument(
        "--log-dir",
        default="/work/Datadonationer/urlextraction_scraping/logs",
        help="Log directory",
    )
    parser.add_argument(
        "--start-batch",
        type=int,
        default=0,
        help="Batch number to start from (0 = auto-resume)",
    )
    parser.add_argument(
        "--max-batches",
        type=int,
        default=None,
        help="Maximum number of batches to process",
    )

    args = parser.parse_args()

    # Create and run scraper
    scraper = BrowserContentScraper(
        data_dir=args.data_dir, output_dir=args.output_dir, log_dir=args.log_dir
    )

    scraper.run_scraping_pipeline(
        start_batch=args.start_batch, max_batches=args.max_batches
    )


if __name__ == "__main__":
    main()
