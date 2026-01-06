#!/usr/bin/env python3
"""
Enhanced Browser Content Scraping Pipeline V2
==============================================
Improved version with:
- Enhanced unique URL detection
- Better stop/restart functionality
- Robust progress tracking
- Signal handling for clean shutdown
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
import signal
from concurrent.futures import ThreadPoolExecutor, as_completed


class EnhancedBrowserContentScraper:
    """
    Enhanced content scraper with improved unique URL handling and stop/restart functionality.
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
        self.url_tracker_file = self.output_dir / "processed_urls.txt"

        # Enhanced URL tracking for uniqueness
        self.processed_urls = set()
        self.processed_url_hashes = set()
        self.load_processed_urls()

        # Load existing content to avoid duplicates
        self.existing_urls = set()
        self.load_existing_content()

        # Combine all unique URLs
        self.all_unique_urls = self.processed_urls.union(self.existing_urls)

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
            "urls_in_current_batch": 0,
            "urls_processed_in_batch": 0,
            "last_processed_url": "",
            "estimated_completion": None,
        }

        # Signal handling for clean shutdown
        self.shutdown_requested = False
        self.setup_signal_handlers()

        self.logger.info(f"Enhanced Browser Content Scraper V2 initialized")
        self.logger.info(f"Data directory: {self.data_dir}")
        self.logger.info(f"Output directory: {self.output_dir}")
        self.logger.info(f"Previously processed URLs: {len(self.processed_urls):,}")
        self.logger.info(
            f"Existing URLs from other sources: {len(self.existing_urls):,}"
        )
        self.logger.info(f"Total unique URLs to skip: {len(self.all_unique_urls):,}")

    def setup_signal_handlers(self):
        """Setup signal handlers for clean shutdown."""

        def signal_handler(signum, frame):
            self.logger.info(f"Received signal {signum}. Initiating clean shutdown...")
            self.shutdown_requested = True

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    def setup_logging(self):
        """Setup comprehensive logging."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = self.log_dir / f"enhanced_browser_scraping_{timestamp}.log"

        # Setup logging to both file and console
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[logging.FileHandler(log_file), logging.StreamHandler(sys.stdout)],
        )
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"Enhanced logging initialized - log file: {log_file}")

    def normalize_url(self, url: str) -> str:
        """Normalize URL for consistent duplicate detection."""
        if not url:
            return ""

        url = url.strip().lower()

        # Remove common tracking parameters
        tracking_params = [
            "utm_source",
            "utm_medium",
            "utm_campaign",
            "utm_content",
            "utm_term",
            "fbclid",
            "gclid",
        ]

        try:
            from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

            parsed = urlparse(url)

            # Remove tracking parameters
            query_params = parse_qs(parsed.query)
            cleaned_params = {
                k: v for k, v in query_params.items() if k not in tracking_params
            }

            # Rebuild URL
            cleaned_query = urlencode(cleaned_params, doseq=True)
            normalized = urlunparse(
                (
                    parsed.scheme,
                    parsed.netloc,
                    parsed.path.rstrip("/"),  # Remove trailing slash
                    parsed.params,
                    cleaned_query,
                    "",  # Remove fragment
                )
            )

            return normalized

        except Exception:
            return url

    def load_processed_urls(self):
        """Load previously processed URLs from tracker file."""
        if self.url_tracker_file.exists():
            try:
                with open(self.url_tracker_file, "r", encoding="utf-8") as f:
                    for line in f:
                        url = line.strip()
                        if url:
                            normalized = self.normalize_url(url)
                            self.processed_urls.add(url)
                            self.processed_urls.add(normalized)
                            self.processed_url_hashes.add(
                                hashlib.md5(normalized.encode()).hexdigest()
                            )

                self.logger.info(
                    f"Loaded {len(self.processed_urls):,} previously processed URLs"
                )
            except Exception as e:
                self.logger.warning(f"Error loading processed URLs: {e}")

    def save_processed_url(self, url: str):
        """Save a processed URL to the tracker file."""
        try:
            with open(self.url_tracker_file, "a", encoding="utf-8") as f:
                f.write(f"{url}\n")

            # Also add to memory sets
            normalized = self.normalize_url(url)
            self.processed_urls.add(url)
            self.processed_urls.add(normalized)
            self.all_unique_urls.add(url)
            self.all_unique_urls.add(normalized)

        except Exception as e:
            self.logger.error(f"Error saving processed URL: {e}")

    def load_existing_content(self):
        """Load existing scraped content to avoid duplicates."""
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

                    for col in ["url", "resolved_url"]:
                        if col in df.columns:
                            urls = df[col].dropna().tolist()
                            for url in urls:
                                normalized = self.normalize_url(str(url))
                                self.existing_urls.add(str(url))
                                self.existing_urls.add(normalized)

                    self.logger.info(f"Loaded URLs from {file_path}")
                except Exception as e:
                    self.logger.warning(f"Error loading {file_path}: {e}")

    def is_url_processed(self, original_url: str, resolved_url: str) -> bool:
        """Check if URL has been processed before (enhanced duplicate detection)."""
        urls_to_check = [original_url, resolved_url]

        for url in urls_to_check:
            if not url:
                continue

            normalized = self.normalize_url(url)
            url_hash = hashlib.md5(normalized.encode()).hexdigest()

            # Check in all unique URL sets
            if (
                url in self.all_unique_urls
                or normalized in self.all_unique_urls
                or url_hash in self.processed_url_hashes
            ):
                return True

        return False

    def get_resolved_batch_files(self) -> List[Path]:
        """Get all resolved browser batch files in order."""
        pattern = "resolved_browser_batch_*.csv"
        batch_files = list(self.data_dir.glob(f"complete_resolved/{pattern}"))

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
            # Check for shutdown request
            if self.shutdown_requested:
                return None

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

            # Filter out unsuccessful resolutions
            if "resolution_success" in df.columns:
                df = df[df["resolution_success"] == True]
                self.logger.info(f"Filtered to {len(df)} successfully resolved URLs")

            # Enhanced duplicate filtering
            urls_to_process = []
            for _, row in df.iterrows():
                original_url = row["original_url"]
                resolved_url = row.get("resolved_url", original_url)

                # Enhanced duplicate check
                if not self.is_url_processed(original_url, resolved_url):
                    urls_to_process.append((original_url, resolved_url))
                else:
                    self.stats["duplicates_skipped"] += 1

            self.logger.info(
                f"Processing {len(urls_to_process)} new URLs from batch {batch_num}"
            )
            self.stats["urls_in_current_batch"] = len(urls_to_process)
            self.stats["urls_processed_in_batch"] = 0

            if not urls_to_process:
                self.logger.info(f"No new URLs to process in batch {batch_num}")
                return 0

            # Process URLs with enhanced tracking
            scraped_results = []
            for i, (original_url, resolved_url) in enumerate(urls_to_process):
                # Check for shutdown request
                if self.shutdown_requested:
                    self.logger.info(
                        "Shutdown requested. Saving progress and exiting..."
                    )
                    break

                try:
                    result = self.scrape_url_content(original_url, resolved_url)

                    if result is None:  # Shutdown requested
                        break

                    scraped_results.append(result)

                    # Save processed URL immediately
                    self.save_processed_url(original_url)
                    if resolved_url != original_url:
                        self.save_processed_url(resolved_url)

                    # Update stats
                    self.stats["total_processed"] += 1
                    self.stats["urls_processed_in_batch"] = i + 1
                    self.stats["last_processed_url"] = original_url

                    if result["status_code"] == 200 and result["content"]:
                        self.stats["successful_scrapes"] += 1
                    else:
                        self.stats["failed_scrapes"] += 1

                    # Progress logging every 50 URLs
                    if (i + 1) % 50 == 0:
                        self.logger.info(
                            f"  Processed {i + 1}/{len(urls_to_process)} URLs in batch {batch_num}"
                        )
                        self.save_progress_immediate(batch_num)

                    # Rate limiting
                    time.sleep(0.5)

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

    def save_progress_immediate(self, current_batch: int):
        """Save progress immediately (called frequently)."""
        try:
            progress_data = {
                "current_batch": current_batch,
                "stats": self.stats,
                "timestamp": datetime.now().isoformat(),
                "shutdown_clean": False,
            }

            with open(self.progress_file, "w") as f:
                json.dump(progress_data, f, indent=2)
        except Exception as e:
            self.logger.error(f"Error saving progress: {e}")

    def save_progress(
        self, current_batch: int, total_batches: int, clean_shutdown: bool = False
    ):
        """Save comprehensive progress."""
        progress_data = {
            "current_batch": current_batch,
            "total_batches": total_batches,
            "stats": self.stats,
            "timestamp": datetime.now().isoformat(),
            "shutdown_clean": clean_shutdown,
            "completion_percentage": (current_batch / total_batches * 100)
            if total_batches > 0
            else 0,
        }

        with open(self.progress_file, "w") as f:
            json.dump(progress_data, f, indent=2)

    def load_progress(self) -> int:
        """Load previous progress if exists."""
        if self.progress_file.exists():
            try:
                with open(self.progress_file, "r") as f:
                    progress_data = json.load(f)

                # Load stats if available
                if "stats" in progress_data:
                    saved_stats = progress_data["stats"]
                    for key in saved_stats:
                        if key in self.stats:
                            self.stats[key] = saved_stats[key]

                return progress_data.get("current_batch", 0)
            except Exception as e:
                self.logger.warning(f"Could not load progress: {e}")
        return 0

    def run_scraping_pipeline(self, start_batch: int = 0, max_batches: int = None):
        """Run the enhanced scraping pipeline with better stop/restart functionality."""
        self.stats["start_time"] = datetime.now().isoformat()
        self.logger.info(
            "=== Starting Enhanced Browser Content Scraping Pipeline V2 ==="
        )

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

        total_batches = len(self.get_resolved_batch_files())

        # Process batches
        for i, batch_file in enumerate(batch_files):
            # Check for shutdown request
            if self.shutdown_requested:
                self.logger.info("Shutdown requested. Performing clean exit...")
                break

            batch_num = start_batch + i + 1
            self.stats["current_batch"] = batch_num

            self.logger.info(f"\n{'=' * 60}")
            self.logger.info(
                f"BATCH {batch_num}/{total_batches} ({((batch_num / total_batches) * 100):.1f}%)"
            )
            self.logger.info(f"{'=' * 60}")

            # Process batch
            results_count = self.process_batch(batch_file, batch_num)

            # Save progress
            self.save_progress(
                batch_num, total_batches, clean_shutdown=self.shutdown_requested
            )

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

            # Check for shutdown request after each batch
            if self.shutdown_requested:
                break

        # Final summary
        self.logger.info("\n" + "=" * 60)
        if self.shutdown_requested:
            self.logger.info("SCRAPING PIPELINE STOPPED (CLEAN SHUTDOWN)")
            self.save_progress(
                self.stats["current_batch"], total_batches, clean_shutdown=True
            )
        else:
            self.logger.info("SCRAPING PIPELINE COMPLETED")
        self.logger.info("=" * 60)
        self.logger.info(f"Total URLs processed: {self.stats['total_processed']:,}")
        self.logger.info(f"Successful scrapes: {self.stats['successful_scrapes']:,}")
        self.logger.info(f"Failed scrapes: {self.stats['failed_scrapes']:,}")
        self.logger.info(f"Duplicates skipped: {self.stats['duplicates_skipped']:,}")
        self.logger.info(f"Output file: {self.scraped_content_file}")
        self.logger.info(f"URL tracker: {self.url_tracker_file}")


def main():
    parser = argparse.ArgumentParser(
        description="Enhanced Browser Content Scraping Pipeline V2"
    )
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
    scraper = EnhancedBrowserContentScraper(
        data_dir=args.data_dir, output_dir=args.output_dir, log_dir=args.log_dir
    )

    scraper.run_scraping_pipeline(
        start_batch=args.start_batch, max_batches=args.max_batches
    )


if __name__ == "__main__":
    main()
