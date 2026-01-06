#!/usr/bin/env python3
"""
Turbo Browser Content Scraping Pipeline
=======================================
Ultra-fast version with:
- Parallel request processing (ThreadPoolExecutor)
- Optimized timeouts and retries
- Smart batch processing
- Enhanced performance monitoring
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
import threading
from queue import Queue


class TurboBrowserContentScraper:
    """
    Ultra-fast content scraper with parallel processing and optimized performance.
    """

    def __init__(
        self,
        data_dir: str,
        output_dir: str,
        log_dir: str,
        max_workers: int = 20,  # Parallel threads
        timeout: int = 15,  # Reduced timeout
        max_batches: Optional[int] = None,
    ):
        self.data_dir = Path(data_dir)
        self.output_dir = Path(output_dir)
        self.log_dir = Path(log_dir)
        self.max_workers = max_workers
        self.timeout = timeout
        self.max_batches = max_batches

        # Create directories
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.log_dir.mkdir(parents=True, exist_ok=True)

        # Setup logging
        self.setup_logging()

        # File paths
        self.scraped_content_file = self.output_dir / "scraped_content_turbo.csv"
        self.progress_file = self.output_dir / "turbo_scraping_progress.json"
        self.url_tracker_file = self.output_dir / "turbo_processed_urls.txt"

        # Enhanced URL tracking
        self.processed_urls = set()
        self.processed_url_hashes = set()
        self.url_lock = threading.Lock()  # Thread-safe URL tracking
        self.load_processed_urls()

        # Load existing content
        self.existing_urls = set()
        self.load_existing_content()

        # Combine all unique URLs
        self.all_unique_urls = self.processed_urls.union(self.existing_urls)

        # Create session factory for thread-safe requests
        self.session_local = threading.local()

        # Enhanced statistics with thread safety
        self.stats_lock = threading.Lock()
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
            "urls_per_second": 0.0,
            "parallel_workers": max_workers,
        }

        # Signal handling
        self.shutdown_requested = False
        self.setup_signal_handlers()

        self.logger.info(f"🚀 Turbo Browser Content Scraper initialized")
        self.logger.info(f"⚡ Parallel workers: {self.max_workers}")
        self.logger.info(f"⏱️  Timeout: {self.timeout}s")
        self.logger.info(f"📁 Data directory: {self.data_dir}")
        self.logger.info(f"📤 Output directory: {self.output_dir}")
        self.logger.info(f"🔄 Previously processed URLs: {len(self.processed_urls):,}")
        self.logger.info(
            f"📊 Existing URLs from other sources: {len(self.existing_urls):,}"
        )
        self.logger.info(f"🎯 Total unique URLs to skip: {len(self.all_unique_urls):,}")

    def get_session(self):
        """Get thread-local session for thread-safe requests."""
        if not hasattr(self.session_local, "session"):
            session = requests.Session()
            session.headers.update(
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
            # Configure adapter for better performance
            adapter = requests.adapters.HTTPAdapter(
                pool_connections=10, pool_maxsize=10, pool_block=False
            )
            session.mount("http://", adapter)
            session.mount("https://", adapter)
            self.session_local.session = session
        return self.session_local.session

    def setup_signal_handlers(self):
        """Setup signal handlers for clean shutdown."""

        def signal_handler(signum, frame):
            self.logger.info(
                f"🛑 Received signal {signum}. Initiating turbo shutdown..."
            )
            self.shutdown_requested = True

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    def setup_logging(self):
        """Setup turbo logging."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = self.log_dir / f"turbo_browser_scraping_{timestamp}.log"

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[
                logging.FileHandler(log_file, encoding="utf-8"),
                logging.StreamHandler(sys.stdout),
            ],
        )
        self.logger = logging.getLogger(__name__)

    def normalize_url(self, url: str) -> str:
        """Enhanced URL normalization for better duplicate detection."""
        if not url:
            return ""

        try:
            # Remove common tracking parameters
            tracking_params = {
                "utm_source",
                "utm_medium",
                "utm_campaign",
                "utm_term",
                "utm_content",
                "fbclid",
                "gclid",
                "ref",
                "source",
                "campaign",
                "medium",
            }

            parsed = urlparse(url.lower().strip())
            if parsed.query:
                query_params = []
                for param in parsed.query.split("&"):
                    if "=" in param:
                        key = param.split("=")[0]
                        if key not in tracking_params:
                            query_params.append(param)
                query_string = "&".join(query_params) if query_params else ""
            else:
                query_string = ""

            normalized = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
            if query_string:
                normalized += f"?{query_string}"

            return normalized
        except Exception:
            return url.lower().strip()

    def load_processed_urls(self):
        """Load previously processed URLs."""
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
                    f"📥 Loaded {len(self.processed_urls):,} previously processed URLs"
                )
            except Exception as e:
                self.logger.warning(f"⚠️  Error loading processed URLs: {e}")

    def save_processed_url(self, url: str):
        """Thread-safe processed URL saving."""
        try:
            with self.url_lock:
                with open(self.url_tracker_file, "a", encoding="utf-8") as f:
                    f.write(f"{url}\n")

                # Add to memory sets
                normalized = self.normalize_url(url)
                self.processed_urls.add(url)
                self.processed_urls.add(normalized)
                self.all_unique_urls.add(url)
                self.all_unique_urls.add(normalized)
        except Exception as e:
            self.logger.error(f"❌ Error saving processed URL: {e}")

    def load_existing_content(self):
        """Load existing scraped content to avoid duplicates."""
        existing_files = [
            self.scraped_content_file,
            self.output_dir / "scraped_content_from_resolved.csv",
            self.output_dir / "scraped_content.csv",
        ]

        for file_path in existing_files:
            if file_path.exists():
                try:
                    df = pd.read_csv(file_path)
                    for col in ["url", "resolved_url"]:
                        if col in df.columns:
                            urls = df[col].dropna().astype(str)
                            for url in urls:
                                normalized = self.normalize_url(url)
                                self.existing_urls.add(url)
                                self.existing_urls.add(normalized)
                except Exception as e:
                    self.logger.warning(
                        f"⚠️  Error loading existing content from {file_path}: {e}"
                    )

    def is_url_processed(self, original_url: str, resolved_url: str) -> bool:
        """Enhanced duplicate detection with thread safety."""
        with self.url_lock:
            urls_to_check = [original_url, resolved_url]

            for url in urls_to_check:
                if not url:
                    continue
                normalized = self.normalize_url(url)
                url_hash = hashlib.md5(normalized.encode()).hexdigest()

                if (
                    url in self.all_unique_urls
                    or normalized in self.all_unique_urls
                    or url_hash in self.processed_url_hashes
                ):
                    return True
            return False

    def scrape_url_content_turbo(self, url: str, resolved_url: str) -> Dict:
        """Ultra-fast URL content scraping with optimizations."""
        start_time = time.time()

        # Use resolved URL for scraping if available
        scrape_url = resolved_url if resolved_url and resolved_url != url else url

        try:
            # Check for shutdown
            if self.shutdown_requested:
                return None

            # Get thread-local session
            session = self.get_session()

            # Optimized request with shorter timeout
            response = session.get(
                scrape_url,
                timeout=self.timeout,
                allow_redirects=True,
                verify=False,  # Skip SSL verification for speed
                stream=False,  # Don't stream for small content
            )
            response_time = time.time() - start_time

            if response.status_code == 200:
                # Fast content extraction
                try:
                    content = trafilatura.extract(
                        response.text,
                        include_comments=False,
                        include_tables=True,
                        favor_precision=False,  # Speed over precision
                        favor_recall=True,  # Get more content faster
                    )
                except Exception:
                    content = None

                # Quick metadata extraction
                try:
                    metadata = trafilatura.extract_metadata(response.text)
                    title = metadata.title if metadata else ""
                    author = metadata.author if metadata else ""
                    date = metadata.date if metadata else ""
                except Exception:
                    title = author = date = ""

                # Fast content hash
                content_hash = hashlib.md5(
                    (content or "").encode("utf-8", errors="ignore")
                ).hexdigest()

                return {
                    "url": url,
                    "resolved_url": scrape_url,
                    "status_code": response.status_code,
                    "content": content or "",
                    "content_hash": content_hash,
                    "title": title,
                    "author": author,
                    "date": date,
                    "word_count": len((content or "").split()),
                    "content_type": response.headers.get("Content-Type", ""),
                    "response_time": response_time,
                    "error": "",
                    "scraped_at": datetime.now().isoformat(),
                }
            else:
                return {
                    "url": url,
                    "resolved_url": scrape_url,
                    "status_code": response.status_code,
                    "content": "",
                    "content_hash": "",
                    "title": "",
                    "author": "",
                    "date": "",
                    "word_count": 0,
                    "content_type": response.headers.get("Content-Type", ""),
                    "response_time": response_time,
                    "error": f"http_{response.status_code}",
                    "scraped_at": datetime.now().isoformat(),
                }

        except requests.exceptions.Timeout:
            return {
                "url": url,
                "resolved_url": scrape_url,
                "status_code": 0,
                "content": "",
                "content_hash": "",
                "title": "",
                "author": "",
                "date": "",
                "word_count": 0,
                "content_type": "",
                "response_time": time.time() - start_time,
                "error": "timeout",
                "scraped_at": datetime.now().isoformat(),
            }
        except Exception as e:
            return {
                "url": url,
                "resolved_url": scrape_url,
                "status_code": 0,
                "content": "",
                "content_hash": "",
                "title": "",
                "author": "",
                "date": "",
                "word_count": 0,
                "content_type": "",
                "response_time": time.time() - start_time,
                "error": str(e)[:200],  # Truncate long errors
                "scraped_at": datetime.now().isoformat(),
            }

    def update_stats(self, result: Dict):
        """Thread-safe statistics update."""
        with self.stats_lock:
            self.stats["total_processed"] += 1
            self.stats["urls_processed_in_batch"] += 1

            if result and result["status_code"] == 200 and result["content"]:
                self.stats["successful_scrapes"] += 1
            else:
                self.stats["failed_scrapes"] += 1

            # Calculate processing speed
            if self.stats["start_time"]:
                elapsed = time.time() - self.stats["start_time"]
                self.stats["urls_per_second"] = self.stats["total_processed"] / elapsed

    def process_batch_turbo(self, batch_file: Path) -> int:
        """Ultra-fast batch processing with parallel execution."""
        batch_num = int(re.search(r"(\d+)", batch_file.stem).group(1))

        self.logger.info("=" * 60)
        self.logger.info(
            f"🚀 TURBO BATCH {batch_num}/634 ({batch_num / 634 * 100:.1f}%)"
        )
        self.logger.info("=" * 60)
        self.logger.info(f"📂 Processing batch {batch_num}: {batch_file.name}")

        # Load batch data
        try:
            df = pd.read_csv(batch_file)
            self.logger.info(f"📊 Loaded {len(df)} URLs from batch {batch_num}")

            # Filter for successful resolutions
            if "resolution_success" in df.columns:
                df = df[df["resolution_success"] == True]
                self.logger.info(f"✅ Filtered to {len(df)} successfully resolved URLs")

        except Exception as e:
            self.logger.error(f"❌ Error loading batch {batch_num}: {e}")
            return 0

        # Enhanced duplicate filtering
        urls_to_process = []
        for _, row in df.iterrows():
            original_url = row["original_url"]
            resolved_url = row.get("resolved_url", original_url)

            if not self.is_url_processed(original_url, resolved_url):
                urls_to_process.append((original_url, resolved_url))
            else:
                with self.stats_lock:
                    self.stats["duplicates_skipped"] += 1

        self.logger.info(
            f"⚡ Processing {len(urls_to_process)} new URLs with {self.max_workers} parallel workers"
        )

        if not urls_to_process:
            self.logger.info(f"✅ No new URLs to process in batch {batch_num}")
            return 0

        # Update batch stats
        with self.stats_lock:
            self.stats["current_batch"] = batch_num
            self.stats["urls_in_current_batch"] = len(urls_to_process)
            self.stats["urls_processed_in_batch"] = 0

        # 🚀 PARALLEL PROCESSING - This is where the magic happens!
        scraped_results = []
        batch_start_time = time.time()

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all URLs for parallel processing
            future_to_url = {
                executor.submit(
                    self.scrape_url_content_turbo, original_url, resolved_url
                ): (original_url, resolved_url)
                for original_url, resolved_url in urls_to_process
            }

            # Process completed requests as they finish
            for i, future in enumerate(as_completed(future_to_url)):
                if self.shutdown_requested:
                    self.logger.info(
                        "🛑 Shutdown requested. Cancelling remaining requests..."
                    )
                    break

                try:
                    result = future.result(
                        timeout=self.timeout + 5
                    )  # Allow extra time for processing

                    if result is None:  # Shutdown requested
                        break

                    scraped_results.append(result)
                    original_url, resolved_url = future_to_url[future]

                    # Save processed URLs
                    self.save_processed_url(original_url)
                    if resolved_url != original_url:
                        self.save_processed_url(resolved_url)

                    # Update statistics
                    self.update_stats(result)

                    # Progress logging every 100 URLs (faster batches!)
                    if (i + 1) % 100 == 0:
                        elapsed = time.time() - batch_start_time
                        speed = (i + 1) / elapsed if elapsed > 0 else 0
                        self.logger.info(
                            f"⚡ Processed {i + 1}/{len(urls_to_process)} URLs in batch {batch_num} ({speed:.1f} URLs/sec)"
                        )

                except Exception as e:
                    self.logger.error(f"❌ Error processing URL: {e}")
                    continue

        # Save batch results
        if scraped_results:
            try:
                batch_df = pd.DataFrame(scraped_results)

                # Append to main file
                if self.scraped_content_file.exists():
                    batch_df.to_csv(
                        self.scraped_content_file,
                        mode="a",
                        header=False,
                        index=False,
                        encoding="utf-8",
                    )
                else:
                    batch_df.to_csv(
                        self.scraped_content_file, index=False, encoding="utf-8"
                    )

                # Performance summary
                batch_time = time.time() - batch_start_time
                urls_per_sec = (
                    len(scraped_results) / batch_time if batch_time > 0 else 0
                )
                successful = sum(
                    1
                    for r in scraped_results
                    if r["status_code"] == 200 and r["content"]
                )

                self.logger.info(
                    f"💾 Saved {len(scraped_results)} results from batch {batch_num}"
                )
                self.logger.info(f"⚡ Batch performance: {urls_per_sec:.1f} URLs/sec")
                self.logger.info(
                    f"✅ Success rate: {successful}/{len(scraped_results)} ({successful / len(scraped_results) * 100:.1f}%)"
                )

            except Exception as e:
                self.logger.error(f"❌ Error saving batch results: {e}")

        return len(scraped_results)

    def run_turbo_scraping(self):
        """Run the turbo scraping pipeline."""
        self.logger.info("🚀 === Starting Turbo Browser Content Scraping Pipeline ===")

        # Set start time
        with self.stats_lock:
            self.stats["start_time"] = time.time()

        # Get batch files
        batch_files = self.get_batch_files()
        if not batch_files:
            self.logger.error("❌ No batch files found!")
            return

        # Process batches
        processed_batches = 0
        total_urls_processed = 0

        for batch_file in batch_files:
            if self.shutdown_requested:
                self.logger.info("🛑 Shutdown requested. Stopping turbo scraping...")
                break

            if self.max_batches and processed_batches >= self.max_batches:
                self.logger.info(f"🎯 Reached max batches limit: {self.max_batches}")
                break

            try:
                urls_processed = self.process_batch_turbo(batch_file)
                total_urls_processed += urls_processed
                processed_batches += 1

                # Overall progress
                overall_progress = processed_batches / len(batch_files) * 100
                with self.stats_lock:
                    elapsed = time.time() - self.stats["start_time"]
                    overall_speed = (
                        self.stats["total_processed"] / elapsed if elapsed > 0 else 0
                    )

                self.logger.info(
                    f"📈 Overall Progress: {processed_batches}/{len(batch_files)} batches ({overall_progress:.1f}%)"
                )
                self.logger.info(f"⚡ Overall Speed: {overall_speed:.1f} URLs/sec")
                self.logger.info(
                    f"📊 Total URLs processed: {self.stats['total_processed']:,}"
                )

            except Exception as e:
                self.logger.error(f"❌ Error processing batch {batch_file}: {e}")
                continue

        # Final summary
        with self.stats_lock:
            total_time = time.time() - self.stats["start_time"]
            final_speed = (
                self.stats["total_processed"] / total_time if total_time > 0 else 0
            )

        self.logger.info("🏁 === Turbo Scraping Complete ===")
        self.logger.info(f"⚡ Final Performance: {final_speed:.1f} URLs/sec")
        self.logger.info(f"📊 Total processed: {self.stats['total_processed']:,} URLs")
        self.logger.info(f"✅ Successful: {self.stats['successful_scrapes']:,}")
        self.logger.info(f"❌ Failed: {self.stats['failed_scrapes']:,}")
        self.logger.info(f"⏱️  Total time: {total_time / 60:.1f} minutes")

    def get_batch_files(self):
        """Get sorted batch files."""
        pattern = "resolved_browser_batch_*.csv"
        batch_files = list(self.data_dir.glob(f"complete_resolved/{pattern}"))

        def extract_batch_num(filepath):
            match = re.search(r"(\d+)", filepath.stem)
            return int(match.group(1)) if match else 0

        batch_files.sort(key=extract_batch_num)
        self.logger.info(f"📁 Found {len(batch_files)} batch files to process")
        return batch_files


def main():
    parser = argparse.ArgumentParser(description="Turbo Browser Content Scraper")
    parser.add_argument("--data-dir", required=True, help="Data directory path")
    parser.add_argument("--output-dir", required=True, help="Output directory path")
    parser.add_argument("--log-dir", required=True, help="Log directory path")
    parser.add_argument(
        "--max-workers", type=int, default=20, help="Max parallel workers"
    )
    parser.add_argument(
        "--timeout", type=int, default=15, help="Request timeout in seconds"
    )
    parser.add_argument(
        "--max-batches", type=int, help="Max batches to process (for testing)"
    )

    args = parser.parse_args()

    scraper = TurboBrowserContentScraper(
        data_dir=args.data_dir,
        output_dir=args.output_dir,
        log_dir=args.log_dir,
        max_workers=args.max_workers,
        timeout=args.timeout,
        max_batches=args.max_batches,
    )

    scraper.run_turbo_scraping()


if __name__ == "__main__":
    main()
