#!/usr/bin/env python3
"""
Parallel URL Resolution Pipeline (Resume from Batch 181)
=======================================================
Resumes URL resolution from batch 181, using correct paths and placing
new batches in the same folder with a new log file.
"""

import pandas as pd
import os
import sys
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict
import json
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import glob

# Import the enhanced URL resolver
from enhanced_url_resolver import EnhancedURLResolver


class ParallelURLResolutionPipelineResume181:
    def __init__(self, batch_size: int = 1000, max_workers: int = 20):
        # Correct paths for Datadonationer/alteruse structure
        self.base_dir = Path("/work/Datadonationer")
        self.browser_dir = self.base_dir / "data" / "browser_urlextract"
        self.facebook_dir = self.base_dir / "data" / "facebook_urlextract"
        self.output_dir = Path("/work/Datadonationer/alteruse/data/complete_resolved")
        self.output_dir.mkdir(exist_ok=True)
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.setup_logging()

        # Initialize the enhanced URL resolver
        self.url_resolver = EnhancedURLResolver(
            cache_file=str(self.output_dir / "url_resolution_cache.db"),
            timeout=30,
            max_redirects=10,
            max_workers=1,  # We'll handle parallelism at pipeline level
            delay_between_requests=0.1,
        )
        self.logger.info(
            f"Initialized ParallelURLResolutionPipelineResume181 (batch_size={batch_size}, max_workers={max_workers})"
        )

    def setup_logging(self):
        log_file = (
            self.output_dir
            / f"parallel_pipeline_resume_181_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        )
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[logging.FileHandler(log_file), logging.StreamHandler()],
        )
        self.logger = logging.getLogger(__name__)

    def load_urls(self, source: str) -> pd.DataFrame:
        if source == "browser":
            file = self.browser_dir / "extracted_urls_optimized.csv"
        elif source == "facebook":
            file = self.facebook_dir / "extracted_urls_facebook.csv"
        else:
            raise ValueError("Unknown source")
        if not file.exists():
            self.logger.warning(f"File not found: {file}")
            return pd.DataFrame()
        df = pd.read_csv(file)
        self.logger.info(f"Loaded {len(df)} URLs from {file}")
        return df

    def _extract_domain(self, url: str) -> str:
        try:
            return urlparse(url).netloc.lower()
        except:
            return ""

    def get_completed_batches_info(self, source: str) -> Dict:
        """Get information about completed batches for a source."""
        pattern = str(self.output_dir / f"resolved_{source}_batch_*.csv")
        existing_files = glob.glob(pattern)

        if not existing_files:
            return {"completed_batches": 0, "next_batch_num": 1, "urls_processed": 0}

        # Extract batch numbers from existing files
        batch_numbers = []
        for file in existing_files:
            try:
                filename = Path(file).name
                # Extract number from resolved_browser_batch_0123.csv
                batch_num = int(filename.split("_")[-1].replace(".csv", ""))
                batch_numbers.append(batch_num)
            except:
                continue

        if not batch_numbers:
            return {"completed_batches": 0, "next_batch_num": 1, "urls_processed": 0}

        completed_batches = len(batch_numbers)
        next_batch_num = max(batch_numbers) + 1
        urls_processed = completed_batches * self.batch_size

        return {
            "completed_batches": completed_batches,
            "next_batch_num": next_batch_num,
            "urls_processed": urls_processed,
        }

    def resolve_url_worker(self, url: str, source: str) -> Dict:
        """Worker function for parallel URL resolution."""
        try:
            result = self.url_resolver.resolve_single_url(url)

            # Check if resolution actually worked
            resolution_success = result.get("success", False)
            resolved_url = result.get("resolved_url", url)
            redirect_count = result.get("redirect_count", 0)

            # Determine if the URL was actually resolved (changed)
            actually_resolved = url != resolved_url

            return {
                "original_url": url,
                "resolved_url": resolved_url,
                "resolution_success": resolution_success,
                "actually_resolved": actually_resolved,
                "redirect_count": redirect_count,
                "source": source,
                "domain": self._extract_domain(resolved_url),
                "error": result.get("error"),
                "response_time": result.get("response_time", 0),
            }
        except Exception as e:
            self.logger.error(f"Error resolving URL {url}: {e}")
            return {
                "original_url": url,
                "resolved_url": url,
                "resolution_success": False,
                "actually_resolved": False,
                "redirect_count": 0,
                "source": source,
                "domain": self._extract_domain(url),
                "error": str(e),
                "response_time": 0,
            }

    def resolve_and_save_batches_resume_181(self, urls: List[str], source: str):
        total_urls = len(urls)

        # Get information about completed batches
        completed_info = self.get_completed_batches_info(source)
        completed_batches = completed_info["completed_batches"]
        next_batch_num = completed_info["next_batch_num"]
        urls_processed = completed_info["urls_processed"]

        self.logger.info(f"=== {source.upper()} URL RESOLUTION STATUS ===")
        self.logger.info(f"Total URLs in source: {total_urls}")
        self.logger.info(f"Completed batches: {completed_batches}")
        self.logger.info(f"URLs already processed: {urls_processed}")
        self.logger.info(f"Next batch number: {next_batch_num}")

        # Calculate remaining URLs
        remaining_urls = total_urls - urls_processed
        if remaining_urls <= 0:
            self.logger.info(f"All {source} URLs have been processed. Skipping.")
            return

        # Get the remaining URLs to process
        remaining_urls_list = urls[urls_processed:]
        self.logger.info(f"Remaining URLs to process: {remaining_urls}")
        self.logger.info(
            f"Resuming {source} URL resolution from batch {next_batch_num}"
        )
        self.logger.info(
            f"Starting parallel resolution of {remaining_urls} remaining {source} URLs with {self.max_workers} workers..."
        )

        successful_resolutions = 0
        failed_resolutions = 0

        # Process remaining URLs in batches
        for i in range(0, remaining_urls, self.batch_size):
            batch_urls = remaining_urls_list[i : i + self.batch_size]

            self.logger.info(
                f"Processing batch {next_batch_num} ({len(batch_urls)} URLs)..."
            )

            batch_results = []
            completed = 0

            # Use ThreadPoolExecutor for parallel processing
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # Submit all URLs in the batch
                future_to_url = {
                    executor.submit(self.resolve_url_worker, url, source): url
                    for url in batch_urls
                }

                # Collect results as they complete
                for future in as_completed(future_to_url):
                    result = future.result()
                    batch_results.append(result)
                    completed += 1

                    # Update counters
                    if result["resolution_success"] and result["actually_resolved"]:
                        successful_resolutions += 1
                    else:
                        failed_resolutions += 1

                    # Progress logging every 100 URLs
                    if completed % 100 == 0:
                        total_processed = urls_processed + i + completed
                        self.logger.info(
                            f"  Progress: {total_processed}/{total_urls} ({total_processed / total_urls * 100:.1f}%) - "
                            f"Success: {successful_resolutions}, Failed: {failed_resolutions}"
                        )

            # Save batch results
            batch_df = pd.DataFrame(batch_results)
            out_file = (
                self.output_dir / f"resolved_{source}_batch_{next_batch_num:04d}.csv"
            )
            batch_df.to_csv(out_file, index=False)
            self.logger.info(
                f"Saved batch {next_batch_num} ({len(batch_results)} URLs) to {out_file}"
            )

            next_batch_num += 1

        # Final statistics
        self.logger.info(
            f"Completed {source} URL resolution: "
            f"Total: {total_urls}, Successful: {successful_resolutions}, "
            f"Failed: {failed_resolutions}, Success rate: {successful_resolutions / total_urls * 100:.1f}%"
        )

    def run(self):
        self.logger.info(
            "=== Starting Parallel URL Resolution Pipeline (Resume from Batch 181) ==="
        )

        total_processed = 0

        for source in ["browser", "facebook"]:
            df = self.load_urls(source)
            if not df.empty:
                # Get the URL column (handle different column names)
                url_col = None
                for col in ["url", "URL", "link", "Link"]:
                    if col in df.columns:
                        url_col = col
                        break

                if url_col is None:
                    self.logger.error(f"No URL column found in {source} data")
                    continue

                urls = df[url_col].dropna().unique().tolist()
                self.logger.info(f"Found {len(urls)} unique URLs from {source}...")

                self.resolve_and_save_batches_resume_181(urls, source)
                total_processed += len(urls)

        self.logger.info(
            "=== Parallel URL Resolution Pipeline (Resume from Batch 181) Finished ==="
        )
        self.logger.info(f"Total URLs processed: {total_processed}")


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Parallel URL Resolution Pipeline (Resume from Batch 181)"
    )
    parser.add_argument(
        "--batch-size", type=int, default=1000, help="Batch size for saving results"
    )
    parser.add_argument(
        "--max-workers", type=int, default=20, help="Number of parallel workers"
    )
    args = parser.parse_args()

    pipeline = ParallelURLResolutionPipelineResume181(
        batch_size=args.batch_size, max_workers=args.max_workers
    )
    pipeline.run()


if __name__ == "__main__":
    main()
