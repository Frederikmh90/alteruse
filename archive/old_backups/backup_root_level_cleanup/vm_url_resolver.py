#!/usr/bin/env python3
"""
VM URL Resolver
==============
Processes URL chunks on individual VMs with parallel processing.
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

# Import the enhanced URL resolver
from enhanced_url_resolver import EnhancedURLResolver


class VMURLResolver:
    def __init__(
        self,
        input_file: str,
        output_dir: str,
        source: str,
        max_workers: int = 10,
        batch_size: int = 500,
    ):
        self.input_file = Path(input_file)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.source = source
        self.max_workers = max_workers
        self.batch_size = batch_size
        self.setup_logging()

        # Initialize the enhanced URL resolver
        self.url_resolver = EnhancedURLResolver(
            cache_file=str(self.output_dir / "url_resolution_cache.db"),
            timeout=30,
            max_redirects=10,
            max_workers=1,  # We'll handle parallelism at VM level
            delay_between_requests=0.1,
        )
        self.logger.info(
            f"Initialized VMURLResolver (source={source}, max_workers={max_workers}, batch_size={batch_size})"
        )

    def setup_logging(self):
        log_file = (
            self.output_dir
            / f"vm_resolver_{self.source}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        )
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[logging.FileHandler(log_file), logging.StreamHandler()],
        )
        self.logger = logging.getLogger(__name__)

    def load_urls(self) -> List[str]:
        """Load URLs from the input file."""
        if not self.input_file.exists():
            self.logger.error(f"Input file not found: {self.input_file}")
            return []

        df = pd.read_csv(self.input_file)
        self.logger.info(f"Loaded {len(df)} URLs from {self.input_file}")

        # Get URL column
        url_col = None
        for col in ["url", "URL", "link", "Link"]:
            if col in df.columns:
                url_col = col
                break

        if url_col is None:
            self.logger.error(f"No URL column found in {self.input_file}")
            return []

        urls = df[url_col].dropna().unique().tolist()
        self.logger.info(f"Found {len(urls)} unique URLs")
        return urls

    def _extract_domain(self, url: str) -> str:
        try:
            return urlparse(url).netloc.lower()
        except:
            return ""

    def resolve_url_worker(self, url: str) -> Dict:
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
                "source": self.source,
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
                "source": self.source,
                "domain": self._extract_domain(url),
                "error": str(e),
                "response_time": 0,
            }

    def resolve_and_save_batches(self, urls: List[str]):
        total = len(urls)
        successful_resolutions = 0
        failed_resolutions = 0
        batch_num = 0

        self.logger.info(
            f"Starting parallel resolution of {total} URLs with {self.max_workers} workers..."
        )

        # Process URLs in batches
        for i in range(0, total, self.batch_size):
            batch_urls = urls[i : i + self.batch_size]
            batch_num += 1

            self.logger.info(
                f"Processing batch {batch_num} ({len(batch_urls)} URLs)..."
            )

            batch_results = []
            completed = 0

            # Use ThreadPoolExecutor for parallel processing
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # Submit all URLs in the batch
                future_to_url = {
                    executor.submit(self.resolve_url_worker, url): url
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

                    # Progress logging every 50 URLs
                    if completed % 50 == 0:
                        self.logger.info(
                            f"  Progress: {i + completed}/{total} ({(i + completed) / total * 100:.1f}%) - "
                            f"Success: {successful_resolutions}, Failed: {failed_resolutions}"
                        )

            # Save batch results
            batch_df = pd.DataFrame(batch_results)
            out_file = (
                self.output_dir / f"resolved_{self.source}_batch_{batch_num:04d}.csv"
            )
            batch_df.to_csv(out_file, index=False)
            self.logger.info(
                f"Saved batch {batch_num} ({len(batch_results)} URLs) to {out_file}"
            )

        # Final statistics
        self.logger.info(
            f"Completed URL resolution: "
            f"Total: {total}, Successful: {successful_resolutions}, "
            f"Failed: {failed_resolutions}, Success rate: {successful_resolutions / total * 100:.1f}%"
        )

    def run(self):
        self.logger.info(f"=== Starting VM URL Resolution for {self.source} ===")

        urls = self.load_urls()
        if not urls:
            self.logger.error("No URLs to process")
            return

        self.resolve_and_save_batches(urls)

        self.logger.info(f"=== VM URL Resolution for {self.source} Finished ===")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="VM URL Resolver")
    parser.add_argument("--input-file", required=True, help="Input CSV file with URLs")
    parser.add_argument("--output-dir", required=True, help="Output directory")
    parser.add_argument(
        "--source", required=True, help="Source type (browser/facebook)"
    )
    parser.add_argument(
        "--max-workers", type=int, default=10, help="Number of parallel workers"
    )
    parser.add_argument(
        "--batch-size", type=int, default=500, help="Batch size for saving results"
    )

    args = parser.parse_args()

    resolver = VMURLResolver(
        input_file=args.input_file,
        output_dir=args.output_dir,
        source=args.source,
        max_workers=args.max_workers,
        batch_size=args.batch_size,
    )
    resolver.run()


if __name__ == "__main__":
    main()
