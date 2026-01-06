#!/usr/bin/env python3
"""
Complete URL Resolution Pipeline (Batch Saving)
==============================================
Loads raw URLs from browser and Facebook extraction, resolves them in batches,
and saves results incrementally to disk. No scraping or prioritization yet.
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

# Add the current directory to sys.path to allow importing enhanced_url_resolver
current_dir = Path(__file__).parent.absolute()
sys.path.append(str(current_dir))

# Import the enhanced URL resolver
from enhanced_resolver import EnhancedURLResolver


class CompleteURLResolutionPipeline:
    def __init__(self, base_dir: str = "../data", batch_size: int = 1000):
        self.base_dir = Path(base_dir)
        self.browser_dir = self.base_dir / "browser_urlextract"
        self.facebook_dir = self.base_dir / "facebook_urlextract"
        self.output_dir = self.base_dir / "complete_resolved"
        self.output_dir.mkdir(exist_ok=True)
        self.setup_logging()

        # Initialize the enhanced URL resolver
        self.url_resolver = EnhancedURLResolver(
            cache_file=str(self.output_dir / "url_resolution_cache.db"),
            timeout=3,  # Reduced to 3s
            max_redirects=5,
            max_workers=40,  # Increased to 40
            delay_between_requests=0.05,
        )
        self.batch_size = batch_size
        self.logger.info(
            "Initialized CompleteURLResolutionPipeline (batch saving mode)"
        )

    def setup_logging(self):
        log_file = (
            self.output_dir
            / f"complete_pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
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

    def resolve_and_save_batches(self, urls: List[str], source: str):
        total = len(urls)
        batch = []
        batch_num = 0
        successful_resolutions = 0
        failed_resolutions = 0

        self.logger.info(f"Starting to resolve {total} {source} URLs...")

        for i, url in enumerate(urls, 1):
            try:
                # Use the actual robust URL resolver
                result = self.url_resolver.resolve_single_url(url)

                # Check if resolution actually worked
                resolution_success = result.get("success", False)
                resolved_url = result.get("resolved_url", url)
                redirect_count = result.get("redirect_count", 0)

                # Determine if the URL was actually resolved (changed)
                actually_resolved = url != resolved_url

                if resolution_success and actually_resolved:
                    successful_resolutions += 1
                else:
                    failed_resolutions += 1

                batch.append(
                    {
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
                )

                # Progress logging
                if i % 100 == 0:
                    self.logger.info(
                        f"  Progress: {i}/{total} ({i / total * 100:.1f}%) - "
                        f"Success: {successful_resolutions}, Failed: {failed_resolutions}"
                    )

                # Save batch when it reaches the batch size or at the end
                if i % self.batch_size == 0 or i == total:
                    batch_num += 1
                    batch_df = pd.DataFrame(batch)
                    out_file = (
                        self.output_dir / f"resolved_{source}_batch_{batch_num:04d}.csv"
                    )
                    batch_df.to_csv(out_file, index=False)
                    self.logger.info(
                        f"Saved batch {batch_num} ({len(batch)} URLs) to {out_file}"
                    )
                    batch = []

            except Exception as e:
                self.logger.error(f"Error resolving URL {url}: {e}")
                failed_resolutions += 1
                batch.append(
                    {
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
                )

        # Final statistics
        self.logger.info(
            f"Completed {source} URL resolution: "
            f"Total: {total}, Successful: {successful_resolutions}, "
            f"Failed: {failed_resolutions}, Success rate: {successful_resolutions / total * 100:.1f}%"
        )

    def run(self):
        self.logger.info("=== Starting URL Resolution Pipeline (Batch Saving) ===")

        total_processed = 0
        total_successful = 0
        total_failed = 0

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
                self.logger.info(f"Processing {len(urls)} unique URLs from {source}...")

                # Count current stats
                before_successful = total_successful
                before_failed = total_failed

                self.resolve_and_save_batches(urls, source)

                # Update totals
                total_processed += len(urls)
                # Note: We can't easily get the exact counts from the method,
                # but we can estimate from the log messages

        self.logger.info("=== URL Resolution Pipeline Finished ===")
        self.logger.info(f"Total URLs processed: {total_processed}")


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Complete URL Resolution Pipeline (Batch Saving)"
    )
    parser.add_argument("--base-dir", default="../data", help="Base data directory")
    parser.add_argument(
        "--batch-size", type=int, default=1000, help="Batch size for saving results"
    )
    args = parser.parse_args()
    pipeline = CompleteURLResolutionPipeline(
        base_dir=args.base_dir, batch_size=args.batch_size
    )
    pipeline.run()


if __name__ == "__main__":
    main()
