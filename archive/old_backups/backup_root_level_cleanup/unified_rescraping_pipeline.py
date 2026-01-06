#!/usr/bin/env python3
"""
Unified Re-scraping Pipeline
============================
Re-scrapes content from URLs that need updated scraping based on the combined URL resolution results.
Uses resolved URLs and handles both Facebook and browser data uniformly.
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
from urllib.parse import urljoin, urlparse
import logging
import sys
import os
from robust_url_resolver import RobustURLResolver


class UnifiedRescrapingPipeline:
    """
    Unified pipeline for re-scraping content from resolved URLs.
    """

    def __init__(
        self,
        input_file: str = "data/combined_resolved_enhanced/urls_for_rescraping.csv",
        output_dir: str = "data/unified_rescraped",
        batch_size: int = 500,
    ):
        self.input_file = input_file
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.batch_size = batch_size

        # Setup logging
        self.setup_logging()

        # Load existing scraped content to avoid duplicates
        self.scraped_urls = set()
        self.content_hashes = set()
        self.load_existing_content()

        # Initialize URL resolver for any additional resolution needed
        self.url_resolver = RobustURLResolver(
            cache_file=str(self.output_dir / "rescraping_url_cache.db"),
            timeout=30,
            max_redirects=10,
            max_workers=15,
            delay_between_requests=0.1,
        )

        # Session for content scraping
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

        # Known paywalled domains
        self.known_paywall_domains = {
            "politiken.dk": "hard",
            "berlingske.dk": "hard",
            "nytimes.com": "hard",
            "wsj.com": "hard",
            "ft.com": "hard",
            "economist.com": "hard",
            "washingtonpost.com": "soft",
            "theguardian.com": "soft",
            "telegraph.co.uk": "hard",
            "independent.co.uk": "soft",
            "dailymail.co.uk": "soft",
            "mirror.co.uk": "soft",
            "express.co.uk": "soft",
            "thesun.co.uk": "soft",
            "metro.co.uk": "soft",
            "standard.co.uk": "hard",
            "cityam.com": "hard",
            "bloomberg.com": "hard",
            "reuters.com": "soft",
            "cnn.com": "soft",
            "bbc.com": "none",
            "dr.dk": "none",
            "tv2.dk": "none",
        }

        # Statistics
        self.stats = {
            "total_urls_loaded": 0,
            "urls_to_process": 0,
            "total_attempted": 0,
            "successful": 0,
            "failed": 0,
            "duplicates_skipped": 0,
            "paywall_detected": 0,
            "improved_content": 0,
            "no_improvement": 0,
            "additional_resolution": 0,
            "start_time": None,
            "end_time": None,
            "batch_times": [],
            "errors": [],
        }

        print(f"Unified Re-scraping Pipeline initialized")
        print(f"Input file: {self.input_file}")
        print(f"Output directory: {self.output_dir}")
        print(f"Existing scraped URLs: {len(self.scraped_urls):,}")

    def setup_logging(self):
        """Setup logging for the pipeline."""
        log_file = (
            self.output_dir
            / f"unified_rescraping_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        )

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[logging.FileHandler(log_file), logging.StreamHandler()],
        )
        self.logger = logging.getLogger(__name__)

    def load_existing_content(self):
        """Load existing scraped content to avoid duplicates."""
        content_file = self.output_dir / "unified_rescraped_content.csv"

        if content_file.exists():
            try:
                existing_df = pd.read_csv(content_file)
                self.scraped_urls = set(existing_df["original_url"].tolist())

                # Load content hashes if available
                if "content_hash" in existing_df.columns:
                    self.content_hashes = set(
                        existing_df["content_hash"].dropna().tolist()
                    )

                print(f"Loaded {len(self.scraped_urls)} existing URLs")
            except Exception as e:
                print(f"Error loading existing content: {e}")

    def load_urls_for_rescraping(self) -> pd.DataFrame:
        """Load URLs that need re-scraping from the combined resolution output."""
        self.logger.info(f"Loading URLs for re-scraping from: {self.input_file}")

        try:
            df = pd.read_csv(self.input_file)
            self.stats["total_urls_loaded"] = len(df)

            # Filter to only URLs that actually need re-scraping
            urls_to_process = df[df["needs_rescraping"] == True].copy()

            # Prioritize URLs that were successfully resolved
            urls_to_process["priority"] = urls_to_process["resolution_worked"].astype(
                int
            ) * 2 + (urls_to_process["old_word_count"].fillna(0) < 50).astype(int)

            # Sort by priority (higher first)
            urls_to_process = urls_to_process.sort_values("priority", ascending=False)

            self.stats["urls_to_process"] = len(urls_to_process)

            self.logger.info(f"Loaded {len(df)} total URLs")
            self.logger.info(f"URLs needing re-scraping: {len(urls_to_process)}")
            self.logger.info(
                f"URLs with successful resolution: {urls_to_process['resolution_worked'].sum()}"
            )

            return urls_to_process

        except Exception as e:
            self.logger.error(f"Error loading URLs for re-scraping: {e}")
            return pd.DataFrame()

    def get_content_hash(self, content: str) -> str:
        """Generate hash for content deduplication."""
        if not content:
            return ""
        normalized = re.sub(r"\s+", " ", content.strip().lower())
        return hashlib.md5(normalized.encode()).hexdigest()

    def detect_paywall(self, url: str, html: str) -> Dict:
        """Detect if a page is behind a paywall."""
        paywall_info = {
            "paywall_detected": False,
            "paywall_type": None,
            "paywall_confidence": 0.0,
            "paywall_indicators": [],
        }

        html_lower = html.lower()
        domain = urlparse(url).netloc.lower()

        # Check known paywalled domains
        if domain in self.known_paywall_domains:
            paywall_type = self.known_paywall_domains[domain]
            if paywall_type != "none":
                paywall_info["paywall_detected"] = True
                paywall_info["paywall_type"] = paywall_type
                paywall_info["paywall_confidence"] = 0.95
                paywall_info["paywall_indicators"].append(
                    f"known_paywall_domain_{paywall_type}"
                )

        # Generic paywall detection patterns
        paywall_patterns = [
            (
                r"\b(subscribe|subscription|premium|membership|login|sign.?in|register|paywall|metered)\b",
                0.7,
            ),
            (
                r"\b(limited.?access|free.?articles|remaining.?articles|article.?limit)\b",
                0.8,
            ),
            (r"\b(continue.?reading|read.?more|unlock|unlimited.?access)\b", 0.6),
            (r'class=["\'](?:[^"\']*\s)?(paywall|subscription|premium|metered)', 0.8),
            (r"\b(you.?have.?reached.?your.?limit|subscribe.?to.?continue)\b", 0.9),
        ]

        total_confidence = 0.0
        pattern_count = 0

        for pattern, confidence in paywall_patterns:
            matches = re.findall(pattern, html_lower)
            if matches:
                paywall_info["paywall_indicators"].extend(
                    [f"pattern_{pattern[:20]}..." for _ in matches]
                )
                total_confidence += confidence * len(matches)
                pattern_count += len(matches)

        if pattern_count > 0:
            paywall_info["paywall_confidence"] = min(
                total_confidence / pattern_count, 1.0
            )
            paywall_info["paywall_detected"] = paywall_info["paywall_confidence"] > 0.6

        return paywall_info

    def extract_content(self, url: str, html: str) -> Tuple[Optional[str], Dict]:
        """Extract content from HTML using trafilatura."""
        try:
            content = trafilatura.extract(
                html,
                include_comments=False,
                include_tables=True,
                include_formatting=True,
                url=url,
            )

            if content:
                content = re.sub(r"\s+", " ", content.strip())
                word_count = len(content.split())

                extraction_info = {
                    "extraction_success": True,
                    "word_count": word_count,
                    "char_count": len(content),
                    "extraction_method": "trafilatura",
                }

                return content, extraction_info
            else:
                return None, {
                    "extraction_success": False,
                    "extraction_method": "trafilatura",
                }

        except Exception as e:
            return None, {
                "extraction_success": False,
                "error": str(e),
                "extraction_method": "trafilatura",
            }

    def scrape_single_url(self, row: Dict, timeout: int = 15) -> Dict:
        """
        Scrape a single URL using the best available resolved URL.
        """
        start_time = time.time()

        original_url = row["original_url"]

        # Determine which URL to use for scraping
        scrape_url = original_url
        if row.get("resolution_worked") and row.get("new_resolved_url"):
            scrape_url = row["new_resolved_url"]
        elif row.get("old_resolved_url") and row["old_resolved_url"] != original_url:
            scrape_url = row["old_resolved_url"]

        self.logger.debug(f"Scraping: {original_url} -> {scrape_url}")

        try:
            response = self.session.get(scrape_url, timeout=timeout)
            response.raise_for_status()

            html = response.text

            # Extract content
            content, extraction_info = self.extract_content(scrape_url, html)

            # Detect paywall
            paywall_info = self.detect_paywall(scrape_url, html)

            # Generate content hash
            content_hash = self.get_content_hash(content) if content else ""

            # Compare with old content to see if we improved
            old_word_count = row.get("old_word_count", 0)
            new_word_count = extraction_info.get("word_count", 0)
            content_improved = new_word_count > max(
                old_word_count * 1.2, old_word_count + 100
            )

            result = {
                "original_url": original_url,
                "scrape_url": scrape_url,
                "final_url": response.url,
                "source": row.get("source", ""),
                "domain": row.get("domain", ""),
                "success": True,
                "error": None,
                "status_code": response.status_code,
                "content": content,
                "content_hash": content_hash,
                "word_count": new_word_count,
                "char_count": extraction_info.get("char_count", 0),
                "paywall_detected": paywall_info["paywall_detected"],
                "paywall_type": paywall_info.get("paywall_type"),
                "paywall_confidence": paywall_info.get("paywall_confidence", 0.0),
                "response_time": time.time() - start_time,
                "scraped_at": datetime.now().isoformat(),
                # Comparison with old data
                "old_word_count": old_word_count,
                "old_content_hash": row.get("old_content_hash", ""),
                "old_paywall_detected": row.get("old_paywall_detected", False),
                "content_improved": content_improved,
                "resolution_used": row.get("resolution_worked", False),
                "redirect_count": len(response.history),
            }

            if paywall_info["paywall_detected"]:
                self.stats["paywall_detected"] += 1

            if content_improved:
                self.stats["improved_content"] += 1
            else:
                self.stats["no_improvement"] += 1

            return result

        except requests.exceptions.RequestException as e:
            return {
                "original_url": original_url,
                "scrape_url": scrape_url,
                "final_url": scrape_url,
                "source": row.get("source", ""),
                "domain": row.get("domain", ""),
                "success": False,
                "error": f"Request error: {str(e)}",
                "status_code": getattr(e.response, "status_code", None)
                if hasattr(e, "response")
                else None,
                "content": None,
                "content_hash": None,
                "word_count": 0,
                "paywall_detected": False,
                "response_time": time.time() - start_time,
                "scraped_at": datetime.now().isoformat(),
                "old_word_count": row.get("old_word_count", 0),
                "content_improved": False,
                "resolution_used": row.get("resolution_worked", False),
            }
        except Exception as e:
            return {
                "original_url": original_url,
                "scrape_url": scrape_url,
                "final_url": scrape_url,
                "source": row.get("source", ""),
                "domain": row.get("domain", ""),
                "success": False,
                "error": f"Unexpected error: {str(e)}",
                "status_code": None,
                "content": None,
                "content_hash": None,
                "word_count": 0,
                "paywall_detected": False,
                "response_time": time.time() - start_time,
                "scraped_at": datetime.now().isoformat(),
                "old_word_count": row.get("old_word_count", 0),
                "content_improved": False,
                "resolution_used": row.get("resolution_worked", False),
            }

    def process_batch(self, batch_df: pd.DataFrame, delay: float = 2.0) -> pd.DataFrame:
        """Process a batch of URLs for re-scraping."""
        batch_start_time = time.time()
        results = []

        for i, (_, row) in enumerate(batch_df.iterrows()):
            try:
                self.stats["total_attempted"] += 1

                # Skip if URL already processed
                if row["original_url"] in self.scraped_urls:
                    self.stats["duplicates_skipped"] += 1
                    continue

                result = self.scrape_single_url(row.to_dict(), timeout=15)
                results.append(result)

                if result["success"]:
                    self.stats["successful"] += 1
                    self.scraped_urls.add(row["original_url"])
                    if result.get("content_hash"):
                        self.content_hashes.add(result["content_hash"])
                else:
                    self.stats["failed"] += 1
                    self.stats["errors"].append(
                        {
                            "url": row["original_url"],
                            "error": result.get("error", "Unknown error"),
                            "timestamp": datetime.now().isoformat(),
                        }
                    )

                # Progress logging
                if (i + 1) % 10 == 0:
                    self.logger.info(
                        f"  Completed {i + 1}/{len(batch_df)} URLs in batch"
                    )

                # Delay between requests
                if delay > 0 and i < len(batch_df) - 1:
                    time.sleep(delay)

            except Exception as e:
                self.logger.error(f"Error processing URL {row['original_url']}: {e}")
                self.stats["failed"] += 1
                self.stats["errors"].append(
                    {
                        "url": row["original_url"],
                        "error": str(e),
                        "timestamp": datetime.now().isoformat(),
                    }
                )

        batch_time = time.time() - batch_start_time
        self.stats["batch_times"].append(batch_time)

        self.logger.info(f"Batch completed in {batch_time:.2f} seconds")
        self.logger.info(
            f"Successfully scraped: {len([r for r in results if r['success']])}/{len(results)}"
        )

        return pd.DataFrame(results)

    def save_results(self, new_results: pd.DataFrame, append: bool = True):
        """Save scraping results to CSV."""
        if new_results.empty:
            return

        output_file = self.output_dir / "unified_rescraped_content.csv"

        try:
            if append and output_file.exists():
                new_results.to_csv(output_file, mode="a", header=False, index=False)
            else:
                new_results.to_csv(output_file, index=False)

            self.logger.info(f"Saved {len(new_results)} results to {output_file}")
        except Exception as e:
            self.logger.error(f"Error saving results: {e}")

    def save_stats(self):
        """Save processing statistics."""
        stats_file = (
            self.output_dir
            / f"rescraping_stats_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        )

        # Calculate additional stats
        if self.stats["total_attempted"] > 0:
            self.stats["success_rate"] = (
                self.stats["successful"] / self.stats["total_attempted"]
            )
            self.stats["improvement_rate"] = (
                self.stats["improved_content"] / self.stats["successful"]
                if self.stats["successful"] > 0
                else 0
            )

        if self.stats["batch_times"]:
            self.stats["avg_batch_time"] = sum(self.stats["batch_times"]) / len(
                self.stats["batch_times"]
            )

        try:
            with open(stats_file, "w") as f:
                json.dump(self.stats, f, indent=2, default=str)

            self.logger.info(f"Saved statistics to {stats_file}")
        except Exception as e:
            self.logger.error(f"Error saving stats: {e}")

    def run_rescraping_pipeline(
        self, max_urls: Optional[int] = None, delay: float = 2.0
    ):
        """
        Run the complete re-scraping pipeline.

        Args:
            max_urls: Maximum number of URLs to process (for testing)
            delay: Delay between requests in seconds
        """
        self.stats["start_time"] = datetime.now()

        self.logger.info("=== Starting Unified Re-scraping Pipeline ===")

        # Load URLs that need re-scraping
        urls_df = self.load_urls_for_rescraping()

        if urls_df.empty:
            self.logger.warning("No URLs to process. Exiting.")
            return

        # Limit URLs if specified (for testing)
        if max_urls:
            urls_df = urls_df.head(max_urls)
            self.logger.info(f"Limited to first {max_urls} URLs for testing")

        # Process URLs in batches
        total_batches = (len(urls_df) + self.batch_size - 1) // self.batch_size

        for batch_num in range(total_batches):
            start_idx = batch_num * self.batch_size
            end_idx = min((batch_num + 1) * self.batch_size, len(urls_df))

            batch_df = urls_df.iloc[start_idx:end_idx].copy()

            self.logger.info(
                f"\n=== Processing batch {batch_num + 1}/{total_batches} ({len(batch_df)} URLs) ==="
            )

            try:
                batch_results = self.process_batch(batch_df, delay=delay)

                if not batch_results.empty:
                    self.save_results(batch_results)

                # Save stats periodically
                if (batch_num + 1) % 5 == 0:
                    self.save_stats()

            except Exception as e:
                self.logger.error(f"Error processing batch {batch_num + 1}: {e}")
                continue

        # Final statistics
        self.stats["end_time"] = datetime.now()
        self.save_stats()

        self.logger.info("\n=== Re-scraping Pipeline Summary ===")
        self.logger.info(f"URLs loaded: {self.stats['total_urls_loaded']}")
        self.logger.info(f"URLs processed: {self.stats['total_attempted']}")
        self.logger.info(f"Successfully scraped: {self.stats['successful']}")
        self.logger.info(f"Failed: {self.stats['failed']}")
        self.logger.info(f"Duplicates skipped: {self.stats['duplicates_skipped']}")
        self.logger.info(f"Content improved: {self.stats['improved_content']}")
        self.logger.info(f"Paywall detected: {self.stats['paywall_detected']}")
        self.logger.info(f"Success rate: {self.stats.get('success_rate', 0):.2%}")
        self.logger.info(
            f"Improvement rate: {self.stats.get('improvement_rate', 0):.2%}"
        )

        processing_time = self.stats["end_time"] - self.stats["start_time"]
        self.logger.info(f"Total processing time: {processing_time}")


def main():
    """Main function to run the unified re-scraping pipeline."""
    import argparse

    parser = argparse.ArgumentParser(description="Unified Re-scraping Pipeline")
    parser.add_argument(
        "--input-file",
        default="data/combined_resolved_enhanced/urls_for_rescraping.csv",
        help="Input CSV file with URLs to re-scrape",
    )
    parser.add_argument(
        "--output-dir", default="data/unified_rescraped", help="Output directory"
    )
    parser.add_argument(
        "--batch-size", type=int, default=500, help="Batch size for processing"
    )
    parser.add_argument(
        "--max-urls", type=int, help="Maximum number of URLs to process (for testing)"
    )
    parser.add_argument(
        "--delay", type=float, default=2.0, help="Delay between requests"
    )

    args = parser.parse_args()

    pipeline = UnifiedRescrapingPipeline(
        input_file=args.input_file,
        output_dir=args.output_dir,
        batch_size=args.batch_size,
    )

    pipeline.run_rescraping_pipeline(max_urls=args.max_urls, delay=args.delay)


if __name__ == "__main__":
    main()
