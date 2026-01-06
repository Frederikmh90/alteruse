#!/usr/bin/env python3
"""
Step 4: Content Scraping (Enhanced with Paywall Detection)
==========================================================
Scrape content from prioritized URL batches using requests + trafilatura.
Includes paywall detection, deduplication, error handling, and incremental saving.
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


class ContentScraper:
    """Scrape content from URLs with paywall detection and smart deduplication."""

    def __init__(self, data_dir: str, output_dir: str):
        self.data_dir = Path(data_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)

        # Setup logging
        self.setup_logging()

        # Load existing content to avoid duplicates
        self.scraped_urls = set()
        self.content_hashes = set()
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
            "bbc.com": "none",  # BBC is free
            "dr.dk": "none",  # DR is free
            "tv2.dk": "none",  # TV2 is free
        }

        # Scraping stats
        self.stats = {
            "total_attempted": 0,
            "successful": 0,
            "failed": 0,
            "duplicates_skipped": 0,
            "paywall_detected": 0,
            "errors": [],
            "start_time": None,
            "batch_times": [],
        }

        print(f"Content Scraper initialized")
        print(f"Data directory: {self.data_dir}")
        print(f"Output directory: {self.output_dir}")
        print(f"Existing scraped URLs: {len(self.scraped_urls):,}")

    def setup_logging(self):
        """Setup logging for the scraper."""
        log_file = (
            self.output_dir
            / f"scraping_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        )

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[logging.FileHandler(log_file), logging.StreamHandler()],
        )
        self.logger = logging.getLogger(__name__)

    def load_existing_content(self):
        """Load existing scraped content to avoid duplicates."""
        content_file = self.output_dir / "scraped_content.csv"

        if content_file.exists():
            try:
                existing_df = pd.read_csv(content_file)
                self.scraped_urls = set(existing_df["url"].tolist())

                # Load content hashes if available
                if "content_hash" in existing_df.columns:
                    self.content_hashes = set(
                        existing_df["content_hash"].dropna().tolist()
                    )

                print(f"Loaded {len(self.scraped_urls)} existing URLs")
            except Exception as e:
                print(f"Error loading existing content: {e}")

    def get_content_hash(self, content: str) -> str:
        """Generate hash for content deduplication."""
        if not content:
            return ""
        # Normalize content and create hash
        normalized = re.sub(r"\s+", " ", content.strip().lower())
        return hashlib.md5(normalized.encode()).hexdigest()

    def detect_paywall(self, url: str, html: str) -> Dict:
        """Detect if a page is behind a paywall using generic patterns and specific rules."""
        paywall_info = {
            "paywall_detected": False,
            "paywall_type": None,
            "paywall_confidence": 0.0,
            "paywall_indicators": [],
            "paywall_snippet": "",
        }

        # Convert to lowercase for easier matching
        html_lower = html.lower()
        domain = urlparse(url).netloc.lower()

        # Check if it's a known paywalled domain
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
            # Common paywall keywords
            (
                r"\b(subscribe|subscription|premium|membership|login|sign.?in|register|paywall|metered|premium.?content|exclusive.?content)\b",
                0.7,
            ),
            (
                r"\b(limited.?access|free.?articles|remaining.?articles|article.?limit|monthly.?limit)\b",
                0.8,
            ),
            (
                r"\b(continue.?reading|read.?more|unlock|unlimited.?access|full.?access)\b",
                0.6,
            ),
            (
                r"\b(upgrade|premium.?plan|basic.?plan|pro.?plan|enterprise.?plan)\b",
                0.7,
            ),
            # Common paywall CSS classes and IDs
            (
                r'class=["\'](paywall|subscription|premium|metered|overlay|modal|popup)',
                0.8,
            ),
            (
                r'id=["\'](paywall|subscription|premium|metered|overlay|modal|popup)',
                0.8,
            ),
            (r'data-.*?=["\'](paywall|subscription|premium|metered)', 0.7),
            # Common paywall phrases
            (
                r"\b(you.?have.?reached.?your.?limit|you.?have.?read.?all.?free.?articles|subscribe.?to.?continue|log.?in.?to.?read|sign.?up.?to.?read)\b",
                0.9,
            ),
            (
                r"\b(thank.?you.?for.?reading|you.?have.?read.?your.?free.?articles|upgrade.?to.?continue|subscribe.?now)\b",
                0.8,
            ),
            # Overlay/modal indicators
            (r'class=["\'](overlay|modal|popup|lightbox|dialog)', 0.6),
            (
                r'style=["\'][^"\']*(display:\s*block|visibility:\s*visible|opacity:\s*1)',
                0.5,
            ),
            # Content blocking indicators
            (r'class=["\'](blur|fade|hidden|blocked|restricted)', 0.6),
            (r'style=["\'][^"\']*(filter:\s*blur|opacity:\s*0\.|display:\s*none)', 0.7),
        ]

        # Check for paywall patterns
        total_confidence = 0.0
        pattern_count = 0

        for pattern, confidence in paywall_patterns:
            matches = re.findall(pattern, html_lower, re.IGNORECASE)
            if matches:
                paywall_info["paywall_indicators"].append(f"pattern_{pattern_count}")
                total_confidence += confidence
                pattern_count += 1

                # Store a snippet of the paywall text
                if not paywall_info["paywall_snippet"] and len(matches) > 0:
                    paywall_info["paywall_snippet"] = (
                        matches[0][:200]
                        if isinstance(matches[0], str)
                        else str(matches[0])[:200]
                    )

        # Calculate overall confidence
        if pattern_count > 0:
            avg_confidence = total_confidence / pattern_count
            # Boost confidence if multiple patterns found
            if pattern_count >= 3:
                avg_confidence = min(0.95, avg_confidence + 0.2)

            # Update paywall info if confidence is high enough
            if avg_confidence > 0.5 and not paywall_info["paywall_detected"]:
                paywall_info["paywall_detected"] = True
                paywall_info["paywall_type"] = "generic"
                paywall_info["paywall_confidence"] = avg_confidence

        # Check for content length (very short content might indicate paywall)
        if len(html) < 5000:  # Very short HTML
            paywall_info["paywall_indicators"].append("short_content")
            if paywall_info["paywall_confidence"] > 0:
                paywall_info["paywall_confidence"] = min(
                    0.95, paywall_info["paywall_confidence"] + 0.1
                )

        return paywall_info

    def extract_content(self, url: str, html: str) -> Tuple[Optional[str], Dict]:
        """Extract clean content from HTML using trafilatura."""
        metadata = {
            "title": "",
            "author": "",
            "date": "",
            "description": "",
            "language": "",
            "word_count": 0,
            "extraction_method": "trafilatura",
        }

        try:
            # Extract content with trafilatura
            content = trafilatura.extract(
                html,
                include_comments=False,
                include_tables=True,
                include_formatting=False,
                url=url,
            )

            if content:
                # Extract metadata
                metadata_result = trafilatura.extract_metadata(html)
                if metadata_result:
                    metadata.update(
                        {
                            "title": metadata_result.title or "",
                            "author": metadata_result.author or "",
                            "date": str(metadata_result.date)
                            if metadata_result.date
                            else "",
                            "description": metadata_result.description or "",
                            "language": metadata_result.language or "",
                        }
                    )

                # Calculate word count
                metadata["word_count"] = len(content.split())

                return content, metadata
            else:
                # Fallback: try basic text extraction
                from bs4 import BeautifulSoup

                soup = BeautifulSoup(html, "html.parser")

                # Remove script and style elements
                for script in soup(["script", "style"]):
                    script.decompose()

                # Get text
                text = soup.get_text()
                lines = (line.strip() for line in text.splitlines())
                chunks = (
                    phrase.strip() for line in lines for phrase in line.split("  ")
                )
                content = " ".join(chunk for chunk in chunks if chunk)

                if content and len(content) > 100:  # Minimum content length
                    metadata["extraction_method"] = "beautifulsoup_fallback"
                    metadata["word_count"] = len(content.split())
                    return content, metadata

        except Exception as e:
            self.logger.warning(f"Content extraction failed for {url}: {e}")

        return None, metadata

    def scrape_url(self, url: str, timeout: int = 15) -> Dict:
        """Scrape a single URL and extract content."""
        result = {
            "url": url,
            "resolved_url": "",
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
            "paywall_detected": False,
            "paywall_type": None,
            "paywall_confidence": 0.0,
            "paywall_indicators": "",
            "paywall_snippet": "",
            "scrape_time": datetime.now().isoformat(),
            "response_time": 0,
            "error": "",
        }

        start_time = time.time()

        try:
            # Check if already scraped
            if url in self.scraped_urls:
                result["error"] = "already_scraped"
                return result

            # Make request
            response = self.session.get(url, timeout=timeout, allow_redirects=True)
            result["resolved_url"] = response.url
            result["status_code"] = response.status_code

            if response.status_code != 200:
                result["error"] = f"http_{response.status_code}"
                return result

            # Check content type
            content_type = response.headers.get("content-type", "").lower()
            if not any(ct in content_type for ct in ["text/html", "application/xhtml"]):
                result["error"] = "not_html_content"
                return result

            # Extract content
            html = response.text
            content, extraction_info = self.extract_content(url, html)

            # Detect paywall
            paywall_info = self.detect_paywall(url, html)
            result["paywall_detected"] = paywall_info["paywall_detected"]
            result["paywall_type"] = paywall_info["paywall_type"]
            result["paywall_confidence"] = paywall_info["paywall_confidence"]
            result["paywall_indicators"] = ",".join(paywall_info["paywall_indicators"])
            result["paywall_snippet"] = paywall_info["paywall_snippet"]

            # Update statistics
            if paywall_info["paywall_detected"]:
                self.stats["paywall_detected"] += 1

            if not content:
                result["error"] = extraction_info.get("reason", "no_content")
                return result

            # Check for duplicate content
            content_hash = self.get_content_hash(content)
            if content_hash in self.content_hashes:
                result["error"] = "duplicate_content"
                return result

            # Update result with content and metadata
            result["content"] = content
            result["title"] = extraction_info.get("title", "")
            result["author"] = extraction_info.get("author", "")
            result["date"] = extraction_info.get("date", "")
            result["description"] = extraction_info.get("description", "")
            result["language"] = extraction_info.get("language", "")
            result["word_count"] = extraction_info.get("word_count", 0)
            result["content_hash"] = content_hash
            result["extraction_method"] = extraction_info.get("extraction_method", "")

            # Add to sets to avoid future duplicates
            self.scraped_urls.add(url)
            self.content_hashes.add(content_hash)

        except requests.exceptions.Timeout:
            result["error"] = "timeout"
        except requests.exceptions.ConnectionError:
            result["error"] = "connection_error"
        except Exception as e:
            result["error"] = f"scraping_error: {str(e)}"
            self.logger.error(f"Error scraping {url}: {e}")

        result["response_time"] = time.time() - start_time
        return result

    def scrape_batch(self, batch_file: Path, delay: float = 2.0) -> pd.DataFrame:
        """Scrape all URLs in a batch file."""
        print(f"\nScraping batch: {batch_file.name}")

        try:
            batch_df = pd.read_csv(batch_file)
        except Exception as e:
            self.logger.error(f"Error reading batch file {batch_file}: {e}")
            return pd.DataFrame()

        results = []
        total_urls = len(batch_df)

        for idx, row in batch_df.iterrows():
            url = row["url"]
            self.stats["total_attempted"] += 1

            print(f"  [{idx + 1}/{total_urls}] Scraping: {url[:80]}...")

            result = self.scrape_url(url)

            if result["error"] == "already_scraped":
                self.stats["duplicates_skipped"] += 1
                print(f"    ⏭️  Already scraped")
            elif result["error"] == "duplicate_content":
                self.stats["duplicates_skipped"] += 1
                print(f"    ⏭️  Duplicate content")
            elif result["error"]:
                self.stats["failed"] += 1
                print(f"    ❌ Failed: {result['error']}")
            else:
                self.stats["successful"] += 1
                paywall_status = (
                    "🔒 PAYWALL" if result["paywall_detected"] else "✅ Success"
                )
                print(f"    {paywall_status} ({result['word_count']} words)")

            results.append(result)

            # Add delay between requests
            if delay > 0 and idx < total_urls - 1:
                time.sleep(delay)

        return pd.DataFrame(results)

    def save_results(self, new_results: pd.DataFrame, append: bool = True):
        """Save scraping results to CSV file."""
        if new_results.empty:
            return

        output_file = self.output_dir / "scraped_content.csv"

        if append and output_file.exists():
            # Load existing data and append
            try:
                existing_df = pd.read_csv(output_file)
                combined_df = pd.concat([existing_df, new_results], ignore_index=True)
                combined_df.to_csv(output_file, index=False)
                print(f"Appended {len(new_results)} results to {output_file}")
            except Exception as e:
                self.logger.error(f"Error appending to existing file: {e}")
                new_results.to_csv(output_file, index=False)
        else:
            # Create new file
            new_results.to_csv(output_file, index=False)
            print(f"Saved {len(new_results)} results to {output_file}")

    def save_stats(self):
        """Save scraping statistics."""
        stats_file = (
            self.output_dir
            / f"scraping_stats_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        )

        stats_data = {
            "timestamp": datetime.now().isoformat(),
            "total_attempted": self.stats["total_attempted"],
            "successful": self.stats["successful"],
            "failed": self.stats["failed"],
            "duplicates_skipped": self.stats["duplicates_skipped"],
            "paywall_detected": self.stats["paywall_detected"],
            "success_rate": self.stats["successful"]
            / max(1, self.stats["total_attempted"]),
            "paywall_rate": self.stats["paywall_detected"]
            / max(1, self.stats["successful"]),
        }

        with open(stats_file, "w") as f:
            json.dump(stats_data, f, indent=2)

        print(f"Statistics saved to {stats_file}")

    def scrape_batches(
        self,
        tier_filter: Optional[str] = None,
        max_batches: Optional[int] = None,
        delay: float = 2.0,
    ):
        """Scrape content from all batch files."""
        batches_dir = self.data_dir / "scraping_batches"

        if not batches_dir.exists():
            print(f"Error: Batches directory not found: {batches_dir}")
            return

        # Get all batch files
        batch_files = list(batches_dir.glob("batch_*.csv"))
        batch_files.sort()

        if tier_filter:
            batch_files = [f for f in batch_files if tier_filter in f.name]
            print(f"Filtering for tier: {tier_filter}")

        if max_batches:
            batch_files = batch_files[:max_batches]
            print(f"Limiting to {max_batches} batches")

        print(f"Found {len(batch_files)} batch files to process")

        if not batch_files:
            print("No batch files found to process")
            return

        self.stats["start_time"] = time.time()

        for i, batch_file in enumerate(batch_files):
            batch_start = time.time()

            print(f"\n{'=' * 60}")
            print(f"Processing batch {i + 1}/{len(batch_files)}: {batch_file.name}")
            print(f"{'=' * 60}")

            # Scrape the batch
            results = self.scrape_batch(batch_file, delay)

            # Save results
            if not results.empty:
                self.save_results(results, append=True)

            # Update batch statistics
            batch_time = time.time() - batch_start
            self.stats["batch_times"].append(batch_time)

            print(f"\nBatch completed in {batch_time:.1f} seconds")
            print(f"Progress: {i + 1}/{len(batch_files)} batches")
            print(f"Total URLs processed: {self.stats['total_attempted']:,}")
            print(f"Successful: {self.stats['successful']:,}")
            print(f"Failed: {self.stats['failed']:,}")
            print(f"Paywall detected: {self.stats['paywall_detected']:,}")

            # Save stats after each batch
            self.save_stats()

        # Final summary
        total_time = time.time() - self.stats["start_time"]
        print(f"\n{'=' * 60}")
        print(f"SCRAPING COMPLETED")
        print(f"{'=' * 60}")
        print(f"Total URLs attempted: {self.stats['total_attempted']:,}")
        print(f"Successful extractions: {self.stats['successful']:,}")
        print(f"Failed attempts: {self.stats['failed']:,}")
        print(f"Duplicates skipped: {self.stats['duplicates_skipped']:,}")
        print(f"Paywall detected: {self.stats['paywall_detected']:,}")
        print(
            f"Success rate: {(self.stats['successful'] / max(1, self.stats['total_attempted'])) * 100:.1f}%"
        )
        print(
            f"Paywall rate: {(self.stats['paywall_detected'] / max(1, self.stats['successful'])) * 100:.1f}%"
        )
        print(f"Total time: {total_time / 3600:.2f} hours")


def main():
    """Main function to run content scraping."""
    print("Content Scraping - Step 4 (Enhanced with Paywall Detection)")
    print("=" * 60)

    # Define paths
    data_dir = "data/url_extract"
    output_dir = "data/url_extract/scraped_content"

    # Check if required files exist
    batches_dir = Path(data_dir) / "scraping_batches"
    if not batches_dir.exists():
        print(f"Error: Batches directory not found: {batches_dir}")
        print("Please run step3_prioritize_domains.py first")
        return

    # Initialize scraper
    scraper = ContentScraper(data_dir, output_dir)

    # Ask user for scraping options
    print("\nScraping Options:")
    print("1. Start with Tier 1 News (highest priority, ~4k URLs)")
    print("2. Scrape all news tiers (~8.5k URLs)")
    print("3. Scrape everything (~9.7k URLs)")
    print("4. Test run (first 2 batches only)")

    choice = input("\nEnter your choice (1-4): ").strip()

    if choice == "1":
        scraper.scrape_batches(tier_filter="tier_1_news")
    elif choice == "2":
        scraper.scrape_batches(tier_filter="tier_")
    elif choice == "3":
        scraper.scrape_batches()
    elif choice == "4":
        scraper.scrape_batches(max_batches=2)
    else:
        print("Invalid choice, scraping all batches")
        scraper.scrape_batches()

    print(f"\nScraping completed!")
    print(f"Total URLs processed: {scraper.stats['total_attempted']}")
    print(f"Successful: {scraper.stats['successful']}")
    print(f"Failed: {scraper.stats['failed']}")
    print(f"Paywall detected: {scraper.stats['paywall_detected']}")
    print(
        f"Success rate: {(scraper.stats['successful'] / max(1, scraper.stats['total_attempted'])) * 100:.1f}%"
    )


if __name__ == "__main__":
    main()
