#!/usr/bin/env python3
"""
Step 4: Facebook Content Scraping (Enhanced with Paywall Detection)
===================================================================
Scrape content from prioritized Facebook URL batches using requests + trafilatura.
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


class FacebookContentScraper:
    """Scrape content from Facebook URLs with paywall detection and smart deduplication."""

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

        # Statistics
        self.stats = {
            "start_time": None,
            "total_attempted": 0,
            "successful": 0,
            "failed": 0,
            "duplicates_skipped": 0,
            "paywall_detected": 0,
            "batch_times": [],
            "success_rate": 0.0,
        }

    def setup_logging(self):
        """Setup logging for the scraping process."""
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
        existing_file = self.output_dir / "scraped_content_facebook.csv"
        if existing_file.exists():
            try:
                existing_df = pd.read_csv(existing_file)
                self.scraped_urls = set(existing_df["url"].tolist())
                self.logger.info(f"Loaded {len(self.scraped_urls)} existing URLs")
            except Exception as e:
                self.logger.warning(f"Could not load existing content: {e}")

    def get_content_hash(self, content: str) -> str:
        """Generate hash for content to detect duplicates."""
        return hashlib.md5(content.encode("utf-8")).hexdigest()

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
        """Extract content from HTML using trafilatura with fallback."""
        try:
            # Try trafilatura first
            extracted = trafilatura.extract(
                html, include_links=True, include_images=True
            )

            if extracted and len(extracted.strip()) > 50:  # Minimum content length
                return extracted.strip(), {"method": "trafilatura", "success": True}

            # Fallback: extract title and meta description
            from bs4 import BeautifulSoup

            soup = BeautifulSoup(html, "html.parser")

            # Get title
            title = soup.find("title")
            title_text = title.get_text().strip() if title else ""

            # Get meta description
            meta_desc = soup.find("meta", attrs={"name": "description"})
            desc_text = meta_desc.get("content", "").strip() if meta_desc else ""

            # Combine title and description
            fallback_content = f"{title_text}\n\n{desc_text}".strip()

            if fallback_content and len(fallback_content) > 20:
                return fallback_content, {"method": "fallback", "success": True}

            return None, {
                "method": "none",
                "success": False,
                "reason": "no_content_extracted",
            }

        except Exception as e:
            return None, {"method": "error", "success": False, "reason": str(e)}

    def scrape_url(self, url: str, timeout: int = 15) -> Dict:
        """Scrape a single URL and return content with metadata including paywall detection."""
        result = {
            "url": url,
            "resolved_url": url,
            "domain": "",
            "content": "",
            "content_hash": "",
            "word_count": 0,
            "status_code": 0,
            "content_type": "",
            "scraping_time": 0.0,
            "error": "",
            "extraction_method": "",
            "paywall_detected": False,
            "paywall_type": None,
            "paywall_confidence": 0.0,
            "paywall_indicators": "",
            "paywall_snippet": "",
            "scraped_at": datetime.now().isoformat(),
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
            result["content_type"] = response.headers.get("content-type", "")
            result["domain"] = urlparse(response.url).netloc

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

            # Success!
            result["content"] = content
            result["content_hash"] = content_hash
            result["word_count"] = len(content.split())
            result["extraction_method"] = extraction_info.get("method", "unknown")
            result["scraping_time"] = time.time() - start_time

            # Update tracking sets
            self.scraped_urls.add(url)
            self.content_hashes.add(content_hash)

            return result

        except Exception as e:
            result["error"] = str(e)
            result["scraping_time"] = time.time() - start_time
            return result

    def scrape_batch(self, batch_file: Path, delay: float = 2.0) -> pd.DataFrame:
        """Scrape a batch of URLs."""
        self.logger.info(f"Scraping batch: {batch_file.name}")

        try:
            batch_df = pd.read_csv(batch_file)
        except Exception as e:
            self.logger.error(f"Could not read batch file {batch_file}: {e}")
            return pd.DataFrame()

        results = []
        batch_start = time.time()

        for idx, row in batch_df.iterrows():
            url = row["url"]
            self.logger.info(f"Scraping {idx + 1}/{len(batch_df)}: {url[:60]}...")

            result = self.scrape_url(url)
            results.append(result)

            # Update statistics
            self.stats["total_attempted"] += 1
            if result["error"] == "":
                self.stats["successful"] += 1
            elif result["error"] in ["already_scraped", "duplicate_content"]:
                self.stats["duplicates_skipped"] += 1
            else:
                self.stats["failed"] += 1

            # Delay between requests
            if delay > 0:
                time.sleep(delay)

        batch_time = time.time() - batch_start
        self.stats["batch_times"].append(batch_time)

        self.logger.info(f"Batch completed in {batch_time:.2f}s")

        return pd.DataFrame(results)

    def save_results(self, new_results: pd.DataFrame, append: bool = True):
        """Save scraping results to CSV."""
        output_file = self.output_dir / "scraped_content_facebook.csv"

        if append and output_file.exists():
            try:
                existing_df = pd.read_csv(output_file)
                combined_df = pd.concat([existing_df, new_results], ignore_index=True)
                combined_df.to_csv(output_file, index=False)
                self.logger.info(
                    f"Appended {len(new_results)} results to {output_file}"
                )
            except Exception as e:
                self.logger.error(f"Error appending results: {e}")
                new_results.to_csv(output_file, index=False)
        else:
            new_results.to_csv(output_file, index=False)
            self.logger.info(f"Saved {len(new_results)} results to {output_file}")

    def save_stats(self):
        """Save scraping statistics."""
        if self.stats["total_attempted"] > 0:
            self.stats["success_rate"] = (
                self.stats["successful"] / self.stats["total_attempted"]
            )

        stats_file = self.output_dir / "scraping_stats_facebook.json"
        with open(stats_file, "w") as f:
            json.dump(self.stats, f, indent=2, default=str)

        self.logger.info(f"Statistics saved to {stats_file}")

    def scrape_batches(
        self,
        tier_filter: Optional[str] = None,
        max_batches: Optional[int] = None,
        delay: float = 2.0,
    ):
        """Scrape multiple batches."""
        self.stats["start_time"] = datetime.now().isoformat()

        batches_dir = self.data_dir / "scraping_batches"
        if not batches_dir.exists():
            self.logger.error(f"Batches directory not found: {batches_dir}")
            return

        batch_files = list(batches_dir.glob("*.csv"))

        if tier_filter:
            batch_files = [f for f in batch_files if tier_filter in f.name]

        if max_batches:
            batch_files = batch_files[:max_batches]

        self.logger.info(f"Found {len(batch_files)} batch files to process")

        for i, batch_file in enumerate(batch_files, 1):
            self.logger.info(
                f"Processing batch {i}/{len(batch_files)}: {batch_file.name}"
            )

            results = self.scrape_batch(batch_file, delay)

            if not results.empty:
                self.save_results(results, append=True)

            # Save stats after each batch
            self.save_stats()

            # Print progress
            success_rate = (
                self.stats["successful"] / max(1, self.stats["total_attempted"])
            ) * 100
            paywall_rate = (
                self.stats["paywall_detected"] / max(1, self.stats["total_attempted"])
            ) * 100

            print(f"Progress: {i}/{len(batch_files)} batches")
            print(f"Success rate: {success_rate:.1f}%")
            print(f"Paywall detection rate: {paywall_rate:.1f}%")
            print(f"Total URLs processed: {self.stats['total_attempted']}")
            print("-" * 50)

        self.logger.info("All batches completed!")


def main():
    """Main function to run Facebook content scraping."""
    print("Facebook Content Scraping - Step 4 (Enhanced with Paywall Detection)")
    print("=" * 70)

    # Define paths
    data_dir = "../../data/url_extract_facebook"
    output_dir = "../../data/url_extract_facebook"

    # Check if required files exist
    batches_dir = Path(data_dir) / "scraping_batches"
    if not batches_dir.exists():
        print(f"Error: Batches directory not found: {batches_dir}")
        print("Please run step3_prioritize_domains_facebook.py first")
        return

    # Initialize scraper
    scraper = FacebookContentScraper(data_dir, output_dir)

    # Ask user for scraping options
    print("\nScraping Options:")
    print("1. Scrape all batches")
    print("2. Scrape specific tier (e.g., tier_1_high_priority)")
    print("3. Scrape limited number of batches")

    choice = input("\nEnter your choice (1-3): ").strip()

    if choice == "1":
        scraper.scrape_batches()
    elif choice == "2":
        tier = input("Enter tier name (e.g., tier_1_high_priority): ").strip()
        scraper.scrape_batches(tier_filter=tier)
    elif choice == "3":
        try:
            max_batches = int(input("Enter number of batches to scrape: ").strip())
            scraper.scrape_batches(max_batches=max_batches)
        except ValueError:
            print("Invalid number, scraping all batches")
            scraper.scrape_batches()
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
