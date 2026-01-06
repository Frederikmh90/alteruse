#!/usr/bin/env python3
"""
Enhanced Content Scraper
Scrapes content from resolved URLs with duplicate avoidance and paywall detection.
"""

import requests
import pandas as pd
import time
import logging
import os
import hashlib
import sqlite3
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urlparse, urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed
import trafilatura
from trafilatura.settings import use_config
import justext
from bs4 import BeautifulSoup
import re


class EnhancedContentScraper:
    """
    Enhanced content scraper with robust URL resolution, duplicate avoidance, and paywall detection.
    """

    def __init__(
        self,
        cache_file: str = "enhanced_scraping_cache.db",
        max_workers: int = 8,
        delay_between_requests: float = 0.2,
        timeout: int = 30,
        batch_size: int = 100,
    ):
        self.cache_file = cache_file
        self.max_workers = max_workers
        self.delay_between_requests = delay_between_requests
        self.timeout = timeout
        self.batch_size = batch_size

        # Setup logging
        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
        )
        self.logger = logging.getLogger(__name__)

        # Initialize cache database
        self._init_cache_db()

        # Setup session with robust headers
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9,da;q=0.8",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "none",
            }
        )

        # Configure trafilatura for better extraction
        self.trafilatura_config = use_config()
        self.trafilatura_config.set("DEFAULT", "EXTRACTION_TIMEOUT", "30")

        # Paywall detection patterns
        self.paywall_domains = {
            "nytimes.com",
            "wsj.com",
            "ft.com",
            "economist.com",
            "politiken.dk",
            "berlingske.dk",
            "weekendavisen.dk",
            "information.dk",
            "jyllands-posten.dk",
        }

        self.paywall_indicators = [
            "subscribe",
            "subscription",
            "paywall",
            "premium",
            "member",
            "membership",
            "login to continue",
            "sign in to read",
            "become a subscriber",
            "this article is for subscribers",
            "abonner",
            "abonnement",
            "tilmeld",
            "log ind for at læse",
            "denne artikel er for abonnenter",
        ]

        self.paywall_css_selectors = [
            ".paywall",
            ".subscription-wall",
            ".premium-content",
            ".subscriber-only",
            ".member-content",
            '[class*="paywall"]',
            '[class*="subscription"]',
            '[class*="premium"]',
        ]

    def _init_cache_db(self):
        """Initialize SQLite cache database for scraped content."""
        conn = sqlite3.connect(self.cache_file)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS scraped_cache (
                url_hash TEXT PRIMARY KEY,
                original_url TEXT,
                resolved_url TEXT,
                content TEXT,
                content_hash TEXT,
                title TEXT,
                author TEXT,
                date TEXT,
                description TEXT,
                language TEXT,
                word_count INTEGER,
                status_code INTEGER,
                content_type TEXT,
                scraping_time REAL,
                error TEXT,
                extraction_method TEXT,
                paywall_detected BOOLEAN,
                paywall_type TEXT,
                paywall_confidence REAL,
                paywall_indicators TEXT,
                paywall_snippet TEXT,
                scraped_at TIMESTAMP
            )
        """)
        conn.commit()
        conn.close()

    def _get_url_hash(self, url: str) -> str:
        """Generate a hash for URL caching."""
        return hashlib.md5(url.encode()).hexdigest()

    def _get_from_cache(self, url: str) -> Optional[Dict]:
        """Get scraped content from cache."""
        url_hash = self._get_url_hash(url)
        conn = sqlite3.connect(self.cache_file)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM scraped_cache WHERE url_hash = ?", (url_hash,))
        result = cursor.fetchone()
        conn.close()

        if result:
            columns = [
                "url_hash",
                "original_url",
                "resolved_url",
                "content",
                "content_hash",
                "title",
                "author",
                "date",
                "description",
                "language",
                "word_count",
                "status_code",
                "content_type",
                "scraping_time",
                "error",
                "extraction_method",
                "paywall_detected",
                "paywall_type",
                "paywall_confidence",
                "paywall_indicators",
                "paywall_snippet",
                "scraped_at",
            ]
            return dict(zip(columns, result))
        return None

    def _save_to_cache(self, result: Dict):
        """Save scraped content to cache."""
        url_hash = self._get_url_hash(result["resolved_url"])
        conn = sqlite3.connect(self.cache_file)
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT OR REPLACE INTO scraped_cache 
            (url_hash, original_url, resolved_url, content, content_hash, title, author, date,
             description, language, word_count, status_code, content_type, scraping_time, error,
             extraction_method, paywall_detected, paywall_type, paywall_confidence, 
             paywall_indicators, paywall_snippet, scraped_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                url_hash,
                result["original_url"],
                result["resolved_url"],
                result["content"],
                result["content_hash"],
                result["title"],
                result["author"],
                result["date"],
                result["description"],
                result["language"],
                result["word_count"],
                result["status_code"],
                result["content_type"],
                result["scraping_time"],
                result["error"],
                result["extraction_method"],
                result["paywall_detected"],
                result["paywall_type"],
                result["paywall_confidence"],
                result["paywall_indicators"],
                result["paywall_snippet"],
                datetime.now().isoformat(),
            ),
        )
        conn.commit()
        conn.close()

    def detect_paywall(self, html_content: str, url: str) -> Dict:
        """Detect if content is behind a paywall."""
        domain = urlparse(url).netloc.lower()

        # Check known paywall domains
        is_paywall_domain = any(domain.endswith(pd) for pd in self.paywall_domains)

        # Parse HTML for analysis
        soup = BeautifulSoup(html_content, "html.parser")
        text_content = soup.get_text().lower()

        # Check for paywall indicators in text
        text_indicators = [
            indicator
            for indicator in self.paywall_indicators
            if indicator in text_content
        ]

        # Check for paywall CSS selectors
        css_indicators = []
        for selector in self.paywall_css_selectors:
            elements = soup.select(selector)
            if elements:
                css_indicators.extend([elem.get_text()[:100] for elem in elements[:3]])

        # Calculate confidence score
        confidence = 0.0
        if is_paywall_domain:
            confidence += 0.3
        if text_indicators:
            confidence += 0.4
        if css_indicators:
            confidence += 0.3

        # Additional checks for partial content
        if len(text_content) < 200 and any(
            word in text_content for word in ["subscribe", "login"]
        ):
            confidence += 0.2

        paywall_detected = confidence > 0.5
        paywall_type = (
            "hard" if confidence > 0.7 else "soft" if paywall_detected else None
        )

        # Get snippet of paywall content
        paywall_snippet = ""
        if paywall_detected and text_indicators:
            for indicator in text_indicators[:2]:
                start = text_content.find(indicator)
                if start != -1:
                    paywall_snippet += (
                        text_content[max(0, start - 50) : start + 100] + "... "
                    )

        return {
            "paywall_detected": paywall_detected,
            "paywall_type": paywall_type,
            "paywall_confidence": confidence,
            "paywall_indicators": "; ".join(text_indicators + css_indicators),
            "paywall_snippet": paywall_snippet[:200],
        }

    def extract_content(self, html_content: str, url: str) -> Dict:
        """Extract content using multiple methods."""
        extraction_results = {}

        # Method 1: Trafilatura (primary)
        try:
            trafilatura_result = trafilatura.extract(
                html_content,
                config=self.trafilatura_config,
                include_comments=False,
                include_tables=True,
                url=url,
            )
            if trafilatura_result:
                extraction_results["trafilatura"] = trafilatura_result
        except Exception as e:
            self.logger.debug(f"Trafilatura extraction failed for {url}: {e}")

        # Method 2: jusText (fallback)
        try:
            paragraphs = justext.justext(html_content, justext.get_stoplist("English"))
            justext_content = "\n".join(
                [p.text for p in paragraphs if not p.is_boilerplate]
            )
            if len(justext_content) > 100:
                extraction_results["justext"] = justext_content
        except Exception as e:
            self.logger.debug(f"jusText extraction failed for {url}: {e}")

        # Method 3: BeautifulSoup (basic fallback)
        try:
            soup = BeautifulSoup(html_content, "html.parser")

            # Remove script and style elements
            for script in soup(["script", "style"]):
                script.decompose()

            # Try to find main content areas
            main_content = (
                soup.find("main")
                or soup.find("article")
                or soup.find("div", class_=re.compile(r"content|article|main", re.I))
                or soup.find("div", id=re.compile(r"content|article|main", re.I))
            )

            if main_content:
                bs_content = main_content.get_text()
            else:
                bs_content = soup.get_text()

            # Clean up whitespace
            bs_content = re.sub(r"\s+", " ", bs_content).strip()
            if len(bs_content) > 100:
                extraction_results["beautifulsoup"] = bs_content

        except Exception as e:
            self.logger.debug(f"BeautifulSoup extraction failed for {url}: {e}")

        # Choose best extraction
        if "trafilatura" in extraction_results:
            content = extraction_results["trafilatura"]
            method = "trafilatura"
        elif "justext" in extraction_results:
            content = extraction_results["justext"]
            method = "justext"
        elif "beautifulsoup" in extraction_results:
            content = extraction_results["beautifulsoup"]
            method = "beautifulsoup"
        else:
            content = ""
            method = "failed"

        return {
            "content": content,
            "extraction_method": method,
            "word_count": len(content.split()) if content else 0,
        }

    def extract_metadata(self, html_content: str, url: str) -> Dict:
        """Extract metadata from HTML."""
        soup = BeautifulSoup(html_content, "html.parser")

        # Title
        title = ""
        title_tag = soup.find("title")
        if title_tag:
            title = title_tag.get_text().strip()

        # Meta description
        description = ""
        desc_tag = soup.find("meta", attrs={"name": "description"})
        if desc_tag:
            description = desc_tag.get("content", "").strip()

        # Author
        author = ""
        author_tag = (
            soup.find("meta", attrs={"name": "author"})
            or soup.find("meta", attrs={"property": "article:author"})
            or soup.find("span", class_=re.compile(r"author", re.I))
            or soup.find("div", class_=re.compile(r"author", re.I))
        )
        if author_tag:
            if author_tag.name == "meta":
                author = author_tag.get("content", "").strip()
            else:
                author = author_tag.get_text().strip()

        # Date
        date = ""
        date_tag = (
            soup.find("meta", attrs={"property": "article:published_time"})
            or soup.find("meta", attrs={"name": "date"})
            or soup.find("time")
        )
        if date_tag:
            if date_tag.name == "meta":
                date = date_tag.get("content", "").strip()
            else:
                date = date_tag.get("datetime") or date_tag.get_text().strip()

        # Language
        language = ""
        lang_tag = soup.find("html")
        if lang_tag:
            language = lang_tag.get("lang", "").strip()

        return {
            "title": title,
            "description": description,
            "author": author,
            "date": date,
            "language": language,
        }

    def scrape_url(self, original_url: str, resolved_url: str) -> Dict:
        """Scrape content from a resolved URL."""
        # Check cache first
        cached_result = self._get_from_cache(resolved_url)
        if cached_result:
            self.logger.debug(f"Cache hit for {resolved_url}")
            return cached_result

        start_time = time.time()

        try:
            self.logger.debug(f"Scraping: {resolved_url}")

            # Make request
            response = self.session.get(resolved_url, timeout=self.timeout)
            response.raise_for_status()

            scraping_time = time.time() - start_time
            html_content = response.text

            # Extract content
            content_data = self.extract_content(html_content, resolved_url)

            # Extract metadata
            metadata = self.extract_metadata(html_content, resolved_url)

            # Detect paywall
            paywall_data = self.detect_paywall(html_content, resolved_url)

            # Generate content hash
            content_hash = hashlib.md5(content_data["content"].encode()).hexdigest()

            result = {
                "original_url": original_url,
                "resolved_url": resolved_url,
                "content": content_data["content"],
                "content_hash": content_hash,
                "title": metadata["title"],
                "author": metadata["author"],
                "date": metadata["date"],
                "description": metadata["description"],
                "language": metadata["language"],
                "word_count": content_data["word_count"],
                "status_code": response.status_code,
                "content_type": response.headers.get("content-type", ""),
                "scraping_time": scraping_time,
                "error": None,
                "extraction_method": content_data["extraction_method"],
                **paywall_data,
            }

        except requests.exceptions.Timeout:
            result = {
                "original_url": original_url,
                "resolved_url": resolved_url,
                "content": "",
                "content_hash": "",
                "title": "",
                "author": "",
                "date": "",
                "description": "",
                "language": "",
                "word_count": 0,
                "status_code": None,
                "content_type": "",
                "scraping_time": time.time() - start_time,
                "error": f"timeout_after_{self.timeout}s",
                "extraction_method": "failed",
                "paywall_detected": False,
                "paywall_type": None,
                "paywall_confidence": 0.0,
                "paywall_indicators": "",
                "paywall_snippet": "",
            }

        except Exception as e:
            result = {
                "original_url": original_url,
                "resolved_url": resolved_url,
                "content": "",
                "content_hash": "",
                "title": "",
                "author": "",
                "date": "",
                "description": "",
                "language": "",
                "word_count": 0,
                "status_code": getattr(e.response, "status_code", None)
                if hasattr(e, "response")
                else None,
                "content_type": "",
                "scraping_time": time.time() - start_time,
                "error": str(e),
                "extraction_method": "failed",
                "paywall_detected": False,
                "paywall_type": None,
                "paywall_confidence": 0.0,
                "paywall_indicators": "",
                "paywall_snippet": "",
            }

        # Cache the result
        self._save_to_cache(result)

        # Rate limiting
        time.sleep(self.delay_between_requests)

        return result

    def scrape_urls_batch(self, url_pairs: List[Tuple[str, str]]) -> List[Dict]:
        """Scrape URLs in batches with parallel processing."""
        results = []
        total_urls = len(url_pairs)

        self.logger.info(f"Starting content scraping for {total_urls} URLs")
        self.logger.info(
            f"Using {self.max_workers} workers with {self.delay_between_requests}s delay"
        )

        # Process URLs in batches
        for i in range(0, total_urls, self.batch_size):
            batch = url_pairs[i : i + self.batch_size]
            batch_num = (i // self.batch_size) + 1
            total_batches = (total_urls + self.batch_size - 1) // self.batch_size

            self.logger.info(
                f"Processing batch {batch_num}/{total_batches} ({len(batch)} URLs)"
            )

            # Parallel processing within batch
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_url = {
                    executor.submit(self.scrape_url, original_url, resolved_url): (
                        original_url,
                        resolved_url,
                    )
                    for original_url, resolved_url in batch
                }

                batch_results = []
                for future in as_completed(future_to_url):
                    result = future.result()
                    batch_results.append(result)

                    if len(batch_results) % 10 == 0:
                        self.logger.info(
                            f"  Completed {len(batch_results)}/{len(batch)} in current batch"
                        )

            results.extend(batch_results)

            # Longer pause between batches
            if i + self.batch_size < total_urls:
                self.logger.info(f"Pausing 5 seconds between batches...")
                time.sleep(5)

        self.logger.info(f"Content scraping completed. Processed {len(results)} URLs")
        return results

    def generate_scraping_report(self, results: List[Dict]) -> Dict:
        """Generate a summary report of scraping results."""
        total = len(results)
        successful = sum(
            1 for r in results if r["error"] is None and r["word_count"] > 0
        )
        failed = total - successful
        paywall_detected = sum(1 for r in results if r["paywall_detected"])

        # Calculate average metrics
        avg_word_count = (
            sum(r["word_count"] for r in results) / total if total > 0 else 0
        )
        avg_scraping_time = (
            sum(r["scraping_time"] for r in results) / total if total > 0 else 0
        )

        # Analyze extraction methods
        method_counts = {}
        for result in results:
            method = result["extraction_method"]
            method_counts[method] = method_counts.get(method, 0) + 1

        # Analyze error types
        error_counts = {}
        for result in results:
            if result["error"]:
                error_type = result["error"].split(":")[0]
                error_counts[error_type] = error_counts.get(error_type, 0) + 1

        report = {
            "total_urls": total,
            "successful_scrapes": successful,
            "failed_scrapes": failed,
            "success_rate": (successful / total * 100) if total > 0 else 0,
            "paywall_detected_count": paywall_detected,
            "paywall_rate": (paywall_detected / total * 100) if total > 0 else 0,
            "avg_word_count": avg_word_count,
            "avg_scraping_time": avg_scraping_time,
            "extraction_methods": dict(
                sorted(method_counts.items(), key=lambda x: x[1], reverse=True)
            ),
            "error_breakdown": dict(
                sorted(error_counts.items(), key=lambda x: x[1], reverse=True)
            ),
        }

        return report


def main():
    """Main function to test the enhanced scraper."""
    # Test URLs
    test_urls = [
        ("https://politiken.dk", "https://politiken.dk/"),
        ("https://dr.dk", "https://www.dr.dk/"),
        (
            "https://youtu.be/dQw4w9WgXcQ",
            "https://www.youtube.com/watch?v=dQw4w9WgXcQ&feature=youtu.be",
        ),
    ]

    scraper = EnhancedContentScraper(
        cache_file="test_enhanced_scraping.db",
        max_workers=3,
        delay_between_requests=1.0,
    )

    print("=== Testing Enhanced Content Scraper ===")
    results = scraper.scrape_urls_batch(test_urls)

    print("\n=== Results ===")
    for result in results:
        status = "✓" if result["error"] is None else "✗"
        paywall = "🔒" if result["paywall_detected"] else ""
        print(f"{status} {paywall} {result['resolved_url']}")
        print(
            f"    Words: {result['word_count']}, Method: {result['extraction_method']}"
        )
        if result["error"]:
            print(f"    Error: {result['error']}")
        if result["paywall_detected"]:
            print(
                f"    Paywall: {result['paywall_type']} (confidence: {result['paywall_confidence']:.2f})"
            )

    print("\n=== Summary Report ===")
    report = scraper.generate_scraping_report(results)
    for key, value in report.items():
        if isinstance(value, dict):
            print(f"{key}:")
            for k, v in value.items():
                print(f"  {k}: {v}")
        else:
            print(
                f"{key}: {value:.2f}" if isinstance(value, float) else f"{key}: {value}"
            )


if __name__ == "__main__":
    main()
