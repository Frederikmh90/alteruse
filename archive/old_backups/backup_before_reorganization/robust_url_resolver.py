import requests
import pandas as pd
import time
import logging
from urllib.parse import urlparse, urljoin
from typing import Dict, List, Optional, Tuple
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import sqlite3
import os
from datetime import datetime


class RobustURLResolver:
    """
    A robust URL resolver that handles redirects properly and caches results.
    """

    def __init__(
        self,
        cache_file: str = "url_resolution_cache.db",
        timeout: int = 30,
        max_redirects: int = 10,
        max_workers: int = 10,
        delay_between_requests: float = 0.1,
    ):
        self.timeout = timeout
        self.max_redirects = max_redirects
        self.max_workers = max_workers
        self.delay_between_requests = delay_between_requests
        self.cache_file = cache_file

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

    def _init_cache_db(self):
        """Initialize SQLite cache database."""
        conn = sqlite3.connect(self.cache_file)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS url_cache (
                original_url TEXT PRIMARY KEY,
                resolved_url TEXT,
                status_code INTEGER,
                redirect_count INTEGER,
                success BOOLEAN,
                error TEXT,
                resolution_worked BOOLEAN,
                cached_at TIMESTAMP,
                response_time REAL
            )
        """)
        conn.commit()
        conn.close()

    def _get_from_cache(self, url: str) -> Optional[Dict]:
        """Get URL resolution from cache."""
        conn = sqlite3.connect(self.cache_file)
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM url_cache WHERE original_url = ?", (url,))
        result = cursor.fetchone()
        conn.close()

        if result:
            return {
                "original_url": result[0],
                "resolved_url": result[1],
                "status_code": result[2],
                "redirect_count": result[3],
                "success": bool(result[4]),
                "error": result[5],
                "resolution_worked": bool(result[6]),
                "cached_at": result[7],
                "response_time": result[8],
            }
        return None

    def _save_to_cache(self, result: Dict):
        """Save URL resolution to cache."""
        conn = sqlite3.connect(self.cache_file)
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT OR REPLACE INTO url_cache 
            (original_url, resolved_url, status_code, redirect_count, success, 
             error, resolution_worked, cached_at, response_time)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
            (
                result["original_url"],
                result["resolved_url"],
                result["status_code"],
                result["redirect_count"],
                result["success"],
                result["error"],
                result["resolution_worked"],
                datetime.now().isoformat(),
                result.get("response_time", 0.0),
            ),
        )
        conn.commit()
        conn.close()

    def resolve_single_url(self, url: str) -> Dict:
        """
        Resolve a single URL, handling redirects properly.
        """
        # Check cache first
        cached_result = self._get_from_cache(url)
        if cached_result:
            self.logger.debug(f"Cache hit for {url}")
            return cached_result

        start_time = time.time()

        try:
            self.logger.debug(f"Resolving URL: {url}")

            # Make request with robust settings
            response = self.session.get(
                url,
                allow_redirects=True,
                timeout=self.timeout,
                verify=True,  # Keep SSL verification
            )

            response_time = time.time() - start_time
            final_url = response.url
            redirect_count = len(response.history)

            result = {
                "original_url": url,
                "resolved_url": final_url,
                "status_code": response.status_code,
                "redirect_count": redirect_count,
                "success": True,
                "error": None,
                "resolution_worked": url != final_url,
                "response_time": response_time,
            }

            self.logger.debug(
                f"  Success: {url} -> {final_url} ({redirect_count} redirects)"
            )

        except requests.exceptions.SSLError as e:
            # Try without SSL verification for problematic sites
            try:
                self.logger.warning(
                    f"SSL error for {url}, retrying without verification"
                )
                response = self.session.get(
                    url, allow_redirects=True, timeout=self.timeout, verify=False
                )
                response_time = time.time() - start_time
                final_url = response.url
                redirect_count = len(response.history)

                result = {
                    "original_url": url,
                    "resolved_url": final_url,
                    "status_code": response.status_code,
                    "redirect_count": redirect_count,
                    "success": True,
                    "error": "ssl_warning",
                    "resolution_worked": url != final_url,
                    "response_time": response_time,
                }

            except Exception as e2:
                response_time = time.time() - start_time
                result = {
                    "original_url": url,
                    "resolved_url": url,
                    "status_code": None,
                    "redirect_count": 0,
                    "success": False,
                    "error": f"ssl_error: {str(e2)}",
                    "resolution_worked": False,
                    "response_time": response_time,
                }

        except requests.exceptions.Timeout:
            response_time = time.time() - start_time
            result = {
                "original_url": url,
                "resolved_url": url,
                "status_code": None,
                "redirect_count": 0,
                "success": False,
                "error": f"timeout_after_{self.timeout}s",
                "resolution_worked": False,
                "response_time": response_time,
            }

        except requests.exceptions.TooManyRedirects:
            response_time = time.time() - start_time
            result = {
                "original_url": url,
                "resolved_url": url,
                "status_code": None,
                "redirect_count": self.max_redirects,
                "success": False,
                "error": f"too_many_redirects_{self.max_redirects}",
                "resolution_worked": False,
                "response_time": response_time,
            }

        except Exception as e:
            response_time = time.time() - start_time
            result = {
                "original_url": url,
                "resolved_url": url,
                "status_code": None,
                "redirect_count": 0,
                "success": False,
                "error": str(e),
                "resolution_worked": False,
                "response_time": response_time,
            }

        # Cache the result
        self._save_to_cache(result)

        # Rate limiting
        time.sleep(self.delay_between_requests)

        return result

    def resolve_urls_batch(self, urls: List[str], batch_size: int = 100) -> List[Dict]:
        """
        Resolve URLs in batches with parallel processing.
        """
        results = []
        total_urls = len(urls)

        self.logger.info(f"Starting URL resolution for {total_urls} URLs")
        self.logger.info(
            f"Using {self.max_workers} workers with {self.delay_between_requests}s delay"
        )

        # Process URLs in batches to avoid overwhelming servers
        for i in range(0, total_urls, batch_size):
            batch = urls[i : i + batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (total_urls + batch_size - 1) // batch_size

            self.logger.info(
                f"Processing batch {batch_num}/{total_batches} ({len(batch)} URLs)"
            )

            # Parallel processing within batch
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_url = {
                    executor.submit(self.resolve_single_url, url): url for url in batch
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
            if i + batch_size < total_urls:
                self.logger.info(f"Pausing 5 seconds between batches...")
                time.sleep(5)

        self.logger.info(f"URL resolution completed. Processed {len(results)} URLs")
        return results

    def generate_resolution_report(self, results: List[Dict]) -> Dict:
        """Generate a summary report of URL resolution results."""
        total = len(results)
        successful = sum(1 for r in results if r["success"])
        failed = total - successful
        redirected = sum(1 for r in results if r["resolution_worked"])

        # Analyze failure reasons
        error_counts = {}
        for result in results:
            if not result["success"] and result["error"]:
                error_type = result["error"].split(":")[0]  # Get error type
                error_counts[error_type] = error_counts.get(error_type, 0) + 1

        # Analyze domains with most redirects
        redirect_domains = {}
        for result in results:
            if result["resolution_worked"]:
                domain = urlparse(result["original_url"]).netloc
                redirect_domains[domain] = redirect_domains.get(domain, 0) + 1

        report = {
            "total_urls": total,
            "successful_resolutions": successful,
            "failed_resolutions": failed,
            "success_rate": (successful / total * 100) if total > 0 else 0,
            "urls_redirected": redirected,
            "redirect_rate": (redirected / total * 100) if total > 0 else 0,
            "error_breakdown": dict(
                sorted(error_counts.items(), key=lambda x: x[1], reverse=True)
            ),
            "top_redirect_domains": dict(
                sorted(redirect_domains.items(), key=lambda x: x[1], reverse=True)[:10]
            ),
        }

        return report


def test_resolver():
    """Test the robust URL resolver with various URL types."""
    test_urls = [
        # YouTube short URLs (should redirect)
        "https://youtu.be/dQw4w9WgXcQ",
        # Danish news sites (might have redirects)
        "https://politiken.dk",
        "https://ekstrabladet.dk",
        "https://tv2.dk",
        "https://dr.dk",
        # Social media redirects
        "https://reddit.com/r/technology",
        "https://tiktok.com/@username",
        # Common redirects
        "https://google.com",
        "https://github.com",
        # Problematic t.co URLs
        "https://t.co/pg5iHV0s0U",
    ]

    resolver = RobustURLResolver(
        cache_file="test_url_cache.db", max_workers=3, delay_between_requests=0.5
    )

    print("=== Testing Robust URL Resolver ===")
    results = resolver.resolve_urls_batch(test_urls, batch_size=5)

    print("\n=== Results ===")
    for result in results:
        status = "✓" if result["success"] else "✗"
        redirected = "→" if result["resolution_worked"] else "="
        print(f"{status} {redirected} {result['original_url']}")
        if result["resolution_worked"]:
            print(f"    → {result['resolved_url']}")
        if not result["success"]:
            print(f"    Error: {result['error']}")

    print("\n=== Summary Report ===")
    report = resolver.generate_resolution_report(results)
    for key, value in report.items():
        if isinstance(value, dict):
            print(f"{key}:")
            for k, v in list(value.items())[:5]:  # Show top 5
                print(f"  {k}: {v}")
        else:
            print(f"{key}: {value}")


if __name__ == "__main__":
    test_resolver()
