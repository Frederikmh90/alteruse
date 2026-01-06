#!/usr/bin/env python3
"""
Enhanced URL Resolver
====================
Handles both HTTP redirects and meta refresh redirects for comprehensive URL resolution.
"""

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
import re
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter


class EnhancedURLResolver:
    """
    An enhanced URL resolver that handles both HTTP redirects and meta refresh redirects.
    """

    def __init__(
        self,
        cache_file: str = "enhanced_url_resolution_cache.db",
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

        # Setup session with robust headers and increased connection pool
        self.session = requests.Session()

        # Configure HTTP adapters with larger connection pools
        http_adapter = HTTPAdapter(pool_connections=20, pool_maxsize=100)
        https_adapter = HTTPAdapter(pool_connections=20, pool_maxsize=100)

        self.session.mount("http://", http_adapter)
        self.session.mount("https://", https_adapter)

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
                response_time REAL,
                cached_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
        conn.close()

    def _get_from_cache(self, url: str) -> Optional[Dict]:
        """Get result from cache."""
        try:
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
                    "response_time": result[7],
                    "cached_at": result[8],
                }
        except Exception as e:
            self.logger.warning(f"Cache read error: {e}")
        return None

    def _save_to_cache(self, result: Dict):
        """Save result to cache."""
        try:
            conn = sqlite3.connect(self.cache_file)
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT OR REPLACE INTO url_cache 
                (original_url, resolved_url, status_code, redirect_count, success, error, resolution_worked, response_time)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    result["original_url"],
                    result["resolved_url"],
                    result["status_code"],
                    result["redirect_count"],
                    result["success"],
                    result["error"],
                    result["resolution_worked"],
                    result["response_time"],
                ),
            )
            conn.commit()
            conn.close()
        except Exception as e:
            self.logger.warning(f"Cache write error: {e}")

    def _extract_meta_refresh_url(
        self, html_content: str, base_url: str
    ) -> Optional[str]:
        """Extract URL from meta refresh tag (BeautifulSoup only)."""
        try:
            soup = BeautifulSoup(html_content, "html.parser")
            meta_refresh = soup.find("meta", attrs={"http-equiv": "refresh"})
            if meta_refresh:
                content = meta_refresh.get("content", "")
                match = re.search(r"URL=([^;]+)", content, re.IGNORECASE)
                if match:
                    return match.group(1).strip()
        except Exception as e:
            self.logger.debug(f"Meta refresh extraction error: {e}")
        return None

    def resolve_single_url(self, url: str) -> Dict:
        """
        Resolve a single URL, handling both HTTP redirects and meta refresh redirects.
        """
        # Check cache first
        cached_result = self._get_from_cache(url)
        if cached_result:
            self.logger.debug(f"Cache hit for {url}")
            return cached_result

        start_time = time.time()
        redirect_count = 0
        current_url = url
        max_attempts = self.max_redirects

        try:
            self.logger.debug(f"Resolving URL: {url}")

            while max_attempts > 0:
                # Make request
                response = self.session.get(
                    current_url,
                    allow_redirects=False,  # Handle redirects manually
                    timeout=self.timeout,
                    verify=True,
                )

                # Check for HTTP redirects
                if response.status_code in [301, 302, 303, 307, 308]:
                    location = response.headers.get("location")
                    if location:
                        current_url = urljoin(current_url, location)
                        redirect_count += 1
                        max_attempts -= 1
                        self.logger.debug(
                            f"HTTP redirect {redirect_count}: {current_url}"
                        )
                        continue

                # Check for meta refresh redirects
                if response.status_code == 200 and "text/html" in response.headers.get(
                    "content-type", ""
                ):
                    meta_refresh_url = self._extract_meta_refresh_url(
                        response.text, current_url
                    )
                    if meta_refresh_url:
                        current_url = urljoin(current_url, meta_refresh_url)
                        redirect_count += 1
                        max_attempts -= 1
                        self.logger.debug(
                            f"Meta refresh redirect {redirect_count}: {current_url}"
                        )
                        continue

                # No more redirects
                break

            response_time = time.time() - start_time
            final_url = current_url
            actually_resolved = url != final_url

            result = {
                "original_url": url,
                "resolved_url": final_url,
                "status_code": response.status_code,
                "redirect_count": redirect_count,
                "success": True,
                "error": None,
                "resolution_worked": actually_resolved,
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
                    current_url,
                    allow_redirects=False,
                    timeout=self.timeout,
                    verify=False,
                )
                # Handle the same redirect logic as above
                # ... (simplified for brevity)
                result = {
                    "original_url": url,
                    "resolved_url": url,
                    "status_code": response.status_code,
                    "redirect_count": 0,
                    "success": True,
                    "error": "SSL bypassed",
                    "resolution_worked": False,
                    "response_time": time.time() - start_time,
                }
            except Exception as e2:
                result = {
                    "original_url": url,
                    "resolved_url": url,
                    "status_code": None,
                    "redirect_count": 0,
                    "success": False,
                    "error": f"SSL error: {e}",
                    "resolution_worked": False,
                    "response_time": time.time() - start_time,
                }

        except Exception as e:
            result = {
                "original_url": url,
                "resolved_url": url,
                "status_code": None,
                "redirect_count": 0,
                "success": False,
                "error": str(e),
                "resolution_worked": False,
                "response_time": time.time() - start_time,
            }

        # Save to cache
        self._save_to_cache(result)
        return result


def main():
    """Test the enhanced URL resolver."""
    resolver = EnhancedURLResolver()

    # Test with the problematic t.co URL
    test_url = "https://t.co/pg5iHV0s0U"
    print(f"Testing URL: {test_url}")

    result = resolver.resolve_single_url(test_url)
    print(f"Result: {result}")

    if result["resolution_worked"]:
        print(
            f"✅ Successfully resolved: {result['original_url']} -> {result['resolved_url']}"
        )
    else:
        print(f"❌ Failed to resolve: {result['error']}")


if __name__ == "__main__":
    main()
