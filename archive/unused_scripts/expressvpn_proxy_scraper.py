#!/usr/bin/env python3
"""
ExpressVPN Proxy-Based Content Scraper
=====================================
Uses ExpressVPN's proxy servers directly through Python requests.
This is much more elegant than OpenVPN configs and fully VM-compatible.

Based on: https://dev.to/thughes24/how-to-turn-your-vpn-into-a-proxy-using-python-28ag

Key Benefits:
- Uses existing ExpressVPN subscription ($0 additional cost)
- VM-compatible (no OpenVPN installation needed)
- Programmatic country rotation per request
- Thread-safe proxy rotation
- Much simpler than system-level VPN management
"""

import os
import sys
import json
import time
import logging
import threading
import random
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Set, Optional, Tuple
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed
import signal
import urllib.parse

import requests
import trafilatura
import polars as pl
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
import urllib3

# Suppress SSL warnings for proxy usage
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


@dataclass
class ExpressVPNConfig:
    """Configuration for ExpressVPN proxy access."""

    # ExpressVPN Credentials (same as your regular login)
    username: str = ""  # Your ExpressVPN email
    password: str = ""  # Your ExpressVPN password

    # ExpressVPN Proxy Servers (Nordic countries)
    # Format: "country_code.host:port"
    proxy_servers: Dict[str, str] = None

    # Scraping Configuration
    max_workers: int = 8  # Reduced for proxy stability
    request_timeout: int = 25  # Longer timeout for proxy requests
    request_delay: float = 1.5  # Slightly longer delay for proxy stability
    max_retries: int = 3

    # Rotation Configuration
    rotation_interval: int = 30  # URLs per proxy rotation
    batch_size: int = 100

    # Output Configuration
    data_dir: Path = Path("data")

    def __post_init__(self):
        if self.proxy_servers is None:
            # ExpressVPN proxy servers for Nordic countries
            # These are the actual proxy endpoints from ExpressVPN
            self.proxy_servers = {
                "denmark": "denmark-2.expressnetw.com:1080",
                "sweden": "sweden-1.expressnetw.com:1080",
                "norway": "norway-1.expressnetw.com:1080",
                "finland": "finland-1.expressnetw.com:1080",
                # Additional servers for rotation
                "denmark_alt": "denmark-1.expressnetw.com:1080",
                "sweden_alt": "sweden-2.expressnetw.com:1080",
            }

        # Create directories
        self.data_dir.mkdir(exist_ok=True)
        (self.data_dir / "logs").mkdir(exist_ok=True)
        (self.data_dir / "progress").mkdir(exist_ok=True)

        # Validate credentials
        if not self.username or not self.password:
            raise ValueError("ExpressVPN username and password must be provided")


class ExpressVPNProxyManager:
    """Manages ExpressVPN proxy rotation and connection testing."""

    def __init__(self, config: ExpressVPNConfig):
        self.config = config
        self.current_proxy = None
        self.current_country = None
        self.connection_count = 0
        self.failed_proxies = set()
        self.logger = logging.getLogger(f"{__name__}.ProxyManager")

        # URL encode credentials for proxy authentication
        self.encoded_username = urllib.parse.quote(self.config.username, safe="")
        self.encoded_password = urllib.parse.quote(self.config.password, safe="")

        # Test initial proxy connection
        self._initialize_proxy()

    def _initialize_proxy(self):
        """Initialize with a working proxy server."""
        self.logger.info("🔍 Testing ExpressVPN proxy servers...")

        for country, server in self.config.proxy_servers.items():
            if self._test_proxy_connection(country, server):
                self.current_proxy = self._create_proxy_dict(server)
                self.current_country = country
                self.logger.info(f"✅ Connected to ExpressVPN proxy: {country}")
                return

        raise ConnectionError("❌ Could not connect to any ExpressVPN proxy servers")

    def _create_proxy_dict(self, server: str) -> Dict[str, str]:
        """Create proxy dictionary for requests library."""
        # Format: "username:password@host:port"
        proxy_url = f"{self.encoded_username}:{self.encoded_password}@{server}"

        return {
            "http": f"http://{proxy_url}",
            "https": f"http://{proxy_url}",  # Use HTTP for HTTPS tunneling
        }

    def _test_proxy_connection(self, country: str, server: str) -> bool:
        """Test if a proxy server is working."""
        try:
            proxy_dict = self._create_proxy_dict(server)

            # Test with a simple request
            response = requests.get(
                "https://httpbin.org/ip", proxies=proxy_dict, timeout=15, verify=False
            )

            if response.status_code == 200:
                ip_data = response.json()
                self.logger.info(
                    f"🌍 {country} proxy IP: {ip_data.get('origin', 'Unknown')}"
                )
                return True
            else:
                self.logger.warning(
                    f"⚠️  {country} proxy returned status {response.status_code}"
                )
                return False

        except Exception as e:
            self.logger.warning(f"⚠️  {country} proxy failed: {str(e)[:100]}")
            return False

    def should_rotate_proxy(self) -> bool:
        """Check if proxy should be rotated."""
        self.connection_count += 1
        return self.connection_count >= self.config.rotation_interval

    def rotate_proxy(self) -> bool:
        """Rotate to next working proxy."""
        self.logger.info("🔄 Rotating ExpressVPN proxy...")

        # Get available proxies (excluding current and failed)
        available_proxies = {
            country: server
            for country, server in self.config.proxy_servers.items()
            if country != self.current_country and country not in self.failed_proxies
        }

        if not available_proxies:
            self.logger.warning("⚠️  No alternative proxies available, keeping current")
            self.connection_count = 0  # Reset counter
            return True

        # Try to connect to a random alternative proxy
        country = random.choice(list(available_proxies.keys()))
        server = available_proxies[country]

        if self._test_proxy_connection(country, server):
            self.current_proxy = self._create_proxy_dict(server)
            self.current_country = country
            self.connection_count = 0
            self.logger.info(f"✅ Rotated to {country} proxy")
            return True
        else:
            self.failed_proxies.add(country)
            self.logger.warning(f"❌ Failed to rotate to {country}")
            return False

    def get_current_proxy(self) -> Dict[str, str]:
        """Get current proxy configuration for requests."""
        return self.current_proxy


class ExpressVPNScraper:
    """Main scraper using ExpressVPN proxies."""

    def __init__(self, config: ExpressVPNConfig):
        self.config = config
        self.proxy_manager = ExpressVPNProxyManager(config)
        self.session_local = threading.local()
        self.stats_lock = threading.Lock()
        self.shutdown_requested = False

        # Statistics
        self.stats = {
            "total_processed": 0,
            "successful_scrapes": 0,
            "failed_scrapes": 0,
            "proxy_rotations": 0,
            "start_time": None,
            "current_country": self.proxy_manager.current_country,
        }

        # Setup logging
        self.setup_logging()
        self.setup_signal_handlers()

        self.logger.info("🚀 ExpressVPN Proxy Scraper initialized")
        self.logger.info(
            f"📊 Configuration: {config.max_workers} workers, "
            f"{config.request_timeout}s timeout, "
            f"proxy rotation every {config.rotation_interval} URLs"
        )

    def setup_logging(self):
        """Setup logging system."""
        log_file = (
            self.config.data_dir
            / "logs"
            / f"expressvpn_scraping_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        )

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            handlers=[
                logging.FileHandler(log_file, encoding="utf-8"),
                logging.StreamHandler(sys.stdout),
            ],
        )
        self.logger = logging.getLogger(__name__)

    def setup_signal_handlers(self):
        """Setup graceful shutdown."""

        def signal_handler(signum, frame):
            self.logger.info(
                f"🛑 Received signal {signum}. Shutting down gracefully..."
            )
            self.shutdown_requested = True

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

    def get_session(self):
        """Get thread-local requests session."""
        if not hasattr(self.session_local, "session"):
            session = requests.Session()
            session.headers.update(
                {
                    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.5",
                    "Accept-Encoding": "gzip, deflate",
                    "Connection": "keep-alive",
                    "Upgrade-Insecure-Requests": "1",
                }
            )

            # Optimize for proxy usage
            adapter = requests.adapters.HTTPAdapter(
                pool_connections=3, pool_maxsize=5, pool_block=False
            )
            session.mount("http://", adapter)
            session.mount("https://", adapter)

            self.session_local.session = session

        return self.session_local.session

    def scrape_url(self, url: str) -> Dict:
        """Scrape content from URL using ExpressVPN proxy."""
        start_time = time.time()

        try:
            if self.shutdown_requested:
                return None

            session = self.get_session()
            proxy_config = self.proxy_manager.get_current_proxy()

            # Make request through ExpressVPN proxy
            for attempt in range(self.config.max_retries):
                try:
                    response = session.get(
                        url,
                        proxies=proxy_config,
                        timeout=self.config.request_timeout,
                        allow_redirects=True,
                        verify=False,  # For proxy usage
                    )
                    break
                except (
                    requests.exceptions.Timeout,
                    requests.exceptions.ProxyError,
                ) as e:
                    if attempt == self.config.max_retries - 1:
                        raise
                    self.logger.warning(
                        f"⚠️  Attempt {attempt + 1} failed for {url}: {str(e)[:100]}"
                    )
                    time.sleep(2**attempt)  # Exponential backoff
                except requests.exceptions.RequestException as e:
                    if attempt == self.config.max_retries - 1:
                        raise
                    time.sleep(2**attempt)

            response_time = time.time() - start_time

            if response.status_code == 200:
                # Extract content with trafilatura
                content = trafilatura.extract(
                    response.text,
                    include_comments=False,
                    include_tables=True,
                    favor_precision=True,
                )

                # Extract metadata
                metadata = trafilatura.extract_metadata(response.text)

                return {
                    "url": url,
                    "status_code": response.status_code,
                    "content": content or "",
                    "title": metadata.title if metadata else "",
                    "author": metadata.author if metadata else "",
                    "date": metadata.date if metadata else "",
                    "description": metadata.description if metadata else "",
                    "word_count": len((content or "").split()),
                    "content_type": response.headers.get("Content-Type", ""),
                    "response_time": response_time,
                    "scraped_at": datetime.now().isoformat(),
                    "proxy_country": self.proxy_manager.current_country,
                    "error": "",
                }
            else:
                return {
                    "url": url,
                    "status_code": response.status_code,
                    "content": "",
                    "title": "",
                    "author": "",
                    "date": "",
                    "description": "",
                    "word_count": 0,
                    "content_type": "",
                    "response_time": response_time,
                    "scraped_at": datetime.now().isoformat(),
                    "proxy_country": self.proxy_manager.current_country,
                    "error": f"HTTP {response.status_code}",
                }

        except Exception as e:
            return {
                "url": url,
                "status_code": 0,
                "content": "",
                "title": "",
                "author": "",
                "date": "",
                "description": "",
                "word_count": 0,
                "content_type": "",
                "response_time": time.time() - start_time,
                "scraped_at": datetime.now().isoformat(),
                "proxy_country": self.proxy_manager.current_country,
                "error": str(e)[:200],
            }

    def update_stats(self, result: Dict):
        """Thread-safe statistics update."""
        with self.stats_lock:
            self.stats["total_processed"] += 1
            self.stats["current_country"] = self.proxy_manager.current_country

            if result and result["status_code"] == 200 and result["content"]:
                self.stats["successful_scrapes"] += 1
            else:
                self.stats["failed_scrapes"] += 1

    def save_progress(self, domain: str, batch_results: List[Dict], batch_num: int):
        """Save batch results and progress."""
        domain_dir = self.config.data_dir / domain
        domain_dir.mkdir(exist_ok=True)

        # Save batch results
        batch_file = domain_dir / f"batch_{batch_num:03d}.json"
        with open(batch_file, "w", encoding="utf-8") as f:
            json.dump(batch_results, f, indent=2, ensure_ascii=False)

        # Save progress
        progress_file = self.config.data_dir / "progress" / f"{domain}_progress.json"
        progress_data = {
            "domain": domain,
            "last_batch": batch_num,
            "total_processed": len(batch_results),
            "stats": self.stats,
            "proxy_country": self.proxy_manager.current_country,
            "timestamp": datetime.now().isoformat(),
        }

        with open(progress_file, "w") as f:
            json.dump(progress_data, f, indent=2)

        self.logger.info(
            f"💾 Saved batch {batch_num} with {len(batch_results)} results"
        )

    def scrape_domain(self, domain: str, urls: List[str], resume_from_batch: int = 0):
        """Scrape all URLs for a domain using ExpressVPN proxies."""
        self.stats["start_time"] = time.time()
        self.logger.info(
            f"🎯 Starting ExpressVPN proxy scraping for {domain} - {len(urls)} URLs"
        )

        # Process URLs in batches
        total_batches = (
            len(urls) + self.config.batch_size - 1
        ) // self.config.batch_size

        for batch_num in range(resume_from_batch, total_batches):
            if self.shutdown_requested:
                break

            start_idx = batch_num * self.config.batch_size
            end_idx = min(start_idx + self.config.batch_size, len(urls))
            batch_urls = urls[start_idx:end_idx]

            self.logger.info(
                f"📦 Processing batch {batch_num + 1}/{total_batches} "
                f"({len(batch_urls)} URLs) via {self.proxy_manager.current_country}"
            )

            # Process batch with threading
            batch_results = []
            with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
                future_to_url = {
                    executor.submit(self.scrape_url, url): url for url in batch_urls
                }

                for future in as_completed(future_to_url):
                    if self.shutdown_requested:
                        break

                    result = future.result()
                    if result:
                        batch_results.append(result)
                        self.update_stats(result)

                        # Check for proxy rotation
                        if self.proxy_manager.should_rotate_proxy():
                            self.logger.info("🔄 Rotating ExpressVPN proxy...")
                            if self.proxy_manager.rotate_proxy():
                                self.stats["proxy_rotations"] += 1
                            time.sleep(3)  # Allow proxy to stabilize

                    # Rate limiting
                    time.sleep(self.config.request_delay)

            # Save batch results
            self.save_progress(domain, batch_results, batch_num + 1)

            # Progress report
            success_rate = (
                self.stats["successful_scrapes"]
                / max(1, self.stats["total_processed"])
                * 100
            )

            self.logger.info(
                f"📊 Progress: {self.stats['total_processed']} processed, "
                f"{success_rate:.1f}% success rate, "
                f"{self.stats['proxy_rotations']} proxy rotations"
            )

        self.logger.info(f"🎉 Completed ExpressVPN proxy scraping for {domain}")
        self.logger.info(
            f"📈 Final stats: {self.stats['successful_scrapes']} successful, "
            f"{self.stats['failed_scrapes']} failed"
        )


def main():
    """Main entry point for ExpressVPN proxy scraper."""
    import argparse

    parser = argparse.ArgumentParser(description="ExpressVPN Proxy Content Scraper")
    parser.add_argument("domain", help="Domain to scrape")
    parser.add_argument("--urls-file", help="CSV file containing URLs to scrape")
    parser.add_argument("--username", help="ExpressVPN username (email)")
    parser.add_argument("--password", help="ExpressVPN password")
    parser.add_argument(
        "--workers", type=int, default=8, help="Number of parallel workers"
    )
    parser.add_argument(
        "--timeout", type=int, default=25, help="Request timeout in seconds"
    )
    parser.add_argument(
        "--resume-batch", type=int, default=0, help="Batch number to resume from"
    )

    args = parser.parse_args()

    # Get credentials from environment or arguments
    username = args.username or os.getenv("EXPRESSVPN_USERNAME")
    password = args.password or os.getenv("EXPRESSVPN_PASSWORD")

    if not username or not password:
        print("❌ ExpressVPN credentials required!")
        print("Provide via:")
        print("  --username YOUR_EMAIL --password YOUR_PASSWORD")
        print("Or set environment variables:")
        print("  export EXPRESSVPN_USERNAME=your_email@example.com")
        print("  export EXPRESSVPN_PASSWORD=your_password")
        return

    # Create configuration
    config = ExpressVPNConfig(
        username=username,
        password=password,
        max_workers=args.workers,
        request_timeout=args.timeout,
    )

    # Load URLs
    if args.urls_file:
        df = pl.read_csv(args.urls_file)
        urls = df.get_column("url").to_list()
    else:
        # Load from existing CSV files in data directory
        domain_dir = Path("data") / args.domain
        if (domain_dir / "urls.csv").exists():
            df = pl.read_csv(domain_dir / "urls.csv")
            urls = df.get_column("url").to_list()
        else:
            print(f"❌ No URLs file found for domain {args.domain}")
            return

    # Create and run scraper
    try:
        scraper = ExpressVPNScraper(config)
        scraper.scrape_domain(args.domain, urls, args.resume_batch)
    except Exception as e:
        print(f"❌ Scraper failed: {e}")
        print("💡 Make sure your ExpressVPN credentials are correct")
        print("💡 Try testing proxy connection manually first")


if __name__ == "__main__":
    main()
