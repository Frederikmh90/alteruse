#!/usr/bin/env python3
"""
Test Bulk Processing
====================
Test if the issue is with memory/bulk processing vs individual file processing
"""

import os
import json
import sqlite3
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse, unquote
from typing import Dict, List, Set, Optional
import time
import traceback


# Copy the exact same classes and logic from the fixed version
class URLExtractorTest:
    """Extract unique URLs from browser history data - TEST VERSION."""

    def __init__(self, browser_dir: str, output_dir: str):
        self.browser_dir = Path(browser_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)

        # Track unique URLs and their metadata
        self.url_data = {}
        self.processed_files = []
        self.error_files = []
        self.error_details = []  # Store detailed error info

        print(f"Initialized Test URL Extractor")
        print(f"Browser data directory: {self.browser_dir}")
        print(f"Output directory: {self.output_dir}")

    def convert_webkit_timestamp(self, webkit_timestamp):
        """Convert WebKit timestamp (seconds since 2001-01-01) to datetime."""
        try:
            if not webkit_timestamp or pd.isna(webkit_timestamp):
                return None

            # Apple epoch offset (seconds from 1970-01-01 to 2001-01-01)
            APPLE_EPOCH_TO_UNIX_OFFSET = 978307200

            # Convert to float and add Apple epoch offset to get Unix timestamp
            timestamp = float(webkit_timestamp)
            unix_timestamp = timestamp + APPLE_EPOCH_TO_UNIX_OFFSET

            # Create datetime
            dt = datetime.fromtimestamp(unix_timestamp, tz=timezone.utc)
            copenhagen_tz = timezone(timedelta(hours=1))  # CET (simplified)
            return dt.astimezone(copenhagen_tz)

        except (ValueError, TypeError, OverflowError) as e:
            print(
                f"Warning: Invalid WebKit timestamp value: {webkit_timestamp}, error: {e}"
            )
            return None

    def convert_chrome_timestamp(self, chrome_timestamp):
        """Convert Chrome timestamp (milliseconds since 1970-01-01) to datetime."""
        try:
            if not chrome_timestamp:
                return None

            # Handle string input with European number format (comma as decimal separator)
            if isinstance(chrome_timestamp, str):
                chrome_timestamp = chrome_timestamp.replace(",", ".")

            # Convert to float - Chrome timestamps are in milliseconds since Unix epoch
            timestamp = float(chrome_timestamp)

            try:
                # Unix epoch base date
                base_date = datetime(1970, 1, 1)
                delta = timedelta(milliseconds=timestamp)
                return base_date + delta
            except (ValueError, OverflowError):
                print(f"Warning: Invalid timestamp value: {chrome_timestamp}")
                return None

        except (ValueError, TypeError) as e:
            print(f"Error converting timestamp {chrome_timestamp}: {str(e)}")
            return None

    def extract_domain(self, url: str) -> str:
        """Extract clean domain from URL."""
        try:
            if not url or pd.isna(url):
                return ""

            # Clean the URL
            url = str(url).strip()
            if not url.startswith(("http://", "https://")):
                url = "https://" + url

            parsed = urlparse(url)
            domain = parsed.netloc.lower()

            # Remove www. prefix
            if domain.startswith("www."):
                domain = domain[4:]

            return domain
        except Exception as e:
            print(f"Error extracting domain from {url}: {e}")
            return ""

    def clean_url(self, url: str) -> str:
        """Clean and normalize URL."""
        try:
            if not url or pd.isna(url):
                return ""

            url = str(url).strip()

            # Decode URL encoding
            url = unquote(url)

            # Remove fragment (everything after #)
            if "#" in url:
                url = url.split("#")[0]

            # Remove common tracking parameters
            tracking_params = [
                "utm_source",
                "utm_medium",
                "utm_campaign",
                "utm_content",
                "utm_term",
                "fbclid",
                "gclid",
                "_ga",
                "_gl",
                "mc_cid",
                "mc_eid",
                "ref",
                "referrer",
                "source",
            ]

            if "?" in url:
                base_url, params = url.split("?", 1)
                param_pairs = params.split("&")
                filtered_params = []

                for param in param_pairs:
                    if "=" in param:
                        key = param.split("=")[0].lower()
                        if key not in tracking_params:
                            filtered_params.append(param)

                if filtered_params:
                    url = base_url + "?" + "&".join(filtered_params)
                else:
                    url = base_url

            return url
        except Exception as e:
            print(f"Error cleaning URL {url}: {e}")
            return str(url) if url else ""

    def is_generic_url(self, url: str, domain: str) -> bool:
        """Check if URL is generic/low-value for scraping."""
        if not url or not domain:
            return True

        url_lower = url.lower()

        # Generic domains to skip
        generic_domains = {
            "google.com",
            "google.dk",
            "google.co.uk",
            "bing.com",
            "yahoo.com",
            "duckduckgo.com",
            "facebook.com",
            "instagram.com",
            "twitter.com",
            "linkedin.com",
            "youtube.com",
            "youtu.be",
            "login.microsoftonline.com",
            "accounts.google.com",
            "localhost",
            "127.0.0.1",
            "file://",
        }

        if domain in generic_domains:
            return True

        # Generic URL patterns
        generic_patterns = [
            "/search?",
            "/search/",
            "q=",
            "query=",
            "login",
            "signin",
            "signup",
            "auth",
            "logout",
            "signout",
            "redirect",
            "callback",
            "oauth",
        ]

        for pattern in generic_patterns:
            if pattern in url_lower:
                return True

        return False

    def add_url(
        self,
        url: str,
        title: str = "",
        visit_time: datetime = None,
        source_file: str = "",
    ):
        """Add URL to the dataset if it's unique and valid."""
        if not url or pd.isna(url):
            return

        # Clean the URL
        clean_url = self.clean_url(url)
        if not clean_url:
            return

        # Extract domain
        domain = self.extract_domain(clean_url)
        if not domain:
            return

        # Skip if it's a generic URL
        if self.is_generic_url(clean_url, domain):
            return

        # Add to dataset if new, or update if we have better metadata
        if clean_url not in self.url_data:
            self.url_data[clean_url] = {
                "url": clean_url,
                "resolved_url": "",  # Will be filled in step 2
                "domain": domain,
                "content": "",  # Will be filled in steps 4-5
                "title": title or "",
                "first_seen": visit_time,
                "last_seen": visit_time,
                "visit_count": 1,
                "source_files": [source_file] if source_file else [],
            }
        else:
            # Update existing entry
            existing = self.url_data[clean_url]
            if title and not existing["title"]:
                existing["title"] = title
            if visit_time:
                if not existing["first_seen"] or visit_time < existing["first_seen"]:
                    existing["first_seen"] = visit_time
                if not existing["last_seen"] or visit_time > existing["last_seen"]:
                    existing["last_seen"] = visit_time
            existing["visit_count"] += 1
            if source_file and source_file not in existing["source_files"]:
                existing["source_files"].append(source_file)

    def process_json_file(self, file_path: Path) -> int:
        """Process a JSON history file and extract URLs with detailed error tracking."""
        print(f"Processing JSON file: {file_path.name}")
        urls_added = 0

        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            # Handle different JSON structures
            if isinstance(data, list):
                records = data
            elif isinstance(data, dict) and "Browser History" in data:
                records = data["Browser History"]
            elif isinstance(data, dict) and "history" in data:
                records = data["history"]
            else:
                records = [data] if isinstance(data, dict) else []

            print(f"  Found {len(records)} records")

            for record in records:
                if not isinstance(record, dict):
                    continue

                url = record.get("url", "")
                title = record.get("title", "")
                visit_time = (
                    record.get("visitTime")
                    or record.get("visit_time")
                    or record.get("time_usec")
                )

                # Convert timestamp
                if visit_time:
                    if (
                        isinstance(visit_time, (int, float))
                        and visit_time > 1000000000000
                    ):
                        # Chrome timestamp (milliseconds)
                        visit_time = self.convert_chrome_timestamp(visit_time)
                    elif isinstance(visit_time, (int, float)):
                        # WebKit timestamp (seconds)
                        visit_time = self.convert_webkit_timestamp(visit_time)

                self.add_url(url, title, visit_time, file_path.name)
                urls_added += 1

            self.processed_files.append(str(file_path))
            print(f"  Processed {urls_added} records")

        except json.JSONDecodeError as e:
            error_msg = f"JSON decode error in {file_path.name}: {e}"
            print(f"  {error_msg}")
            self.error_files.append(str(file_path))
            self.error_details.append(error_msg)
        except MemoryError as e:
            error_msg = f"Memory error in {file_path.name}: {e}"
            print(f"  {error_msg}")
            self.error_files.append(str(file_path))
            self.error_details.append(error_msg)
        except Exception as e:
            error_msg = f"Error processing {file_path.name}: {e}"
            print(f"  {error_msg}")
            print(f"  Traceback: {traceback.format_exc()}")
            self.error_files.append(str(file_path))
            self.error_details.append(error_msg)

        return urls_added


def test_subset_processing():
    """Test processing a subset of JSON files to check for bulk processing issues."""
    print("Testing Bulk Processing")
    print("=" * 40)

    browser_dir = Path("data/Samlet_06112025/Browser")
    output_dir = Path("data/url_extract")

    # Test with just the first 5 "problematic" files
    test_files = [
        "474-4477-c-146271_2025-05-08T07__4477g1746690538960sn2dqgvrdtNju5517uu5517uhistory-5MIbsIS.json",
        "474-4477-c-146104_2025-05-01T12__4477g1746102510141sMbw5mjCqiNju5137uu5137uhistory-mY1tCwu.json",
        "474-4477-c-146693_2025-05-14T08__4477g1747211634451szao1a9FTQcju5728uu5728uhistory-m2iOymH.json",
        "474-4477-c-146768_2025-05-21T12__4477g1747831999195sj7OCfjvYgJju5965uu5965ualphaNhistory8110-IY0GZ3F.json",
        "474-4477-c-146173_2025-05-02T06__4477g1746166839321s4rWioTOt9rju5277uu5277uhistory-E8x0tr3.json",
    ]

    # Initialize extractor
    extractor = URLExtractorTest(browser_dir, output_dir)

    total_urls_processed = 0
    start_time = time.time()

    for filename in test_files:
        file_path = browser_dir / filename
        if file_path.exists():
            urls_processed = extractor.process_json_file(file_path)
            total_urls_processed += urls_processed
        else:
            print(f"File not found: {filename}")

    end_time = time.time()

    print(f"\n" + "=" * 60)
    print(f"SUBSET TEST RESULTS")
    print("=" * 60)
    print(f"Total URLs processed: {total_urls_processed:,}")
    print(f"Unique URLs found: {len(extractor.url_data):,}")
    print(f"Files processed successfully: {len(extractor.processed_files)}")
    print(f"Files with errors: {len(extractor.error_files)}")
    print(f"Processing time: {end_time - start_time:.1f} seconds")

    if extractor.error_files:
        print(f"\nError files:")
        for error_file, error_detail in zip(
            extractor.error_files, extractor.error_details
        ):
            print(f"  {error_file}: {error_detail}")

    # Check memory usage (rough estimate)
    import sys

    memory_usage = sys.getsizeof(extractor.url_data) / (1024 * 1024)  # MB
    print(f"Approximate memory usage: {memory_usage:.1f} MB")


if __name__ == "__main__":
    test_subset_processing()
