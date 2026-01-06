#!/usr/bin/env python3
"""
Step 1: URL Extraction from Browser Data (FIXED VERSION)
=========================================================
Extract unique URLs from all browser history files (SQLite and JSON)
and create initial dataset with columns: url, resolved_url, domain, content

FIXES:
- Corrected SQLite query to get title from history_visits table instead of history_items
- This should eliminate all .db file processing errors
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


class URLExtractor:
    """Extract unique URLs from browser history data."""

    def __init__(self, browser_dir: str, output_dir: str):
        self.browser_dir = Path(browser_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)

        # Track unique URLs and their metadata
        self.url_data = {}
        self.processed_files = []
        self.error_files = []

        print(f"Initialized URL Extractor")
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
            existing["visit_count"] += 1
            if title and not existing["title"]:
                existing["title"] = title
            if visit_time:
                if not existing["first_seen"] or visit_time < existing["first_seen"]:
                    existing["first_seen"] = visit_time
                if not existing["last_seen"] or visit_time > existing["last_seen"]:
                    existing["last_seen"] = visit_time
            if source_file and source_file not in existing["source_files"]:
                existing["source_files"].append(source_file)

    def process_sqlite_file(self, file_path: Path) -> int:
        """Process a SQLite database file and extract URLs."""
        print(f"Processing SQLite file: {file_path.name}")
        urls_added = 0

        try:
            # Check if file is empty
            if not os.path.getsize(file_path):
                print(f"  Empty database file: {file_path.name}")
                return 0

            # Connect to database
            conn = sqlite3.connect(str(file_path))

            # Check if it's a valid Safari history database
            try:
                test_query = "SELECT name FROM sqlite_master WHERE type='table'"
                tables = pd.read_sql_query(test_query, conn)
                table_names = tables["name"].values

                if (
                    "history_items" not in table_names
                    or "history_visits" not in table_names
                ):
                    print(f"  Not a valid Safari history database: {file_path.name}")
                    conn.close()
                    return 0

            except Exception as e:
                print(f"  Not a valid SQLite database: {file_path.name}")
                conn.close()
                return 0

            # Execute query to get URLs (FIXED: title is in history_visits, not history_items)
            query = """
            SELECT 
                history_items.url,
                history_visits.title,
                history_visits.visit_time,
                history_items.visit_count
            FROM history_items
            JOIN history_visits ON history_items.id = history_visits.history_item
            ORDER BY history_visits.visit_time DESC
            """

            df = pd.read_sql_query(query, conn)
            conn.close()

            print(f"  Found {len(df)} records")

            for _, row in df.iterrows():
                url = row.get("url", "")
                title = row.get("title", "")
                visit_time = row.get("visit_time")

                # Convert visit_time if it's a WebKit timestamp
                if visit_time and not isinstance(visit_time, datetime):
                    visit_time = self.convert_webkit_timestamp(visit_time)

                self.add_url(url, title, visit_time, file_path.name)
                urls_added += 1

            self.processed_files.append(str(file_path))
            print(f"  Processed {urls_added} records")

        except Exception as e:
            print(f"  Error processing {file_path.name}: {e}")
            self.error_files.append(str(file_path))

        return urls_added

    def process_json_file(self, file_path: Path) -> int:
        """Process a JSON history file and extract URLs."""
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

        except Exception as e:
            print(f"  Error processing {file_path.name}: {e}")
            self.error_files.append(str(file_path))

        return urls_added

    def extract_all_urls(self) -> pd.DataFrame:
        """Extract URLs from all browser history files."""
        print(f"\nStarting URL extraction from {self.browser_dir}")
        print("=" * 60)

        # Find all browser history files
        sqlite_files = list(self.browser_dir.glob("*.db"))
        json_files = list(self.browser_dir.glob("*.json"))

        print(
            f"Found {len(sqlite_files)} SQLite files and {len(json_files)} JSON files"
        )

        total_urls_processed = 0

        # Process SQLite files
        print(f"\nProcessing {len(sqlite_files)} SQLite files...")
        for db_file in sqlite_files:
            urls_processed = self.process_sqlite_file(db_file)
            total_urls_processed += urls_processed

        # Process JSON files
        print(f"\nProcessing {len(json_files)} JSON files...")
        for json_file in json_files:
            urls_processed = self.process_json_file(json_file)
            total_urls_processed += urls_processed

        print(f"\n" + "=" * 60)
        print(f"URL extraction completed!")
        print(f"Total URLs processed: {total_urls_processed:,}")
        print(f"Unique URLs found: {len(self.url_data):,}")
        print(f"Files processed successfully: {len(self.processed_files)}")
        print(f"Files with errors: {len(self.error_files)}")

        if self.error_files:
            print(f"Error files: {self.error_files[:5]}...")  # Show first 5

        # Convert to DataFrame
        if self.url_data:
            df = pd.DataFrame.from_dict(self.url_data, orient="index")
            df = df.reset_index(drop=True)

            # Reorder columns for final dataset
            final_columns = ["url", "resolved_url", "domain", "content"]
            metadata_columns = [
                "title",
                "first_seen",
                "last_seen",
                "visit_count",
                "source_files",
            ]
            df = df[final_columns + metadata_columns]

            return df
        else:
            print("No URLs found!")
            return pd.DataFrame(columns=["url", "resolved_url", "domain", "content"])

    def save_dataset(
        self, df: pd.DataFrame, filename: str = "extracted_urls_fixed.csv"
    ):
        """Save the URL dataset to CSV."""
        output_path = self.output_dir / filename

        # Convert datetime columns to strings for CSV
        df_save = df.copy()
        for col in ["first_seen", "last_seen"]:
            if col in df_save.columns:
                df_save[col] = df_save[col].astype(str)

        # Convert list columns to strings
        if "source_files" in df_save.columns:
            df_save["source_files"] = df_save["source_files"].apply(
                lambda x: ";".join(x) if isinstance(x, list) else str(x)
            )

        df_save.to_csv(output_path, index=False, encoding="utf-8")
        print(f"\nDataset saved to: {output_path}")
        print(f"Dataset shape: {df_save.shape}")

        # Print summary statistics
        print(f"\nDataset Summary:")
        print(f"- Total URLs: {len(df_save):,}")
        print(f"- Unique domains: {df_save['domain'].nunique():,}")
        print(f"- Top 10 domains:")
        domain_counts = df_save["domain"].value_counts().head(10)
        for domain, count in domain_counts.items():
            print(f"  {domain}: {count:,} URLs")

        return output_path


def main():
    """Main function to run URL extraction."""
    print("URL Extraction - Step 1")
    print("=" * 40)

    # Define paths
    browser_dir = "data/Samlet_06112025/Browser"
    output_dir = "data/url_extract"

    # Check if directories exist
    if not Path(browser_dir).exists():
        print(f"Error: Browser directory not found: {browser_dir}")
        return

    # Initialize extractor
    extractor = URLExtractor(browser_dir, output_dir)

    # Extract URLs
    start_time = time.time()
    df = extractor.extract_all_urls()
    end_time = time.time()

    if len(df) > 0:
        # Save dataset
        output_path = extractor.save_dataset(df)

        print(f"\nExtraction completed in {end_time - start_time:.2f} seconds")
        print(f"Next step: Run step2_analyze_urls.py to analyze and resolve URLs")
    else:
        print("No URLs extracted. Please check your browser data directory.")


if __name__ == "__main__":
    main()
