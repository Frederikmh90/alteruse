#!/usr/bin/env python3
"""
Step 1: URL Extraction from Browser Data (OPTIMIZED VERSION)
==============================================================
Extract unique URLs from all browser history files (SQLite and JSON)
with optimized memory management and batch processing.

OPTIMIZATIONS:
- Batch processing to avoid memory exhaustion
- Periodic garbage collection
- Memory-efficient data structures
- Progress tracking and error recovery
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
import gc
import traceback


class URLExtractorOptimized:
    """Extract unique URLs with memory optimization and batch processing."""

    def __init__(self, browser_dir: str, output_dir: str, batch_size: int = 10):
        self.browser_dir = Path(browser_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.batch_size = batch_size

        # Track unique URLs and their metadata
        self.url_data = {}
        self.processed_files = []
        self.error_files = []
        self.error_details = []

        print(f"Initialized Optimized URL Extractor")
        print(f"Browser data directory: {self.browser_dir}")
        print(f"Output directory: {self.output_dir}")
        print(f"Batch size: {self.batch_size}")

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
                return None

        except (ValueError, TypeError) as e:
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
                # Handle timezone-aware vs timezone-naive datetime comparison
                try:
                    if (
                        not existing["first_seen"]
                        or visit_time < existing["first_seen"]
                    ):
                        existing["first_seen"] = visit_time
                    if not existing["last_seen"] or visit_time > existing["last_seen"]:
                        existing["last_seen"] = visit_time
                except TypeError:
                    # Can't compare timezone-aware and naive - just update if empty
                    if not existing["first_seen"]:
                        existing["first_seen"] = visit_time
                    if not existing["last_seen"]:
                        existing["last_seen"] = visit_time
            existing["visit_count"] += 1
            if source_file and source_file not in existing["source_files"]:
                existing["source_files"].append(source_file)

    def process_sqlite_file(self, file_path: Path) -> int:
        """Process a SQLite database file and extract URLs with FIXED query."""
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

            # FIXED: Get title from history_visits, not history_items
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
            error_msg = f"Error processing {file_path.name}: {e}"
            print(f"  {error_msg}")
            self.error_files.append(str(file_path))
            self.error_details.append(error_msg)

        return urls_added

    def process_json_file(self, file_path: Path) -> int:
        """Process a JSON history file with memory optimization."""
        print(f"Processing JSON file: {file_path.name}")
        urls_added = 0

        try:
            # Check file size and warn for very large files
            file_size = file_path.stat().st_size
            if file_size > 50 * 1024 * 1024:  # 50MB
                print(
                    f"  Large file detected ({file_size / (1024 * 1024):.1f}MB) - processing carefully"
                )

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

            # Process in chunks for very large files
            chunk_size = 5000
            for i in range(0, len(records), chunk_size):
                chunk = records[i : i + chunk_size]

                for record in chunk:
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

                # Force garbage collection after each chunk for large files
                if len(records) > chunk_size and i % (chunk_size * 5) == 0:
                    gc.collect()

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
            self.error_files.append(str(file_path))
            self.error_details.append(error_msg)

        return urls_added

    def extract_all_urls(self) -> pd.DataFrame:
        """Extract URLs with optimized batch processing."""
        print(f"\nStarting OPTIMIZED URL extraction from {self.browser_dir}")
        print("=" * 60)

        # Find all browser history files
        sqlite_files = list(self.browser_dir.glob("*.db"))
        json_files = list(self.browser_dir.glob("*.json"))

        print(
            f"Found {len(sqlite_files)} SQLite files and {len(json_files)} JSON files"
        )

        total_urls_processed = 0

        # Process SQLite files first (smaller, less memory intensive)
        print(f"\nProcessing {len(sqlite_files)} SQLite files...")
        for db_file in sqlite_files:
            urls_processed = self.process_sqlite_file(db_file)
            total_urls_processed += urls_processed

            # Garbage collection after each SQLite file
            gc.collect()

        print(
            f"\nSQLite processing complete. Current unique URLs: {len(self.url_data):,}"
        )

        # Process JSON files in batches
        print(
            f"\nProcessing {len(json_files)} JSON files in batches of {self.batch_size}..."
        )

        for i in range(0, len(json_files), self.batch_size):
            batch = json_files[i : i + self.batch_size]
            batch_num = (i // self.batch_size) + 1
            total_batches = (len(json_files) + self.batch_size - 1) // self.batch_size

            print(f"\n--- Batch {batch_num}/{total_batches} ({len(batch)} files) ---")

            batch_start_time = time.time()
            batch_urls = 0

            for json_file in batch:
                urls_processed = self.process_json_file(json_file)
                total_urls_processed += urls_processed
                batch_urls += urls_processed

            batch_end_time = time.time()
            batch_time = batch_end_time - batch_start_time

            print(f"Batch {batch_num} complete:")
            print(f"  URLs processed: {batch_urls:,}")
            print(f"  Time: {batch_time:.1f}s")
            print(f"  Total unique URLs: {len(self.url_data):,}")

            # Force garbage collection after each batch
            gc.collect()

            # Show memory usage estimate
            import sys

            memory_mb = sys.getsizeof(self.url_data) / (1024 * 1024)
            print(f"  Memory usage: {memory_mb:.1f} MB")

        print(f"\n" + "=" * 60)
        print(f"OPTIMIZED URL extraction completed!")
        print(f"Total URLs processed: {total_urls_processed:,}")
        print(f"Unique URLs found: {len(self.url_data):,}")
        print(f"Files processed successfully: {len(self.processed_files)}")
        print(f"Files with errors: {len(self.error_files)}")

        if self.error_files:
            print(f"\nError files:")
            for error_file, error_detail in zip(
                self.error_files[:10], self.error_details[:10]
            ):
                print(f"  {Path(error_file).name}: {error_detail}")
            if len(self.error_files) > 10:
                print(f"  ... and {len(self.error_files) - 10} more")

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
        self, df: pd.DataFrame, filename: str = "extracted_urls_optimized.csv"
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
                lambda x: "; ".join(x) if isinstance(x, list) else str(x)
            )

        df_save.to_csv(output_path, index=False, encoding="utf-8")
        print(f"\nDataset saved to: {output_path}")
        print(f"Dataset shape: {df_save.shape}")

        return output_path


def main():
    """Run optimized URL extraction."""
    import argparse

    print("Optimized URL Extraction from Browser Data")
    print("=" * 40)

    parser = argparse.ArgumentParser(description="Extract URLs from browser history")
    parser.add_argument("--browser-dir", default="data/Samlet_06112025/Browser", help="Input directory containing browser history files")
    parser.add_argument("--output-dir", default="data/url_extract", help="Output directory for extracted URLs")
    
    args = parser.parse_args()
    
    # Define paths
    browser_dir = args.browser_dir
    output_dir = args.output_dir

    # Check if browser directory exists
    if not Path(browser_dir).exists():
        print(f"Error: Browser directory not found: {browser_dir}")
        return

    # Initialize extractor with batch processing
    extractor = URLExtractorOptimized(browser_dir, output_dir, batch_size=15)

    # Extract URLs
    start_time = time.time()
    df = extractor.extract_all_urls()
    end_time = time.time()

    print(f"\nExtraction completed in {end_time - start_time:.1f} seconds")

    # Save dataset
    if not df.empty:
        output_file = extractor.save_dataset(df)

        # Print summary stats
        print(f"\n" + "=" * 60)
        print("EXTRACTION SUMMARY")
        print("=" * 60)
        print(f"Total unique URLs: {len(df):,}")
        print(f"Unique domains: {df['domain'].nunique():,}")
        print(f"Files processed: {len(extractor.processed_files)}")
        print(f"Files with errors: {len(extractor.error_files)}")

        # Top domains
        print(f"\nTop 10 domains:")
        top_domains = df["domain"].value_counts().head(10)
        for domain, count in top_domains.items():
            print(f"  {domain}: {count:,} URLs")

    else:
        print("No URLs extracted!")


if __name__ == "__main__":
    main()
