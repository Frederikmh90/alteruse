#!/usr/bin/env python3
"""
Step 1: Facebook URL Extraction
===============================
Extract URLs from Facebook data files (JSON and HTML) and create a dataset
with columns: url, resolved_url, domain, content, source_directory, source_file
"""

import pandas as pd
import numpy as np
from pathlib import Path
import json
import re
import time
from datetime import datetime
from typing import Dict, List, Set, Optional, Tuple
from urllib.parse import urlparse
import logging
from bs4 import BeautifulSoup
import os


class FacebookURLExtractor:
    """Extract URLs from Facebook data files."""

    def __init__(self, data_dir: str, output_dir: str):
        self.data_dir = Path(data_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)

        # Setup logging
        self.setup_logging()

        # URL pattern for extraction - captures full URLs including paths, query params, and fragments
        self.url_pattern = r'https?://[^\s<>"{}|\\^`\[\]]+'

        # Track processed files and URLs
        self.processed_files = set()
        self.extracted_urls = set()
        self.results = []

    def setup_logging(self):
        """Setup logging for the extraction process."""
        log_file = (
            self.output_dir
            / f"facebook_url_extraction_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        )

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[logging.FileHandler(log_file), logging.StreamHandler()],
        )
        self.logger = logging.getLogger(__name__)

    def extract_urls_from_text(self, text: str) -> List[str]:
        """Extract URLs from text content using regex."""
        if not isinstance(text, str):
            return []

        return re.findall(self.url_pattern, text)

    def extract_domain(self, url: str) -> str:
        """Extract domain from URL and remove www."""
        try:
            parsed = urlparse(url)
            domain = re.sub(r"^www\.", "", parsed.netloc.lower())
            return domain
        except:
            return ""

    def should_skip_url(self, url: str) -> bool:
        """Check if URL should be skipped (Facebook-related or generic)."""
        domain = self.extract_domain(url)

        # Skip Facebook-related domains
        facebook_domains = {
            "facebook.com",
            "fb.com",
            "facebook.dk",
            "fbcdn.net",
            "messenger.com",
            "workplace.com",
            "instagram.com",  # Keep Instagram as requested
        }

        if domain in facebook_domains:
            return True

        # Skip generic search URLs
        if "google.com/search" in url or "bing.com/search" in url:
            return True

        return False

    def extract_content_from_item(self, item: Dict) -> str:
        """Extract content from a Facebook data item, handling various structures."""
        content = ""

        # For posts and comments with 'data' list
        if "data" in item and isinstance(item["data"], list):
            for data_item in item["data"]:
                if isinstance(data_item, dict):
                    # Try different possible content fields
                    content = (
                        data_item.get("post", "")
                        or data_item.get("comment", "")
                        or data_item.get("text", "")
                        or ""
                    )
                    if content:
                        break

        # For items with direct 'title' field
        if not content and "title" in item:
            content = item["title"]

        # For items with attachments
        if "attachments" in item and isinstance(item["attachments"], list):
            for attachment in item["attachments"]:
                if isinstance(attachment, dict):
                    if isinstance(attachment.get("data"), dict):
                        content = attachment["data"].get("text", "") or content
                    elif isinstance(attachment.get("data"), list):
                        for data_item in attachment["data"]:
                            if isinstance(data_item, dict):
                                content = data_item.get("text", "") or content
                                if content:
                                    break

        # For items with media
        if "media" in item and isinstance(item["media"], list):
            for media_item in item["media"]:
                if isinstance(media_item, dict):
                    content = (
                        media_item.get("description", "")
                        or media_item.get("title", "")
                        or content
                    )
                    if content:
                        break

        return content

    def process_json_file(self, file_path: Path, source_dir: str) -> List[Dict]:
        """Process a JSON file and extract URLs with metadata."""
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            results = []
            items_to_process = []

            # Handle different file structures
            if isinstance(data, dict):
                # Handle v2 structures
                if "comments_v2" in data:
                    items_to_process = data["comments_v2"]
                elif "group_posts_v2" in data:
                    items_to_process = data["group_posts_v2"]
                elif "group_comments_v2" in data:
                    items_to_process = data["group_comments_v2"]
                elif "posts" in data:
                    items_to_process = data["posts"]
                elif "recently_viewed" in data:
                    items_to_process = data["recently_viewed"]
                elif "pages" in data:
                    items_to_process = data["pages"]
                elif "groups" in data:
                    items_to_process = data["groups"]
                elif "events" in data:
                    items_to_process = data["events"]
                elif "comments_and_reactions" in data:
                    items_to_process = data["comments_and_reactions"]
            elif isinstance(data, list):
                items_to_process = data

            if not isinstance(items_to_process, list):
                items_to_process = [items_to_process]

            for item in items_to_process:
                if not isinstance(item, dict):
                    continue

                # Extract timestamp
                timestamp = item.get("timestamp", "")

                # Extract content using the helper function
                content = self.extract_content_from_item(item)

                if content:
                    urls = self.extract_urls_from_text(content)
                    if urls:
                        for url in urls:
                            # Skip Facebook-related URLs
                            if self.should_skip_url(url):
                                continue

                            # Skip if already processed
                            if url in self.extracted_urls:
                                continue

                            domain = self.extract_domain(url)
                            results.append(
                                {
                                    "url": url,
                                    "resolved_url": "",  # Empty for now
                                    "domain": domain,
                                    "content": "",  # Empty for now
                                    "source_directory": source_dir,
                                    "source_file": file_path.name,
                                    "timestamp": timestamp,
                                    "extracted_at": datetime.now().isoformat(),
                                }
                            )
                            self.extracted_urls.add(url)

            return results
        except Exception as e:
            self.logger.error(f"Error processing {file_path}: {e}")
            return []

    def process_html_file(self, file_path: Path, source_dir: str) -> List[Dict]:
        """Process an HTML file and extract URLs with metadata."""
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()

            soup = BeautifulSoup(content, "html.parser")

            # Extract all text content
            text_content = soup.get_text()

            # Extract URLs from text
            urls = self.extract_urls_from_text(text_content)

            results = []
            for url in urls:
                # Skip Facebook-related URLs
                if self.should_skip_url(url):
                    continue

                # Skip if already processed
                if url in self.extracted_urls:
                    continue

                domain = self.extract_domain(url)
                results.append(
                    {
                        "url": url,
                        "resolved_url": "",  # Empty for now
                        "domain": domain,
                        "content": "",  # Empty for now
                        "source_directory": source_dir,
                        "source_file": file_path.name,
                        "timestamp": "",
                        "extracted_at": datetime.now().isoformat(),
                    }
                )
                self.extracted_urls.add(url)

            return results
        except Exception as e:
            self.logger.error(f"Error processing {file_path}: {e}")
            return []

    def find_facebook_files(self) -> List[Tuple[Path, str]]:
        """Find all Facebook data files (JSON and HTML) in the data directory."""
        files_to_process = []

        for root, dirs, files in os.walk(self.data_dir):
            root_path = Path(root)

            # Skip system files and hidden directories
            dirs[:] = [d for d in dirs if not d.startswith(".")]

            for file in files:
                if file.endswith((".json", ".html", ".htm")):
                    file_path = root_path / file

                    # Get the source directory (the main Facebook account directory)
                    source_dir = self.get_source_directory(file_path)

                    files_to_process.append((file_path, source_dir))

        return files_to_process

    def get_source_directory(self, file_path: Path) -> str:
        """Get the source directory name from the file path."""
        # Navigate up to find the main Facebook account directory
        current = file_path.parent

        # Look for the pattern that indicates a Facebook account directory
        while current != self.data_dir and current != current.parent:
            dir_name = current.name
            # Check if this looks like a Facebook account directory
            if "facebook" in dir_name.lower() or any(
                pattern in dir_name for pattern in ["4477g", "ju5", "uu5"]
            ):
                return dir_name
            current = current.parent

        # If not found, return the immediate parent directory
        return file_path.parent.name

    def extract_urls(self) -> pd.DataFrame:
        """Main method to extract URLs from all Facebook data files."""
        self.logger.info("Starting Facebook URL extraction...")

        # Find all files to process
        files_to_process = self.find_facebook_files()
        self.logger.info(f"Found {len(files_to_process)} files to process")

        total_urls = 0
        processed_files = 0

        for file_path, source_dir in files_to_process:
            try:
                self.logger.info(f"Processing: {file_path.name} from {source_dir}")

                if file_path.suffix.lower() == ".json":
                    results = self.process_json_file(file_path, source_dir)
                elif file_path.suffix.lower() in [".html", ".htm"]:
                    results = self.process_html_file(file_path, source_dir)
                else:
                    continue

                self.results.extend(results)
                total_urls += len(results)
                processed_files += 1

                # Log progress every 100 files
                if processed_files % 100 == 0:
                    self.logger.info(
                        f"Processed {processed_files} files, extracted {total_urls} URLs"
                    )

            except Exception as e:
                self.logger.error(f"Error processing {file_path}: {e}")
                continue

        # Create DataFrame
        if self.results:
            df = pd.DataFrame(self.results)

            # Remove duplicates based on URL
            df = df.drop_duplicates(subset=["url"])

            # Sort by domain and URL
            df = df.sort_values(["domain", "url"])

            self.logger.info(f"Extraction completed!")
            self.logger.info(f"Total files processed: {processed_files}")
            self.logger.info(f"Total unique URLs extracted: {len(df)}")
            self.logger.info(f"Unique domains: {df['domain'].nunique()}")

            return df
        else:
            self.logger.warning("No URLs extracted!")
            return pd.DataFrame(
                columns=[
                    "url",
                    "resolved_url",
                    "domain",
                    "content",
                    "source_directory",
                    "source_file",
                    "timestamp",
                    "extracted_at",
                ]
            )

    def save_results(self, df: pd.DataFrame):
        """Save the extracted URLs to CSV file."""
        output_file = self.output_dir / "extracted_urls_facebook.csv"
        df.to_csv(output_file, index=False, encoding="utf-8")

        self.logger.info(f"Results saved to: {output_file}")

        # Also save a summary
        summary_file = self.output_dir / "extraction_summary.txt"
        with open(summary_file, "w") as f:
            f.write("Facebook URL Extraction Summary\n")
            f.write("=" * 40 + "\n")
            f.write(f"Extraction date: {datetime.now()}\n")
            f.write(f"Total URLs extracted: {len(df)}\n")
            f.write(f"Unique domains: {df['domain'].nunique()}\n")
            f.write(f"Source directories: {df['source_directory'].nunique()}\n")
            f.write(f"Source files: {df['source_file'].nunique()}\n\n")

            f.write("Top 10 domains:\n")
            domain_counts = df["domain"].value_counts().head(10)
            for domain, count in domain_counts.items():
                f.write(f"  {domain}: {count}\n")

        self.logger.info(f"Summary saved to: {summary_file}")


def main():
    """Main function to run Facebook URL extraction."""
    import argparse
    
    print("Facebook URL Extraction - Step 1")
    print("=" * 40)

    parser = argparse.ArgumentParser(description="Extract URLs from Facebook data")
    parser.add_argument("--input-dir", default="../../data/Kantar_download_398_unzipped_new", help="Input directory containing Facebook data")
    parser.add_argument("--output-dir", default="../../data/url_extract_facebook", help="Output directory for extracted URLs")
    
    args = parser.parse_args()

    # Define paths
    data_dir = args.input_dir
    output_dir = args.output_dir

    # Check if data directory exists
    if not Path(data_dir).exists():
        print(f"Error: Data directory not found: {data_dir}")
        return

    # Initialize extractor
    extractor = FacebookURLExtractor(data_dir, output_dir)

    # Extract URLs
    print(f"\nStarting URL extraction from Facebook data...")
    start_time = time.time()

    df = extractor.extract_urls()

    # Save results
    if not df.empty:
        extractor.save_results(df)

        end_time = time.time()
        print(f"\nExtraction completed in {end_time - start_time:.2f} seconds")
        print(f"Next step: Run step2_analyze_urls_facebook.py")
    else:
        print("No URLs were extracted. Check the data directory and file formats.")


if __name__ == "__main__":
    main()
