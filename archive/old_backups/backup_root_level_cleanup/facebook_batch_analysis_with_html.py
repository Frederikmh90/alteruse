#!/usr/bin/env python3
"""
Enhanced Facebook Batch Analysis - includes both JSON and HTML processing
Integrates the HTML parser for profiles that have HTML format instead of JSON
"""

import os
import json
import csv
import re
from datetime import datetime
from collections import defaultdict
from bs4 import BeautifulSoup
from urllib.parse import unquote

# Import the HTML parsing functions
from facebook_html_parser import (
    parse_recently_viewed_html,
    parse_facebook_timestamp,
    extract_urls_from_facebook_links,
)


def classify_news_source(domain):
    """Classify a domain as news source type"""

    # Alternative news sources (exact domain matching)
    alternative_news_sources = {
        "nyhedsbrevet.dk",
        "uriasposten.net",
        "frihedsbrevet.dk",
        "contra.dk",
        "ing.dk",
        "nordfront.dk",
        "konservativtnet.dk",
        "danskfolkeparti.dk",
        "frihedens-stemme.dk",
        "rights.dk",
        "theeuropean.de",
        "breitbart.com",
        "infowars.com",
        "zerohedge.com",
        "denfrieavis.dk",
        "nationnews.dk",
        "patriotpost.dk",
        "danmarkshistorie.dk",
        "borgerforslag.dk",
    }

    # Mainstream news sources
    mainstream_news_sources = {
        "dr.dk",
        "tv2.dk",
        "bt.dk",
        "ekstrabladet.dk",
        "politiken.dk",
        "jyllands-posten.dk",
        "berlingske.dk",
        "information.dk",
        "weekendavisen.dk",
        "lokalavisen.dk",
        "fyns.dk",
        "nordjyske.dk",
        "aoh.dk",
        "sn.dk",
        "bbc.com",
        "cnn.com",
        "reuters.com",
        "theguardian.com",
        "nytimes.com",
        "washingtonpost.com",
        "france24.com",
        "dw.com",
        "euronews.com",
    }

    # Clean domain
    domain = domain.lower().strip()
    if domain.startswith("www."):
        domain = domain[4:]

    # Check for exact matches
    if domain in alternative_news_sources:
        return "alternative"
    elif domain in mainstream_news_sources:
        return "mainstream"
    else:
        return "other"


def extract_domain_from_url(url):
    """Extract domain from URL"""
    try:
        # Handle Facebook redirect URLs
        if "facebook.com/l.php" in url or "facebook.com/dyi/l" in url:
            # Try to extract the actual URL from Facebook redirect
            import re
            from urllib.parse import parse_qs, urlparse

            parsed = urlparse(url)
            if parsed.query:
                query_params = parse_qs(parsed.query)
                if "u" in query_params:
                    actual_url = query_params["u"][0]
                    return extract_domain_from_url(actual_url)

        # Regular URL processing
        from urllib.parse import urlparse

        parsed = urlparse(url)
        domain = parsed.netloc.lower()

        if domain.startswith("www."):
            domain = domain[4:]

        return domain
    except:
        return None


def process_json_file(file_path):
    """Process JSON format Facebook data"""
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        processed_items = []

        if isinstance(data, list):
            for item in data:
                processed_item = {
                    "timestamp": item.get("timestamp", None),
                    "title": item.get("title", ""),
                    "data": item.get("data", []),
                    "entry_type": "json_data",
                }
                processed_items.append(processed_item)

        return processed_items
    except Exception as e:
        print(f"Error processing JSON file {file_path}: {e}")
        return []


def process_html_file(file_path):
    """Process HTML format Facebook data using the HTML parser"""
    try:
        return parse_recently_viewed_html(file_path)
    except Exception as e:
        print(f"Error processing HTML file {file_path}: {e}")
        return []


def process_facebook_folder(folder_path):
    """Process a Facebook folder with both JSON and HTML support"""

    interactions_path = os.path.join(folder_path, "logged_information", "interactions")

    if not os.path.exists(interactions_path):
        return None

    # Files to look for (both JSON and HTML)
    target_files = [
        "recently_viewed.json",
        "recently_viewed.html",
        "recently_visited.json",
        "recently_visited.html",
        "groups_you've_visited.json",
        "groups_you've_visited.html",
    ]

    all_entries = []
    files_processed = []

    for filename in target_files:
        file_path = os.path.join(interactions_path, filename)

        if os.path.exists(file_path):
            print(f"  Processing {filename}...")

            if filename.endswith(".json"):
                entries = process_json_file(file_path)
            elif filename.endswith(".html"):
                entries = process_html_file(file_path)
            else:
                continue

            all_entries.extend(entries)
            files_processed.append(filename)

    if not all_entries:
        print(f"  No valid data found in {folder_path}")
        return None

    print(f"  Found {len(all_entries)} total entries from {len(files_processed)} files")

    return {
        "folder_path": folder_path,
        "entries": all_entries,
        "files_processed": files_processed,
        "total_entries": len(all_entries),
    }


def extract_urls_from_entries(entries):
    """Extract and classify URLs from processed entries"""

    url_data = []

    for entry in entries:
        timestamp = entry.get("timestamp")
        title = entry.get("title", "")
        data_items = entry.get("data", [])

        for item in data_items:
            urls = []

            if isinstance(item, str):
                # Check if it's a URL or contains URLs
                if item.startswith("http") or "." in item:
                    urls.append(item)
                else:
                    # Might be encoded data, try to extract URLs
                    url_pattern = r'https?://[^\s<>"]+|www\.[^\s<>"]+'
                    found_urls = re.findall(url_pattern, item)
                    urls.extend(found_urls)
            elif isinstance(item, dict):
                # Handle structured data
                if "uri" in item:
                    urls.append(item["uri"])
                elif "url" in item:
                    urls.append(item["url"])

            # Process each URL
            for url in urls:
                domain = extract_domain_from_url(url)
                if domain:
                    news_source = classify_news_source(domain)

                    url_data.append(
                        {
                            "timestamp": timestamp,
                            "url": url,
                            "domain": domain,
                            "news_source": news_source,
                            "title": title,
                        }
                    )

    return url_data


def process_all_facebook_folders(base_path):
    """Process all Facebook folders in the base directory"""

    print(f"Scanning for Facebook folders in: {base_path}")

    if not os.path.exists(base_path):
        print(f"Base path does not exist: {base_path}")
        return

    results = []
    processed_count = 0
    html_count = 0
    json_count = 0

    # Get all folders
    folders = [
        f for f in os.listdir(base_path) if os.path.isdir(os.path.join(base_path, f))
    ]
    folders.sort()

    print(f"Found {len(folders)} potential folders to check")

    for folder_name in folders:
        folder_path = os.path.join(base_path, folder_name)

        # Check if it has Facebook data structure
        interactions_path = os.path.join(
            folder_path, "logged_information", "interactions"
        )

        if os.path.exists(interactions_path):
            print(f"\nProcessing: {folder_name}")

            # Check what type of files this folder has
            files_in_folder = os.listdir(interactions_path)
            has_json = any(f.endswith(".json") for f in files_in_folder)
            has_html = any(f.endswith(".html") for f in files_in_folder)

            print(f"  Files found: JSON={has_json}, HTML={has_html}")

            result = process_facebook_folder(folder_path)

            if result:
                # Extract URLs and classify them
                url_data = extract_urls_from_entries(result["entries"])
                result["url_data"] = url_data
                result["url_count"] = len(url_data)
                result["has_json"] = has_json
                result["has_html"] = has_html

                # Count news sources
                news_counts = defaultdict(int)
                for url_item in url_data:
                    news_counts[url_item["news_source"]] += 1

                result["news_counts"] = dict(news_counts)

                results.append(result)
                processed_count += 1

                if has_html:
                    html_count += 1
                if has_json:
                    json_count += 1

                print(f"  ✅ URLs extracted: {len(url_data)}")
                print(f"  News sources: {dict(news_counts)}")
            else:
                print(f"  ❌ No data extracted")

    print(f"\n=== PROCESSING SUMMARY ===")
    print(f"Total folders processed: {processed_count}")
    print(f"Folders with HTML data: {html_count}")
    print(f"Folders with JSON data: {json_count}")
    print(f"Mixed format folders: {min(html_count, json_count)}")

    return results


def save_results_to_csv(results, output_path):
    """Save results to CSV files"""

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Save summary data
    summary_file = os.path.join(
        output_path, f"facebook_analysis_summary_{timestamp}.csv"
    )

    with open(summary_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "folder_name",
                "total_entries",
                "url_count",
                "has_json",
                "has_html",
                "files_processed",
                "mainstream_news",
                "alternative_news",
                "other_sources",
            ]
        )

        for result in results:
            folder_name = os.path.basename(result["folder_path"])
            news_counts = result.get("news_counts", {})

            writer.writerow(
                [
                    folder_name,
                    result["total_entries"],
                    result["url_count"],
                    result["has_json"],
                    result["has_html"],
                    "; ".join(result["files_processed"]),
                    news_counts.get("mainstream", 0),
                    news_counts.get("alternative", 0),
                    news_counts.get("other", 0),
                ]
            )

    # Save detailed URL data
    urls_file = os.path.join(output_path, f"facebook_urls_detailed_{timestamp}.csv")

    with open(urls_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            ["folder_name", "timestamp", "url", "domain", "news_source", "title"]
        )

        for result in results:
            folder_name = os.path.basename(result["folder_path"])

            for url_item in result.get("url_data", []):
                writer.writerow(
                    [
                        folder_name,
                        url_item["timestamp"],
                        url_item["url"],
                        url_item["domain"],
                        url_item["news_source"],
                        url_item["title"],
                    ]
                )

    print(f"\nResults saved to:")
    print(f"  Summary: {summary_file}")
    print(f"  Detailed URLs: {urls_file}")


def main():
    """Main function"""

    print("=== Enhanced Facebook Batch Analysis (JSON + HTML) ===")

    # Configuration
    base_path = "data/Kantar_download_398_unzipped_new"
    output_path = "data/processed"

    # Create output directory
    os.makedirs(output_path, exist_ok=True)

    # Process all folders
    results = process_all_facebook_folders(base_path)

    if results:
        # Save results
        save_results_to_csv(results, output_path)

        # Print summary statistics
        total_urls = sum(r["url_count"] for r in results)
        total_entries = sum(r["total_entries"] for r in results)

        print(f"\n=== FINAL SUMMARY ===")
        print(f"Accounts processed: {len(results)}")
        print(f"Total entries: {total_entries}")
        print(f"Total URLs extracted: {total_urls}")

        # Count by format
        html_only = sum(1 for r in results if r["has_html"] and not r["has_json"])
        json_only = sum(1 for r in results if r["has_json"] and not r["has_html"])
        mixed = sum(1 for r in results if r["has_json"] and r["has_html"])

        print(f"HTML-only accounts: {html_only}")
        print(f"JSON-only accounts: {json_only}")
        print(f"Mixed format accounts: {mixed}")

    else:
        print("No Facebook data found to process")


if __name__ == "__main__":
    main()
