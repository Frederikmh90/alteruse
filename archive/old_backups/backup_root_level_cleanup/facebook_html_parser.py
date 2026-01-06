#!/usr/bin/env python3
"""
Facebook HTML Parser - extracts data from HTML format Facebook files
to match the same structure as JSON processing.
"""

import os
import json
import re
from bs4 import BeautifulSoup
from datetime import datetime
from urllib.parse import urlparse, parse_qs, unquote


def parse_facebook_timestamp(timestamp_str):
    """Parse various Facebook timestamp formats"""

    # Try different timestamp patterns
    patterns = [
        "%b %d, %Y %I:%M:%S %p",  # "maj 06, 2025 1:15:11 pm"
        "%B %d, %Y %I:%M:%S %p",  # "May 06, 2025 1:15:11 pm"
        "%Y-%m-%d",  # "2025-05-06"
        "%d/%m/%Y",  # "06/05/2025"
        "%m/%d/%Y",  # "05/06/2025"
    ]

    # Danish month mapping
    danish_months = {
        "jan": "Jan",
        "feb": "Feb",
        "mar": "Mar",
        "apr": "Apr",
        "maj": "May",
        "jun": "Jun",
        "jul": "Jul",
        "aug": "Aug",
        "sep": "Sep",
        "okt": "Oct",
        "nov": "Nov",
        "dec": "Dec",
    }

    # Replace Danish month names
    normalized_str = timestamp_str.lower()
    for danish, english in danish_months.items():
        normalized_str = normalized_str.replace(danish, english)

    # Try to parse with different formats
    for pattern in patterns:
        try:
            dt = datetime.strptime(normalized_str.title(), pattern)
            return int(dt.timestamp())
        except ValueError:
            continue

    # If no pattern matches, try to extract year and return approximate timestamp
    year_match = re.search(r"20\d{2}", timestamp_str)
    if year_match:
        year = int(year_match.group())
        # Return January 1st of that year as fallback
        dt = datetime(year, 1, 1)
        return int(dt.timestamp())

    return None


def extract_urls_from_facebook_links(content):
    """Extract actual URLs from Facebook redirect links"""

    # Pattern for Facebook redirect links
    fb_link_pattern = r'https://www\.facebook\.com/dyi/l/\?l=([^"&\s]+)'

    matches = re.findall(fb_link_pattern, content)
    extracted_urls = []

    for match in matches:
        try:
            # Decode the URL
            decoded_url = unquote(match)
            extracted_urls.append(decoded_url)
        except Exception:
            continue

    # Also look for direct URLs
    direct_url_pattern = r'https?://(?!www\.facebook\.com|static\.xx\.fbcdn\.net)[^\s<>"]+(?:\.com|\.org|\.net|\.dk|\.no|\.se|\.de|\.uk)[^\s<>"]*'
    direct_matches = re.findall(direct_url_pattern, content)
    extracted_urls.extend(direct_matches)

    return extracted_urls


def parse_recently_viewed_html(html_file_path):
    """Parse recently_viewed.html file and extract data"""

    print(f"Parsing HTML file: {html_file_path}")

    if not os.path.exists(html_file_path):
        print(f"File not found: {html_file_path}")
        return []

    try:
        with open(html_file_path, "r", encoding="utf-8") as f:
            content = f.read()

        soup = BeautifulSoup(content, "html.parser")

        # Find data containers
        viewed_items = []

        # Look for patterns that indicate viewed content
        # Facebook HTML often has patterns like "timestamp + content"

        # Extract timestamp and content patterns
        timestamp_pattern = r"(maj|may|apr|jun|jul|aug|sep|okt|oct|nov|dec|jan|feb|mar)\s+\d{1,2},?\s+20\d{2}\s+\d{1,2}:\d{2}:\d{2}\s*(am|pm)?"

        # Split content into potential entries
        entries = re.split(
            r"(?=(?:maj|may|apr|jun|jul|aug|sep|okt|oct|nov|dec|jan|feb|mar)\s+\d{1,2},?\s+20\d{2})",
            content,
            flags=re.IGNORECASE,
        )

        for entry in entries:
            if len(entry.strip()) < 50:  # Skip very short entries
                continue

            # Find timestamp in this entry
            timestamp_match = re.search(timestamp_pattern, entry, re.IGNORECASE)

            if timestamp_match:
                timestamp_str = timestamp_match.group()
                timestamp = parse_facebook_timestamp(timestamp_str)

                # Extract URLs from this entry
                urls = extract_urls_from_facebook_links(entry)

                # Extract text content (clean up HTML)
                entry_soup = BeautifulSoup(entry, "html.parser")
                text_content = entry_soup.get_text(separator=" ", strip=True)

                # Remove very long repeated text
                if len(text_content) > 500:
                    text_content = text_content[:500] + "..."

                if urls or text_content:
                    viewed_item = {
                        "timestamp": timestamp,
                        "title": text_content[:100] if text_content else "",
                        "data": urls if urls else [text_content],
                        "entry_type": "recently_viewed",
                    }
                    viewed_items.append(viewed_item)

        # If no timestamped entries found, try alternative parsing
        if not viewed_items:
            print("No timestamped entries found, trying alternative parsing...")

            # Look for all URLs in the document
            all_urls = extract_urls_from_facebook_links(content)

            # Look for any timestamps
            all_timestamps = re.findall(timestamp_pattern, content, re.IGNORECASE)

            if all_urls:
                # Create entries by grouping URLs
                urls_per_entry = max(1, len(all_urls) // max(1, len(all_timestamps)))

                for i in range(0, len(all_urls), urls_per_entry):
                    url_group = all_urls[i : i + urls_per_entry]

                    # Use timestamp if available
                    timestamp = None
                    if i // urls_per_entry < len(all_timestamps):
                        timestamp = parse_facebook_timestamp(
                            all_timestamps[i // urls_per_entry]
                        )

                    viewed_item = {
                        "timestamp": timestamp,
                        "title": f"Viewed content group {i // urls_per_entry + 1}",
                        "data": url_group,
                        "entry_type": "recently_viewed",
                    }
                    viewed_items.append(viewed_item)

        print(f"Extracted {len(viewed_items)} items from HTML")
        return viewed_items

    except Exception as e:
        print(f"Error parsing HTML file: {e}")
        return []


def process_facebook_html_folder(folder_path):
    """Process a Facebook folder with HTML files"""

    interactions_path = os.path.join(folder_path, "logged_information", "interactions")

    if not os.path.exists(interactions_path):
        print(f"Interactions folder not found: {interactions_path}")
        return {}

    results = {}

    # List of HTML files to process
    html_files = [
        "recently_viewed.html",
        "recently_visited.html",
        "groups_you've_visited.html",
    ]

    for html_file in html_files:
        html_path = os.path.join(interactions_path, html_file)

        if os.path.exists(html_path):
            print(f"Processing {html_file}...")

            if html_file == "recently_viewed.html":
                data = parse_recently_viewed_html(html_path)
            else:
                # Use similar parsing for other HTML files
                data = parse_recently_viewed_html(html_path)

            results[html_file] = data
        else:
            print(f"File not found: {html_file}")

    return results


def main():
    """Test the HTML parser on the specific folder"""

    # Test folder
    folder_path = "data/Kantar_download_398_unzipped_new/474-4477-c-146228_2025-05-06T11__4477g1746531668711sRpIAQEMxihju5394uu5394ufacebookfinnrj06052025jXOVMn29-Uv3JePv"

    print("=== Facebook HTML Parser Test ===")
    print(f"Processing folder: {folder_path}")

    # Process the folder
    results = process_facebook_html_folder(folder_path)

    # Display results
    print("\n=== PARSING RESULTS ===")

    total_items = 0
    for file_name, items in results.items():
        print(f"\n{file_name}: {len(items)} items")
        total_items += len(items)

        # Show first few items
        for i, item in enumerate(items[:3]):
            print(f"  Item {i + 1}:")
            print(f"    Timestamp: {item.get('timestamp', 'None')}")
            print(f"    Title: {item.get('title', 'None')[:50]}...")
            print(f"    Data entries: {len(item.get('data', []))}")
            if item.get("data"):
                print(f"    Sample data: {str(item['data'][0])[:50]}...")

    print(f"\nTotal items extracted: {total_items}")

    # Save results to JSON for comparison
    output_file = "html_parsing_results.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False, default=str)

    print(f"Results saved to: {output_file}")

    # Provide integration guidance
    print("\n=== INTEGRATION GUIDANCE ===")
    if total_items > 0:
        print("✅ HTML parsing successful!")
        print("🔧 Next steps:")
        print("   1. Integrate this parser into facebook_batch_analysis.py")
        print("   2. Update the processing pipeline to handle HTML files")
        print("   3. Test with more HTML profiles")
    else:
        print("❌ No data extracted from HTML files")
        print("🔧 Next steps:")
        print("   1. Check HTML file structure manually")
        print("   2. Adjust parsing patterns")
        print("   3. Debug with smaller HTML samples")


if __name__ == "__main__":
    main()
