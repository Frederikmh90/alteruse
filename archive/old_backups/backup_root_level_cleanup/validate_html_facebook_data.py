#!/usr/bin/env python3
"""
Validation script to test HTML format Facebook data extraction.
Specifically checks recently_viewed.html data collection.
"""

import os
import json
from bs4 import BeautifulSoup
from datetime import datetime
import re


def extract_html_recently_viewed(html_file_path):
    """Extract recently viewed data from HTML file"""
    print(f"\n=== Analyzing HTML file: {html_file_path} ===")

    if not os.path.exists(html_file_path):
        print(f"ERROR: File not found: {html_file_path}")
        return None

    try:
        with open(html_file_path, "r", encoding="utf-8") as f:
            content = f.read()

        print(f"File size: {len(content)} characters")

        # Parse HTML
        soup = BeautifulSoup(content, "html.parser")

        # Look for various patterns in HTML structure
        print("\n=== HTML Structure Analysis ===")

        # Check for divs with data
        divs = soup.find_all("div")
        print(f"Total divs found: {len(divs)}")

        # Look for potential data containers
        data_containers = []

        # Check for divs with text content
        for i, div in enumerate(divs[:10]):  # Check first 10 divs
            if div.get_text(strip=True):
                text = div.get_text(strip=True)[:100]  # First 100 chars
                print(f"Div {i}: {text}...")
                if any(
                    keyword in text.lower()
                    for keyword in ["viewed", "visited", "facebook", "page", "post"]
                ):
                    data_containers.append(div)

        print(f"\nPotential data containers found: {len(data_containers)}")

        # Look for timestamps
        timestamp_patterns = [
            r"\d{4}-\d{2}-\d{2}",  # YYYY-MM-DD
            r"\d{1,2}/\d{1,2}/\d{4}",  # MM/DD/YYYY or M/D/YYYY
            r"\d{1,2}\.\d{1,2}\.\d{4}",  # DD.MM.YYYY
            r"\d{10,}",  # Unix timestamps
        ]

        timestamps_found = []
        for pattern in timestamp_patterns:
            matches = re.findall(pattern, content)
            if matches:
                timestamps_found.extend(matches[:5])  # First 5 matches

        print(f"\nTimestamp patterns found: {timestamps_found}")

        # Look for URLs
        url_pattern = r'https?://[^\s<>"]+|www\.[^\s<>"]+'
        urls = re.findall(url_pattern, content)
        print(f"\nURLs found: {len(urls)}")
        if urls:
            print("Sample URLs:")
            for url in urls[:5]:
                print(f"  - {url}")

        # Look for JSON-like data embedded in HTML
        json_patterns = [
            r'\{[^{}]*"[^"]*"[^{}]*\}',  # Simple JSON objects
            r"\[[^\[\]]*\{[^{}]*\}[^\[\]]*\]",  # JSON arrays
        ]

        json_data = []
        for pattern in json_patterns:
            matches = re.findall(pattern, content)
            json_data.extend(matches[:3])  # First 3 matches

        if json_data:
            print(f"\nPotential JSON data found: {len(json_data)} patterns")
            for i, data in enumerate(json_data):
                print(f"  JSON {i}: {data[:100]}...")

        # Extract specific Facebook data structure
        extracted_data = {
            "total_content_length": len(content),
            "divs_count": len(divs),
            "potential_data_containers": len(data_containers),
            "timestamps_found": timestamps_found,
            "urls_count": len(urls),
            "sample_urls": urls[:5] if urls else [],
            "json_patterns_found": len(json_data),
            "sample_text_content": [],
        }

        # Get sample text content from data containers
        for container in data_containers[:3]:
            text = container.get_text(strip=True)
            if text:
                extracted_data["sample_text_content"].append(text[:200])

        return extracted_data

    except Exception as e:
        print(f"ERROR processing HTML file: {e}")
        return None


def compare_with_json_structure(folder_path):
    """Check if there's a JSON equivalent and compare structure"""
    print(f"\n=== Checking for JSON equivalent in {folder_path} ===")

    json_files = []
    html_files = []

    interactions_path = os.path.join(folder_path, "logged_information", "interactions")

    if os.path.exists(interactions_path):
        for file in os.listdir(interactions_path):
            if file.endswith(".json"):
                json_files.append(file)
            elif file.endswith(".html"):
                html_files.append(file)

    print(f"JSON files found: {json_files}")
    print(f"HTML files found: {html_files}")

    # Try to find recently_viewed.json
    recently_viewed_json = os.path.join(interactions_path, "recently_viewed.json")
    if os.path.exists(recently_viewed_json):
        print("\nFound recently_viewed.json - analyzing structure...")
        try:
            with open(recently_viewed_json, "r", encoding="utf-8") as f:
                json_data = json.load(f)

            print(f"JSON structure type: {type(json_data)}")
            if isinstance(json_data, dict):
                print(f"JSON keys: {list(json_data.keys())}")
            elif isinstance(json_data, list):
                print(f"JSON list length: {len(json_data)}")
                if json_data and isinstance(json_data[0], dict):
                    print(f"First item keys: {list(json_data[0].keys())}")
        except Exception as e:
            print(f"Error reading JSON: {e}")
    else:
        print("No recently_viewed.json found for comparison")


def main():
    """Main validation function"""

    # Specific folder from user
    folder_path = "data/Kantar_download_398_unzipped_new/474-4477-c-146228_2025-05-06T11__4477g1746531668711sRpIAQEMxihju5394uu5394ufacebookfinnrj06052025jXOVMn29-Uv3JePv"

    print("=== Facebook HTML Data Validation Script ===")
    print(f"Target folder: {folder_path}")

    # Check if folder exists
    if not os.path.exists(folder_path):
        print(f"ERROR: Folder not found: {folder_path}")
        return

    # Check HTML file
    html_file = os.path.join(
        folder_path, "logged_information", "interactions", "recently_viewed.html"
    )

    # Extract data from HTML
    html_data = extract_html_recently_viewed(html_file)

    if html_data:
        print("\n=== EXTRACTION RESULTS ===")
        print(json.dumps(html_data, indent=2, ensure_ascii=False))

        # Determine if data extraction is successful
        success_indicators = [
            html_data["urls_count"] > 0,
            len(html_data["timestamps_found"]) > 0,
            html_data["potential_data_containers"] > 0,
            len(html_data["sample_text_content"]) > 0,
        ]

        success_score = sum(success_indicators)
        print(f"\n=== VALIDATION SUMMARY ===")
        print(f"Success indicators met: {success_score}/4")

        if success_score >= 2:
            print("✅ HTML data extraction appears to be working")
        else:
            print("❌ HTML data extraction may have issues")
            print("Recommendation: Implement dedicated HTML parser")

    # Compare with JSON structure
    compare_with_json_structure(folder_path)

    print("\n=== RECOMMENDATIONS ===")
    if (
        html_data
        and html_data["urls_count"] == 0
        and len(html_data["timestamps_found"]) == 0
    ):
        print("🔧 HTML file appears to have minimal extractable data")
        print("🔧 Consider implementing BeautifulSoup-based HTML parser")
        print("🔧 Look for specific Facebook HTML patterns")
    else:
        print("✅ HTML file contains extractable data")
        print("🔧 Verify current processing pipeline handles HTML correctly")


if __name__ == "__main__":
    main()
