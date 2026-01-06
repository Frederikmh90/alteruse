#!/usr/bin/env python3
"""
Test script for the enhanced Facebook analysis - tests both HTML and JSON processing
"""

import os
from facebook_batch_analysis_with_html import (
    process_facebook_folder,
    extract_urls_from_entries,
)


def test_specific_folders():
    """Test the enhanced processing on specific folders"""

    base_path = "data/Kantar_download_398_unzipped_new"

    # Test folders - mix of HTML and JSON
    test_folders = [
        # HTML folder
        "474-4477-c-146228_2025-05-06T11__4477g1746531668711sRpIAQEMxihju5394uu5394ufacebookfinnrj06052025jXOVMn29-Uv3JePv",
        # JSON folders (first few we can find)
        "474-4477-c-146090_2025-04-28T19__4477g1745868500768sYYBRQ0PHTwju5101uu5101ufacebookamandaphansen280420253RyCoark-uvbgfch",
        "474-4477-c-146093_2025-04-30T06__4477g1745992936165sylBlUjracXju5105uu5105ufacebookregitzelarsen7300420258wWAZXeM-AySesBi",
    ]

    print("=== Testing Enhanced Facebook Analysis ===\n")

    for folder_name in test_folders:
        folder_path = os.path.join(base_path, folder_name)

        if not os.path.exists(folder_path):
            print(f"❌ Folder not found: {folder_name}")
            continue

        print(f"📁 Testing: {folder_name}")

        # Check what type of files this folder has
        interactions_path = os.path.join(
            folder_path, "logged_information", "interactions"
        )
        if os.path.exists(interactions_path):
            files_in_folder = os.listdir(interactions_path)
            has_json = any(f.endswith(".json") for f in files_in_folder)
            has_html = any(f.endswith(".html") for f in files_in_folder)

            print(f"  Format: JSON={has_json}, HTML={has_html}")

            # Process the folder
            result = process_facebook_folder(folder_path)

            if result:
                # Extract URLs and show results
                url_data = extract_urls_from_entries(result["entries"])

                print(f"  ✅ Success!")
                print(f"     Total entries: {result['total_entries']}")
                print(f"     Files processed: {', '.join(result['files_processed'])}")
                print(f"     URLs extracted: {len(url_data)}")

                if url_data:
                    # Show sample URLs
                    print(f"     Sample URLs:")
                    for i, url_item in enumerate(url_data[:3]):
                        print(
                            f"       {i + 1}. {url_item['domain']} ({url_item['news_source']})"
                        )

                print()
            else:
                print(f"  ❌ No data extracted")
                print()
        else:
            print(f"  ❌ No interactions folder found")
            print()


def main():
    test_specific_folders()


if __name__ == "__main__":
    main()
