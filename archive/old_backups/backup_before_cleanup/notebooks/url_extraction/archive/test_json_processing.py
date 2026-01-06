#!/usr/bin/env python3
"""
Test JSON Processing
====================
Isolate and debug JSON file processing issues
"""

import json
import pandas as pd
from pathlib import Path


def test_json_file(file_path):
    """Test processing a single JSON file."""
    print(f"Testing JSON file: {file_path}")

    try:
        # Check file size
        file_size = file_path.stat().st_size
        print(f"File size: {file_size:,} bytes")

        # Try to load JSON
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        print(f"JSON loaded successfully")
        print(f"Data type: {type(data)}")

        # Handle different JSON structures
        if isinstance(data, list):
            records = data
            print(f"Direct list with {len(records)} records")
        elif isinstance(data, dict) and "Browser History" in data:
            records = data["Browser History"]
            print(f"Browser History key with {len(records)} records")
        elif isinstance(data, dict) and "history" in data:
            records = data["history"]
            print(f"History key with {len(records)} records")
        else:
            records = [data] if isinstance(data, dict) else []
            print(f"Single record or empty: {len(records)} records")

        print(f"Total records to process: {len(records)}")

        # Check first few records
        valid_urls = 0
        for i, record in enumerate(records[:5]):
            if not isinstance(record, dict):
                print(f"  Record {i}: Not a dict - {type(record)}")
                continue

            url = record.get("url", "")
            title = record.get("title", "")
            visit_time = (
                record.get("visitTime")
                or record.get("visit_time")
                or record.get("time_usec")
            )

            if url:
                valid_urls += 1
                print(
                    f"  Record {i}: URL='{url[:60]}...' Title='{title[:40]}...' Time={visit_time}"
                )
            else:
                print(f"  Record {i}: NO URL - Keys: {list(record.keys())}")

        # Count total valid URLs
        total_valid = sum(1 for r in records if isinstance(r, dict) and r.get("url"))
        print(
            f"Total records with URLs: {total_valid}/{len(records)} ({total_valid / len(records) * 100:.1f}%)"
        )

        return True, len(records), total_valid

    except json.JSONDecodeError as e:
        print(f"JSON decode error: {e}")
        return False, 0, 0
    except Exception as e:
        print(f"Other error: {e}")
        return False, 0, 0


def main():
    """Test problematic JSON files."""
    print("JSON Processing Test")
    print("=" * 40)

    # Test the first few problematic files
    problematic_files = [
        "474-4477-c-146271_2025-05-08T07__4477g1746690538960sn2dqgvrdtNju5517uu5517uhistory-5MIbsIS.json",
        "474-4477-c-146104_2025-05-01T12__4477g1746102510141sMbw5mjCqiNju5137uu5137uhistory-mY1tCwu.json",
        "474-4477-c-146693_2025-05-14T08__4477g1747211634451szao1a9FTQcju5728uu5728uhistory-m2iOymH.json",
    ]

    browser_dir = Path("data/Samlet_06112025/Browser")

    for filename in problematic_files:
        file_path = browser_dir / filename
        if file_path.exists():
            print(f"\n{'-' * 60}")
            success, total_records, valid_urls = test_json_file(file_path)
            print(
                f"Success: {success}, Records: {total_records}, Valid URLs: {valid_urls}"
            )
        else:
            print(f"File not found: {filename}")

    # Also test a file that worked to compare
    print(f"\n{'-' * 60}")
    print("Testing a working JSON file for comparison...")

    # Find any JSON file that's not in the error list
    all_json_files = list(browser_dir.glob("*.json"))
    print(f"Found {len(all_json_files)} total JSON files")

    # Test a small one that might work
    for json_file in all_json_files:
        if json_file.stat().st_size < 1000000:  # Less than 1MB
            print(f"\nTesting smaller file: {json_file.name}")
            success, total_records, valid_urls = test_json_file(json_file)
            print(
                f"Success: {success}, Records: {total_records}, Valid URLs: {valid_urls}"
            )
            break


if __name__ == "__main__":
    main()
