import json
import pandas as pd
from datetime import datetime, timedelta, timezone
from pathlib import Path
import os


def parse_facebook_timestamp(timestamp):
    """Test different Facebook timestamp parsing approaches."""
    if not timestamp:
        return None, "no_timestamp"

    try:
        # Approach 1: Direct Unix timestamp (integer/float)
        if isinstance(timestamp, (int, float)):
            # Test if it looks like Unix timestamp in seconds
            if (
                timestamp > 1000000000 and timestamp < 9999999999
            ):  # Reasonable range for Unix seconds
                dt = datetime.fromtimestamp(timestamp)
                if dt.year >= 2000:
                    return dt, "unix_seconds"

            # Test if it looks like Unix timestamp in milliseconds
            if timestamp > 1000000000000:  # Looks like milliseconds
                dt = datetime.fromtimestamp(timestamp / 1000)
                if dt.year >= 2000:
                    return dt, "unix_milliseconds"

        # Approach 2: String timestamp
        if isinstance(timestamp, str):
            # Try ISO 8601 format
            clean_timestamp = timestamp.split("+")[0]
            if "T" in clean_timestamp:
                dt = datetime.strptime(clean_timestamp, "%Y-%m-%dT%H:%M:%S")
                if dt.year >= 2000:
                    return dt, "iso_format"
            else:
                dt = datetime.strptime(clean_timestamp, "%Y-%m-%d %H:%M:%S")
                if dt.year >= 2000:
                    return dt, "datetime_format"

        return None, "unparseable"

    except Exception as e:
        return None, f"error: {str(e)}"


def extract_timestamps_from_item(item):
    """Extract all possible timestamps from a Facebook data item."""
    timestamps = []

    # Direct timestamp fields
    for field in ["timestamp", "time", "date", "created_time"]:
        if field in item:
            timestamps.append((field, item[field]))

    # Timestamps in data arrays
    if "data" in item and isinstance(item["data"], list):
        for data_item in item["data"]:
            if isinstance(data_item, dict):
                for field in ["timestamp", "time", "date", "created_time"]:
                    if field in data_item:
                        timestamps.append((f"data.{field}", data_item[field]))

    return timestamps


def analyze_facebook_file(file_path):
    """Analyze timestamps in a single Facebook JSON file."""
    if not os.path.exists(file_path):
        return None

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        results = {
            "file": os.path.basename(file_path),
            "timestamp_analysis": {},
            "sample_timestamps": [],
            "parsing_methods": {},
            "total_items": 0,
            "items_with_timestamps": 0,
        }

        # Handle different JSON structures
        items_to_process = []
        if isinstance(data, dict):
            # Handle v2 structures
            for key in [
                "comments_v2",
                "group_posts_v2",
                "group_comments_v2",
                "posts",
                "page_likes_v2",
            ]:
                if key in data:
                    items_to_process = data[key]
                    break
            if not items_to_process and isinstance(data, dict):
                items_to_process = [data]
        elif isinstance(data, list):
            items_to_process = data

        results["total_items"] = len(items_to_process)

        timestamp_methods = {}
        all_timestamps = []

        for item in items_to_process[:100]:  # Limit to first 100 items for speed
            if not isinstance(item, dict):
                continue

            timestamps = extract_timestamps_from_item(item)
            if timestamps:
                results["items_with_timestamps"] += 1

                for field, ts_value in timestamps:
                    # Parse timestamp
                    parsed_dt, method = parse_facebook_timestamp(ts_value)

                    # Store sample
                    if len(results["sample_timestamps"]) < 10:
                        results["sample_timestamps"].append(
                            {
                                "field": field,
                                "original": ts_value,
                                "parsed": parsed_dt.isoformat() if parsed_dt else None,
                                "method": method,
                            }
                        )

                    # Count methods
                    if method not in timestamp_methods:
                        timestamp_methods[method] = 0
                    timestamp_methods[method] += 1

                    if parsed_dt:
                        all_timestamps.append(parsed_dt)

        results["parsing_methods"] = timestamp_methods

        if all_timestamps:
            results["date_range"] = {
                "earliest": min(all_timestamps).isoformat(),
                "latest": max(all_timestamps).isoformat(),
                "total_valid": len(all_timestamps),
            }

        return results

    except Exception as e:
        return {"file": os.path.basename(file_path), "error": str(e)}


def analyze_facebook_directory(base_dir):
    """Analyze all JSON files in a Facebook directory."""
    print(f"Analyzing Facebook directory: {base_dir}")

    json_files = []

    # Find all JSON files
    for root, dirs, files in os.walk(base_dir):
        for file in files:
            if file.endswith(".json"):
                json_files.append(os.path.join(root, file))

    print(f"Found {len(json_files)} JSON files")

    all_results = []

    for json_file in json_files[:10]:  # Limit to first 10 files for quick check
        print(f"  Processing: {os.path.basename(json_file)}")
        result = analyze_facebook_file(json_file)
        if result:
            all_results.append(result)

    return all_results


def main():
    # List of Facebook directories to check - including the specific one mentioned by user
    facebook_dirs = [
        "data/Kantar_download_398_unzipped_new/474-4477-c-146189_2025-05-02T14__4477g1746194515336sE6EzyMOWskju5307uu5307ufacebookjepperoege02052025x4jfH67T-dbBSdwf",
        "data/Kantar_download_398_unzipped_new/facebook-sorenheller-09-05-2025-5oChRLZS",
        "data/Kantar_download_398_unzipped_new/facebook-simonallanachextra-19_05_2025-L8xrODV7",
    ]

    print("Analyzing Facebook Timestamp Parsing...")
    print("-" * 50)

    for fb_dir in facebook_dirs:
        if os.path.exists(fb_dir):
            results = analyze_facebook_directory(fb_dir)

            print(f"\nResults for {os.path.basename(fb_dir)}:")
            print("-" * 40)

            for result in results[:5]:  # Show first 5 files
                if "error" in result:
                    print(f"  {result['file']}: ERROR - {result['error']}")
                else:
                    print(f"  {result['file']}:")
                    print(f"    Total items: {result['total_items']}")
                    print(
                        f"    Items with timestamps: {result['items_with_timestamps']}"
                    )

                    if result["parsing_methods"]:
                        print(f"    Parsing methods used:")
                        for method, count in result["parsing_methods"].items():
                            print(f"      {method}: {count}")

                    if "date_range" in result:
                        print(
                            f"    Date range: {result['date_range']['earliest']} to {result['date_range']['latest']}"
                        )
                        print(
                            f"    Valid timestamps: {result['date_range']['total_valid']}"
                        )

                    if result["sample_timestamps"]:
                        print(f"    Sample timestamps:")
                        for sample in result["sample_timestamps"][:3]:
                            print(
                                f"      {sample['field']}: {sample['original']} -> {sample['parsed']} ({sample['method']})"
                            )

                print()
        else:
            print(f"Directory not found: {fb_dir}")


if __name__ == "__main__":
    main()
