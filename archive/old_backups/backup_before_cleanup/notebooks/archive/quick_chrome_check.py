import json
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

# Chrome epoch for timestamp conversion
CHROME_EPOCH = datetime(1601, 1, 1)


def convert_chrome_timestamp(chrome_timestamp):
    """Convert Chrome timestamp (milliseconds since 1970-01-01) to datetime."""
    try:
        if not chrome_timestamp:
            return None

        # Handle string input with European number format
        if isinstance(chrome_timestamp, str):
            chrome_timestamp = chrome_timestamp.replace(",", ".")

        # Convert to float - Chrome timestamps appear to be milliseconds since Unix epoch (1970-01-01)
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


def analyze_chrome_file(file_path):
    """Analyze a single Chrome history JSON file."""
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        # Handle different JSON structures
        if isinstance(data, dict):
            if "Browser History" in data:
                data = data["Browser History"]
            else:
                for key in data:
                    if isinstance(data[key], list) and len(data[key]) > 0:
                        if isinstance(data[key][0], dict) and any(
                            k in data[key][0] for k in ["url", "title", "time"]
                        ):
                            data = data[key]
                            break

        df = pd.DataFrame(data)

        # Handle different timestamp column names
        time_columns = ["visitTime", "time", "visit_time", "timestamp"]
        timestamp_col = next((col for col in time_columns if col in df.columns), None)

        if timestamp_col is None:
            print(f"No timestamp column found in {file_path}")
            return None

        df["visit_datetime"] = df[timestamp_col].apply(convert_chrome_timestamp)

        # Extract domains from URLs
        df["domain"] = df["url"].apply(
            lambda x: x.split("/")[2] if len(x.split("/")) > 2 else x
        )

        # Calculate basic statistics
        stats = {
            "file_name": Path(file_path).name,
            "total_visits": len(df),
            "unique_urls": df["url"].nunique(),
            "unique_domains": df["domain"].nunique(),
            "earliest_visit": df["visit_datetime"].min(),
            "latest_visit": df["visit_datetime"].max(),
            "top_5_domains": df["domain"].value_counts().head(5).to_dict(),
        }

        return stats

    except Exception as e:
        print(f"Error processing file {file_path}: {str(e)}")
        return None


def main():
    # List of Chrome history files to check
    chrome_files = [
        "data/Kantar_download_398_unzipped_new/474-4477-c-146774_2025-05-22T17__4477g1747934802993sOqGDq5AgZOju5981uu5981uhistory-oh1NU1b.json",
        "data/Kantar_download_398_unzipped_new/474-4477-c-146770_2025-05-21T15__4477g1747843073916sf9wdJivpVUju5970uu5970uhistory-OdoznWP.json",
        "data/Kantar_download_398_unzipped_new/474-4477-c-146769_2025-05-21T15__4477g1747841023306sKnlgAKmDCnju5968uu5968uhistory-CocZKB8.json",
    ]

    print("Analyzing Chrome History Files...")
    print("-" * 50)

    for file_path in chrome_files:
        print(f"\nAnalyzing: {file_path}")
        stats = analyze_chrome_file(file_path)

        if stats:
            print(f"\nResults for {stats['file_name']}:")
            print(f"Total visits: {stats['total_visits']}")
            print(f"Unique URLs: {stats['unique_urls']}")
            print(f"Unique domains: {stats['unique_domains']}")
            print(f"Date range: {stats['earliest_visit']} to {stats['latest_visit']}")
            print("\nTop 5 domains visited:")
            for domain, count in stats["top_5_domains"].items():
                print(f"  - {domain}: {count} visits")
        else:
            print("Failed to analyze file")

        print("-" * 50)


if __name__ == "__main__":
    main()
