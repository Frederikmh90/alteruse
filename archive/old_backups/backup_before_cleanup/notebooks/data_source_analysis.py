import os
import re
from collections import defaultdict
from datetime import datetime
import pandas as pd


def extract_file_info(filename):
    """Extract information from the complex filename structure."""
    # Initialize default values
    info = {"timestamp": None, "user": None, "source_type": None, "file_type": None}

    # Extract timestamp (format: YYYY-MM-DDTHh)
    timestamp_match = re.search(r"(\d{4}-\d{2}-\d{2}T\d{2})", filename)
    if timestamp_match:
        info["timestamp"] = timestamp_match.group(1)

    # Identify source type and user
    if "facebook" in filename.lower():
        info["source_type"] = "facebook"
        # Extract facebook username
        user_match = re.search(
            r"facebook([^/]+?)(?:\d{8}|\d{2}_\d{2}_\d{4})", filename.lower()
        )
        if user_match:
            info["user"] = user_match.group(1)

    elif "history" in filename.lower():
        info["source_type"] = "browser_history"
        # For browser history, user might be in a different pattern
        # We'll mark it as 'unknown_user' for now
        info["user"] = "unknown_user"

    # Determine file type
    if filename.endswith(".json"):
        info["file_type"] = "json"
    elif filename.endswith(".csv"):
        info["file_type"] = "csv"
    elif filename.endswith(".db"):
        info["file_type"] = "sqlite"
    elif "facebook" in filename.lower():
        info["file_type"] = "facebook_data"

    return info


def analyze_data_sources(directory_listing):
    """Analyze the directory listing to identify unique users and data sources."""
    # Initialize collectors
    users = defaultdict(set)  # user -> set of dates
    sources = defaultdict(int)  # source_type -> count
    file_types = defaultdict(int)  # file_type -> count
    user_sources = defaultdict(set)  # user -> set of sources

    # Process each file/directory
    for item in directory_listing.split("\n"):
        if not item.strip():
            continue

        # Extract if it's a file or directory and the name
        if "[file]" in item or "[dir]" in item:
            is_dir = "[dir]" in item
            name = item.split(None, 2)[-1].strip()

            # Skip macOS metadata files
            if name.startswith("._") or name == ".DS_Store":
                continue

            # Extract information
            info = extract_file_info(name)

            if info["user"]:
                if info["timestamp"]:
                    users[info["user"]].add(info["timestamp"][:10])  # Add just the date
                if info["source_type"]:
                    user_sources[info["user"]].add(info["source_type"])

            if info["source_type"]:
                sources[info["source_type"]] += 1

            if info["file_type"]:
                file_types[info["file_type"]] += 1

    # Convert to DataFrames for better visualization
    users_df = pd.DataFrame(
        [
            {
                "user": user,
                "dates_count": len(dates),
                "dates": sorted(list(dates)),
                "sources": sorted(list(user_sources[user])),
            }
            for user, dates in users.items()
        ]
    )

    sources_df = pd.DataFrame(
        [{"source": source, "count": count} for source, count in sources.items()]
    )

    file_types_df = pd.DataFrame(
        [{"file_type": ftype, "count": count} for ftype, count in file_types.items()]
    )

    return {"users": users_df, "sources": sources_df, "file_types": file_types_df}


def save_analysis(results, output_dir="../data/processed/"):
    """Save analysis results to CSV files."""
    # Create timestamp for unique filenames
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Save each DataFrame
    results["users"].to_csv(f"{output_dir}unique_users_{timestamp}.csv", index=False)
    results["sources"].to_csv(f"{output_dir}data_sources_{timestamp}.csv", index=False)
    results["file_types"].to_csv(f"{output_dir}file_types_{timestamp}.csv", index=False)

    # Also save as Excel with multiple sheets
    with pd.ExcelWriter(f"{output_dir}data_source_analysis_{timestamp}.xlsx") as writer:
        results["users"].to_excel(writer, sheet_name="Unique Users", index=False)
        results["sources"].to_excel(writer, sheet_name="Data Sources", index=False)
        results["file_types"].to_excel(writer, sheet_name="File Types", index=False)

    print(f"\nResults saved in {output_dir}:")
    print(f"- unique_users_{timestamp}.csv")
    print(f"- data_sources_{timestamp}.csv")
    print(f"- file_types_{timestamp}.csv")
    print(f"- data_source_analysis_{timestamp}.xlsx")


def main():
    # Read the directory listing from a file or input
    with open("../data/directory_listing.txt", "r") as f:
        directory_listing = f.read()

    # Analyze the data
    results = analyze_data_sources(directory_listing)

    # Print summary
    print("\nUnique Users Summary:")
    print("-" * 50)
    print(f"Total unique users: {len(results['users'])}")
    print("\nTop users by number of dates:")
    print(results["users"].sort_values("dates_count", ascending=False).head())

    print("\nData Sources Summary:")
    print("-" * 50)
    print(results["sources"])

    print("\nFile Types Summary:")
    print("-" * 50)
    print(results["file_types"])

    # Save results
    try:
        save_analysis(results)
    except Exception as e:
        print(f"\nError saving results: {e}")
        print("Please make sure the output directory exists and is writable.")


if __name__ == "__main__":
    main()
