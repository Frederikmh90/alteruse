import os
from pathlib import Path
import pandas as pd
from Processing_facebook_news_analysis import analyze_facebook_directory, save_analysis
from datetime import datetime, timezone
import re
import json
import random
from typing import Dict, Any, Tuple, List, Optional
from bs4 import BeautifulSoup


def is_browser_related_file(file_path: str) -> bool:
    """Check if a file is browser-related and should be excluded from Facebook analysis."""
    file_path_lower = file_path.lower()
    browser_indicators = [
        "history",
        "historik",
        "safari",
        "chrome",
        "firefox",
        "edge",
        "browser",
        "webkit",
        "bookmarks",
        "cookies",
        "cache",
    ]

    return any(indicator in file_path_lower for indicator in browser_indicators)


def is_facebook_related_file(file_path: str) -> bool:
    """Check if a file is Facebook-related."""
    file_path_lower = file_path.lower()
    facebook_indicators = [
        # "facebook",
        "your_facebook_activity",
        # "posts_and_comments",
        # "comments.json",
        # "posts.json",
        # "likes_and_reactions",
        # "pages_you",
        # "recently_viewed",
        "logged_information",
    ]

    return any(indicator in file_path_lower for indicator in facebook_indicators)


def create_simple_unprocessed_report(
    base_dir: str, output_dir: str, processed_folders: List[str]
):
    """Create a simple report of main folders that were not processed."""
    print("\nAnalyzing unprocessed Facebook folders...")

    # Get all main folders in the Facebook directory
    all_main_folders = []
    for item in os.listdir(base_dir):
        item_path = os.path.join(base_dir, item)
        if os.path.isdir(item_path) and not item.startswith("."):
            all_main_folders.append(item_path)

    # Get just the folder names (not full paths) for processed folders
    processed_folder_names = [os.path.basename(folder) for folder in processed_folders]

    # Find unprocessed folders
    unprocessed_folders = []
    for folder_path in all_main_folders:
        folder_name = os.path.basename(folder_path)
        if folder_name not in processed_folder_names:
            # Try to determine why it wasn't processed
            reason = "Unknown"
            if not is_valid_facebook_folder(folder_path):
                reason = "Failed folder validation"
            elif not extract_account_name(folder_path):
                reason = "Could not extract account name"
            else:
                reason = "Other processing error"

            unprocessed_folders.append(
                {"folder_name": folder_name, "full_path": folder_path, "reason": reason}
            )

    # Create summary
    total_folders = len(all_main_folders)
    processed_count = len(processed_folders)
    unprocessed_count = len(unprocessed_folders)

    print(f"\nFOLDER PROCESSING SUMMARY:")
    print(f"Total main folders found: {total_folders}")
    print(f"Successfully processed: {processed_count}")
    print(f"Not processed: {unprocessed_count}")

    if unprocessed_folders:
        print(f"\nUNPROCESSED FOLDERS ({unprocessed_count}):")
        for i, folder in enumerate(unprocessed_folders, 1):
            print(f"{i:2d}. {folder['folder_name']}")
            print(f"    Reason: {folder['reason']}")

    # Save to file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = os.path.join(output_dir, f"unprocessed_folders_{timestamp}.txt")

    with open(output_file, "w", encoding="utf-8") as f:
        f.write("FACEBOOK FOLDERS PROCESSING REPORT\n")
        f.write(f"Generated: {datetime.now()}\n")
        f.write("=" * 50 + "\n\n")

        f.write(f"Total main folders found: {total_folders}\n")
        f.write(f"Successfully processed: {processed_count}\n")
        f.write(f"Not processed: {unprocessed_count}\n\n")

        if unprocessed_folders:
            f.write(f"UNPROCESSED FOLDERS ({unprocessed_count}):\n")
            f.write("-" * 30 + "\n")
            for i, folder in enumerate(unprocessed_folders, 1):
                f.write(f"{i:2d}. {folder['folder_name']}\n")
                f.write(f"    Reason: {folder['reason']}\n")
                f.write(f"    Path: {folder['full_path']}\n\n")
        else:
            f.write("All folders were successfully processed!\n")

    print(f"\nUnprocessed folders report saved to: {output_file}")
    return unprocessed_folders


def parse_html_facebook_data(folder_path: str) -> Dict[str, Any]:
    """Parse Facebook data from HTML format files."""
    print(f"Parsing HTML Facebook data from: {os.path.basename(folder_path)}")

    data = {
        "posts": [],
        "messages": [],
        "activities": [],
        "photos": [],
        "videos": [],
        "account_info": {"name": "Unknown"},
        "total_activities": 0,
    }

    html_files = []
    for root, _, files in os.walk(folder_path):
        for file in files:
            if file.endswith((".html", ".htm")):
                html_files.append(os.path.join(root, file))

    print(f"Found {len(html_files)} HTML files to process")

    for html_file in html_files:
        try:
            with open(html_file, "r", encoding="utf-8") as f:
                content = f.read()

            soup = BeautifulSoup(content, "html.parser")

            # Extract account name from the first header if available
            if data["account_info"]["name"] == "Unknown":
                aside_elem = soup.find("aside", {"role": "contentinfo"})
                if aside_elem:
                    text = aside_elem.get_text()
                    # Look for "Genereret af [Name]" pattern
                    name_match = re.search(
                        r"Genereret af\s+(.+?)\s+<time", str(aside_elem)
                    )
                    if not name_match:
                        name_match = re.search(r"Genereret af\s+([^<]+)", text)
                    if name_match:
                        data["account_info"]["name"] = name_match.group(1).strip()

            # Extract timestamps and content
            file_basename = os.path.basename(html_file)

            # Look for time elements
            time_elements = soup.find_all("time", {"datetime": True})
            for time_elem in time_elements:
                try:
                    datetime_str = time_elem.get("datetime")
                    timestamp = pd.to_datetime(datetime_str).timestamp()

                    # Find associated content
                    content_text = ""
                    parent = time_elem.find_parent()
                    if parent:
                        # Look for text content in nearby elements
                        content_divs = parent.find_all("div", class_="_3-95")
                        for div in content_divs:
                            text = div.get_text(strip=True)
                            if text and len(text) > 10:  # Ignore very short text
                                content_text = text
                                break

                    data["activities"].append(
                        {
                            "timestamp": timestamp,
                            "datetime": datetime_str,
                            "content": content_text,
                            "file": file_basename,
                            "type": "html_activity",
                        }
                    )
                except Exception as e:
                    print(f"Error parsing time element: {e}")

            # Look for media content (images and videos)
            media_elements = soup.find_all(["img", "video"])
            for media in media_elements:
                try:
                    src = media.get("src", "")
                    if src and not src.startswith("data:"):
                        # Find associated timestamp if available
                        timestamp = None
                        datetime_str = None

                        # Look for nearby time elements
                        parent_section = media.find_parent("section")
                        if parent_section:
                            time_elem = parent_section.find("time", {"datetime": True})
                            if time_elem:
                                datetime_str = time_elem.get("datetime")
                                timestamp = pd.to_datetime(datetime_str).timestamp()

                        # Look for date patterns in tables
                        if not timestamp:
                            parent_table = media.find_parent("table")
                            if parent_table:
                                cells = parent_table.find_all("td")
                                for cell in cells:
                                    text = cell.get_text()
                                    # Look for date patterns like "jan. 10, 2025 3:50:51 pm"
                                    date_match = re.search(
                                        r"(\w{3}\.\s+\d{1,2},\s+\d{4}\s+\d{1,2}:\d{2}:\d{2}\s+[ap]m)",
                                        text,
                                    )
                                    if date_match:
                                        try:
                                            date_str = date_match.group(1)
                                            # Convert to standard format
                                            dt = pd.to_datetime(
                                                date_str,
                                                format="%b. %d, %Y %I:%M:%S %p",
                                            )
                                            timestamp = dt.timestamp()
                                            datetime_str = dt.isoformat()
                                            break
                                        except:
                                            pass

                        if not timestamp:
                            timestamp = 0  # Default timestamp if none found
                            datetime_str = "unknown"

                        media_data = {
                            "src": src,
                            "timestamp": timestamp,
                            "datetime": datetime_str,
                            "file": file_basename,
                            "type": media.name,
                        }

                        if media.name == "img":
                            data["photos"].append(media_data)
                        else:
                            data["videos"].append(media_data)

                except Exception as e:
                    print(f"Error parsing media element: {e}")

            # Look for text content in posts
            content_divs = soup.find_all("div", class_="_3-95")
            for div in content_divs:
                text = div.get_text(strip=True)
                if text and len(text) > 10:  # Ignore very short text
                    # Try to find associated timestamp
                    timestamp = None
                    datetime_str = None

                    parent_section = div.find_parent("section")
                    if parent_section:
                        time_elem = parent_section.find("time", {"datetime": True})
                        if time_elem:
                            datetime_str = time_elem.get("datetime")
                            timestamp = pd.to_datetime(datetime_str).timestamp()

                    if not timestamp:
                        timestamp = 0
                        datetime_str = "unknown"

                    data["posts"].append(
                        {
                            "content": text,
                            "timestamp": timestamp,
                            "datetime": datetime_str,
                            "file": file_basename,
                            "type": "html_post",
                        }
                    )

        except Exception as e:
            print(f"Error processing HTML file {html_file}: {e}")

    # Calculate totals
    data["total_activities"] = (
        len(data["activities"])
        + len(data["posts"])
        + len(data["photos"])
        + len(data["videos"])
    )

    print(
        f"Extracted: {len(data['activities'])} activities, {len(data['posts'])} posts, {len(data['photos'])} photos, {len(data['videos'])} videos"
    )

    return data


def is_valid_facebook_folder(folder_path: str) -> bool:
    """Check if a folder contains valid Facebook data for processing (JSON or HTML)."""
    # Skip folders that just contain 'your_facebook_activity'
    if "your_facebook_activity" in folder_path.lower():
        return False

    # Check for essential JSON data files first
    json_patterns = [
        "posts_and_comments.json",
        "posts.json",
        "comments.json",
    ]

    has_json_file = False
    for root, _, files in os.walk(folder_path):
        for pattern in json_patterns:
            matching_files = [f for f in files if pattern in f.lower()]
            if matching_files:
                has_json_file = True
                break
        if has_json_file:
            break

    if has_json_file:
        return True

    # If no JSON files, check for HTML files with Facebook content
    html_files = []
    for root, _, files in os.walk(folder_path):
        for file in files:
            if file.endswith((".html", ".htm")):
                # Store both filename and relative path for better matching
                rel_path = os.path.relpath(os.path.join(root, file), folder_path)
                html_files.append((file, rel_path))

    # Consider it valid if it has a reasonable number of HTML files
    # and contains typical Facebook HTML structure
    if len(html_files) >= 50:  # Arbitrary threshold
        # Check for typical Facebook HTML files - expanded indicators
        facebook_html_indicators = [
            "your_posts",
            "your_videos",
            "your_photos",
            "shared_memories",
            "messages",
            "posts",
            "activity",
            "comments",
            "groups",
            "pages",
            "events",
            "photos",
            "videos",
            "check_ins",
            "reactions",
            "likes",
        ]

        # Check more files (up to 50 instead of just 10) and check both filename and path
        files_to_check = min(50, len(html_files))

        for filename, rel_path in html_files[:files_to_check]:
            filename_lower = filename.lower()
            rel_path_lower = rel_path.lower()

            # Check both filename and relative path for indicators
            for indicator in facebook_html_indicators:
                if indicator in filename_lower or indicator in rel_path_lower:
                    return True

    return False


def find_facebook_folders(base_dir: str) -> list:
    """Find all folders that likely contain Facebook data."""
    facebook_folders = []

    # Walk through the directory
    for root, dirs, files in os.walk(base_dir):
        for dir_name in dirs:
            full_path = os.path.join(root, dir_name)
            if is_valid_facebook_folder(full_path):
                facebook_folders.append(full_path)

    return facebook_folders


def extract_account_name(folder_path: str) -> Optional[str]:
    """Extract account name from folder path with validation."""
    folder_name = os.path.basename(folder_path)

    # Skip invalid folder names
    invalid_patterns = ["your_facebook_activity", "facebook_data", "facebook_files"]
    if any(pattern in folder_name.lower() for pattern in invalid_patterns):
        return None

    # Try to extract name from common patterns
    patterns = [
        r"facebook-([^-]+)-\d",  # matches facebook-username-date
        r"facebook([^/]+)\d",  # matches facebookusernamedate
        r"facebook_(.+?)_\d",  # matches facebook_username_date
    ]

    for pattern in patterns:
        match = re.search(pattern, folder_name)
        if match:
            name = match.group(1)
            # Validate extracted name
            if len(name) > 2 and not name.isdigit():
                return name

    # If no pattern matches but folder name seems valid, use it
    if len(folder_name) > 2 and not folder_name.isdigit():
        return folder_name

    return None


def parse_facebook_timestamp(timestamp: Any) -> Optional[datetime]:
    """Parse Facebook timestamp to datetime object.
    Handles both Unix timestamps (integers) and ISO 8601 format strings."""
    if not timestamp:
        return None

    try:
        # If timestamp is already a datetime
        if isinstance(timestamp, datetime):
            # Validate the date is reasonable
            if timestamp.year >= 2000 and timestamp.year <= 2030:
                return timestamp
            return None

        # If timestamp is a Unix timestamp (integer or float)
        if isinstance(timestamp, (int, float)):
            # Filter out obviously invalid timestamps (too small)
            if (
                timestamp < 946684800
            ):  # Before year 2000 (Unix timestamp for 2000-01-01)
                return None

            try:
                dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
                # Validate the date is reasonable
                if dt.year >= 2000 and dt.year <= 2030:
                    return dt
                return None
            except (ValueError, OSError):
                return None

        # If timestamp is an ISO 8601 string
        if isinstance(timestamp, str):
            # Remove timezone offset as we'll standardize to UTC
            clean_timestamp = timestamp.split("+")[0]
            try:
                if "T" in clean_timestamp:
                    dt = datetime.strptime(clean_timestamp, "%Y-%m-%dT%H:%M:%S")
                else:
                    dt = datetime.strptime(clean_timestamp, "%Y-%m-%d %H:%M:%S")
                # Validate the date is reasonable
                if dt.year >= 2000 and dt.year <= 2030:
                    return dt
                return None
            except ValueError:
                return None

    except Exception as e:
        print(f"Error parsing timestamp {timestamp}: {e}")
        return None


def get_activity_period(
    folder_path: str,
) -> Tuple[Optional[datetime], Optional[datetime], int]:
    """Get the earliest and latest activity dates from all relevant files.
    Returns (earliest_date, latest_date, total_valid_timestamps)"""
    earliest_date = None
    latest_date = None
    total_timestamps = 0
    valid_timestamps = 0
    invalid_timestamps = 0

    def update_dates(timestamp):
        nonlocal \
            earliest_date, \
            latest_date, \
            total_timestamps, \
            valid_timestamps, \
            invalid_timestamps
        total_timestamps += 1
        parsed_date = parse_facebook_timestamp(timestamp)
        if parsed_date:
            valid_timestamps += 1
            if not earliest_date or parsed_date < earliest_date:
                earliest_date = parsed_date
            if not latest_date or parsed_date > latest_date:
                latest_date = parsed_date
        else:
            invalid_timestamps += 1

    # Check if this is an HTML-format folder
    has_json = False
    has_html = False

    for root, _, files in os.walk(folder_path):
        for file in files:
            if file.startswith("._"):
                continue
            if file.endswith(".json"):
                has_json = True
            elif file.endswith((".html", ".htm")):
                has_html = True

    # Process JSON files if available
    if has_json:
        for root, _, files in os.walk(folder_path):
            for file in files:
                # Skip macOS resource fork files (._filename)
                if file.startswith("._"):
                    continue

                if file.endswith(".json"):
                    try:
                        with open(os.path.join(root, file), "r", encoding="utf-8") as f:
                            data = json.load(f)

                            # Handle different JSON structures
                            if isinstance(data, dict):
                                for key in [
                                    "timestamp",
                                    "time",
                                    "date",
                                    "created_time",
                                ]:
                                    if key in data:
                                        update_dates(data[key])
                            elif isinstance(data, list):
                                for item in data:
                                    if isinstance(item, dict):
                                        for key in [
                                            "timestamp",
                                            "time",
                                            "date",
                                            "created_time",
                                        ]:
                                            if key in item:
                                                update_dates(item[key])
                    except Exception as e:
                        print(f"Error processing JSON file {file}: {e}")
                        continue

    # Process HTML files if no JSON files or if this is primarily an HTML folder
    elif has_html:
        from bs4 import BeautifulSoup
        import re

        # Process HTML files for timestamps
        html_files = []
        for root, _, files in os.walk(folder_path):
            for file in files:
                if file.endswith((".html", ".htm")):
                    html_files.append(os.path.join(root, file))

        # Sample a subset of HTML files to get timestamps (processing all can be slow)
        sample_size = min(20, len(html_files))

        sampled_files = (
            random.sample(html_files, sample_size)
            if len(html_files) > sample_size
            else html_files
        )

        for html_file in sampled_files:
            try:
                with open(html_file, "r", encoding="utf-8") as f:
                    content = f.read()

                soup = BeautifulSoup(content, "html.parser")

                # Extract timestamps from time elements
                time_elements = soup.find_all("time", {"datetime": True})
                for time_elem in time_elements:
                    try:
                        datetime_str = time_elem.get("datetime")
                        if datetime_str:
                            # Convert to timestamp
                            dt = pd.to_datetime(datetime_str)
                            timestamp = dt.timestamp()
                            update_dates(timestamp)
                    except Exception as e:
                        continue

                # Look for date patterns in text
                # Find table cells that might contain dates
                cells = soup.find_all("td")
                for cell in cells:
                    text = cell.get_text()
                    # Look for date patterns like "jan. 10, 2025 3:50:51 pm"
                    date_patterns = [
                        r"(\w{3}\.\s+\d{1,2},\s+\d{4}\s+\d{1,2}:\d{2}:\d{2}\s+[ap]m)",
                        r"(\d{1,2}\.\s+\w{3}\s+\d{4}\s+\d{1,2}:\d{2}:\d{2})",
                        r"(\w{3}\s+\d{1,2},\s+\d{4}\s+\d{1,2}:\d{2}:\d{2}\s+[AP]M)",
                    ]

                    for pattern in date_patterns:
                        matches = re.findall(pattern, text)
                        for match in matches:
                            try:
                                # Try different date formats
                                formats = [
                                    "%b. %d, %Y %I:%M:%S %p",
                                    "%d. %b %Y %H:%M:%S",
                                    "%b %d, %Y %I:%M:%S %p",
                                ]

                                for fmt in formats:
                                    try:
                                        dt = pd.to_datetime(match, format=fmt)
                                        timestamp = dt.timestamp()
                                        update_dates(timestamp)
                                        break
                                    except:
                                        continue
                            except:
                                continue

            except Exception as e:
                print(f"Error processing HTML file {html_file}: {e}")
                continue

    if invalid_timestamps > 0:
        print(f"\nTimestamp Statistics for {os.path.basename(folder_path)}:")
        print(f"Total timestamps found: {total_timestamps}")
        print(f"Valid timestamps: {valid_timestamps}")
        print(f"Invalid timestamps (pre-2000): {invalid_timestamps}")
        print(f"Percentage valid: {(valid_timestamps / total_timestamps * 100):.1f}%")

    return earliest_date, latest_date, valid_timestamps


def analyze_facebook_json_data(folder_path: str) -> Dict[str, Any]:
    """Analyze Facebook JSON data from a folder."""
    print(f"Parsing JSON Facebook data from: {os.path.basename(folder_path)}")

    data = {
        "posts": [],
        "messages": [],
        "activities": [],
        "photos": [],
        "videos": [],
        "account_info": {"name": "Unknown"},
        "total_activities": 0,
    }

    # Use existing analyze_facebook_directory to get data
    df_urls, df_pages = analyze_facebook_directory(folder_path)

    # Extract account name
    account_name = extract_account_name(folder_path)
    if account_name:
        data["account_info"]["name"] = account_name

    # Convert URLs to activities
    if not df_urls.empty:
        for _, row in df_urls.iterrows():
            data["activities"].append(
                {
                    "timestamp": row.get("timestamp", 0),
                    "content": row.get("url", ""),
                    "type": "url_share",
                    "data": row.to_dict(),
                }
            )

    # Get activity period information
    earliest_date, latest_date, valid_timestamps = get_activity_period(folder_path)
    data["total_activities"] = len(data["activities"])

    return data


def analyze_account_activity(folder_path: str) -> Optional[Dict[str, Any]]:
    """Analyze activity metrics for a single Facebook account."""
    # Validate folder and extract account name
    if not is_valid_facebook_folder(folder_path):
        print(f"Skipping invalid folder: {folder_path}")
        return None

    # Use full folder name instead of extracted partial name for easier identification
    full_folder_name = os.path.basename(folder_path)

    # Still check if we can extract a partial name for validation, but don't use it
    extracted_account_name = extract_account_name(folder_path)
    if not extracted_account_name:
        print(f"Could not extract valid account name from: {folder_path}")
        return None

    earliest_date, latest_date, valid_timestamps = get_activity_period(folder_path)

    if not earliest_date or not latest_date:
        print(f"No valid activity dates found for: {folder_path}")
        return None

    if (latest_date - earliest_date).days < 0:
        print(f"Warning: Invalid date range for {full_folder_name}")
        print(f"Earliest: {earliest_date}")
        print(f"Latest: {latest_date}")
        return None

    # Get URL and pages data using existing function
    df_urls, df_pages = analyze_facebook_directory(folder_path)

    # Filter out rows with null values in important columns
    if not df_urls.empty:
        df_urls = df_urls.dropna(subset=["url", "domain"])
    if not df_pages.empty:
        df_pages = df_pages.dropna(subset=["page_name"])

    # Separate recently viewed from other URL data
    recently_viewed_df = pd.DataFrame()
    shared_urls_df = df_urls.copy() if not df_urls.empty else pd.DataFrame()

    if not df_urls.empty and "content_type" in df_urls.columns:
        recently_viewed_df = df_urls[df_urls["content_type"] == "recently_viewed"]
        shared_urls_df = df_urls[df_urls["content_type"] != "recently_viewed"]
    elif not df_urls.empty:
        # If no content_type column, treat all URLs as shared URLs
        recently_viewed_df = pd.DataFrame()
        shared_urls_df = df_urls.copy()

    # Initialize metrics dictionary with validated data - use full folder name
    metrics = {
        "account_name": full_folder_name,  # Use full folder name instead of extracted name
        "earliest_activity": earliest_date.isoformat(),
        "latest_activity": latest_date.isoformat(),
        "activity_days": (latest_date - earliest_date).days,
        "valid_timestamps": valid_timestamps,
        "total_urls_shared": len(shared_urls_df) if not shared_urls_df.empty else 0,
        "mainstream_news_shared": len(
            shared_urls_df[shared_urls_df["classification"] == "mainstream"]
        )
        if not shared_urls_df.empty
        else 0,
        "alternative_news_shared": len(
            shared_urls_df[shared_urls_df["classification"] == "alternative"]
        )
        if not shared_urls_df.empty
        else 0,
        "other_urls_shared": len(
            shared_urls_df[shared_urls_df["classification"] == "other"]
        )
        if not shared_urls_df.empty
        else 0,
        "total_news_pages_liked": len(df_pages) if not df_pages.empty else 0,
        "unique_domains_shared": shared_urls_df["domain"].nunique()
        if not shared_urls_df.empty
        else 0,
        # Recently viewed metrics
        "recently_viewed_mainstream": len(
            recently_viewed_df[recently_viewed_df["classification"] == "mainstream"]
        )
        if not recently_viewed_df.empty
        else 0,
        "recently_viewed_alternative": len(
            recently_viewed_df[recently_viewed_df["classification"] == "alternative"]
        )
        if not recently_viewed_df.empty
        else 0,
        "total_recently_viewed_news": len(recently_viewed_df)
        if not recently_viewed_df.empty
        else 0,
        "recently_viewed_watch_time": recently_viewed_df["watch_time_seconds"].sum()
        if not recently_viewed_df.empty
        and "watch_time_seconds" in recently_viewed_df.columns
        else 0,
    }

    return metrics


def process_all_accounts(kantar_dir: str, output_dir: str):
    """Process all Facebook accounts and create summary report."""
    # Find all Facebook data folders
    facebook_folders = find_facebook_folders(kantar_dir)
    print(f"Found {len(facebook_folders)} Facebook accounts")

    # Process each account
    all_accounts_metrics = []
    all_domains_data = []  # Store domain data separately
    all_pages_data = []  # Store pages data separately
    processed_folders = []  # Track successfully processed folders

    for folder in facebook_folders:
        try:
            metrics = analyze_account_activity(folder)
            if metrics:  # Only add if we got valid metrics
                all_accounts_metrics.append(metrics)
                processed_folders.append(folder)  # Track processed folder

                # Get URLs and pages data for this account
                df_urls, df_pages = analyze_facebook_directory(folder)

                # Store domain statistics
                if not df_urls.empty:
                    domain_stats = df_urls["domain"].value_counts()
                    for domain, count in domain_stats.items():
                        all_domains_data.append(
                            {
                                "account_name": metrics["account_name"],
                                "domain": domain,
                                "count": count,
                                "classification": df_urls[df_urls["domain"] == domain][
                                    "classification"
                                ].iloc[0],
                            }
                        )

                # Store pages statistics
                if not df_pages.empty:
                    page_stats = df_pages["page_name"].value_counts()
                    for page, count in page_stats.items():
                        all_pages_data.append(
                            {
                                "account_name": metrics["account_name"],
                                "page_name": page,
                                "count": count,
                            }
                        )

        except Exception as e:
            print(f"Error processing account {folder}: {e}")
            continue

    if not all_accounts_metrics:
        print("No valid accounts found to process!")
        return

    # Create summary DataFrames
    df_summary = pd.DataFrame(all_accounts_metrics)
    df_domains = pd.DataFrame(all_domains_data)
    df_pages = pd.DataFrame(all_pages_data)

    # Clean up the summary DataFrame
    df_summary = df_summary.dropna(
        subset=["account_name", "earliest_activity", "latest_activity"]
    )

    # Save summary to Excel
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    summary_file = os.path.join(
        output_dir, f"facebook_accounts_summary_{timestamp}.xlsx"
    )

    with pd.ExcelWriter(summary_file, engine="openpyxl") as writer:
        # Write main summary sheet
        df_summary.to_excel(writer, sheet_name="Account Summary", index=False)

        # Create activity metrics sheet with only relevant columns
        activity_cols = [
            "account_name",
            "activity_days",
            "total_urls_shared",
            "mainstream_news_shared",
            "alternative_news_shared",
            "other_urls_shared",
            "total_news_pages_liked",
            "recently_viewed_mainstream",
            "recently_viewed_alternative",
            "total_recently_viewed_news",
            "recently_viewed_watch_time",
        ]
        activity_metrics = df_summary[activity_cols].copy()
        activity_metrics.to_excel(writer, sheet_name="Activity Metrics", index=False)

        # Write domain statistics
        if not df_domains.empty:
            df_domains.sort_values(
                ["account_name", "count"], ascending=[True, False]
            ).to_excel(writer, sheet_name="Domain Statistics", index=False)

        # Write pages statistics
        if not df_pages.empty:
            df_pages.sort_values(
                ["account_name", "count"], ascending=[True, False]
            ).to_excel(writer, sheet_name="Pages Statistics", index=False)

    print(f"Summary saved to: {summary_file}")

    # Generate unprocessed files report
    create_simple_unprocessed_report(kantar_dir, output_dir, processed_folders)

    # Generate text summary with validated data
    summary_text_file = os.path.join(output_dir, f"analysis_summary_{timestamp}.txt")
    with open(summary_text_file, "w") as f:
        f.write("Facebook Accounts Analysis Summary\n")
        f.write(f"Generated on: {datetime.now()}\n\n")
        f.write(f"Total valid accounts analyzed: {len(all_accounts_metrics)}\n\n")

        # Activity period summary
        avg_activity_days = df_summary["activity_days"].mean()
        f.write(f"Average activity period: {avg_activity_days:.1f} days\n")

        # News sharing summary
        total_mainstream = df_summary["mainstream_news_shared"].sum()
        total_alternative = df_summary["alternative_news_shared"].sum()
        total_recently_viewed_mainstream = df_summary[
            "recently_viewed_mainstream"
        ].sum()
        total_recently_viewed_alternative = df_summary[
            "recently_viewed_alternative"
        ].sum()

        f.write(f"Total mainstream news shared: {total_mainstream}\n")
        f.write(f"Total alternative news shared: {total_alternative}\n")
        f.write(
            f"Total mainstream news recently viewed: {total_recently_viewed_mainstream}\n"
        )
        f.write(
            f"Total alternative news recently viewed: {total_recently_viewed_alternative}\n"
        )
        f.write(
            f"Total watch time for recently viewed news: {df_summary['recently_viewed_watch_time'].sum():.0f} seconds\n"
        )

        # Most active accounts (only include accounts with actual activity)
        active_accounts = (
            df_summary[df_summary["total_urls_shared"] > 0]
            .sort_values("total_urls_shared", ascending=False)
            .head(5)
        )

        f.write("\nTop 5 most active accounts by URLs shared:\n")
        for _, account in active_accounts.iterrows():
            f.write(
                f"- {account['account_name']}: {account['total_urls_shared']} URLs\n"
            )


def main():
    # Define paths
    kantar_dir = "/Users/Codebase/projects/alteruse/data/Kantar_download_0608/Facebook/"
    output_dir = (
        "/Users/Codebase/projects/alteruse/data/Analyzed_Kantar_060825/Facebook"
    )

    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Process all accounts
    process_all_accounts(kantar_dir, output_dir)


if __name__ == "__main__":
    main()
