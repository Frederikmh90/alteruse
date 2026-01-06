import os
import json
import polars as pl
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone, timedelta
import sqlite3
from typing import Dict, List, Tuple, Optional
import re
from urllib.parse import urlparse
from test_news_source_analysis import (
    alternative_news_sources,
    mainstream_news_sources,
    classify_news_source,
    extract_domain,
)

# Constants
WEBKIT_EPOCH = datetime(
    2001, 1, 1, tzinfo=timezone.utc
)  # WebKit epoch for Safari timestamps
CHROME_EPOCH = datetime(1601, 1, 1)  # Chrome epoch for timestamp conversion

# Apple epoch offset (seconds from 1970-01-01 to 2001-01-01) - validated from tests
APPLE_EPOCH_TO_UNIX_OFFSET = 978307200


def convert_webkit_timestamp(webkit_timestamp):
    """Convert WebKit timestamp (seconds since 2001-01-01) to datetime with proper timezone handling."""
    try:
        if not webkit_timestamp or pd.isna(webkit_timestamp):
            return None

        # Convert to float and add Apple epoch offset to get Unix timestamp
        timestamp = float(webkit_timestamp)
        unix_timestamp = timestamp + APPLE_EPOCH_TO_UNIX_OFFSET

        # Create datetime and localize to Europe/Copenhagen timezone
        dt = datetime.fromtimestamp(unix_timestamp, tz=timezone.utc)
        copenhagen_tz = timezone(timedelta(hours=1))  # CET (simplified)
        return dt.astimezone(copenhagen_tz)

    except (ValueError, TypeError, OverflowError) as e:
        print(
            f"Warning: Invalid WebKit timestamp value: {webkit_timestamp}, error: {e}"
        )
        return None


def convert_chrome_timestamp(chrome_timestamp):
    """Convert Chrome timestamp (milliseconds since 1970-01-01) to datetime."""
    try:
        if not chrome_timestamp:
            return None

        # Handle string input with European number format (comma as decimal separator)
        if isinstance(chrome_timestamp, str):
            # Replace comma with dot for decimal point
            chrome_timestamp = chrome_timestamp.replace(",", ".")

        # Convert to float - Chrome timestamps are in milliseconds since Unix epoch (1970-01-01)
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


def process_sqlite_db(db_path: str) -> Optional[pd.DataFrame]:
    """Process a Safari history database file with enhanced error handling and tag support."""
    try:
        # Verify this is a valid SQLite database
        if not os.path.getsize(db_path) > 0:
            print(f"Empty database file: {db_path}")
            return None

        # Try to open the database
        conn = sqlite3.connect(db_path)

        # Test if this is a valid Safari history database
        try:
            test_query = "SELECT name FROM sqlite_master WHERE type='table'"
            tables = pd.read_sql_query(test_query, conn)
            table_names = tables["name"].values

            if (
                "history_items" not in table_names
                or "history_visits" not in table_names
            ):
                print(f"Not a valid Safari history database: {db_path}")
                conn.close()
                return None

            print(f"Valid Safari database found: {os.path.basename(db_path)}")
            print(f"Available tables: {', '.join(table_names)}")

        except Exception as e:
            print(f"Not a valid SQLite database: {db_path}")
            conn.close()
            return None

        cursor = conn.cursor()

        # Get column information for both tables
        cursor.execute("PRAGMA table_info(history_items)")
        history_items_columns = [col[1] for col in cursor.fetchall()]

        cursor.execute("PRAGMA table_info(history_visits)")
        history_visits_columns = [col[1] for col in cursor.fetchall()]

        # Build the main query dynamically based on available columns
        select_columns = ["history_items.id", "history_items.url"]

        # Handle title column
        if "title" in history_items_columns:
            select_columns.append("history_items.title")
        else:
            select_columns.append("NULL as title")

        # Add domain expansion if available
        if "domain_expansion" in history_items_columns:
            select_columns.append("history_items.domain_expansion")
        else:
            select_columns.append("NULL as domain_expansion")

        # Add visit count if available
        if "visit_count" in history_items_columns:
            select_columns.append("history_items.visit_count")
        else:
            select_columns.append("NULL as visit_count")

        # Handle visit time column (required)
        if "visit_time" in history_visits_columns:
            select_columns.append("history_visits.visit_time")
        else:
            print(
                f"Error: Required column 'visit_time' not found in history_visits table"
            )
            conn.close()
            return None

        # Add visit-specific columns
        visit_columns = ["title", "load_successful", "http_non_get", "synthesized"]
        for col in visit_columns:
            if col in history_visits_columns:
                select_columns.append(f"history_visits.{col} AS visit_{col}")

        # Handle referring visit column - try different possible column names
        referring_visit_columns = ["from_visit", "referring_visit", "source"]
        found_referring_col = next(
            (col for col in referring_visit_columns if col in history_visits_columns),
            None,
        )
        if found_referring_col:
            select_columns.append(
                f"history_visits.{found_referring_col} AS referring_visit_id"
            )
        else:
            select_columns.append("NULL AS referring_visit_id")

        # Add visit ID
        select_columns.append("history_visits.id AS visit_id")

        # Construct and execute the main query
        main_query = f"""
        SELECT 
            {", ".join(select_columns)}
        FROM history_items
        JOIN history_visits ON history_items.id = history_visits.history_item
        ORDER BY history_visits.visit_time DESC
        """

        try:
            df = pd.read_sql_query(main_query, conn)
            print(f"Loaded {len(df)} visit records from main tables")
        except Exception as e:
            print(f"Error executing main query on {db_path}: {str(e)}")
            conn.close()
            return None

        # Try to load tag data if available
        if "history_tags" in table_names and "history_items_to_tags" in table_names:
            try:
                tag_query = """
                SELECT 
                    hit.history_item,
                    ht.id as tag_id,
                    ht.title as tag_title
                FROM history_items_to_tags hit
                JOIN history_tags ht ON hit.tag_id = ht.id
                """
                tag_df = pd.read_sql_query(tag_query, conn)

                if len(tag_df) > 0:
                    # Group tags by history_item to handle multiple tags per item
                    tag_summary = (
                        tag_df.groupby("history_item")
                        .agg(
                            {
                                "tag_title": lambda x: ", ".join(x.unique()),
                                "tag_id": "count",
                            }
                        )
                        .reset_index()
                    )
                    tag_summary.columns = ["id", "tags", "tag_count"]

                    # Merge with main dataframe
                    df = df.merge(tag_summary, on="id", how="left")
                    print(f"Added tag information for {tag_summary.shape[0]} items")

            except Exception as e:
                print(f"Warning: Could not load tag data: {str(e)}")

        conn.close()

        # Convert Safari/WebKit timestamps using the validated method
        df["visit_datetime"] = df["visit_time"].apply(convert_webkit_timestamp)

        # Count successful timestamp conversions
        valid_timestamps = df["visit_datetime"].notna().sum()
        print(
            f"Successfully converted {valid_timestamps}/{len(df)} timestamps ({valid_timestamps / len(df) * 100:.1f}%)"
        )

        # Add source type and file
        df["source_type"] = "safari_db"
        df["source_file"] = os.path.basename(db_path)

        # Reorder columns to put source_file first
        cols = ["source_file"] + [col for col in df.columns if col != "source_file"]
        df = df[cols]

        return df

    except Exception as e:
        print(f"Error processing SQLite file {db_path}: {str(e)}")
        if "conn" in locals():
            conn.close()
        return None


def convert_safari_json_timestamp(time_usec):
    """Convert Safari JSON timestamp (microseconds since Unix epoch) to datetime."""
    try:
        if not time_usec:
            return None

        # Convert microseconds to seconds
        timestamp_seconds = float(time_usec) / 1_000_000

        # Create datetime from Unix timestamp
        dt = datetime.fromtimestamp(timestamp_seconds, tz=timezone.utc)

        # Convert to Copenhagen timezone
        copenhagen_tz = timezone(timedelta(hours=1))  # CET (simplified)
        return dt.astimezone(copenhagen_tz)

    except (ValueError, TypeError, OverflowError) as e:
        print(f"Warning: Invalid Safari JSON timestamp value: {time_usec}, error: {e}")
        return None


def process_safari_json(json_path: str) -> Optional[pd.DataFrame]:
    """Process a Safari history JSON file with the new export format."""
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        # Check if this is the new Safari JSON format
        if (
            not isinstance(data, dict)
            or "metadata" not in data
            or "history" not in data
        ):
            print(f"Not a Safari JSON format file: {json_path}")
            return None

        # Validate metadata
        metadata = data.get("metadata", {})
        if (
            metadata.get("browser_name") != "Safari"
            or metadata.get("data_type") != "history"
        ):
            print(f"Not a Safari history file: {json_path}")
            return None

        print(f"Processing Safari JSON export:")
        print(
            f"  Browser: {metadata.get('browser_name')} {metadata.get('browser_version')}"
        )
        print(f"  Schema version: {metadata.get('schema_version')}")
        print(f"  Export time: {metadata.get('export_time_usec')}")

        # Extract history data
        history_data = data.get("history", [])
        if not history_data:
            print(f"No history data found in {json_path}")
            return None

        # Convert to DataFrame
        df = pd.DataFrame(history_data)
        print(f"Found {len(df)} history entries")

        # Convert timestamps
        print("Converting Safari JSON timestamps...")
        df["visit_datetime"] = df["time_usec"].apply(convert_safari_json_timestamp)

        # Also convert destination timestamps if available
        if "destination_time_usec" in df.columns:
            df["destination_datetime"] = df["destination_time_usec"].apply(
                convert_safari_json_timestamp
            )

        # Count successful timestamp conversions
        valid_timestamps = df["visit_datetime"].notna().sum()
        print(
            f"Successfully converted {valid_timestamps}/{len(df)} timestamps ({valid_timestamps / len(df) * 100:.1f}%)"
        )

        # Rename columns to match standard format
        column_mapping = {
            "time_usec": "visit_time",
            "url": "url",
            "title": "title",
            "visit_count": "visit_count",
            "destination_url": "destination_url",
            "destination_time_usec": "destination_time",
        }

        # Only rename columns that exist
        existing_mapping = {k: v for k, v in column_mapping.items() if k in df.columns}
        df = df.rename(columns=existing_mapping)

        # Add source type and file
        df["source_type"] = "safari_json"
        df["source_file"] = os.path.basename(json_path)

        # Add visit_id (use index as Safari JSON doesn't have explicit IDs)
        df["visit_id"] = df.index

        # Reorder columns to put source_file first
        cols = ["source_file"] + [col for col in df.columns if col != "source_file"]
        df = df[cols]

        # Ensure required columns exist
        required_columns = [
            "source_file",
            "visit_id",
            "url",
            "title",
            "visit_time",
            "visit_datetime",
            "source_type",
        ]
        for col in required_columns:
            if col not in df.columns:
                df[col] = None

        return df

    except Exception as e:
        print(f"Error processing Safari JSON file {json_path}: {str(e)}")
        return None


def process_chrome_json(json_path: str) -> Optional[pd.DataFrame]:
    """Process a Chrome history JSON file."""
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        # Skip Creator Studio history files as they have a different format
        if "creator_studio_history" in json_path.lower():
            print(f"Skipping Creator Studio history file: {json_path}")
            return None

        # Handle different JSON structures
        if isinstance(data, dict):
            # If data is a dictionary with 'Browser History' key
            if "Browser History" in data:
                data = data["Browser History"]
            # If data has a different structure, try to find the history data
            for key in data:
                if isinstance(data[key], list) and len(data[key]) > 0:
                    if isinstance(data[key][0], dict) and any(
                        k in data[key][0] for k in ["url", "title", "time"]
                    ):
                        data = data[key]
                        break

        # Convert to DataFrame
        df = pd.DataFrame(data)

        # Handle different timestamp column names
        time_columns = ["visitTime", "time", "visit_time", "timestamp"]
        timestamp_col = next((col for col in time_columns if col in df.columns), None)

        if timestamp_col is None:
            print(f"No timestamp column found in {json_path}")
            print(f"Available columns: {df.columns.tolist()}")
            return None

        # Convert Chrome timestamp to datetime
        print(f"\nProcessing timestamps from {os.path.basename(json_path)}")
        print("Sample of original timestamps:", df[timestamp_col].head().tolist())

        df["visit_datetime"] = df[timestamp_col].apply(convert_chrome_timestamp)

        # Print sample of converted timestamps for verification
        print("Sample of converted timestamps:", df["visit_datetime"].head().tolist())

        # Handle different column names for referrer
        referrer_columns = ["referringVisitId", "referring_visit_id", "referrer_id"]
        referrer_col = next(
            (col for col in referrer_columns if col in df.columns), None
        )
        if referrer_col:
            df = df.rename(columns={referrer_col: "referring_visit_id"})

        # Rename other columns to match SQLite format
        column_mapping = {
            "id": "visit_id",
            timestamp_col: "visit_time",
        }
        df = df.rename(columns=column_mapping)

        # Add source type and file
        df["source_type"] = "chrome_json"
        df["source_file"] = os.path.basename(json_path)

        # Reorder columns to put source_file first
        cols = ["source_file"] + [col for col in df.columns if col != "source_file"]
        df = df[cols]

        # Ensure required columns exist
        required_columns = [
            "source_file",
            "visit_id",
            "url",
            "title",
            "visit_time",
            "visit_datetime",
            "source_type",
        ]
        for col in required_columns:
            if col not in df.columns:
                df[col] = None

        return df

    except Exception as e:
        print(f"Error processing JSON file {json_path}: {str(e)}")
        return None


def find_browser_history_files(base_dir: str) -> Tuple[List[str], List[str]]:
    """Find all browser history files (both SQLite and JSON) with improved filtering."""
    db_files = []
    json_files = []

    for root, _, files in os.walk(base_dir):
        for file in files:
            # Skip macOS metadata files - validated fix from testing
            if file.startswith("._"):
                continue

            # Skip temporary and system files
            if file.startswith(".") and file != ".json":
                continue

            if file.endswith(".db"):
                # More specific Safari history detection
                if any(
                    keyword in file.lower()
                    for keyword in ["history", "historik", "safari"]
                ):
                    db_path = os.path.join(root, file)
                    # Quick validation - check if file is not empty and is SQLite
                    try:
                        if os.path.getsize(db_path) > 0:
                            db_files.append(db_path)
                    except OSError:
                        continue

            elif file.endswith(".json"):
                # Enhanced JSON file detection
                if any(
                    keyword in file.lower()
                    for keyword in ["history", "historik", "browser"]
                ):
                    # Skip Facebook's search_history and other non-browser files
                    if not any(
                        skip in file.lower()
                        for skip in ["search_history", "creator_studio"]
                    ):
                        json_files.append(os.path.join(root, file))

    return db_files, json_files


def analyze_browser_data(df: pd.DataFrame) -> Dict:
    """Analyze browser history data for news sources and visit patterns with enhanced Safari support."""
    # Extract domains and classify news sources
    df["domain"] = df["url"].apply(extract_domain)
    df["news_classification"] = df["domain"].apply(classify_news_source)

    # Enhanced metrics including Safari-specific data
    metrics = {
        "total_visits": len(df),
        "unique_urls": df["url"].nunique(),
        "unique_domains": df["domain"].nunique(),
        "mainstream_news_visits": len(df[df["news_classification"] == "mainstream"]),
        "alternative_news_visits": len(df[df["news_classification"] == "alternative"]),
        "other_visits": len(df[df["news_classification"] == "other"]),
        "earliest_visit": df["visit_datetime"].min(),
        "latest_visit": df["visit_datetime"].max(),
        "top_domains": df["domain"].value_counts().head(10).to_dict(),
        "news_domains": df[
            df["news_classification"].isin(["mainstream", "alternative"])
        ]["domain"]
        .value_counts()
        .to_dict(),
    }

    # Add Safari-specific metrics if available
    if "domain_expansion" in df.columns:
        metrics["safari_domain_expansion_available"] = (
            df["domain_expansion"].notna().sum()
        )

    if "visit_count" in df.columns:
        metrics["avg_visit_count_per_item"] = df["visit_count"].mean()

    if "tags" in df.columns:
        tagged_items = df["tags"].notna().sum()
        metrics["tagged_items"] = tagged_items
        metrics["tagged_items_percentage"] = (
            (tagged_items / len(df) * 100) if len(df) > 0 else 0
        )

    if "visit_load_successful" in df.columns:
        successful_visits = df["visit_load_successful"].sum()
        metrics["successful_visit_rate"] = (
            (successful_visits / len(df) * 100) if len(df) > 0 else 0
        )

    # Calculate time span in days
    if metrics["earliest_visit"] and metrics["latest_visit"]:
        try:
            time_span = (metrics["latest_visit"] - metrics["earliest_visit"]).days
            metrics["activity_span_days"] = time_span
        except:
            metrics["activity_span_days"] = 0

    return metrics


def save_large_dataframe(
    df: pd.DataFrame, base_filename: str, output_dir: str, max_rows: int = 1000000
):
    """Save a large DataFrame to multiple CSV files if needed."""
    if len(df) <= max_rows:
        df.to_csv(os.path.join(output_dir, f"{base_filename}.csv"), index=False)
    else:
        num_files = (len(df) // max_rows) + 1
        for i in range(num_files):
            start_idx = i * max_rows
            end_idx = min((i + 1) * max_rows, len(df))
            chunk = df.iloc[start_idx:end_idx]
            chunk.to_csv(
                os.path.join(output_dir, f"{base_filename}_part{i + 1}.csv"),
                index=False,
            )


def convert_to_naive_datetime(dt):
    """Convert a datetime to naive datetime, handling None and invalid values."""
    if pd.isna(dt):
        return None
    try:
        if isinstance(dt, pd.Timestamp):
            return dt.tz_localize(None) if dt.tzinfo else dt
        elif isinstance(dt, datetime):
            return dt.replace(tzinfo=None) if dt.tzinfo else dt
        return None
    except Exception:
        return None


def process_all_browser_histories(base_dir: str, output_dir: str):
    """Process all browser history files and create combined analysis with enhanced error handling."""
    print(f"Searching for browser history files in: {base_dir}")

    # Find all history files
    db_files, json_files = find_browser_history_files(base_dir)
    print(f"Found {len(db_files)} SQLite files and {len(json_files)} JSON files")

    if not db_files and not json_files:
        print("No browser history files found!")
        return

    # Process all files
    all_dfs = []
    account_metrics = []
    processing_errors = []
    processed_files = []  # Track successfully processed files

    # Process SQLite (Safari) files with enhanced error tracking
    print(f"\n{'=' * 60}")
    print("PROCESSING SAFARI DATABASES")
    print(f"{'=' * 60}")

    for db_file in db_files:
        print(f"\nProcessing Safari history: {os.path.basename(db_file)}")
        account_name = os.path.basename(os.path.dirname(db_file))

        try:
            df = process_sqlite_db(db_file)
            if df is not None and len(df) > 0:
                # Convert datetime column to naive datetime
                df["visit_datetime"] = df["visit_datetime"].apply(
                    convert_to_naive_datetime
                )
                df["account_name"] = account_name
                all_dfs.append(df)
                processed_files.append(db_file)  # Track processed file

                # Calculate metrics
                metrics = analyze_browser_data(df)
                # Convert timezone-aware datetimes to naive for Excel compatibility
                metrics["earliest_visit"] = convert_to_naive_datetime(
                    metrics["earliest_visit"]
                )
                metrics["latest_visit"] = convert_to_naive_datetime(
                    metrics["latest_visit"]
                )
                metrics["account_name"] = account_name
                metrics["data_source"] = "safari_db"
                metrics["source_file"] = os.path.basename(db_file)
                account_metrics.append(metrics)

                print(f"✅ Successfully processed: {len(df)} records")
            else:
                print(f"❌ No data extracted from {os.path.basename(db_file)}")
                processing_errors.append(
                    f"Safari DB {os.path.basename(db_file)}: No data extracted"
                )

        except Exception as e:
            print(f"❌ Error processing {os.path.basename(db_file)}: {str(e)}")
            processing_errors.append(f"Safari DB {os.path.basename(db_file)}: {str(e)}")

    # Process JSON files (both Chrome and Safari formats) with enhanced error tracking
    print(f"\n{'=' * 60}")
    print("PROCESSING JSON FILES (CHROME & SAFARI)")
    print(f"{'=' * 60}")

    for json_file in json_files:
        print(f"\nProcessing JSON history: {os.path.basename(json_file)}")
        account_name = os.path.basename(os.path.dirname(json_file))

        try:
            # First try Safari JSON format
            df = process_safari_json(json_file)
            data_source = "safari_json"

            # If Safari processing failed, try Chrome JSON format
            if df is None:
                print("Not Safari JSON format, trying Chrome JSON format...")
                df = process_chrome_json(json_file)
                data_source = "chrome_json"

            if df is not None and len(df) > 0:
                # Convert datetime column to naive datetime
                df["visit_datetime"] = df["visit_datetime"].apply(
                    convert_to_naive_datetime
                )
                df["account_name"] = account_name
                all_dfs.append(df)
                processed_files.append(json_file)  # Track processed file

                # Calculate metrics
                metrics = analyze_browser_data(df)
                # Convert timezone-aware datetimes to naive for Excel compatibility
                metrics["earliest_visit"] = convert_to_naive_datetime(
                    metrics["earliest_visit"]
                )
                metrics["latest_visit"] = convert_to_naive_datetime(
                    metrics["latest_visit"]
                )
                metrics["account_name"] = account_name
                metrics["data_source"] = data_source
                metrics["source_file"] = os.path.basename(json_file)
                account_metrics.append(metrics)

                print(f"✅ Successfully processed as {data_source}: {len(df)} records")
            else:
                print(f"❌ No data extracted from {os.path.basename(json_file)}")
                processing_errors.append(
                    f"JSON {os.path.basename(json_file)}: No data extracted (tried both Safari and Chrome formats)"
                )

        except Exception as e:
            print(f"❌ Error processing {os.path.basename(json_file)}: {str(e)}")
            processing_errors.append(f"JSON {os.path.basename(json_file)}: {str(e)}")

    # Combine all data
    if not all_dfs:
        print("\n❌ No data to process! All files failed or contained no data.")
        # Save error report
        if processing_errors:
            error_file = os.path.join(
                output_dir,
                f"browser_processing_errors_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt",
            )
            with open(error_file, "w") as f:
                f.write("Browser History Processing Errors\n")
                f.write(f"Generated on: {datetime.now()}\n\n")
                for error in processing_errors:
                    f.write(f"{error}\n")
            print(f"Error report saved to: {error_file}")
        return

    combined_df = pd.concat(all_dfs, ignore_index=True)
    metrics_df = pd.DataFrame(account_metrics)

    print(f"\n{'=' * 60}")
    print("SAVING RESULTS")
    print(f"{'=' * 60}")

    # Save results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Save the main data files as CSV
    print("Saving combined browser history data...")
    save_large_dataframe(
        combined_df, f"browser_history_all_visits_{timestamp}", output_dir
    )

    # Save news sources data
    news_df = combined_df[
        combined_df["news_classification"].isin(["mainstream", "alternative"])
    ]
    if len(news_df) > 0:
        print("Saving news sources data...")
        save_large_dataframe(
            news_df, f"browser_history_news_sources_{timestamp}", output_dir
        )

    # Save visit patterns
    pattern_columns = [
        "account_name",
        "source_type",
        "source_file",
        "url",
        "visit_datetime",
        "visit_id",
        "referring_visit_id",
    ]
    # Only include columns that exist
    available_pattern_columns = [
        col for col in pattern_columns if col in combined_df.columns
    ]
    visit_patterns = combined_df[available_pattern_columns].copy()

    print("Saving visit patterns...")
    save_large_dataframe(
        visit_patterns, f"browser_history_visit_patterns_{timestamp}", output_dir
    )

    # Save metrics to Excel (this should be small enough)
    metrics_file = os.path.join(output_dir, f"browser_history_metrics_{timestamp}.xlsx")
    metrics_df.to_excel(metrics_file, index=False)

    print(f"\n✅ Analysis saved to files in: {output_dir}")

    # Generate unprocessed files report
    create_browser_unprocessed_report(base_dir, output_dir, processed_files)

    # Generate enhanced summary statistics
    print(f"\n{'=' * 60}")
    print("ANALYSIS SUMMARY")
    print(f"{'=' * 60}")

    print(f"Total accounts analyzed: {metrics_df['account_name'].nunique()}")
    print(f"Total visits processed: {len(combined_df):,}")
    print(
        f"Successful processing rate: {len(all_dfs)}/{len(db_files + json_files)} files"
    )

    if processing_errors:
        print(f"Processing errors: {len(processing_errors)}")

    print(f"\nData Sources:")
    source_summary = (
        combined_df.groupby("source_type")
        .agg({"visit_datetime": "count", "url": "nunique", "account_name": "nunique"})
        .round(2)
    )
    source_summary.columns = ["Total Visits", "Unique URLs", "Accounts"]
    print(source_summary)

    print(f"\nNews Source Distribution:")
    news_dist = combined_df["news_classification"].value_counts()
    print(news_dist)

    # Time range analysis
    if "visit_datetime" in combined_df.columns:
        valid_dates = combined_df["visit_datetime"].dropna()
        if len(valid_dates) > 0:
            print(f"\nTime Range Analysis:")
            print(f"  Earliest visit: {valid_dates.min()}")
            print(f"  Latest visit: {valid_dates.max()}")
            print(f"  Time span: {(valid_dates.max() - valid_dates.min()).days} days")

    # Save detailed summary to text file
    summary_file = os.path.join(output_dir, f"browser_history_summary_{timestamp}.txt")
    with open(summary_file, "w") as f:
        f.write("Browser History Analysis Summary\n")
        f.write(f"Generated on: {datetime.now()}\n\n")

        f.write("Processing Summary:\n")
        f.write(f"Total files found: {len(db_files + json_files)}\n")
        f.write(f"Successfully processed: {len(all_dfs)}\n")
        f.write(f"Total records: {len(combined_df):,}\n\n")

        f.write("Accounts Analyzed:\n")
        for _, row in metrics_df.iterrows():
            f.write(f"\nAccount: {row['account_name']}\n")
            f.write(f"Source: {row['data_source']}\n")
            f.write(f"File: {row['source_file']}\n")
            f.write(f"Total Visits: {row['total_visits']:,}\n")
            f.write(f"Unique Domains: {row['unique_domains']:,}\n")
            f.write(f"Mainstream News Visits: {row['mainstream_news_visits']:,}\n")
            f.write(f"Alternative News Visits: {row['alternative_news_visits']:,}\n")
            f.write(
                f"Activity Period: {row['earliest_visit']} to {row['latest_visit']}\n"
            )
            if "activity_span_days" in row:
                f.write(f"Activity Span: {row['activity_span_days']} days\n")

            # Add Safari-specific metrics if available
            if row["data_source"] in ["safari_db", "safari_json"]:
                if "tagged_items" in row:
                    f.write(
                        f"Tagged Items: {row['tagged_items']} ({row.get('tagged_items_percentage', 0):.1f}%)\n"
                    )
                if "successful_visit_rate" in row:
                    f.write(
                        f"Successful Visit Rate: {row['successful_visit_rate']:.1f}%\n"
                    )
                if row["data_source"] == "safari_json":
                    f.write("Data Format: Safari JSON Export\n")

        if processing_errors:
            f.write(f"\nProcessing Errors:\n")
            for error in processing_errors:
                f.write(f"  {error}\n")

    print(f"\nDetailed summary saved to: {summary_file}")

    if processing_errors:
        error_file = os.path.join(
            output_dir, f"browser_processing_errors_{timestamp}.txt"
        )
        with open(error_file, "w") as f:
            f.write("Browser History Processing Errors\n")
            f.write(f"Generated on: {datetime.now()}\n\n")
            for error in processing_errors:
                f.write(f"{error}\n")
        print(f"Error report saved to: {error_file}")


def is_facebook_related_file(file_path: str) -> bool:
    """Check if a file is Facebook-related and should be excluded from browser analysis."""
    file_path_lower = file_path.lower()
    facebook_indicators = [
        "facebook",
        "your_facebook_activity",
        "posts_and_comments",
        "comments.json",
        "posts.json",
        "likes_and_reactions",
        "pages_you",
        "recently_viewed",
        "logged_information",
    ]

    return any(indicator in file_path_lower for indicator in facebook_indicators)


def analyze_unprocessed_browser_files(
    base_dir: str, processed_files: List[str]
) -> pd.DataFrame:
    """Analyze files that could be browser-related but weren't processed."""
    unprocessed_files = []
    processed_file_set = set(processed_files)

    for root, dirs, files in os.walk(base_dir):
        # Skip system directories
        dirs[:] = [d for d in dirs if not d.startswith(".")]

        for file in files:
            # Skip system files
            if file.startswith(".") or file.startswith("._"):
                continue

            full_path = os.path.join(root, file)
            relative_path = os.path.relpath(full_path, base_dir)

            # Skip Facebook-related files
            if is_facebook_related_file(relative_path):
                continue

            # Skip very large files (> 1GB) that are unlikely to be browser data
            try:
                file_size = os.path.getsize(full_path)
                if file_size > 1024 * 1024 * 1024:
                    continue
            except OSError:
                continue

            # Check if file could potentially be browser data
            file_extension = os.path.splitext(file)[1].lower()
            potentially_browser = file_extension in [
                ".db",
                ".json",
                ".sqlite",
                ".sqlite3",
                ".plist",
            ] or any(
                keyword in file.lower()
                for keyword in [
                    "history",
                    "historik",
                    "safari",
                    "chrome",
                    "firefox",
                    "browser",
                ]
            )

            # If potentially browser-related but not processed
            if potentially_browser and full_path not in processed_file_set:
                # Determine reason not processed
                if file_extension in [".db", ".sqlite", ".sqlite3"]:
                    if not any(
                        keyword in file.lower() for keyword in ["history", "historik"]
                    ):
                        reason = 'Database file but does not contain "history" or "historik" keywords'
                    else:
                        reason = (
                            "Database file with history keywords but failed processing"
                        )
                elif file_extension == ".json":
                    if not any(
                        keyword in file.lower()
                        for keyword in ["history", "historik", "browser"]
                    ):
                        reason = (
                            "JSON file but does not contain browser-related keywords"
                        )
                    else:
                        reason = "JSON file with browser keywords but failed processing"
                else:
                    reason = f"File type {file_extension} not supported for browser processing"

                unprocessed_files.append(
                    {
                        "full_path": full_path,
                        "relative_path": relative_path,
                        "filename": file,
                        "directory": os.path.dirname(relative_path),
                        "extension": file_extension,
                        "size_bytes": file_size,
                        "size_mb": round(file_size / (1024 * 1024), 2),
                        "reason_not_processed": reason,
                        "potentially_relevant": any(
                            keyword in file.lower()
                            for keyword in ["history", "historik", "safari", "chrome"]
                        ),
                    }
                )

    return pd.DataFrame(unprocessed_files)


def create_browser_unprocessed_report(
    base_dir: str, output_dir: str, processed_files: List[str]
):
    """Create a report of files not processed by browser analysis."""
    print("\nAnalyzing unprocessed browser files...")

    unprocessed_df = analyze_unprocessed_browser_files(base_dir, processed_files)

    if unprocessed_df.empty:
        print("No unprocessed browser files found.")
        return

    # Create timestamp for filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = os.path.join(
        output_dir, f"browser_unprocessed_files_{timestamp}.xlsx"
    )

    # Sort by potential relevance and size
    unprocessed_sorted = unprocessed_df.sort_values(
        ["potentially_relevant", "size_mb"], ascending=[False, False]
    )

    with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
        # Main sheet with all unprocessed files
        unprocessed_sorted.to_excel(writer, sheet_name="All Unprocessed", index=False)

        # Sheet with only potentially relevant files
        potentially_relevant = unprocessed_sorted[
            unprocessed_sorted["potentially_relevant"] == True
        ]
        if not potentially_relevant.empty:
            potentially_relevant.to_excel(
                writer, sheet_name="Potentially Relevant", index=False
            )

        # Summary sheet
        total_files_found = 0
        browser_related_found = 0

        # Count all files for summary
        for root, _, files in os.walk(base_dir):
            for file in files:
                if not file.startswith(".") and not file.startswith("._"):
                    total_files_found += 1
                    full_path = os.path.join(root, file)
                    relative_path = os.path.relpath(full_path, base_dir)
                    if not is_facebook_related_file(relative_path):
                        file_extension = os.path.splitext(file)[1].lower()
                        if file_extension in [
                            ".db",
                            ".json",
                            ".sqlite",
                            ".sqlite3",
                            ".plist",
                        ] or any(
                            keyword in file.lower()
                            for keyword in [
                                "history",
                                "historik",
                                "safari",
                                "chrome",
                                "firefox",
                                "browser",
                            ]
                        ):
                            browser_related_found += 1

        summary_data = {
            "Metric": [
                "Total files in dataset",
                "Browser-related files found (excluding Facebook)",
                "Files processed by browser analysis",
                "Files not processed (potentially browser)",
                "Files not processed (likely relevant)",
            ],
            "Count": [
                total_files_found,
                browser_related_found,
                len(processed_files),
                len(unprocessed_df),
                len(potentially_relevant) if not potentially_relevant.empty else 0,
            ],
        }
        pd.DataFrame(summary_data).to_excel(writer, sheet_name="Summary", index=False)

    print(f"Browser unprocessed files report saved to: {output_file}")
    print(
        f"Found {len(unprocessed_df)} unprocessed files, {len(potentially_relevant) if not potentially_relevant.empty else 0} potentially relevant"
    )


def main():
    # Define paths
    base_dir = "/Users/Codebase/projects/alteruse/data/Samlet_06112025/Browser"
    output_dir = "/Users/Codebase/projects/alteruse/data/final_11june/"

    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Process all browser histories
    process_all_browser_histories(base_dir, output_dir)


if __name__ == "__main__":
    main()
