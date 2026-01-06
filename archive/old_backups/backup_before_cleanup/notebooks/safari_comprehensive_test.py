import sqlite3
import polars as pl
import pandas as pd
from datetime import datetime, timezone
from typing import Optional, Tuple, List
import os
from pathlib import Path

# Apple epoch offset (seconds from 1970-01-01 to 2001-01-01)
APPLE_EPOCH_TO_UNIX_OFFSET = 978307200


def load_safari_db_to_polars(
    db_path: str,
) -> Tuple[Optional[pl.DataFrame], Optional[pl.DataFrame]]:
    """Load Safari database into separate Polars DataFrames for items and visits."""
    print(f"Loading Safari database: {os.path.basename(db_path)}")

    try:
        conn = sqlite3.connect(db_path)

        # Load history_items
        items_query = "SELECT * FROM history_items"
        df_items = pl.read_database(items_query, conn)
        print(f"  Loaded {len(df_items)} history items")

        # Load history_visits
        visits_query = "SELECT * FROM history_visits"
        df_visits = pl.read_database(visits_query, conn)
        print(f"  Loaded {len(df_visits)} history visits")

        conn.close()
        return df_items, df_visits

    except Exception as e:
        print(f"Error loading {db_path}: {e}")
        return None, None


def join_safari_data(
    df_items: pl.DataFrame, df_visits: pl.DataFrame
) -> Optional[pl.DataFrame]:
    """Join Safari items with visits data using Polars."""
    if df_items is None or df_visits is None:
        print("Cannot join data as one or both DataFrames failed to load.")
        return None

    print("Performing join between history_items and history_visits...")
    try:
        df_joined = df_items.join(
            df_visits,
            left_on="id",
            right_on="history_item",
            how="inner",
            suffix="_visit",
        )
        print(f"Join successful. Result: {len(df_joined)} records")
        return df_joined
    except Exception as e:
        print(f"Error during join: {e}")
        return None


def convert_safari_timestamps(
    df: pl.DataFrame,
    time_column: str = "visit_time",
    new_column_name: str = "visit_datetime",
) -> pl.DataFrame:
    """Convert Safari timestamp (Apple epoch) to datetime in Europe/Copenhagen timezone."""
    print(f"Converting timestamp column '{time_column}' to datetime...")

    if time_column not in df.columns:
        print(f"Error: Timestamp column '{time_column}' not found in DataFrame.")
        return df

    # Convert Apple epoch to Unix timestamp, then to datetime with timezone
    df_with_datetime = df.with_columns(
        [
            ((pl.col(time_column).cast(pl.Float64) + APPLE_EPOCH_TO_UNIX_OFFSET) * 1000)
            .cast(pl.Datetime("ms"))
            .dt.convert_time_zone("Europe/Copenhagen")
            .alias(new_column_name)
        ]
    )

    print(
        f"Timestamp conversion complete. New column: '{new_column_name}' (Europe/Copenhagen)"
    )
    return df_with_datetime


def load_safari_tags(
    db_path: str,
) -> Tuple[Optional[pl.DataFrame], Optional[pl.DataFrame]]:
    """Load Safari tag tables from database."""
    try:
        conn = sqlite3.connect(db_path)

        # Load history_tags
        tags_query = "SELECT * FROM history_tags"
        df_tags = pl.read_database(tags_query, conn)

        # Load history_items_to_tags
        item_tags_query = "SELECT * FROM history_items_to_tags"
        df_item_tags = pl.read_database(item_tags_query, conn)

        conn.close()
        return df_tags, df_item_tags

    except Exception as e:
        print(f"Error loading tags from {db_path}: {e}")
        return None, None


def process_single_safari_db(db_path: str) -> Optional[pl.DataFrame]:
    """Process a single Safari database and return combined data with tags."""
    print(f"\n{'=' * 60}")
    print(f"Processing: {os.path.basename(db_path)}")
    print(f"{'=' * 60}")

    # Load main data
    df_items, df_visits = load_safari_db_to_polars(db_path)
    if df_items is None or df_visits is None:
        return None

    # Join data
    df_joined = join_safari_data(df_items, df_visits)
    if df_joined is None:
        return None

    # Convert timestamps
    df_processed = convert_safari_timestamps(df_joined, "visit_time")

    # Load and join tags
    df_tags, df_item_tags = load_safari_tags(db_path)
    if df_tags is not None and df_item_tags is not None:
        print(
            f"Loading tags: {len(df_tags)} tags, {len(df_item_tags)} tag associations"
        )

        # Join with tag associations
        df_with_item_tags = df_processed.join(
            df_item_tags, left_on="id", right_on="history_item", how="left"
        )

        # Join with tag metadata
        df_with_tags = df_with_item_tags.join(
            df_tags, left_on="tag_id", right_on="id", how="left", suffix="_tag"
        )

        df_processed = df_with_tags
        print(f"Tags joined successfully")
    else:
        print("No tags found or error loading tags")

    print(f"\nProcessed DataFrame info:")
    print(f"  Columns: {df_processed.columns}")
    print(f"  Shape: {df_processed.shape}")
    print(f"  Estimated size: {df_processed.estimated_size('mb'):.2f} MB")

    # Show sample data
    if len(df_processed) > 0:
        print(f"\nSample data (first 3 rows):")
        print(df_processed.head(3))

        # Date range analysis
        if "visit_datetime" in df_processed.columns:
            min_date = df_processed["visit_datetime"].min()
            max_date = df_processed["visit_datetime"].max()
            print(f"\nDate range: {min_date} to {max_date}")

        # URL analysis
        if "url" in df_processed.columns:
            unique_urls = df_processed["url"].n_unique()
            print(f"Unique URLs: {unique_urls}")

            # Top URLs
            print(f"\nTop 5 most visited URLs:")
            top_urls = df_processed["url"].value_counts().head(5)
            print(top_urls)

    return df_processed


def test_multiple_safari_databases(data_dir: str, max_databases: int = 3):
    """Test multiple Safari databases with comprehensive analysis."""
    # Find all Safari databases
    db_files = []
    for root, dirs, files in os.walk(data_dir):
        for file in files:
            if file.endswith(".db") and "History" in file:
                db_files.append(os.path.join(root, file))

    print(f"Found {len(db_files)} Safari History databases")
    if not db_files:
        print("No Safari databases found!")
        return

    # Limit for testing
    db_files = db_files[:max_databases]
    print(f"Testing first {len(db_files)} databases")

    all_processed_data = []

    for db_path in db_files:
        processed_df = process_single_safari_db(db_path)
        if processed_df is not None:
            # Add source database identifier
            processed_df = processed_df.with_columns(
                [pl.lit(os.path.basename(db_path)).alias("source_database")]
            )
            all_processed_data.append(processed_df)

    if not all_processed_data:
        print("No data was successfully processed!")
        return

    # Combine all data
    print(f"\n{'=' * 60}")
    print("COMBINING ALL SAFARI DATA")
    print(f"{'=' * 60}")

    combined_df = pl.concat(all_processed_data, how="diagonal")
    print(f"Combined dataset: {combined_df.shape}")
    print(f"Total estimated size: {combined_df.estimated_size('mb'):.2f} MB")

    # Convert to pandas for final analysis and saving
    print("\nConverting to Pandas for final analysis...")
    df_pandas = combined_df.to_pandas()

    # Remove timezone for Excel compatibility
    if "visit_datetime" in df_pandas.columns:
        df_pandas["visit_datetime"] = df_pandas["visit_datetime"].dt.tz_localize(None)

    # Save combined data
    csv_file = "../data/safari_comprehensive_analysis.csv"
    excel_file = "../data/safari_comprehensive_analysis.xlsx"

    df_pandas.to_csv(csv_file, index=False)
    df_pandas.to_excel(excel_file, index=False)

    print(f"Saved combined data:")
    print(f"  CSV: {csv_file}")
    print(f"  Excel: {excel_file}")

    # Final analysis
    print(f"\n{'=' * 60}")
    print("FINAL ANALYSIS SUMMARY")
    print(f"{'=' * 60}")

    print(f"Total records: {len(df_pandas):,}")
    print(f"Unique URLs: {df_pandas['url'].nunique():,}")
    print(
        f"Date range: {df_pandas['visit_datetime'].min()} to {df_pandas['visit_datetime'].max()}"
    )
    print(
        f"Time span: {(df_pandas['visit_datetime'].max() - df_pandas['visit_datetime'].min()).days} days"
    )

    if "domain_expansion" in df_pandas.columns:
        print(f"\nTop 10 domains:")
        top_domains = df_pandas["domain_expansion"].value_counts().head(10)
        for domain, count in top_domains.items():
            print(f"  {domain}: {count:,} visits")

    if "title_tag" in df_pandas.columns:
        print(f"\nTag analysis:")
        tagged_records = df_pandas["title_tag"].notna().sum()
        print(
            f"  Records with tags: {tagged_records:,} ({tagged_records / len(df_pandas) * 100:.1f}%)"
        )

        if tagged_records > 0:
            print(f"  Most common tags:")
            top_tags = df_pandas["title_tag"].value_counts().head(5)
            for tag, count in top_tags.items():
                print(f"    {tag}: {count} records")

    print(f"\nDatabases processed:")
    for db in df_pandas["source_database"].unique():
        count = len(df_pandas[df_pandas["source_database"] == db])
        print(f"  {db}: {count:,} records")


def main():
    data_dir = "../data/Kantar_download_398_unzipped_new"

    print("Safari Comprehensive Testing Script")
    print("=" * 70)

    # Test multiple databases (limit to 3 for performance)
    test_multiple_safari_databases(data_dir, max_databases=3)

    print(f"\n{'=' * 70}")
    print("✅ Safari comprehensive testing completed!")
    print(f"{'=' * 70}")


if __name__ == "__main__":
    main()
