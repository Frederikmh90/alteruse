import polars as pl
import pandas as pd
from datetime import datetime, timezone
from typing import Optional, Tuple

# Define the offset from Apple's epoch (Jan 1, 2001) to Unix epoch (Jan 1, 1970)
# This is 978307200 seconds.
APPLE_EPOCH_TO_UNIX_OFFSET = 978307200


def load_history_data(
    items_path: str, visits_path: str
) -> Tuple[Optional[pl.DataFrame], Optional[pl.DataFrame]]:
    """Loads history items and history visits data into Polars DataFrames."""
    print(f"Loading history items from: {items_path}")
    try:
        df_items = pl.read_parquet(items_path)
        print("Columns in history_items (df_items):")
        print(df_items.columns)
        df_items.head(1)  # Print head to confirm load
    except Exception as e:
        print(f"Error loading {items_path}: {e}")
        df_items = None

    print(f"\nLoading history visits from: {visits_path}")
    try:
        df_visits = pl.read_csv(visits_path)
        print("Columns in history_visits (df_visits):")
        print(df_visits.columns)
        df_visits.head(1)  # Print head to confirm load
    except Exception as e:
        print(f"Error loading {visits_path}: {e}")
        df_visits = None
    return df_items, df_visits


def join_history_data(
    df_items: pl.DataFrame, df_visits: pl.DataFrame
) -> Optional[pl.DataFrame]:
    """Joins history items with history visits data."""
    if df_items is None or df_visits is None:
        print("Cannot join data as one or both DataFrames failed to load.")
        return None

    print("\nPerforming join between history_items and history_visits...")
    try:
        df_joined = df_items.join(
            df_visits,
            left_on="id",
            right_on="history_item",
            how="inner",
            suffix="_visit",
        )
        print("\nJoin successful.")
        return df_joined
    except pl.exceptions.ColumnNotFoundError as e:
        print(f"\nError during join: A specified join column was not found. {e}")
        print(
            "Please verify the join columns: 'id' in history_items and 'history_item' in history_visits."
        )
        print(f"Columns available in history_items: {df_items.columns}")
        print(f"Columns available in history_visits: {df_visits.columns}")
    except Exception as e:
        print(f"\nAn unexpected error occurred during the join: {e}")
    return None


def convert_visit_timestamps(
    df: pl.DataFrame, time_column: str, new_column_name: str = "visit_datetime"
) -> pl.DataFrame:
    """Converts a timestamp column (seconds since Apple epoch) to datetime objects in Europe/Copenhagen timezone."""
    print(f"\nConverting timestamp column '{time_column}' to datetime...")
    if time_column not in df.columns:
        print(f"Error: Timestamp column '{time_column}' not found in DataFrame.")
        return df

    # Add offset, convert to milliseconds, then cast to Datetime and set timezone
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


def main():
    items_file = "../data/history_items.parquet"
    visits_file = "../data/history_visits.csv"

    df_items, df_visits = load_history_data(items_file, visits_file)

    if df_items is None or df_visits is None:
        print("Exiting due to data loading errors.")
        return

    df_joined = join_history_data(df_items, df_visits)

    if df_joined is None:
        print("Exiting due to join error.")
        return

    print("\nHead of joined DataFrame (before timestamp conversion):")
    print(df_joined.head(3))
    print("\nSchema of joined DataFrame (before timestamp conversion):")
    print(df_joined.schema)
    print(
        f"Estimated size of joined DataFrame: {df_joined.estimated_size('mb'):.2f} MB"
    )

    df_processed = convert_visit_timestamps(df_joined, "visit_time")

    print("\nHead of processed DataFrame (with converted timestamps):")
    print(df_processed.head(3))
    print("\nSchema of processed DataFrame (with converted timestamps):")
    print(df_processed.schema)
    print(
        f"Estimated size of processed DataFrame: {df_processed.estimated_size('mb'):.2f} MB"
    )

    # --- Pandas Operations ---
    # Now converting the fully processed Polars DataFrame to Pandas
    print("\nConverting processed Polars DataFrame to Pandas DataFrame...")
    df_pandas = df_processed.to_pandas()
    print("Pandas DataFrame head:")
    df_pandas.head()

    # Remove timezone info for Excel compatibility
    if "visit_datetime" in df_pandas.columns:
        df_pandas["visit_datetime"] = df_pandas["visit_datetime"].dt.tz_localize(None)

    # Save the pandas DataFrame if needed (optional)
    # print("\nSaving Pandas DataFrame to CSV...")
    df_pandas.to_csv("../data/history_joined_pandas.csv", index=False)
    df_pandas.to_excel("../data/history_joined_pandas.xlsx", index=False)
    # print("Pandas DataFrame saved to ../data/history_joined_pandas.csv")

    print("\nPandas DataFrame info:")
    df_pandas.info()

    print(
        "\nOriginal Polars history_items columns (df_items):"
    )  # For reference to what df.columns was printing before
    print(df_items.columns)

    # Value counts, nunique, isna on the 'url' column from the Pandas DataFrame
    # Ensure 'url' column exists in df_pandas (it should come from df_items)
    if "url" in df_pandas.columns:
        print("\nValue counts for 'url' column in Pandas DataFrame:")
        print(df_pandas.url.value_counts())
        print("\nNumber of unique URLs in Pandas DataFrame:")
        print(df_pandas.url.nunique())
        print("\nNumber of NA/null values in 'url' column in Pandas DataFrame:")
        print(df_pandas.url.isna().sum())
    else:
        print("\n'url' column not found in the final Pandas DataFrame.")

    # Example of using datetime from the original script (for reference)
    # unix_timestamp_example = 1714407200.0
    # readable_date_example = datetime.fromtimestamp(unix_timestamp_example, tz=timezone.utc)
    # print(f"\nExample readable date from Unix timestamp {unix_timestamp_example}: {readable_date_example}")
    print("\nScript finished.")

    # Load the tag tables
    df_item_tags = pl.read_csv("../data/history_items_to_tags.csv")
    df_tags = pl.read_csv("../data/history_tags.csv")

    # Join tags to the main processed DataFrame (df_processed)
    # 1. Join df_processed.id == df_item_tags.history_item
    df_with_item_tags = df_processed.join(
        df_item_tags, left_on="id", right_on="history_item", how="left"
    )

    # 2. Join tag metadata: df_with_item_tags.tag_id == df_tags.id
    df_with_tags = df_with_item_tags.join(
        df_tags, left_on="tag_id", right_on="id", how="left", suffix="_tag"
    )

    # Now df_with_tags contains all your main data, plus tag info (title, etc.)
    print(df_with_tags.head())

    # Load main data
    df_main = pd.read_csv("../data/history_joined_pandas.csv")

    # Load tag link and tag metadata
    df_item_tags = pd.read_csv("../data/history_items_to_tags.csv")
    df_tags = pd.read_csv("../data/history_tags.csv")

    # Merge main with item_tags
    df_with_item_tags = df_main.merge(
        df_item_tags, left_on="id", right_on="history_item", how="left"
    )

    # Merge with tag metadata
    df_with_tags = df_with_item_tags.merge(
        df_tags, left_on="tag_id", right_on="id", how="left", suffixes=("", "_tag")
    )

    # Save to new CSV
    df_with_tags.to_csv("../data/history_joined_with_tags.csv", index=False)
    print("Saved: ../data/history_joined_with_tags.csv")

    # Save to new Excel file
    df_with_tags.to_excel("../data/history_joined_with_tags.xlsx", index=False)
    print("Saved: ../data/history_joined_with_tags.xlsx")


if __name__ == "__main__":
    main()
