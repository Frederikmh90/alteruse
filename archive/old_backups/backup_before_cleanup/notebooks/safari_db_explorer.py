import sqlite3
import pandas as pd
import polars as pl
from datetime import datetime, timezone
from pathlib import Path
import os
from typing import List, Dict, Any, Optional

# Apple epoch offset (seconds from 1970-01-01 to 2001-01-01)
APPLE_EPOCH_TO_UNIX_OFFSET = 978307200


def find_safari_databases(data_dir: str) -> List[str]:
    """Find all Safari History database files in the data directory."""
    db_files = []
    for root, dirs, files in os.walk(data_dir):
        for file in files:
            if file.endswith(".db") and "History" in file:
                db_files.append(os.path.join(root, file))
    return db_files


def analyze_database_structure(db_path: str) -> Dict[str, Any]:
    """Analyze the structure and contents of a Safari database."""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        # Get table list
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [row[0] for row in cursor.fetchall()]

        analysis = {
            "file": os.path.basename(db_path),
            "tables": tables,
            "table_counts": {},
            "sample_data": {},
            "timestamp_range": {},
            "errors": [],
        }

        # Analyze each table
        for table in tables:
            try:
                # Get row count
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                analysis["table_counts"][table] = count

                # Get sample data (first 3 rows)
                cursor.execute(f"SELECT * FROM {table} LIMIT 3")
                columns = [description[0] for description in cursor.description]
                rows = cursor.fetchall()
                analysis["sample_data"][table] = {
                    "columns": columns,
                    "sample_rows": rows,
                }

                # For tables with timestamps, analyze date range
                if table in ["history_visits", "history_tombstones"] and count > 0:
                    # Find timestamp columns
                    timestamp_cols = [col for col in columns if "time" in col.lower()]
                    for col in timestamp_cols:
                        try:
                            cursor.execute(
                                f"SELECT MIN({col}), MAX({col}) FROM {table} WHERE {col} IS NOT NULL"
                            )
                            min_ts, max_ts = cursor.fetchone()
                            if min_ts and max_ts:
                                # Convert Apple epoch to human readable
                                min_date = datetime.fromtimestamp(
                                    min_ts + APPLE_EPOCH_TO_UNIX_OFFSET
                                )
                                max_date = datetime.fromtimestamp(
                                    max_ts + APPLE_EPOCH_TO_UNIX_OFFSET
                                )
                                analysis["timestamp_range"][f"{table}.{col}"] = {
                                    "min_timestamp": min_ts,
                                    "max_timestamp": max_ts,
                                    "min_date": min_date.isoformat(),
                                    "max_date": max_date.isoformat(),
                                    "date_span_days": (max_date - min_date).days,
                                }
                        except Exception as e:
                            analysis["errors"].append(
                                f"Error analyzing {table}.{col}: {str(e)}"
                            )

            except Exception as e:
                analysis["errors"].append(f"Error analyzing table {table}: {str(e)}")

        conn.close()
        return analysis

    except Exception as e:
        return {
            "file": os.path.basename(db_path),
            "error": f"Failed to open database: {str(e)}",
        }


def load_safari_data_to_pandas(db_path: str) -> Optional[pd.DataFrame]:
    """Load Safari database data into a pandas DataFrame with proper timestamp conversion."""
    try:
        conn = sqlite3.connect(db_path)

        # Join history_items with history_visits (similar to your original script)
        query = """
        SELECT 
            hi.id,
            hi.url,
            hi.domain_expansion,
            hi.visit_count,
            hv.visit_time,
            hv.title,
            hv.load_successful,
            hv.http_non_get,
            hv.synthesized
        FROM history_items hi
        JOIN history_visits hv ON hi.id = hv.history_item
        ORDER BY hv.visit_time DESC
        """

        df = pd.read_sql_query(query, conn)
        conn.close()

        # Convert Apple epoch timestamps to datetime
        if "visit_time" in df.columns:
            df["visit_datetime"] = pd.to_datetime(
                (df["visit_time"] + APPLE_EPOCH_TO_UNIX_OFFSET) * 1000, unit="ms"
            )
            # Convert to Copenhagen timezone
            df["visit_datetime"] = (
                df["visit_datetime"]
                .dt.tz_localize("UTC")
                .dt.tz_convert("Europe/Copenhagen")
            )

        return df

    except Exception as e:
        print(f"Error loading data from {db_path}: {e}")
        return None


def test_multiple_databases(data_dir: str, max_databases: int = 5) -> Dict[str, Any]:
    """Test multiple Safari databases and provide summary statistics."""
    db_files = find_safari_databases(data_dir)
    print(f"Found {len(db_files)} Safari History databases")

    if not db_files:
        return {"error": "No Safari databases found"}

    # Limit the number of databases to test
    db_files = db_files[:max_databases]

    results = {
        "total_databases_found": len(find_safari_databases(data_dir)),
        "databases_tested": len(db_files),
        "database_analyses": [],
        "summary_stats": {},
        "combined_data_stats": {},
    }

    all_data = []

    for db_path in db_files:
        print(f"\nAnalyzing: {os.path.basename(db_path)}")

        # Analyze structure
        analysis = analyze_database_structure(db_path)
        results["database_analyses"].append(analysis)

        # Load data
        df = load_safari_data_to_pandas(db_path)
        if df is not None:
            all_data.append(df)
            print(f"  Loaded {len(df)} records")

            # Print sample of the data
            if len(df) > 0:
                print(
                    f"  Date range: {df['visit_datetime'].min()} to {df['visit_datetime'].max()}"
                )
                print(f"  Unique URLs: {df['url'].nunique()}")
                print(f"  Sample URLs:")
                for url in df["url"].value_counts().head(3).index:
                    print(f"    {url}")

    # Combine all data for summary stats
    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        results["combined_data_stats"] = {
            "total_records": len(combined_df),
            "unique_urls": combined_df["url"].nunique(),
            "date_range": {
                "earliest": combined_df["visit_datetime"].min().isoformat(),
                "latest": combined_df["visit_datetime"].max().isoformat(),
                "span_days": (
                    combined_df["visit_datetime"].max()
                    - combined_df["visit_datetime"].min()
                ).days,
            },
            "top_domains": combined_df["domain_expansion"]
            .value_counts()
            .head(10)
            .to_dict(),
            "visit_success_rate": (
                combined_df["load_successful"].sum() / len(combined_df) * 100
            ),
        }

        # Save combined data for further analysis
        output_file = "../data/safari_combined_analysis.csv"
        combined_df["visit_datetime"] = combined_df["visit_datetime"].dt.tz_localize(
            None
        )  # Remove timezone for CSV
        combined_df.to_csv(output_file, index=False)
        print(f"\nCombined data saved to: {output_file}")

        # Also save to Excel
        excel_file = "../data/safari_combined_analysis.xlsx"
        combined_df.to_excel(excel_file, index=False)
        print(f"Combined data saved to: {excel_file}")

    return results


def main():
    data_dir = "../data/Kantar_download_398_unzipped_new"

    print("Safari Database Explorer")
    print("=" * 50)

    # Test multiple databases
    results = test_multiple_databases(data_dir, max_databases=3)

    if "error" in results:
        print(f"Error: {results['error']}")
        return

    print(f"\n{'=' * 50}")
    print("SUMMARY RESULTS")
    print(f"{'=' * 50}")

    print(f"Total Safari databases found: {results['total_databases_found']}")
    print(f"Databases analyzed: {results['databases_tested']}")

    # Print individual database stats
    print(f"\nIndividual Database Analysis:")
    for analysis in results["database_analyses"]:
        if "error" not in analysis:
            print(f"\n  {analysis['file']}:")
            print(f"    Tables: {', '.join(analysis['tables'])}")
            for table, count in analysis["table_counts"].items():
                print(f"    {table}: {count} records")

            if analysis["timestamp_range"]:
                print(f"    Timestamp ranges:")
                for ts_col, range_info in analysis["timestamp_range"].items():
                    print(
                        f"      {ts_col}: {range_info['min_date']} to {range_info['max_date']} ({range_info['date_span_days']} days)"
                    )

            if analysis["errors"]:
                print(f"    Errors: {analysis['errors']}")
        else:
            print(f"\n  {analysis['file']}: ERROR - {analysis['error']}")

    # Print combined stats
    if "combined_data_stats" in results and results["combined_data_stats"]:
        stats = results["combined_data_stats"]
        print(f"\nCombined Data Statistics:")
        print(f"  Total records: {stats['total_records']}")
        print(f"  Unique URLs: {stats['unique_urls']}")
        print(
            f"  Date range: {stats['date_range']['earliest']} to {stats['date_range']['latest']}"
        )
        print(f"  Time span: {stats['date_range']['span_days']} days")
        print(f"  Visit success rate: {stats['visit_success_rate']:.1f}%")

        print(f"\n  Top domains:")
        for domain, count in list(stats["top_domains"].items())[:5]:
            print(f"    {domain}: {count} visits")


if __name__ == "__main__":
    main()
