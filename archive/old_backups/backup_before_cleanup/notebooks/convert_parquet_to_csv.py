import polars as pl
import os


def convert_parquet_to_json(parquet_path: str, json_path: str):
    """
    Reads a Parquet file and attempts to save it as a JSON file.

    Args:
        parquet_path: Path to the input Parquet file.
        json_path: Path for the output JSON file.
    """
    if not os.path.exists(parquet_path):
        print(f"Error: Input Parquet file not found at {parquet_path}")
        return

    print(f"Reading Parquet file from: {parquet_path}")
    try:
        df = pl.read_parquet(parquet_path)
        print("Parquet file read successfully.")
    except Exception as e:
        print(f"Error reading Parquet file {parquet_path}: {e}")
        return

    print(f"Attempting to write DataFrame to JSON at: {json_path}")
    try:
        # Using row_oriented=True for a common JSON structure (array of objects)
        df.write_json(json_path, row_oriented=True)
        print(f"Successfully converted and saved to {json_path}")
    except Exception as e:
        # Broad exception for JSON, as specific errors might vary
        print(f"An unexpected error occurred during JSON writing: {e}")


if __name__ == "__main__":
    # --- Configuration ---
    # *** Please change these paths if needed ***
    input_parquet_file = "../data/history_items.parquet"  # Example input
    output_json_file = "../data/history_items.json"  # Example output
    # --- End Configuration ---

    convert_parquet_to_json(input_parquet_file, output_json_file)
