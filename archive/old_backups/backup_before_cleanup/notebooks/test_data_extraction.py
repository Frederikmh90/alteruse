import polars as pl
import sqlite3
import os  # Added for file existence check

# Define the path to the database file
db_file = "data/4477g1745848474351sZXohotBPtpju5098uu5098uHistory-l49TJLE.db"

# Connect to the SQLite database
conn = sqlite3.connect(db_file)

# Create a cursor object
cursor = conn.cursor()

# Query to get the list of tables
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")

# Fetch all results
tables = cursor.fetchall()

print("Tables in the database:")
for table_info in tables:
    table_name = table_info[0]
    print(table_name)

    if table_name == "history_items":
        print(f"--- Debugging table: {table_name} ---")
        query = f"SELECT * FROM {table_name}"
        print(f"Loading table: {table_name}...")
        df = None  # Initialize df
        try:
            df = pl.read_database(query=query, connection=conn)
            print(f"Successfully loaded {table_name}. Head of DataFrame:")
            print(df.head())
        except pl.exceptions.SchemaError as se_initial:
            print(f"Schema inference failed for table {table_name}: {se_initial}")
            print(f"Retrying with infer_schema_length=None for table {table_name}...")
            try:
                df = pl.read_database(
                    query=query, connection=conn, infer_schema_length=None
                )
                print(
                    f"Successfully loaded {table_name} after retry. Head of DataFrame:"
                )
                print(df.head())
            except Exception as e_retry:
                print(f"Could not load table {table_name} even after retry: {e_retry}")
                continue
        except Exception as e_other:
            print(
                f"Could not load table {table_name} due to an unexpected error: {e_other}"
            )
            continue

        if df is not None:
            parquet_file_path = f"data/{table_name}.parquet"
            csv_file_path = (
                f"data/{table_name}.csv"  # Still define for context if needed
            )

            # Attempt to write to Parquet directly for history_items
            try:
                print(
                    f"Attempting to save table {table_name} to {parquet_file_path}..."
                )
                df.write_parquet(parquet_file_path)
                print(f"Polars write_parquet for {table_name} completed.")
                # Explicitly check if file exists
                if os.path.exists(parquet_file_path):
                    print(f"SUCCESS: {parquet_file_path} exists on disk.")
                    file_size = os.path.getsize(parquet_file_path)
                    print(f"File size: {file_size} bytes.")
                else:
                    print(
                        f"FAILURE: {parquet_file_path} DOES NOT exist on disk after write attempt."
                    )
            except Exception as e_parquet_write:
                print(f"Error during write_parquet for {table_name}: {e_parquet_write}")
        else:
            print(f"Skipping save for {table_name} as DataFrame was not loaded.")
        print(f"--- Finished debugging table: {table_name} ---")

    else:  # Original logic for other tables
        # Load the table into a Polars DataFrame
        query = f"SELECT * FROM {table_name}"
        print(f"Loading table: {table_name}...")
        df = None  # Initialize df
        try:
            df = pl.read_database(query=query, connection=conn)
        except pl.exceptions.SchemaError as se_initial:
            print(f"Schema inference failed for table {table_name}: {se_initial}")
            print(f"Retrying with infer_schema_length=None for table {table_name}...")
            try:
                df = pl.read_database(
                    query=query, connection=conn, infer_schema_length=None
                )
            except Exception as e_retry:
                print(f"Could not load table {table_name} even after retry: {e_retry}")
                continue  # Skip to the next table
        except Exception as e_other:
            print(
                f"Could not load table {table_name} due to an unexpected error: {e_other}"
            )
            continue  # Skip to the next table

        if df is not None:
            # Define the output CSV file path
            csv_file_path = f"data/{table_name}.csv"
            parquet_file_path = f"data/{table_name}.parquet"

            # Try to write the DataFrame to a CSV file, fallback to Parquet for incompatible types
            try:
                print(f"Attempting to save table {table_name} to {csv_file_path}...")
                df.write_csv(csv_file_path)
                print(f"Successfully saved {table_name} to {csv_file_path}")
            except pl.exceptions.ComputeError as e_csv:
                print(
                    f"Could not save {table_name} as CSV due to incompatible data types (e.g., binary): {e_csv}"
                )
                print(
                    f"Attempting to save table {table_name} to {parquet_file_path}..."
                )
                try:
                    df.write_parquet(parquet_file_path)
                    print(f"Successfully saved {table_name} to {parquet_file_path}")
                except Exception as e_parquet:
                    print(
                        f"Failed to save {table_name} to Parquet after CSV failed: {e_parquet}"
                    )
            except Exception as e_general_save:
                print(
                    f"An unexpected error occurred while trying to save {table_name}: {e_general_save}"
                )
        else:
            print(f"Skipping save for {table_name} as DataFrame was not loaded.")


# Close the connection
conn.close()


import numpy as np

# Example bytes object from your data
b = b"d\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x19\x00\x00\x00\x00\x00\x00\x00\x86\x00\x00\x00"

# Convert to numpy array of 32-bit integers (little-endian)
arr = np.frombuffer(b, dtype="<i4")  # '<i4' means little-endian 4-byte (32-bit) integer
print(arr)

import struct

# Unpack as a sequence of 32-bit little-endian integers
ints = struct.unpack("<" + "i" * (len(b) // 4), b)
print(ints)
