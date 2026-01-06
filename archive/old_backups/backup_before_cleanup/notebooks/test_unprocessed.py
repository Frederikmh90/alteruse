#!/usr/bin/env python3
import os
import sys
from analyze_unprocessed_files import create_unprocessed_files_report


def main():
    base_dir = "/Users/Codebase/projects/alteruse/data/Kantar_download_398_unzipped_new"
    output_dir = "/Users/Codebase/projects/alteruse/data/processed/"

    print(f"Starting analysis of: {base_dir}")
    print(f"Output directory: {output_dir}")

    # Check if directories exist
    if not os.path.exists(base_dir):
        print(f"ERROR: Base directory does not exist: {base_dir}")
        return

    if not os.path.exists(output_dir):
        print(f"Creating output directory: {output_dir}")
        os.makedirs(output_dir, exist_ok=True)

    try:
        create_unprocessed_files_report(base_dir, output_dir)
        print("Analysis completed successfully!")
    except Exception as e:
        print(f"ERROR during analysis: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
