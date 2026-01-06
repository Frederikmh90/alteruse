#!/usr/bin/env python3
"""
Facebook URL Extraction and Scraping Pipeline
=============================================
Complete pipeline for extracting URLs from Facebook data and scraping content.
"""

import subprocess
import sys
import time
from pathlib import Path
from datetime import datetime


def run_step(step_name: str, script_path: str, description: str):
    """Run a pipeline step and handle errors."""
    print(f"\n{'=' * 60}")
    print(f"STEP: {step_name}")
    print(f"DESCRIPTION: {description}")
    print(f"{'=' * 60}")

    try:
        result = subprocess.run(
            [sys.executable, script_path], capture_output=True, text=True
        )

        if result.returncode == 0:
            print(f"[SUCCESS] {step_name} completed successfully")
            return True
        else:
            print(f"[ERROR] {step_name} failed with return code {result.returncode}")
            print(f"STDOUT: {result.stdout}")
            print(f"STDERR: {result.stderr}")
            return False

    except Exception as e:
        print(f"[ERROR] Failed to run {step_name}: {e}")
        return False


def main():
    """Run the complete Facebook URL extraction pipeline."""
    print("Facebook URL Extraction and Scraping Pipeline")
    print("=" * 60)
    print(f"Started at: {datetime.now()}")

    # Define paths
    base_dir = Path(__file__).parent
    data_dir = Path("data/url_extract_facebook")

    # Create data directory
    data_dir.mkdir(exist_ok=True)

    # Define steps
    steps = [
        {
            "name": "Step 1: Extract URLs from Facebook data",
            "script": base_dir / "step1_extract_urls_facebook.py",
            "description": "Scan all Facebook data files and extract URLs with metadata",
        },
        {
            "name": "Step 2: Analyze and resolve URLs",
            "script": base_dir / "step2_analyze_urls_facebook.py",
            "description": "Analyze domain patterns and test URL resolution",
        },
        {
            "name": "Step 3: Prioritize domains and create batches",
            "script": base_dir / "step3_prioritize_domains_facebook.py",
            "description": "Create prioritized batches for efficient scraping",
        },
        {
            "name": "Step 4: Scrape content from URLs",
            "script": base_dir / "step4_scrape_content_facebook.py",
            "description": "Scrape content with deduplication and error handling",
        },
    ]

    # Run each step
    start_time = time.time()
    successful_steps = 0

    for i, step in enumerate(steps, 1):
        print(f"\n[STEP {i}/{len(steps)}] {step['name']}")

        if run_step(step["name"], str(step["script"]), step["description"]):
            successful_steps += 1
        else:
            print(f"\n[ERROR] Pipeline failed at step {i}")
            print("Please fix the error and restart the pipeline")
            return False

    # Pipeline completed
    end_time = time.time()
    elapsed_time = end_time - start_time

    print(f"\n{'=' * 60}")
    print(f"PIPELINE COMPLETED")
    print(f"{'=' * 60}")
    print(f"Successful steps: {successful_steps}/{len(steps)}")
    print(f"Total time: {elapsed_time / 60:.1f} minutes")

    if successful_steps == len(steps):
        print(f"\n[SUCCESS] All steps completed successfully!")
        print(f"Check the following output files:")
        print(f"  - {data_dir}/extracted_urls_facebook.csv")
        print(f"  - {data_dir}/domain_analysis_facebook.csv")
        print(f"  - {data_dir}/scraping_plan_facebook.csv")
        print(f"  - {data_dir}/scraped_content/scraped_content_facebook.csv")
        print(f"  - {data_dir}/scraped_content/scraping_log_*.log")
        return True
    else:
        print(
            f"\n[WARNING] Pipeline completed with {len(steps) - successful_steps} failed steps"
        )
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
