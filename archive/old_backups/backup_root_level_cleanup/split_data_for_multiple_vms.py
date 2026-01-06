#!/usr/bin/env python3
"""
Split Data for Multiple VMs
===========================
Splits the URL data into chunks for parallel processing across multiple VMs.
"""

import pandas as pd
import os
import sys
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict
import json
from urllib.parse import urlparse
import math


def split_urls_for_vms(
    base_dir: str = "../data", num_vms: int = 4, output_dir: str = "vm_chunks"
):
    """
    Split URL data into chunks for multiple VMs.

    Args:
        base_dir: Base data directory
        num_vms: Number of VMs to split data for
        output_dir: Directory to save chunks
    """

    base_path = Path(base_dir)
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)

    # Setup logging
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )
    logger = logging.getLogger(__name__)

    logger.info(f"Splitting data for {num_vms} VMs...")

    # Process browser URLs
    browser_file = base_path / "browser_urlextract" / "extracted_urls_optimized.csv"
    if browser_file.exists():
        logger.info(f"Processing browser URLs from {browser_file}")
        browser_df = pd.read_csv(browser_file)

        # Get URL column
        url_col = None
        for col in ["url", "URL", "link", "Link"]:
            if col in browser_df.columns:
                url_col = col
                break

        if url_col:
            browser_urls = browser_df[url_col].dropna().unique().tolist()
            logger.info(f"Found {len(browser_urls)} unique browser URLs")

            # Split browser URLs
            chunk_size = math.ceil(len(browser_urls) / num_vms)
            for i in range(num_vms):
                start_idx = i * chunk_size
                end_idx = min((i + 1) * chunk_size, len(browser_urls))
                chunk_urls = browser_urls[start_idx:end_idx]

                chunk_df = pd.DataFrame({url_col: chunk_urls})
                chunk_file = output_path / f"browser_chunk_vm{i + 1:02d}.csv"
                chunk_df.to_csv(chunk_file, index=False)
                logger.info(
                    f"Saved browser chunk {i + 1}: {len(chunk_urls)} URLs -> {chunk_file}"
                )

    # Process Facebook URLs
    facebook_file = base_path / "facebook_urlextract" / "extracted_urls_facebook.csv"
    if facebook_file.exists():
        logger.info(f"Processing Facebook URLs from {facebook_file}")
        facebook_df = pd.read_csv(facebook_file)

        # Get URL column
        url_col = None
        for col in ["url", "URL", "link", "Link"]:
            if col in facebook_df.columns:
                url_col = col
                break

        if url_col:
            facebook_urls = facebook_df[url_col].dropna().unique().tolist()
            logger.info(f"Found {len(facebook_urls)} unique Facebook URLs")

            # Split Facebook URLs
            chunk_size = math.ceil(len(facebook_urls) / num_vms)
            for i in range(num_vms):
                start_idx = i * chunk_size
                end_idx = min((i + 1) * chunk_size, len(facebook_urls))
                chunk_urls = facebook_urls[start_idx:end_idx]

                chunk_df = pd.DataFrame({url_col: chunk_urls})
                chunk_file = output_path / f"facebook_chunk_vm{i + 1:02d}.csv"
                chunk_df.to_csv(chunk_file, index=False)
                logger.info(
                    f"Saved Facebook chunk {i + 1}: {len(chunk_urls)} URLs -> {chunk_file}"
                )

    # Create VM configuration files
    create_vm_configs(output_path, num_vms, logger)

    logger.info("Data splitting completed!")


def create_vm_configs(output_path: Path, num_vms: int, logger):
    """Create configuration files for each VM."""

    for vm_id in range(1, num_vms + 1):
        config = {
            "vm_id": vm_id,
            "browser_chunk": f"browser_chunk_vm{vm_id:02d}.csv",
            "facebook_chunk": f"facebook_chunk_vm{vm_id:02d}.csv",
            "output_prefix": f"vm{vm_id:02d}",
            "max_workers": 10,  # Adjust based on VM capacity
            "batch_size": 500,
        }

        config_file = output_path / f"vm{vm_id:02d}_config.json"
        with open(config_file, "w") as f:
            json.dump(config, f, indent=2)

        logger.info(f"Created config for VM {vm_id}: {config_file}")


def create_vm_runner_script(output_path: Path, num_vms: int, logger):
    """Create runner scripts for each VM."""

    script_template = """#!/bin/bash
# VM {vm_id} URL Resolution Runner
# Run this script on VM {vm_id}

echo "=== Starting URL Resolution on VM {vm_id} ==="

# Create output directory
mkdir -p vm{vm_id:02d}_output

# Run browser URL resolution
if [ -f "browser_chunk_vm{vm_id:02d}.csv" ]; then
    echo "Processing browser URLs..."
    python3 vm_url_resolver.py \\
        --input-file browser_chunk_vm{vm_id:02d}.csv \\
        --output-dir vm{vm_id:02d}_output \\
        --source browser \\
        --max-workers 10 \\
        --batch-size 500
fi

# Run Facebook URL resolution
if [ -f "facebook_chunk_vm{vm_id:02d}.csv" ]; then
    echo "Processing Facebook URLs..."
    python3 vm_url_resolver.py \\
        --input-file facebook_chunk_vm{vm_id:02d}.csv \\
        --output-dir vm{vm_id:02d}_output \\
        --source facebook \\
        --max-workers 10 \\
        --batch-size 500
fi

echo "=== VM {vm_id} processing completed ==="
"""

    for vm_id in range(1, num_vms + 1):
        script_content = script_template.format(vm_id=vm_id)
        script_file = output_path / f"run_vm{vm_id:02d}.sh"

        with open(script_file, "w") as f:
            f.write(script_content)

        # Make executable
        os.chmod(script_file, 0o755)

        logger.info(f"Created runner script for VM {vm_id}: {script_file}")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Split URL data for multiple VMs")
    parser.add_argument("--base-dir", default="../data", help="Base data directory")
    parser.add_argument("--num-vms", type=int, default=4, help="Number of VMs")
    parser.add_argument(
        "--output-dir", default="vm_chunks", help="Output directory for chunks"
    )

    args = parser.parse_args()

    split_urls_for_vms(args.base_dir, args.num_vms, args.output_dir)

    # Create runner scripts
    output_path = Path(args.output_dir)
    create_vm_runner_script(output_path, args.num_vms, logging.getLogger(__name__))


if __name__ == "__main__":
    main()
