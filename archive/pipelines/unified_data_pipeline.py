#!/usr/bin/env python3
"""
Unified Data Pipeline CLI Tool
==============================
Combines URL resolution and content scraping into a single, reliable workflow.
This tool orchestrates the existing VM scripts to provide a unified interface.

Usage:
    python unified_data_pipeline.py --mode resolve --input data.csv --output results/
    python unified_data_pipeline.py --mode scrape --input resolved_urls.csv --output scraped/
    python unified_data_pipeline.py --mode full --input raw_data.csv --output results/
"""

import argparse
import logging
import sys
import os
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import pandas as pd
import json
import subprocess
import time
import signal
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from queue import Queue


class UnifiedDataPipeline:
    """
    Unified pipeline that combines URL resolution and content scraping.
    """

    def __init__(self, base_dir: str = "/work/Datadonationer/urlextraction_scraping"):
        self.base_dir = Path(base_dir)
        self.scripts_dir = self.base_dir / "scripts"
        self.data_dir = self.base_dir / "data"
        self.logs_dir = self.base_dir / "logs"

        # Ensure directories exist
        self.data_dir.mkdir(exist_ok=True)
        self.logs_dir.mkdir(exist_ok=True)

        # Setup logging
        self.setup_logging()

        # Initialize pipeline state
        self.pipeline_state = {
            "start_time": None,
            "current_step": None,
            "processed_count": 0,
            "error_count": 0,
            "current_batch": None,
        }

        self.logger.info("Unified Data Pipeline initialized")

    def setup_logging(self):
        """Setup comprehensive logging."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = self.logs_dir / f"unified_pipeline_{timestamp}.log"

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            handlers=[logging.FileHandler(log_file), logging.StreamHandler(sys.stdout)],
        )
        self.logger = logging.getLogger("UnifiedPipeline")

    def validate_input(self, input_file: str) -> bool:
        """Validate input file exists and is readable."""
        input_path = Path(input_file)
        if not input_path.exists():
            self.logger.error(f"Input file not found: {input_file}")
            return False

        if not input_path.is_file():
            self.logger.error(f"Input is not a file: {input_file}")
            return False

        self.logger.info(f"Input file validated: {input_file}")
        return True

    def resolve_urls(
        self, input_file: str, output_dir: str, batch_size: int = 1000
    ) -> bool:
        """
        Execute URL resolution using the existing VM scripts.
        """
        self.logger.info("Starting URL resolution phase")
        self.pipeline_state["current_step"] = "url_resolution"
        self.pipeline_state["start_time"] = datetime.now()

        try:
            # Use the complete URL resolution pipeline
            script_path = (
                self.scripts_dir
                / "url_resolution"
                / "complete_url_resolution_pipeline.py"
            )

            if not script_path.exists():
                self.logger.error(f"URL resolution script not found: {script_path}")
                return False

            # The script uses fixed directory structure, so we need to work with the existing structure
            # Copy input file to the expected location if needed
            input_path = Path(input_file)
            if not input_path.is_absolute():
                input_path = self.base_dir / input_file

            # Prepare command - the script uses --base-dir and --batch-size
            cmd = [
                "python3",
                str(script_path),
                "--base-dir",
                str(self.data_dir),
                "--batch-size",
                str(batch_size),
            ]

            self.logger.info(f"Executing: {' '.join(cmd)}")

            # Execute with timeout and monitoring
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=3600,  # 1 hour timeout
                cwd=str(self.base_dir),
            )

            if result.returncode == 0:
                self.logger.info("URL resolution completed successfully")
                return True
            else:
                self.logger.error(f"URL resolution failed: {result.stderr}")
                return False

        except subprocess.TimeoutExpired:
            self.logger.error("URL resolution timed out")
            return False
        except Exception as e:
            self.logger.error(f"URL resolution error: {str(e)}")
            return False

    def scrape_content(
        self, input_file: str, output_dir: str, max_workers: int = 20
    ) -> bool:
        """
        Execute content scraping using the existing VM scripts.
        """
        self.logger.info("Starting content scraping phase")
        self.pipeline_state["current_step"] = "content_scraping"

        try:
            # Use the turbo content scraper
            script_path = (
                self.scripts_dir
                / "content_scraping"
                / "browser_content_scraper_turbo.py"
            )

            if not script_path.exists():
                self.logger.error(f"Content scraping script not found: {script_path}")
                return False

            # Prepare command - the script uses --data-dir, --output-dir, --log-dir
            cmd = [
                "python3",
                str(script_path),
                "--data-dir",
                str(self.data_dir),
                "--output-dir",
                output_dir,
                "--log-dir",
                str(self.logs_dir),
                "--max-workers",
                str(max_workers),
            ]

            self.logger.info(f"Executing: {' '.join(cmd)}")

            # Execute with timeout and monitoring
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=7200,  # 2 hour timeout
                cwd=str(self.base_dir),
            )

            if result.returncode == 0:
                self.logger.info("Content scraping completed successfully")
                return True
            else:
                self.logger.error(f"Content scraping failed: {result.stderr}")
                return False

        except subprocess.TimeoutExpired:
            self.logger.error("Content scraping timed out")
            return False
        except Exception as e:
            self.logger.error(f"Content scraping error: {str(e)}")
            return False

    def run_full_pipeline(
        self,
        input_file: str,
        output_dir: str,
        batch_size: int = 1000,
        max_workers: int = 20,
    ) -> bool:
        """
        Run the complete pipeline: URL resolution + content scraping.
        """
        self.logger.info("Starting full pipeline execution")

        # Create output directories
        output_path = Path(output_dir)
        resolved_dir = output_path / "resolved_urls"
        scraped_dir = output_path / "scraped_content"

        resolved_dir.mkdir(parents=True, exist_ok=True)
        scraped_dir.mkdir(parents=True, exist_ok=True)

        # Step 1: URL Resolution
        self.logger.info("=== STEP 1: URL Resolution ===")
        if not self.resolve_urls(input_file, str(resolved_dir), batch_size):
            self.logger.error("URL resolution failed. Stopping pipeline.")
            return False

        # Find the resolved URLs file
        resolved_files = list(resolved_dir.glob("*.csv"))
        if not resolved_files:
            self.logger.error("No resolved URLs found. Stopping pipeline.")
            return False

        # Use the most recent resolved file
        resolved_file = max(resolved_files, key=lambda x: x.stat().st_mtime)
        self.logger.info(f"Using resolved URLs file: {resolved_file}")

        # Step 2: Content Scraping
        self.logger.info("=== STEP 2: Content Scraping ===")
        if not self.scrape_content(str(resolved_file), str(scraped_dir), max_workers):
            self.logger.error("Content scraping failed.")
            return False

        self.logger.info("Full pipeline completed successfully!")
        return True

    def generate_report(self, output_dir: str) -> str:
        """
        Generate a comprehensive report of the pipeline execution.
        """
        report_file = (
            Path(output_dir)
            / f"pipeline_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        )

        report = {
            "pipeline_execution": {
                "start_time": self.pipeline_state["start_time"].isoformat()
                if self.pipeline_state["start_time"]
                else None,
                "end_time": datetime.now().isoformat(),
                "duration": None,
                "current_step": self.pipeline_state["current_step"],
                "processed_count": self.pipeline_state["processed_count"],
                "error_count": self.pipeline_state["error_count"],
            },
            "output_structure": {
                "resolved_urls": str(Path(output_dir) / "resolved_urls"),
                "scraped_content": str(Path(output_dir) / "scraped_content"),
                "logs": str(self.logs_dir),
            },
            "configuration": {
                "base_dir": str(self.base_dir),
                "scripts_dir": str(self.scripts_dir),
                "data_dir": str(self.data_dir),
            },
        }

        # Calculate duration
        if self.pipeline_state["start_time"]:
            duration = datetime.now() - self.pipeline_state["start_time"]
            report["pipeline_execution"]["duration"] = str(duration)

        # Save report
        with open(report_file, "w") as f:
            json.dump(report, f, indent=2)

        self.logger.info(f"Pipeline report generated: {report_file}")
        return str(report_file)


def main():
    """Main CLI interface."""
    parser = argparse.ArgumentParser(
        description="Unified Data Pipeline - URL Resolution + Content Scraping",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run full pipeline (URL resolution + content scraping)
  python unified_data_pipeline.py --mode full --input data.csv --output results/
  
  # Run only URL resolution
  python unified_data_pipeline.py --mode resolve --input data.csv --output resolved/
  
  # Run only content scraping
  python unified_data_pipeline.py --mode scrape --input resolved_urls.csv --output scraped/
        """,
    )

    parser.add_argument(
        "--mode",
        "-m",
        required=True,
        choices=["resolve", "scrape", "full"],
        help="Pipeline mode: resolve (URL resolution only), scrape (content scraping only), or full (both)",
    )

    parser.add_argument(
        "--input", "-i", required=True, help="Input file path (CSV with URLs)"
    )

    parser.add_argument("--output", "-o", required=True, help="Output directory path")

    parser.add_argument(
        "--batch-size",
        "-b",
        type=int,
        default=1000,
        help="Batch size for URL resolution (default: 1000)",
    )

    parser.add_argument(
        "--max-workers",
        "-w",
        type=int,
        default=20,
        help="Maximum workers for content scraping (default: 20)",
    )

    parser.add_argument(
        "--base-dir",
        default="/work/Datadonationer/urlextraction_scraping",
        help="Base directory for the project (default: /work/Datadonationer/urlextraction_scraping)",
    )

    args = parser.parse_args()

    # Initialize pipeline
    pipeline = UnifiedDataPipeline(args.base_dir)

    # Validate input
    if not pipeline.validate_input(args.input):
        sys.exit(1)

    # Execute based on mode
    success = False

    if args.mode == "resolve":
        success = pipeline.resolve_urls(args.input, args.output, args.batch_size)
    elif args.mode == "scrape":
        success = pipeline.scrape_content(args.input, args.output, args.max_workers)
    elif args.mode == "full":
        success = pipeline.run_full_pipeline(
            args.input, args.output, args.batch_size, args.max_workers
        )

    # Generate report
    if success:
        report_file = pipeline.generate_report(args.output)
        print(f"\n✅ Pipeline completed successfully!")
        print(f"📊 Report generated: {report_file}")
        print(f"📁 Output directory: {args.output}")
    else:
        print(f"\n❌ Pipeline failed. Check logs for details.")
        sys.exit(1)


if __name__ == "__main__":
    main()
