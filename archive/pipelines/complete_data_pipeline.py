#!/usr/bin/env python3
"""
Complete Data Pipeline: End-to-End URL Extraction and Processing
================================================================
This pipeline orchestrates the complete workflow from raw data to processed results:

1. LOCAL: Extract URLs from browser and Facebook data
2. LOCAL: Prepare data for VM processing
3. VM: URL resolution and content scraping
4. LOCAL: Download and analyze results

The pipeline maintains proper data structures and handles the separation between
local extraction and remote processing efficiently.
"""

import os
import sys
import json
import time
import shutil
import subprocess
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import argparse
import logging
import pandas as pd

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from core.news_analysis import analyze_news_sources, save_analysis_results


class CompleteDataPipeline:
    """Complete end-to-end data processing pipeline."""
    
    def __init__(self, 
                 local_data_dir: str = "data",
                 vm_host: str = "ucloud@ssh.cloud.sdu.dk",
                 vm_port: str = "2390",
                 vm_project_dir: str = "/work/Datadonationer/urlextraction_scraping"):
        self.local_data_dir = Path(local_data_dir)
        self.vm_host = vm_host
        self.vm_port = vm_port
        self.vm_project_dir = vm_project_dir
        
        # Setup directories
        self.setup_directories()
        
        # Setup logging
        self.setup_logging()
        
        # Pipeline configuration
        self.config = {
            "browser_extraction": {
                "input_dir": "browser_data",
                "output_dir": "extracted_urls",
                "script": "notebooks/url_extraction/step1_extract_urls_optimized.py"
            },
            "facebook_extraction": {
                "input_dir": "facebook_data", 
                "output_dir": "extracted_urls_facebook",
                "script": "notebooks/url_extraction_facebook/step1_extract_urls_facebook.py"
            },
            "vm_processing": {
                "batch_size": 1000,
                "max_workers": 20,
                "timeout": 3600  # 1 hour per step
            }
        }
    
    def setup_directories(self):
        """Create necessary directories."""
        dirs = [
            self.local_data_dir / "browser_data",
            self.local_data_dir / "facebook_data", 
            self.local_data_dir / "extracted_urls",
            self.local_data_dir / "extracted_urls_facebook",
            self.local_data_dir / "vm_upload",
            self.local_data_dir / "vm_download",
            self.local_data_dir / "final_results",
            self.local_data_dir / "logs"
        ]
        
        for dir_path in dirs:
            dir_path.mkdir(parents=True, exist_ok=True)
    
    def setup_logging(self):
        """Setup logging for the pipeline."""
        log_file = self.local_data_dir / "logs" / f"complete_pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[
                logging.FileHandler(log_file),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def run_local_extraction(self, data_type: str) -> bool:
        """Run URL extraction locally for browser or Facebook data."""
        self.logger.info(f"Starting {data_type} URL extraction...")
        
        config = self.config[f"{data_type}_extraction"]
        input_dir = self.local_data_dir / config["input_dir"]
        output_dir = self.local_data_dir / config["output_dir"]
        script_path = project_root / config["script"]
        
        if not input_dir.exists():
            self.logger.error(f"Input directory not found: {input_dir}")
            return False
        
        if not script_path.exists():
            self.logger.error(f"Extraction script not found: {script_path}")
            return False
        
        try:
            # Run extraction script
            cmd = [
                sys.executable, str(script_path),
                "--browser-dir", str(input_dir),
                "--output-dir", str(output_dir)
            ]
            
            self.logger.info(f"Running: {' '.join(cmd)}")
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)
            
            if result.returncode == 0:
                self.logger.info(f"{data_type} extraction completed successfully")
                return True
            else:
                self.logger.error(f"{data_type} extraction failed: {result.stderr}")
                return False
                
        except subprocess.TimeoutExpired:
            self.logger.error(f"{data_type} extraction timed out")
            return False
        except Exception as e:
            self.logger.error(f"{data_type} extraction error: {e}")
            return False
    
    def prepare_vm_upload(self) -> bool:
        """Prepare extracted data for VM processing."""
        self.logger.info("Preparing data for VM upload...")
        
        upload_dir = self.local_data_dir / "vm_upload"
        
        # Copy extracted URL files to upload directory
        extracted_files = [
            self.local_data_dir / "extracted_urls" / "extracted_urls_optimized.csv",
            self.local_data_dir / "extracted_urls_facebook" / "extracted_urls_facebook.csv"
        ]
        
        for file_path in extracted_files:
            if file_path.exists():
                dest_path = upload_dir / file_path.name
                shutil.copy2(file_path, dest_path)
                self.logger.info(f"Copied {file_path.name} to upload directory")
            else:
                self.logger.warning(f"Extracted file not found: {file_path}")
        
        return True
    
    def upload_to_vm(self) -> bool:
        """Upload prepared data to VM."""
        self.logger.info("Uploading data to VM...")
        
        upload_dir = self.local_data_dir / "vm_upload"
        vm_data_dir = f"{self.vm_project_dir}/data"
        
        try:
            # Create data directory on VM if it doesn't exist
            subprocess.run([
                "ssh", f"{self.vm_host}", "-p", self.vm_port,
                f"mkdir -p {vm_data_dir}"
            ], check=True)
            
            # Upload files
            for file_path in upload_dir.glob("*.csv"):
                remote_path = f"{vm_data_dir}/{file_path.name}"
                subprocess.run([
                    "scp", "-P", self.vm_port, str(file_path), f"{self.vm_host}:{remote_path}"
                ], check=True)
                self.logger.info(f"Uploaded {file_path.name} to VM")
            
            return True
            
        except subprocess.CalledProcessError as e:
            self.logger.error(f"Upload failed: {e}")
            return False
    
    def run_vm_processing(self) -> bool:
        """Run URL resolution and content scraping on VM."""
        self.logger.info("Starting VM processing...")
        
        try:
            # Run the unified pipeline on VM
            cmd = [
                "ssh", f"{self.vm_host}", "-p", self.vm_port,
                f"cd {self.vm_project_dir} && ./run_pipeline.sh full --input data/extracted_urls_optimized.csv --output data/processed_results"
            ]
            
            self.logger.info(f"Running VM processing: {' '.join(cmd)}")
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=7200)  # 2 hours
            
            if result.returncode == 0:
                self.logger.info("VM processing completed successfully")
                return True
            else:
                self.logger.error(f"VM processing failed: {result.stderr}")
                return False
                
        except subprocess.TimeoutExpired:
            self.logger.error("VM processing timed out")
            return False
        except Exception as e:
            self.logger.error(f"VM processing error: {e}")
            return False
    
    def download_from_vm(self) -> bool:
        """Download processed results from VM."""
        self.logger.info("Downloading results from VM...")
        
        download_dir = self.local_data_dir / "vm_download"
        vm_results_dir = f"{self.vm_project_dir}/data/processed_results"
        
        try:
            # Download all result files
            subprocess.run([
                "scp", "-P", self.vm_port, "-r", 
                f"{self.vm_host}:{vm_results_dir}/*", str(download_dir)
            ], check=True)
            
            self.logger.info("Download completed successfully")
            return True
            
        except subprocess.CalledProcessError as e:
            self.logger.error(f"Download failed: {e}")
            return False
    
    def analyze_results(self) -> bool:
        """Analyze downloaded results using news analysis module."""
        self.logger.info("Analyzing results...")
        
        download_dir = self.local_data_dir / "vm_download"
        results_dir = self.local_data_dir / "final_results"
        
        try:
            # Find processed CSV files
            csv_files = list(download_dir.glob("*.csv"))
            
            if not csv_files:
                self.logger.warning("No CSV files found for analysis")
                return False
            
            # Analyze each file
            for csv_file in csv_files:
                self.logger.info(f"Analyzing {csv_file.name}...")
                
                # Read the CSV
                df = pd.read_csv(csv_file)
                
                # Run news analysis
                analysis_results = analyze_news_sources(df)
                
                # Save results
                output_file = results_dir / f"analysis_{csv_file.stem}.json"
                save_analysis_results(analysis_results, str(output_file))
                
                self.logger.info(f"Analysis saved to {output_file}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Analysis failed: {e}")
            return False
    
    def run_complete_pipeline(self, 
                            extract_browser: bool = True,
                            extract_facebook: bool = True,
                            skip_local: bool = False,
                            skip_vm: bool = False) -> bool:
        """Run the complete end-to-end pipeline."""
        self.logger.info("Starting complete data pipeline...")
        start_time = time.time()
        
        try:
            # Step 1: Local URL extraction
            if not skip_local:
                if extract_browser:
                    if not self.run_local_extraction("browser"):
                        return False
                
                if extract_facebook:
                    if not self.run_local_extraction("facebook"):
                        return False
                
                # Prepare data for VM
                if not self.prepare_vm_upload():
                    return False
            
            # Step 2: VM processing
            if not skip_vm:
                if not self.upload_to_vm():
                    return False
                
                if not self.run_vm_processing():
                    return False
                
                if not self.download_from_vm():
                    return False
            
            # Step 3: Local analysis
            if not self.analyze_results():
                return False
            
            end_time = time.time()
            self.logger.info(f"Complete pipeline finished in {end_time - start_time:.2f} seconds")
            return True
            
        except Exception as e:
            self.logger.error(f"Pipeline failed: {e}")
            return False
    
    def get_pipeline_status(self) -> Dict:
        """Get status of pipeline components."""
        status = {
            "local_extraction": {
                "browser_data_exists": (self.local_data_dir / "browser_data").exists(),
                "facebook_data_exists": (self.local_data_dir / "facebook_data").exists(),
                "browser_urls_extracted": (self.local_data_dir / "extracted_urls" / "extracted_urls_optimized.csv").exists(),
                "facebook_urls_extracted": (self.local_data_dir / "extracted_urls_facebook" / "extracted_urls_facebook.csv").exists()
            },
            "vm_processing": {
                "upload_ready": len(list((self.local_data_dir / "vm_upload").glob("*.csv"))) > 0,
                "results_downloaded": len(list((self.local_data_dir / "vm_download").glob("*.csv"))) > 0
            },
            "final_results": {
                "analysis_complete": len(list((self.local_data_dir / "final_results").glob("*.json"))) > 0
            }
        }
        
        return status


def main():
    """Main function for CLI usage."""
    parser = argparse.ArgumentParser(description="Complete Data Pipeline")
    parser.add_argument("--mode", choices=["full", "extract", "process", "analyze"], 
                       default="full", help="Pipeline mode")
    parser.add_argument("--data-type", choices=["browser", "facebook", "both"], 
                       default="both", help="Data type to process")
    parser.add_argument("--skip-local", action="store_true", 
                       help="Skip local extraction")
    parser.add_argument("--skip-vm", action="store_true", 
                       help="Skip VM processing")
    parser.add_argument("--data-dir", default="data", 
                       help="Local data directory")
    parser.add_argument("--status", action="store_true", 
                       help="Show pipeline status")
    
    args = parser.parse_args()
    
    # Initialize pipeline
    pipeline = CompleteDataPipeline(local_data_dir=args.data_dir)
    
    if args.status:
        status = pipeline.get_pipeline_status()
        print(json.dumps(status, indent=2))
        return
    
    # Run pipeline based on mode
    if args.mode == "full":
        extract_browser = args.data_type in ["browser", "both"]
        extract_facebook = args.data_type in ["facebook", "both"]
        
        success = pipeline.run_complete_pipeline(
            extract_browser=extract_browser,
            extract_facebook=extract_facebook,
            skip_local=args.skip_local,
            skip_vm=args.skip_vm
        )
        
        if success:
            print("✅ Pipeline completed successfully!")
        else:
            print("❌ Pipeline failed!")
            sys.exit(1)
    
    elif args.mode == "extract":
        if args.data_type in ["browser", "both"]:
            pipeline.run_local_extraction("browser")
        if args.data_type in ["facebook", "both"]:
            pipeline.run_local_extraction("facebook")
    
    elif args.mode == "process":
        pipeline.upload_to_vm()
        pipeline.run_vm_processing()
        pipeline.download_from_vm()
    
    elif args.mode == "analyze":
        pipeline.analyze_results()


if __name__ == "__main__":
    main()
