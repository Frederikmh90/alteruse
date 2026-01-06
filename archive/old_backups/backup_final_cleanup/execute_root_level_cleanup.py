#!/usr/bin/env python3
"""
Execute Root Level File Cleanup
================================
Safely deletes redundant root-level files identified by the analysis.
"""

import os
import shutil
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict


class RootLevelCleanupExecutor:
    def __init__(self, project_root: str = "."):
        self.project_root = Path(project_root)
        self.backup_dir = Path("backup_root_level_cleanup")
        self.setup_logging()

        # Files to delete (from analysis)
        self.files_to_delete = [
            # Redundant language collection scripts
            "actor_language_filtered_collection_optimized.py",
            "actor_language_filtered_collection_v2.py",
            "all_platforms_fixed_fast.py",
            "fixed_language_collection.py",
            "language_field_patch_rework.py",
            "percentage_based_language_sampler.py",
            "technocracy_datacollection_080825.py",
            "technocracy_datacollection_080825_with_language_filter.py",
            "truly_optimized_language_collection.py",
            # Redundant content scrapers
            "browser_content_scraper.py",  # Keep turbo version instead
            # Redundant URL resolution scripts
            "combined_url_resolution_enhanced.py",
            "enhanced_content_scraper.py",
            # Test and debug files
            "analyze_news_detection.py",
            "find_actor_language_metadata.py",
            "find_actor_language_metadata_with_logging.py",
            "instant_language_check.py",
            "quick_actor_overview.py",
            "simple_language_test.py",
            "single_actor_test.py",
            "ultra_minimal_language_test.py",
            "validate_html_facebook_data.py",
            # Instagram scripts (different platform)
            "instagram_debug_minimal.py",
            "instagram_efficient_sampler.py",
            "instagram_test_simple.py",
            # VM and pipeline scripts (need verification)
            "enhanced_mongo_sampler_with_checkpoints.py",
            "checkpoint_manager.py",
            "CHECKPOINT_USAGE.md",
            "split_data_for_multiple_vms.py",
            "vm_url_resolver.py",
            "vm_run_enhanced_pipeline.sh",
            "transfer_enhanced_pipeline_to_vm.sh",
            "vm_commands_guide.md",
            "vm_temp_key",
            # Redundant pipeline scripts
            "parallel_url_resolution_pipeline.py",
            "parallel_url_resolution_pipeline_resume.py",
            "monitor_resolution_progress.py",
            "unified_rescraping_pipeline.py",
            "final_integration_pipeline.py",
            # Facebook analysis scripts (redundant)
            "facebook_batch_analysis_with_html.py",
            "facebook_html_parser.py",
            # Data files (should be in data directory)
            "gab_target_langs_73_20250812_001102.csv",
            "html_parsing_results.json",
            # Documentation files (redundant)
            "README_URL_Resolution_Pipeline.md",
            "FINAL_PROJECT_CLEANUP_REPORT.md",
            "cursor_log_in_to_virtual_machine_for_pr.md",
            # Empty or minimal files
            "platform_language_field_discovery.js",
            "quick_language_test.js",
            "telegram_debug.js",
            "test_mongo_language_queries.js",
            "update_actor_script.py",
            "mongosh_commands.txt",
        ]

        # Files to keep (for verification)
        self.files_to_keep = [
            "browser_content_scraper_turbo.py",  # User specifically wants to keep this
            "dependency_analysis.py",
            "execute_safe_deletions.py",
            "CORRECTED_PROJECT_CLEANUP_REPORT.md",
            "critical_dependencies_report.md",
            "deletion_report.md",
            "complete_url_resolution_pipeline.py",
            "enhanced_url_resolver.py",
            "robust_url_resolver.py",
            "README.md",
            "requirements.txt",
            "requirements_optimized.txt",
            "run_complete_pipeline.sh",
            "run_content_scraping.sh",
            "run_enhanced_scraping.sh",
            "run_fast_pipeline.sh",
            "run_turbo_scraping.sh",
        ]

    def setup_logging(self):
        """Setup logging for the cleanup process."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = f"root_level_cleanup_{timestamp}.log"

        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[logging.FileHandler(log_file), logging.StreamHandler()],
        )
        self.logger = logging.getLogger(__name__)

    def verify_file_exists(self, file_path: str) -> bool:
        """Verify that a file exists."""
        return (self.project_root / file_path).exists()

    def backup_file(self, file_path: str) -> bool:
        """Backup a file before deletion."""
        try:
            source = self.project_root / file_path
            if source.exists():
                # Create backup directory if it doesn't exist
                self.backup_dir.mkdir(exist_ok=True)

                # Copy file to backup directory
                backup_path = self.backup_dir / file_path
                backup_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(source, backup_path)

                self.logger.info(f"Backed up: {file_path}")
                return True
            else:
                self.logger.warning(f"File not found: {file_path}")
                return False
        except Exception as e:
            self.logger.error(f"Error backing up {file_path}: {e}")
            return False

    def delete_file(self, file_path: str) -> bool:
        """Delete a file after backup."""
        try:
            source = self.project_root / file_path
            if source.exists():
                source.unlink()
                self.logger.info(f"Deleted: {file_path}")
                return True
            else:
                self.logger.warning(f"File not found for deletion: {file_path}")
                return False
        except Exception as e:
            self.logger.error(f"Error deleting {file_path}: {e}")
            return False

    def execute_cleanup(self, dry_run: bool = True) -> Dict:
        """Execute the root-level cleanup."""
        if dry_run:
            self.logger.info("Starting DRY RUN root-level cleanup process...")
        else:
            self.logger.info("Starting ACTUAL root-level cleanup process...")

        # Create backup directory
        if not dry_run:
            self.backup_dir.mkdir(exist_ok=True)

        results = {
            "total_files": len(self.files_to_delete),
            "files_found": 0,
            "files_backed_up": 0,
            "files_deleted": 0,
            "errors": 0,
            "deleted_files": [],
            "error_files": [],
        }

        self.logger.info(f"Found {len(self.files_to_delete)} files to delete")

        for file_path in self.files_to_delete:
            if self.verify_file_exists(file_path):
                results["files_found"] += 1

                if dry_run:
                    self.logger.info(f"DRY RUN - Would delete: {file_path}")
                    results["deleted_files"].append(file_path)
                else:
                    # Backup first
                    if self.backup_file(file_path):
                        results["files_backed_up"] += 1

                    # Then delete
                    if self.delete_file(file_path):
                        results["files_deleted"] += 1
                        results["deleted_files"].append(file_path)
                    else:
                        results["errors"] += 1
                        results["error_files"].append(file_path)
            else:
                self.logger.warning(f"File not found: {file_path}")

        return results

    def generate_report(self, results: Dict):
        """Generate a cleanup report."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_file = f"root_level_cleanup_report_{timestamp}.md"

        with open(report_file, "w") as f:
            f.write("# Root Level Cleanup Report\n\n")
            f.write(f"**Date**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

            f.write("## Summary\n")
            f.write(f"- **Total files targeted**: {results['total_files']}\n")
            f.write(f"- **Files found**: {results['files_found']}\n")
            f.write(f"- **Files backed up**: {results['files_backed_up']}\n")
            f.write(f"- **Files deleted**: {results['files_deleted']}\n")
            f.write(f"- **Errors**: {results['errors']}\n\n")

            if results["deleted_files"]:
                f.write("## Successfully Deleted Files\n")
                for file in sorted(results["deleted_files"]):
                    f.write(f"- {file}\n")
                f.write("\n")

            if results["error_files"]:
                f.write("## Files with Errors\n")
                for file in sorted(results["error_files"]):
                    f.write(f"- {file}\n")
                f.write("\n")

            f.write("## Files Preserved\n")
            for file in sorted(self.files_to_keep):
                f.write(f"- {file}\n")
            f.write("\n")

            f.write("## Backup Location\n")
            if results["files_backed_up"] > 0:
                f.write(f"Backup files are stored in: `{self.backup_dir}`\n")
            else:
                f.write("No files were backed up (dry run mode)\n")

        self.logger.info(f"Report saved to: {report_file}")


def main():
    """Main function."""
    import argparse

    parser = argparse.ArgumentParser(description="Execute Root Level File Cleanup")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=True,
        help="Perform a dry run (default)",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually delete files (requires confirmation)",
    )

    args = parser.parse_args()

    executor = RootLevelCleanupExecutor()

    if args.execute:
        print("\nWARNING: This will actually delete files!")
        print("Files will be backed up to 'backup_root_level_cleanup' directory first.")
        confirm = input("Type 'YES' to continue: ")

        if confirm != "YES":
            print("Cleanup cancelled.")
            return

        results = executor.execute_cleanup(dry_run=False)
    else:
        results = executor.execute_cleanup(dry_run=True)

    executor.generate_report(results)

    print(f"\nRoot-level cleanup completed.")
    print(f"Files found: {results['files_found']}")
    print(f"Files deleted: {results['files_deleted']}")
    print(f"Errors: {results['errors']}")


if __name__ == "__main__":
    main()
