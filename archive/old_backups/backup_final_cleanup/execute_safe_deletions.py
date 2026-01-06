#!/usr/bin/env python3
"""
Execute Safe Deletions - Phase 1
================================
Safely deletes files identified as safe to remove based on the analysis.
This script only deletes files that are clearly redundant or already archived.
"""

import os
import shutil
from pathlib import Path
import logging
from typing import List, Set


class SafeDeletionExecutor:
    def __init__(self, project_root: str = "."):
        self.project_root = Path(project_root)
        self.deleted_files = []
        self.errors = []
        self.setup_logging()

    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
        )
        self.logger = logging.getLogger(__name__)

    def get_safe_deletion_list(self) -> List[str]:
        """Get the list of files safe to delete"""
        return [
            # Redundant version files - Language Field Patches
            "language_field_patch.py",
            "language_field_patch_v2.py",
            "language_field_patch_v4.py",
            "language_field_patch_v5.py",
            # Redundant version files - URL Resolution Pipelines
            "parallel_url_resolution_pipeline_resume_181.py",
            "parallel_url_resolution_pipeline_resume_fixed.py",
            # Redundant version files - Content Scrapers
            "browser_content_scraper_v2.py",
            # NOTE: browser_content_scraper_turbo.py is EXCLUDED - user wants to keep it
            # Redundant version files - Combined URL Resolution
            "combined_url_resolution.py",
            # Archive files (already archived)
            "notebooks/archive/Processing_browser copy.py",
            "notebooks/archive/Processing_facebook_batch_analysis copy.py",
            "notebooks/archive/Processing_facebook_news_analysis copy.py",
            "notebooks/archive/quick_chrome_check.py",
            # Test and debug files (EXCLUDING critical dependencies)
            "test_dependencies.py",
            "test_setup.py",
            "test_url_fix.py",
            "test_url_resolver.py",
            "test_actor_language_approach.py",
            "test_enhanced_facebook_analysis.py",
            "test_mongo_language_fields.py",
            "debug_url_issue.py",
            "notebooks/test_data_exploration.py",
            "notebooks/test_data_extraction.py",
            "notebooks/test_html_facebook_processing.py",
            # NOTE: test_news_source_analysis.py is EXCLUDED - it's imported by Processing_browser.py
            "notebooks/test_specific_fb.py",
            "notebooks/test_unprocessed.py",
            "notebooks/debug_facebook_folders.py",
            "notebooks/debug_fb_timestamps.py",
            "notebooks/safari_comprehensive_test.py",
            "notebooks/safari_db_explorer.py",
            "notebooks/quick_facebook_check.py",
            "notebooks/convert_parquet_to_csv.py",
            "notebooks/data_source_analysis.py",
            "notebooks/unzip.py",
            # Archive files in url_extraction
            "notebooks/url_extraction/archive/step1_extract_urls_fixed.py",
            "notebooks/url_extraction/archive/step1_extract_urls_standalone.py",
            "notebooks/url_extraction/archive/test_bulk_processing.py",
            "notebooks/url_extraction/archive/test_json_processing.py",
            "notebooks/url_extraction/archive/validate_url_extraction.py",
            # Enhanced versions that are redundant
            "notebooks/url_extraction/step4_scrape_content_enhanced.py",
            "notebooks/url_extraction_facebook/step4_scrape_content_facebook_enhanced.py",
            # Analysis tool (temporary)
            "project_cleanup_analysis.py",
        ]

    def verify_file_exists(self, file_path: str) -> bool:
        """Verify that a file exists before attempting deletion"""
        full_path = self.project_root / file_path
        return full_path.exists() and full_path.is_file()

    def backup_file(self, file_path: str) -> bool:
        """Create a backup of a file before deletion"""
        try:
            source = self.project_root / file_path
            backup_dir = self.project_root / "backup_before_cleanup"
            backup_dir.mkdir(exist_ok=True)

            # Create subdirectories if needed
            backup_path = backup_dir / file_path
            backup_path.parent.mkdir(parents=True, exist_ok=True)

            shutil.copy2(source, backup_path)
            self.logger.info(f"Backed up: {file_path}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to backup {file_path}: {e}")
            return False

    def delete_file(self, file_path: str) -> bool:
        """Delete a single file with error handling"""
        try:
            full_path = self.project_root / file_path

            # Verify file exists
            if not full_path.exists():
                self.logger.warning(f"File not found: {file_path}")
                return False

            # Create backup
            if not self.backup_file(file_path):
                self.logger.error(f"Failed to backup {file_path}, skipping deletion")
                return False

            # Delete the file
            full_path.unlink()
            self.deleted_files.append(file_path)
            self.logger.info(f"Deleted: {file_path}")
            return True

        except Exception as e:
            self.logger.error(f"Error deleting {file_path}: {e}")
            self.errors.append((file_path, str(e)))
            return False

    def execute_deletions(self, dry_run: bool = True) -> dict:
        """Execute the safe deletions"""
        self.logger.info(
            f"Starting {'DRY RUN' if dry_run else 'ACTUAL'} deletion process..."
        )

        files_to_delete = self.get_safe_deletion_list()
        existing_files = [f for f in files_to_delete if self.verify_file_exists(f)]

        self.logger.info(
            f"Found {len(existing_files)} out of {len(files_to_delete)} files to delete"
        )

        if dry_run:
            self.logger.info("DRY RUN - Files that would be deleted:")
            for file in existing_files:
                self.logger.info(f"  - {file}")
            return {
                "dry_run": True,
                "files_found": len(existing_files),
                "files_total": len(files_to_delete),
                "deleted_files": [],
                "errors": [],
            }
        else:
            # Actual deletion
            successful_deletions = 0
            for file_path in existing_files:
                if self.delete_file(file_path):
                    successful_deletions += 1

            return {
                "dry_run": False,
                "files_found": len(existing_files),
                "files_total": len(files_to_delete),
                "successful_deletions": successful_deletions,
                "deleted_files": self.deleted_files,
                "errors": self.errors,
            }

    def generate_report(self, results: dict):
        """Generate a report of the deletion process"""
        report = []
        report.append("# Safe Deletion Report")
        report.append("")

        if results["dry_run"]:
            report.append("## DRY RUN RESULTS")
            report.append(f"- Files found: {results['files_found']}")
            report.append(f"- Total files in list: {results['files_total']}")
            report.append("")
            report.append("### Files that would be deleted:")
            for file in results.get("deleted_files", []):
                report.append(f"- {file}")
        else:
            report.append("## ACTUAL DELETION RESULTS")
            report.append(f"- Files found: {results['files_found']}")
            report.append(f"- Successful deletions: {results['successful_deletions']}")
            report.append(f"- Errors: {len(results['errors'])}")
            report.append("")

            if results["deleted_files"]:
                report.append("### Successfully deleted files:")
                for file in results["deleted_files"]:
                    report.append(f"- {file}")
                report.append("")

            if results["errors"]:
                report.append("### Errors encountered:")
                for file, error in results["errors"]:
                    report.append(f"- {file}: {error}")
                report.append("")

        # Save report
        report_path = self.project_root / "deletion_report.md"
        with open(report_path, "w") as f:
            f.write("\n".join(report))

        self.logger.info(f"Report saved to: {report_path}")


def main():
    """Main execution function"""
    import argparse

    parser = argparse.ArgumentParser(description="Execute safe file deletions")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be deleted without actually deleting",
    )
    parser.add_argument(
        "--execute", action="store_true", help="Actually execute the deletions"
    )

    args = parser.parse_args()

    if not args.dry_run and not args.execute:
        print("Please specify either --dry-run or --execute")
        return

    executor = SafeDeletionExecutor()

    if args.dry_run:
        results = executor.execute_deletions(dry_run=True)
        executor.generate_report(results)
        print("Dry run completed. Check deletion_report.md for details.")
    elif args.execute:
        # Double confirmation for actual deletion
        print("WARNING: This will actually delete files!")
        print("Files will be backed up to 'backup_before_cleanup' directory first.")
        response = input("Type 'YES' to continue: ")

        if response == "YES":
            results = executor.execute_deletions(dry_run=False)
            executor.generate_report(results)
            print("Deletion completed. Check deletion_report.md for details.")
        else:
            print("Deletion cancelled.")


if __name__ == "__main__":
    main()
