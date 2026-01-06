#!/usr/bin/env python3
"""
URL Extraction Validation
=========================
Comprehensive validation to ensure all URLs are properly extracted
and verify data quality and completeness.
"""

import pandas as pd
import sqlite3
import json
import os
from pathlib import Path
from datetime import datetime
from urllib.parse import urlparse
from typing import Dict, List, Set, Tuple
import re


class URLExtractionValidator:
    """Validate URL extraction completeness and quality."""

    def __init__(self, browser_dir: str, extracted_csv: str):
        self.browser_dir = Path(browser_dir)
        self.extracted_csv = Path(extracted_csv)

        # Load extracted URLs
        self.extracted_df = pd.read_csv(self.extracted_csv)
        self.extracted_urls = set(self.extracted_df["url"].tolist())

        print(f"Validation initialized")
        print(f"Browser directory: {self.browser_dir}")
        print(f"Extracted URLs file: {self.extracted_csv}")
        print(f"Extracted URLs count: {len(self.extracted_urls):,}")

    def count_raw_urls_in_browser_files(self) -> Dict[str, int]:
        """Count raw URLs in all browser files before extraction."""
        print("\n" + "=" * 60)
        print("COUNTING RAW URLs IN BROWSER FILES")
        print("=" * 60)

        file_counts = {}
        total_raw_urls = 0

        # Process SQLite files
        sqlite_files = list(self.browser_dir.glob("*.db"))
        print(f"Processing {len(sqlite_files)} SQLite files...")

        for db_file in sqlite_files:
            try:
                if not os.path.getsize(db_file):
                    file_counts[db_file.name] = 0
                    continue

                conn = sqlite3.connect(str(db_file))

                # Check if it's a valid Safari history database
                try:
                    test_query = "SELECT name FROM sqlite_master WHERE type='table'"
                    tables = pd.read_sql_query(test_query, conn)
                    table_names = tables["name"].values

                    if "history_items" not in table_names:
                        file_counts[db_file.name] = 0
                        conn.close()
                        continue

                    # Count URLs
                    count_query = "SELECT COUNT(*) as count FROM history_items WHERE url IS NOT NULL"
                    result = pd.read_sql_query(count_query, conn)
                    url_count = result["count"].iloc[0]

                    file_counts[db_file.name] = url_count
                    total_raw_urls += url_count

                    print(f"  {db_file.name}: {url_count:,} URLs")

                except Exception as e:
                    file_counts[db_file.name] = 0
                    print(f"  {db_file.name}: Invalid database format")

                conn.close()

            except Exception as e:
                file_counts[db_file.name] = 0
                print(f"  {db_file.name}: Error - {e}")

        # Process JSON files
        json_files = list(self.browser_dir.glob("*.json"))
        print(f"\nProcessing {len(json_files)} JSON files...")

        for json_file in json_files:
            try:
                with open(json_file, "r", encoding="utf-8") as f:
                    data = json.load(f)

                # Handle different JSON structures
                if isinstance(data, list):
                    records = data
                elif isinstance(data, dict) and "Browser History" in data:
                    records = data["Browser History"]
                elif isinstance(data, dict) and "history" in data:
                    records = data["history"]
                else:
                    records = [data] if isinstance(data, dict) else []

                # Count valid URLs
                url_count = 0
                for record in records:
                    if isinstance(record, dict) and record.get("url"):
                        url_count += 1

                file_counts[json_file.name] = url_count
                total_raw_urls += url_count

                print(f"  {json_file.name}: {url_count:,} URLs")

            except Exception as e:
                file_counts[json_file.name] = 0
                print(f"  {json_file.name}: Error - {e}")

        print(f"\nTotal raw URLs in browser files: {total_raw_urls:,}")
        return file_counts

    def sample_validate_extraction(self, sample_size: int = 100) -> Dict:
        """Sample validate that URLs were properly extracted."""
        print(f"\n" + "=" * 60)
        print(f"SAMPLE VALIDATION ({sample_size} URLs)")
        print("=" * 60)

        # Get a sample of raw URLs from browser files
        sample_urls = set()
        files_checked = 0

        # Sample from SQLite files
        sqlite_files = list(self.browser_dir.glob("*.db"))
        for db_file in sqlite_files[:3]:  # Check first 3 files
            try:
                if not os.path.getsize(db_file):
                    continue

                conn = sqlite3.connect(str(db_file))

                # Get sample URLs
                sample_query = f"""
                SELECT url FROM history_items 
                WHERE url IS NOT NULL 
                LIMIT {sample_size // 3}
                """
                df = pd.read_sql_query(sample_query, conn)
                sample_urls.update(df["url"].tolist())
                files_checked += 1

                conn.close()

                if len(sample_urls) >= sample_size:
                    break

            except Exception as e:
                print(f"Error sampling from {db_file.name}: {e}")

        # Check how many of the sample URLs were extracted
        extracted_count = 0
        missing_urls = []

        for url in list(sample_urls)[:sample_size]:
            # Clean URL for comparison (same as extraction process)
            cleaned_url = self.clean_url_for_comparison(url)
            if cleaned_url in self.extracted_urls:
                extracted_count += 1
            else:
                missing_urls.append(url)

        sample_actual = len(list(sample_urls)[:sample_size])
        extraction_rate = (
            (extracted_count / sample_actual) * 100 if sample_actual > 0 else 0
        )

        print(f"Sample URLs checked: {sample_actual}")
        print(f"Found in extracted dataset: {extracted_count}")
        print(f"Missing from extracted dataset: {len(missing_urls)}")
        print(f"Extraction rate: {extraction_rate:.1f}%")

        if missing_urls:
            print(f"\nFirst 5 missing URLs:")
            for url in missing_urls[:5]:
                print(f"  {url}")

        return {
            "sample_size": sample_actual,
            "extracted_count": extracted_count,
            "missing_count": len(missing_urls),
            "extraction_rate": extraction_rate,
            "missing_urls": missing_urls[:10],  # Store first 10 for analysis
        }

    def clean_url_for_comparison(self, url: str) -> str:
        """Clean URL using same logic as extraction process."""
        if not url:
            return ""

        try:
            url = str(url).strip()

            # Remove fragment
            if "#" in url:
                url = url.split("#")[0]

            # Remove tracking parameters
            tracking_params = [
                "utm_source",
                "utm_medium",
                "utm_campaign",
                "utm_content",
                "utm_term",
                "fbclid",
                "gclid",
                "_ga",
                "_gl",
                "mc_cid",
                "mc_eid",
                "ref",
                "referrer",
                "source",
            ]

            if "?" in url:
                base_url, params = url.split("?", 1)
                param_pairs = params.split("&")
                filtered_params = []

                for param in param_pairs:
                    if "=" in param:
                        key = param.split("=")[0].lower()
                        if key not in tracking_params:
                            filtered_params.append(param)

                if filtered_params:
                    url = base_url + "?" + "&".join(filtered_params)
                else:
                    url = base_url

            return url
        except:
            return str(url)

    def validate_url_quality(self) -> Dict:
        """Validate the quality of extracted URLs."""
        print(f"\n" + "=" * 60)
        print("URL QUALITY VALIDATION")
        print("=" * 60)

        quality_stats = {
            "total_urls": len(self.extracted_df),
            "valid_urls": 0,
            "invalid_urls": 0,
            "missing_domains": 0,
            "empty_urls": 0,
            "duplicate_urls": 0,
            "generic_domains": 0,
            "top_domains": {},
            "url_length_stats": {},
        }

        # Check for empty URLs
        empty_urls = (
            self.extracted_df["url"].isna().sum()
            + (self.extracted_df["url"] == "").sum()
        )
        quality_stats["empty_urls"] = empty_urls

        # Check for duplicates
        duplicate_count = len(self.extracted_df) - len(
            self.extracted_df["url"].unique()
        )
        quality_stats["duplicate_urls"] = duplicate_count

        # Validate URL format and domains
        valid_count = 0
        invalid_count = 0
        missing_domain_count = 0
        generic_count = 0

        # Generic domains to flag
        generic_domains = {
            "google.com",
            "google.dk",
            "facebook.com",
            "instagram.com",
            "twitter.com",
            "linkedin.com",
            "youtube.com",
            "youtu.be",
        }

        domain_counts = {}
        url_lengths = []

        for _, row in self.extracted_df.iterrows():
            url = row["url"]
            domain = row.get("domain", "")

            if pd.isna(url) or url == "":
                continue

            url_lengths.append(len(str(url)))

            # Validate URL format
            try:
                parsed = urlparse(str(url))
                if parsed.scheme in ["http", "https"] and parsed.netloc:
                    valid_count += 1
                else:
                    invalid_count += 1
            except:
                invalid_count += 1

            # Check domain
            if not domain or pd.isna(domain):
                missing_domain_count += 1
            else:
                # Count domains
                domain_counts[domain] = domain_counts.get(domain, 0) + 1

                # Check if generic
                if domain in generic_domains:
                    generic_count += 1

        quality_stats["valid_urls"] = valid_count
        quality_stats["invalid_urls"] = invalid_count
        quality_stats["missing_domains"] = missing_domain_count
        quality_stats["generic_domains"] = generic_count

        # Top domains
        sorted_domains = sorted(domain_counts.items(), key=lambda x: x[1], reverse=True)
        quality_stats["top_domains"] = dict(sorted_domains[:10])

        # URL length statistics
        if url_lengths:
            quality_stats["url_length_stats"] = {
                "min": min(url_lengths),
                "max": max(url_lengths),
                "avg": sum(url_lengths) / len(url_lengths),
                "median": sorted(url_lengths)[len(url_lengths) // 2],
            }

        # Print results
        print(f"Total URLs: {quality_stats['total_urls']:,}")
        print(f"Valid URLs: {quality_stats['valid_urls']:,}")
        print(f"Invalid URLs: {quality_stats['invalid_urls']:,}")
        print(f"Empty URLs: {quality_stats['empty_urls']:,}")
        print(f"Duplicate URLs: {quality_stats['duplicate_urls']:,}")
        print(f"Missing domains: {quality_stats['missing_domains']:,}")
        print(f"Generic domains: {quality_stats['generic_domains']:,}")

        print(f"\nURL Length Statistics:")
        if url_lengths:
            stats = quality_stats["url_length_stats"]
            print(f"  Min: {stats['min']}")
            print(f"  Max: {stats['max']}")
            print(f"  Average: {stats['avg']:.1f}")
            print(f"  Median: {stats['median']}")

        print(f"\nTop 10 Domains:")
        for domain, count in quality_stats["top_domains"].items():
            print(f"  {domain}: {count:,} URLs")

        return quality_stats

    def check_extraction_completeness(self) -> Dict:
        """Check if extraction missed any major browser files or categories."""
        print(f"\n" + "=" * 60)
        print("EXTRACTION COMPLETENESS CHECK")
        print("=" * 60)

        # Count all files in browser directory
        all_files = list(self.browser_dir.iterdir())
        db_files = list(self.browser_dir.glob("*.db"))
        json_files = list(self.browser_dir.glob("*.json"))
        other_files = [f for f in all_files if f.suffix not in [".db", ".json"]]

        print(f"Total files in browser directory: {len(all_files)}")
        print(f"SQLite files (.db): {len(db_files)}")
        print(f"JSON files (.json): {len(json_files)}")
        print(f"Other files: {len(other_files)}")

        # Check if we missed any important file types
        important_extensions = [".sqlite", ".sqlite3", ".history", ".places"]
        missed_important = []

        for ext in important_extensions:
            files_with_ext = list(self.browser_dir.glob(f"*{ext}"))
            if files_with_ext:
                missed_important.extend(files_with_ext)

        if missed_important:
            print(f"\nPotentially missed important files:")
            for file in missed_important:
                print(f"  {file.name}")

        # Check for very large files that might indicate important data
        large_files = []
        for file in all_files:
            if file.is_file():
                size_mb = file.stat().st_size / (1024 * 1024)
                if size_mb > 10:  # Files larger than 10MB
                    large_files.append((file.name, size_mb))

        if large_files:
            print(f"\nLarge files (>10MB) - check if processed:")
            for name, size in sorted(large_files, key=lambda x: x[1], reverse=True):
                print(f"  {name}: {size:.1f} MB")

        return {
            "total_files": len(all_files),
            "db_files": len(db_files),
            "json_files": len(json_files),
            "other_files": len(other_files),
            "missed_important": [f.name for f in missed_important],
            "large_files": large_files,
        }

    def generate_validation_report(self) -> Dict:
        """Generate comprehensive validation report."""
        print(f"\n" + "=" * 80)
        print("COMPREHENSIVE URL EXTRACTION VALIDATION REPORT")
        print("=" * 80)

        # Run all validations
        raw_counts = self.count_raw_urls_in_browser_files()
        sample_validation = self.sample_validate_extraction()
        quality_stats = self.validate_url_quality()
        completeness_check = self.check_extraction_completeness()

        # Calculate overall statistics
        total_raw_urls = sum(raw_counts.values())
        extracted_urls = len(self.extracted_urls)
        overall_extraction_rate = (
            (extracted_urls / total_raw_urls) * 100 if total_raw_urls > 0 else 0
        )

        report = {
            "timestamp": datetime.now().isoformat(),
            "browser_directory": str(self.browser_dir),
            "extracted_csv": str(self.extracted_csv),
            "raw_url_counts": raw_counts,
            "total_raw_urls": total_raw_urls,
            "extracted_urls": extracted_urls,
            "overall_extraction_rate": overall_extraction_rate,
            "sample_validation": sample_validation,
            "quality_stats": quality_stats,
            "completeness_check": completeness_check,
        }

        # Print summary
        print(f"\n" + "=" * 60)
        print("VALIDATION SUMMARY")
        print("=" * 60)
        print(f"Total raw URLs in browser files: {total_raw_urls:,}")
        print(f"URLs extracted: {extracted_urls:,}")
        print(f"Overall extraction rate: {overall_extraction_rate:.1f}%")
        print(
            f"Sample validation extraction rate: {sample_validation['extraction_rate']:.1f}%"
        )
        print(
            f"Valid URL format: {quality_stats['valid_urls']:,} / {quality_stats['total_urls']:,}"
        )
        print(
            f"Files processed: {completeness_check['db_files'] + completeness_check['json_files']}"
        )

        if sample_validation["extraction_rate"] >= 95:
            print(f"✅ VALIDATION PASSED: High extraction rate")
        elif sample_validation["extraction_rate"] >= 80:
            print(f"⚠️  VALIDATION WARNING: Moderate extraction rate")
        else:
            print(f"❌ VALIDATION FAILED: Low extraction rate")

        return report

    def save_validation_report(
        self, report: Dict, filename: str = "url_extraction_validation_report.json"
    ):
        """Save validation report to JSON file."""
        output_path = self.extracted_csv.parent / filename

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2, default=str)

        print(f"\nValidation report saved to: {output_path}")


def main():
    """Run URL extraction validation."""
    print("URL Extraction Validation")
    print("=" * 40)

    # Define paths
    browser_dir = "data/Samlet_06112025/Browser"
    extracted_csv = "data/url_extract/extracted_urls.csv"

    # Check if files exist
    if not Path(browser_dir).exists():
        print(f"Error: Browser directory not found: {browser_dir}")
        return

    if not Path(extracted_csv).exists():
        print(f"Error: Extracted URLs file not found: {extracted_csv}")
        return

    # Initialize validator
    validator = URLExtractionValidator(browser_dir, extracted_csv)

    # Run validation
    report = validator.generate_validation_report()

    # Save report
    validator.save_validation_report(report)


if __name__ == "__main__":
    main()
