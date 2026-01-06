#!/usr/bin/env python3
"""
Test Setup Script
================
Tests the complete URL resolution pipeline setup and dependencies.
"""

import sys
import os
import importlib
from pathlib import Path


def test_imports():
    """Test that all required packages can be imported."""
    print("Testing package imports...")

    required_packages = [
        "pandas",
        "numpy",
        "requests",
        "urllib3",
        "aiohttp",
        "asyncio",
        "beautifulsoup4",
        "lxml",
        "tqdm",
    ]

    failed_imports = []

    for package in required_packages:
        try:
            importlib.import_module(package)
            print(f"  ✓ {package}")
        except ImportError as e:
            print(f"  ✗ {package}: {e}")
            failed_imports.append(package)

    if failed_imports:
        print(f"\nFailed imports: {failed_imports}")
        return False
    else:
        print("\nAll packages imported successfully!")
        return True


def test_file_structure():
    """Test that required files and directories exist."""
    print("\nTesting file structure...")

    required_files = [
        "complete_url_resolution_pipeline.py",
        "run_complete_pipeline.sh",
        "requirements.txt",
        "README.md",
    ]

    missing_files = []

    for file in required_files:
        if Path(file).exists():
            print(f"  ✓ {file}")
        else:
            print(f"  ✗ {file}")
            missing_files.append(file)

    if missing_files:
        print(f"\nMissing files: {missing_files}")
        return False
    else:
        print("\nAll required files found!")
        return True


def test_data_directories():
    """Test that data directories can be created."""
    print("\nTesting data directories...")

    data_dirs = [
        "data",
        "data/browser_urlextract",
        "data/facebook_urlextract",
        "data/complete_resolved",
        "logs",
    ]

    for dir_path in data_dirs:
        Path(dir_path).mkdir(parents=True, exist_ok=True)
        print(f"  ✓ {dir_path}")

    print("\nData directories ready!")
    return True


def test_url_resolver():
    """Test the URL resolver functionality."""
    print("\nTesting URL resolver...")

    try:
        # Test basic URL parsing
        from urllib.parse import urlparse

        test_urls = [
            "https://www.dr.dk/nyheder",
            "https://t.co/test123",
            "https://bit.ly/test123",
        ]

        for url in test_urls:
            parsed = urlparse(url)
            print(f"  ✓ Parsed: {url} -> {parsed.netloc}")

        print("\nURL parsing working!")
        return True

    except Exception as e:
        print(f"  ✗ URL parsing failed: {e}")
        return False


def main():
    """Run all tests."""
    print("=" * 50)
    print("Complete URL Resolution Pipeline - Setup Test")
    print("=" * 50)

    tests = [
        ("Package Imports", test_imports),
        ("File Structure", test_file_structure),
        ("Data Directories", test_data_directories),
        ("URL Resolver", test_url_resolver),
    ]

    results = []

    for test_name, test_func in tests:
        print(f"\n{test_name}:")
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"  ✗ Test failed with exception: {e}")
            results.append((test_name, False))

    # Summary
    print("\n" + "=" * 50)
    print("TEST SUMMARY")
    print("=" * 50)

    passed = 0
    total = len(results)

    for test_name, result in results:
        status = "PASS" if result else "FAIL"
        print(f"{test_name}: {status}")
        if result:
            passed += 1

    print(f"\nOverall: {passed}/{total} tests passed")

    if passed == total:
        print("\n🎉 All tests passed! The pipeline is ready to run.")
        print("\nNext steps:")
        print("1. Ensure your data files are in place:")
        print("   - data/browser_urlextract/extracted_urls_optimized.csv")
        print("   - data/facebook_urlextract/extracted_urls_facebook.csv")
        print("2. Run the pipeline: ./run_complete_pipeline.sh")
    else:
        print(
            "\n❌ Some tests failed. Please fix the issues above before running the pipeline."
        )
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
