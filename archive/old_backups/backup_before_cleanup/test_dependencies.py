#!/usr/bin/env python3
"""
Quick dependency test for fast URL resolution pipeline
"""

import sys
import importlib


def test_imports():
    """Test all required imports"""
    required_packages = [
        "pandas",
        "numpy",
        "requests",
        "trafilatura",
        "beautifulsoup4",
        "justext",
        "urllib3",
        "lxml",
        "regex",
        "psutil",
        "aiohttp",
    ]

    failed_imports = []

    for package in required_packages:
        try:
            importlib.import_module(package)
            print(f"✅ {package}")
        except ImportError as e:
            print(f"❌ {package}: {e}")
            failed_imports.append(package)

    if failed_imports:
        print(f"\n❌ Failed imports: {failed_imports}")
        return False
    else:
        print("\n🎉 All dependencies ready for fast processing!")
        return True


if __name__ == "__main__":
    success = test_imports()
    sys.exit(0 if success else 1)
