#!/usr/bin/env python3
"""
Test script to verify URL resolver fix
"""

from enhanced_url_resolver import EnhancedURLResolver
import requests
from bs4 import BeautifulSoup
import re


def test_problematic_url():
    url = "https://www.rikkejensen.dk/produkt/bispebjerg-hospital-collage/"

    print(f"=== Testing URL: {url} ===")

    # Test the enhanced resolver
    resolver = EnhancedURLResolver()
    result = resolver.resolve_single_url(url)

    print(f"Original URL: {result['original_url']}")
    print(f"Resolved URL: {result['resolved_url']}")
    print(f"Success: {result['success']}")
    print(f"Result keys: {list(result.keys())}")

    # Test direct HTTP request
    print(f"\n=== Direct HTTP test ===")
    try:
        response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=10)
        print(f"Status: {response.status_code}")
        print(f"Content-Type: {response.headers.get('content-type')}")

        # Check for meta refresh
        soup = BeautifulSoup(response.text, "html.parser")
        meta_refresh = soup.find("meta", attrs={"http-equiv": "refresh"})
        print(f"Meta refresh found: {meta_refresh is not None}")

        if meta_refresh:
            print(f"Meta refresh content: {meta_refresh.get('content')}")

        # Check for any URL= patterns in HTML
        url_matches = re.findall(r"URL=([^;]+)", response.text, re.IGNORECASE)
        print(f"URL= patterns found: {len(url_matches)}")
        if url_matches:
            print(f"First few URL= matches: {url_matches[:3]}")

    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    test_problematic_url()
