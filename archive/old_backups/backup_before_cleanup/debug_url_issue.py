#!/usr/bin/env python3
"""
Debug script to understand the URL resolution issue
"""

import requests
from bs4 import BeautifulSoup
import re


def test_url(url):
    print(f"=== Testing URL: {url} ===")

    try:
        response = requests.get(
            url,
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            },
            timeout=10,
        )

        print(f"Status: {response.status_code}")
        print(f"Content-Type: {response.headers.get('content-type')}")

        # Check for meta refresh using BeautifulSoup
        soup = BeautifulSoup(response.text, "html.parser")
        meta_refresh = soup.find("meta", attrs={"http-equiv": "refresh"})
        print(f"Meta refresh found (BeautifulSoup): {meta_refresh is not None}")

        if meta_refresh:
            content = meta_refresh.get("content", "")
            print(f"Meta refresh content: {content}")

            # Extract URL from meta refresh
            match = re.search(r"URL=(.+)", content, re.IGNORECASE)
            if match:
                print(f"Extracted URL: {match.group(1)}")

        # Check for any "URL=" patterns in the HTML
        matches = re.findall(r"URL=(.+)", response.text, re.IGNORECASE)
        print(f"Total 'URL=' matches found: {len(matches)}")

        if matches:
            print("First few matches:")
            for i, match in enumerate(matches[:3]):
                print(f"  {i + 1}: {match[:100]}...")

        # Check if the current regex is too greedy
        print("\n=== Testing current regex pattern ===")
        current_match = re.search(r"URL=(.+)", response.text, re.IGNORECASE)
        if current_match:
            print(f"Current regex match: {current_match.group(1)[:200]}...")

        # Test a more specific pattern
        print("\n=== Testing more specific pattern ===")
        specific_match = re.search(
            r'<meta[^>]*http-equiv=["\']refresh["\'][^>]*content=["\'][^"\']*URL=([^"\']+)["\'][^>]*>',
            response.text,
            re.IGNORECASE,
        )
        if specific_match:
            print(f"Specific regex match: {specific_match.group(1)}")
        else:
            print("No specific regex match found")

    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    # Test the problematic URL
    test_url("https://www.rikkejensen.dk/produkt/bispebjerg-hospital-collage/")
