import requests
import pandas as pd
import time
from urllib.parse import urlparse
import sys


def resolve_url_robust(url, max_redirects=10, timeout=30):
    """
    Robustly resolve a URL by following redirects to get the final destination.
    """
    try:
        # Use a session to handle cookies and maintain connection
        session = requests.Session()

        # Set headers to mimic a real browser
        session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.5",
                "Accept-Encoding": "gzip, deflate",
                "Connection": "keep-alive",
                "Upgrade-Insecure-Requests": "1",
            }
        )

        print(f"Testing URL: {url}")

        # Make request with allow_redirects=True to follow all redirects
        response = session.get(url, allow_redirects=True, timeout=timeout)

        # Get the final URL after all redirects
        final_url = response.url

        print(f"  Status: {response.status_code}")
        print(f"  Original: {url}")
        print(f"  Resolved: {final_url}")
        print(f"  Redirect chain length: {len(response.history)}")

        # Print redirect chain
        if response.history:
            print("  Redirect chain:")
            for i, resp in enumerate(response.history):
                print(f"    {i + 1}. {resp.status_code} -> {resp.url}")
            print(f"    Final: {response.status_code} -> {final_url}")

        return {
            "original_url": url,
            "resolved_url": final_url,
            "status_code": response.status_code,
            "redirect_count": len(response.history),
            "success": True,
            "error": None,
        }

    except requests.exceptions.Timeout:
        print(f"  ERROR: Timeout after {timeout}s")
        return {
            "original_url": url,
            "resolved_url": url,
            "status_code": None,
            "redirect_count": 0,
            "success": False,
            "error": "Timeout",
        }
    except requests.exceptions.TooManyRedirects:
        print(f"  ERROR: Too many redirects (>{max_redirects})")
        return {
            "original_url": url,
            "resolved_url": url,
            "status_code": None,
            "redirect_count": max_redirects,
            "success": False,
            "error": "TooManyRedirects",
        }
    except Exception as e:
        print(f"  ERROR: {str(e)}")
        return {
            "original_url": url,
            "resolved_url": url,
            "status_code": None,
            "redirect_count": 0,
            "success": False,
            "error": str(e),
        }


def test_url_resolution():
    # Test with the provided URLs
    test_urls = [
        "https://t.co/pg5iHV0s0U",
        "https://t.co/pirbiObcKv",
        "https://t.co/piverNRsGK",
        "https://t.co/pn7KAT70N9",
        "https://t.co/poCUgegytX",
    ]

    results = []

    print("=== URL Resolution Test ===")
    print()

    for url in test_urls:
        result = resolve_url_robust(url)
        results.append(result)
        print("-" * 80)
        time.sleep(1)  # Be respectful to servers

    # Create DataFrame and save results
    df = pd.DataFrame(results)
    df.to_csv("url_resolution_test_results.csv", index=False)

    print()
    print("=== Summary ===")
    print(f"Total URLs tested: {len(results)}")
    print(f"Successful resolutions: {sum(1 for r in results if r['success'])}")
    print(f"Failed resolutions: {sum(1 for r in results if not r['success'])}")
    print()
    print("Results saved to: url_resolution_test_results.csv")

    return df


if __name__ == "__main__":
    test_url_resolution()
