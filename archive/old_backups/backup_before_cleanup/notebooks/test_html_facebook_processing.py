import sys
import os

sys.path.append("/Users/Codebase/projects/alteruse/notebooks")

from notebooks.Processing_facebook_batch_analysis import (
    find_facebook_folders,
    is_valid_facebook_folder,
    get_activity_period,
    extract_account_name,
    parse_html_facebook_data,
    analyze_account_activity,
)
import traceback


def test_html_folders():
    """Test processing of all HTML-based Facebook folders."""

    facebook_dir = "/Users/Codebase/projects/alteruse/data/Samlet_06112025/Facebook/"

    print("=" * 80)
    print("TESTING HTML-BASED FACEBOOK FOLDER PROCESSING")
    print("=" * 80)

    # Find all Facebook folders
    all_folders = find_facebook_folders(facebook_dir)
    print(f"Total Facebook folders found: {len(all_folders)}")

    # Identify HTML-only folders
    html_folders = []
    json_folders = []

    for folder in all_folders:
        has_json = False
        has_html = False

        for root, _, files in os.walk(folder):
            for file in files:
                if file.startswith("._"):
                    continue
                if file.endswith(".json"):
                    has_json = True
                elif file.endswith((".html", ".htm")):
                    has_html = True

        if has_html and not has_json:
            html_folders.append(folder)
        elif has_json:
            json_folders.append(folder)

    print(f"JSON-based folders: {len(json_folders)}")
    print(f"HTML-only folders: {len(html_folders)}")
    print()

    # Test each HTML folder
    successful_html = 0
    failed_html = 0

    for i, folder in enumerate(html_folders, 1):
        folder_name = os.path.basename(folder)
        print(f"\n{'=' * 60}")
        print(f"TESTING HTML FOLDER {i}/{len(html_folders)}: {folder_name}")
        print(f"{'=' * 60}")

        try:
            # Test 1: Basic folder validation
            print("1. Testing folder validation...")
            is_valid = is_valid_facebook_folder(folder)
            print(f"   ✅ Folder is valid: {is_valid}")

            # Test 2: Account name extraction
            print("2. Testing account name extraction...")
            account_name = extract_account_name(folder)
            print(f"   ✅ Account name: {account_name}")

            # Test 3: File count analysis
            print("3. Analyzing file structure...")
            html_files = []
            other_files = []
            total_size = 0

            for root, _, files in os.walk(folder):
                for file in files:
                    if file.startswith("._"):
                        continue

                    file_path = os.path.join(root, file)
                    try:
                        file_size = os.path.getsize(file_path)
                        total_size += file_size

                        if file.endswith((".html", ".htm")):
                            html_files.append(file)
                        else:
                            other_files.append(file)
                    except:
                        continue

            print(f"   ✅ HTML files: {len(html_files)}")
            print(f"   ✅ Other files: {len(other_files)}")
            print(f"   ✅ Total size: {total_size / (1024 * 1024):.1f} MB")

            # Test 4: Timestamp extraction
            print("4. Testing timestamp extraction...")
            earliest, latest, valid_count = get_activity_period(folder)

            if earliest and latest:
                days_span = (latest - earliest).days
                print(f"   ✅ Valid timestamps: {valid_count}")
                print(
                    f"   ✅ Date range: {earliest.date()} to {latest.date()} ({days_span} days)"
                )
            else:
                print(f"   ❌ No valid timestamps found (found {valid_count})")
                failed_html += 1
                continue

            # Test 5: HTML data parsing
            print("5. Testing HTML data parsing...")
            try:
                html_data = parse_html_facebook_data(folder)

                if html_data:
                    print(f"   ✅ Posts: {len(html_data.get('posts', []))}")
                    print(f"   ✅ Activities: {len(html_data.get('activities', []))}")
                    print(f"   ✅ Photos: {len(html_data.get('photos', []))}")
                    print(f"   ✅ Videos: {len(html_data.get('videos', []))}")
                    print(
                        f"   ✅ Total activities: {html_data.get('total_activities', 0)}"
                    )
                    print(
                        f"   ✅ Account name from HTML: {html_data.get('account_info', {}).get('name', 'Unknown')}"
                    )
                else:
                    print("   ❌ No data extracted from HTML parsing")
                    failed_html += 1
                    continue

            except Exception as e:
                print(f"   ❌ HTML parsing failed: {e}")
                print(f"   Traceback: {traceback.format_exc()}")
                failed_html += 1
                continue

            # Test 6: Full account activity analysis
            print("6. Testing full account activity analysis...")
            try:
                metrics = analyze_account_activity(folder)

                if metrics:
                    print(f"   ✅ Account analysis successful")
                    print(
                        f"   ✅ Account name: {metrics.get('account_name', 'Unknown')}"
                    )
                    print(f"   ✅ Activity days: {metrics.get('activity_days', 0)}")
                    print(
                        f"   ✅ Valid timestamps: {metrics.get('valid_timestamps', 0)}"
                    )
                    print(f"   ✅ URLs shared: {metrics.get('total_urls_shared', 0)}")
                    print(
                        f"   ✅ News pages liked: {metrics.get('total_news_pages_liked', 0)}"
                    )
                else:
                    print("   ❌ Account analysis returned None")
                    failed_html += 1
                    continue

            except Exception as e:
                print(f"   ❌ Account analysis failed: {e}")
                print(f"   Traceback: {traceback.format_exc()}")
                failed_html += 1
                continue

            successful_html += 1
            print(f"   🎉 FOLDER PROCESSING SUCCESSFUL!")

        except Exception as e:
            print(f"   💥 CRITICAL ERROR: {e}")
            print(f"   Traceback: {traceback.format_exc()}")
            failed_html += 1

    # Summary
    print(f"\n{'=' * 80}")
    print("SUMMARY")
    print(f"{'=' * 80}")
    print(f"Total HTML folders tested: {len(html_folders)}")
    print(f"Successfully processed: {successful_html}")
    print(f"Failed to process: {failed_html}")
    print(
        f"Success rate: {(successful_html / len(html_folders) * 100):.1f}%"
        if html_folders
        else "N/A"
    )

    if failed_html > 0:
        print(f"\n❌ {failed_html} HTML folders failed processing")
        print("Check the detailed output above for specific error messages")
    else:
        print(f"\n✅ All HTML folders processed successfully!")

    return successful_html, failed_html


if __name__ == "__main__":
    test_html_folders()
