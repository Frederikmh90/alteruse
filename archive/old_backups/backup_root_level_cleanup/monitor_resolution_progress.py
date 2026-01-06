#!/usr/bin/env python3
"""
Monitor URL Resolution Progress
Tracks the progress of the combined URL resolution pipeline.
"""

import sqlite3
import time
import os
from datetime import datetime


def check_resolution_progress():
    """Check and display URL resolution progress."""
    cache_file = "combined_url_resolution_cache.db"

    if not os.path.exists(cache_file):
        print("❌ URL resolution cache not found. Pipeline may not have started.")
        return

    try:
        conn = sqlite3.connect(cache_file)
        cursor = conn.cursor()

        # Get total count
        cursor.execute("SELECT COUNT(*) FROM url_cache")
        total_processed = cursor.fetchone()[0]

        # Get success/failure breakdown
        cursor.execute("SELECT success, COUNT(*) FROM url_cache GROUP BY success")
        results = cursor.fetchall()

        success_count = 0
        failure_count = 0
        for success, count in results:
            if success:
                success_count = count
            else:
                failure_count = count

        # Get recent activity (last 5 minutes)
        cursor.execute("""
            SELECT COUNT(*) FROM url_cache 
            WHERE cached_at > datetime('now', '-5 minutes')
        """)
        recent_activity = cursor.fetchone()[0]

        # Get error breakdown
        cursor.execute("""
            SELECT error, COUNT(*) FROM url_cache 
            WHERE success = 0 AND error IS NOT NULL 
            GROUP BY error 
            ORDER BY COUNT(*) DESC 
            LIMIT 5
        """)
        error_breakdown = cursor.fetchall()

        conn.close()

        # Display progress
        print(f"🔄 URL Resolution Progress - {datetime.now().strftime('%H:%M:%S')}")
        print("=" * 50)
        print(f"📊 Total URLs processed: {total_processed:,}")
        print(f"✅ Successful resolutions: {success_count:,}")
        print(f"❌ Failed resolutions: {failure_count:,}")

        if total_processed > 0:
            success_rate = (success_count / total_processed) * 100
            print(f"📈 Success rate: {success_rate:.1f}%")

        print(f"⚡ Recent activity (5 min): {recent_activity} URLs")

        if error_breakdown:
            print("\n🚨 Top error types:")
            for error, count in error_breakdown:
                print(f"   {error}: {count}")

        # Estimate completion (rough)
        if recent_activity > 0:
            # Assume we need to process around 12,000 URLs total
            estimated_total = 12000
            remaining = max(0, estimated_total - total_processed)
            if remaining > 0:
                rate_per_minute = recent_activity / 5
                if rate_per_minute > 0:
                    eta_minutes = remaining / rate_per_minute
                    print(f"⏱️  Estimated completion: {eta_minutes:.0f} minutes")

    except Exception as e:
        print(f"❌ Error checking progress: {e}")


def check_output_files():
    """Check if output files exist."""
    output_dir = "data/combined_resolved"
    required_files = [
        "combined_resolved_urls.csv",
        "urls_for_rescraping.csv",
        "urls_with_good_content.csv",
        "processing_report.txt",
    ]

    print("\n📁 Output Files Status:")
    print("-" * 30)

    for filename in required_files:
        filepath = os.path.join(output_dir, filename)
        if os.path.exists(filepath):
            size = os.path.getsize(filepath)
            size_mb = size / (1024 * 1024)
            print(f"✅ {filename} ({size_mb:.1f} MB)")
        else:
            print(f"⏳ {filename} (not ready)")


def main():
    """Main monitoring function."""
    print("🔍 Monitoring URL Resolution Pipeline")
    print("Press Ctrl+C to stop monitoring\n")

    try:
        while True:
            check_resolution_progress()
            check_output_files()
            print("\n" + "=" * 50 + "\n")
            time.sleep(30)  # Check every 30 seconds
    except KeyboardInterrupt:
        print("\n👋 Monitoring stopped")


if __name__ == "__main__":
    main()
