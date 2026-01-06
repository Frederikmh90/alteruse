#!/usr/bin/env python3
"""
Ultra Minimal Language Test for 1 Billion Post Database
Uses micro-sampling to avoid timeouts
"""

import random
import time
from pymongo import MongoClient
from spreadAnalysis.persistence.mongo import MongoSpread


def connect_to_mongo():
    """Connect with minimal settings for huge database."""
    mdb = MongoSpread()
    host, port = mdb.client.address
    name = mdb.database.name
    client = MongoClient(
        host,
        port,
        serverSelectionTimeoutMS=5000,
        socketTimeoutMS=30000,
        connectTimeoutMS=10000,
        maxPoolSize=1,  # Minimal pool
    )
    return client, client[name]


def micro_sample_language_test(db):
    """Test language distribution using micro-sampling."""

    print("🔬 MICRO-SAMPLING LANGUAGE TEST (1B+ posts)")
    print("=" * 60)

    target_languages = ["da", "de", "sv"]

    # 1. Get platforms (fast - no aggregation)
    print("📊 Available platforms:")
    platforms = list(db.actor_metric.distinct("platform"))
    print(f"Found {len(platforms)} platforms: {platforms}")

    # 2. Micro-sample approach - test small batches per platform
    print(f"\n🔍 Micro-sampling language fields by platform:")

    results = {}

    for platform in platforms:
        print(f"\n📱 {platform}:")

        try:
            # Sample just 100 random documents per platform
            sample_size = 100

            # Get a small sample using MongoDB's $sample
            pipeline = [
                {"$match": {"platform": platform}},
                {"$sample": {"size": sample_size}},
                {"$project": {"_id": 1, "lang": 1, "platform": 1}},
            ]

            start_time = time.time()
            sample_docs = list(db.post.aggregate(pipeline, allowDiskUse=True))
            end_time = time.time()

            if not sample_docs:
                print(f"  ❌ No posts found")
                continue

            print(
                f"  ✅ Sampled {len(sample_docs)} posts in {end_time - start_time:.2f}s"
            )

            # Analyze the sample
            lang_counts = {}
            total_with_lang = 0
            target_lang_posts = 0

            for doc in sample_docs:
                lang = doc.get("lang")
                if lang:
                    total_with_lang += 1
                    lang_counts[lang] = lang_counts.get(lang, 0) + 1

                    # Check if it's a target language
                    if lang.lower() in target_languages:
                        target_lang_posts += 1
                else:
                    lang_counts["null"] = lang_counts.get("null", 0) + 1

            # Calculate percentages
            lang_coverage = (total_with_lang / len(sample_docs)) * 100
            target_percentage = (target_lang_posts / len(sample_docs)) * 100

            print(
                f"  📊 Language coverage: {total_with_lang}/{len(sample_docs)} ({lang_coverage:.1f}%)"
            )
            print(
                f"  🎯 Target languages: {target_lang_posts}/{len(sample_docs)} ({target_percentage:.1f}%)"
            )

            # Show top languages in sample
            sorted_langs = sorted(lang_counts.items(), key=lambda x: x[1], reverse=True)
            print(f"  📋 Top languages in sample:")
            for lang, count in sorted_langs[:5]:
                is_target = (
                    lang.lower() in target_languages if lang != "null" else False
                )
                marker = "🎯" if is_target else "  "
                print(f"    {marker} {lang}: {count}")

            results[platform] = {
                "sample_size": len(sample_docs),
                "lang_coverage_pct": lang_coverage,
                "target_lang_pct": target_percentage,
                "top_languages": dict(sorted_langs[:5]),
            }

        except Exception as e:
            print(f"  ❌ Error: {str(e)[:100]}...")
            results[platform] = {"error": str(e)}

    # 3. Overall summary
    print(f"\n📋 SUMMARY:")
    print("-" * 40)

    platforms_with_targets = 0
    total_target_percentage = 0
    valid_platforms = 0

    for platform, data in results.items():
        if "error" not in data:
            valid_platforms += 1
            target_pct = data["target_lang_pct"]
            total_target_percentage += target_pct

            if target_pct > 0:
                platforms_with_targets += 1

            print(
                f"{platform:12} | Target langs: {target_pct:5.1f}% | Lang coverage: {data['lang_coverage_pct']:5.1f}%"
            )

    if valid_platforms > 0:
        avg_target_pct = total_target_percentage / valid_platforms
        print(f"\n🎯 RESULTS:")
        print(
            f"  Platforms with target languages: {platforms_with_targets}/{valid_platforms}"
        )
        print(f"  Average target language %: {avg_target_pct:.1f}%")

        # Recommendations
        if avg_target_pct > 5:
            print(f"  ✅ Good target language coverage - filtering should work well!")
        elif avg_target_pct > 1:
            print(
                f"  ⚠️  Moderate coverage - filtering will work but expect smaller datasets"
            )
        else:
            print(
                f"  ❌ Low coverage - may need to reconsider language filtering approach"
            )

    return results


def test_direct_language_query(db, platform="twitter"):
    """Test a direct language query on one platform."""

    print(f"\n🔬 DIRECT QUERY TEST on {platform}:")
    print("-" * 40)

    target_languages = ["da", "de", "sv"]

    try:
        # Test if a direct query works at all
        query = {"platform": platform, "lang": {"$in": target_languages}}

        print(f"Testing query: {query}")

        # Use count with limit to avoid full scan
        start_time = time.time()

        # Try to get just 1 document to see if it works
        sample_doc = db.post.find_one(query, {"_id": 1, "lang": 1, "platform": 1})

        end_time = time.time()

        if sample_doc:
            print(f"  ✅ Found matching document in {end_time - start_time:.2f}s")
            print(
                f"     Lang: {sample_doc.get('lang')}, Platform: {sample_doc.get('platform')}"
            )
            print(f"  🎯 This confirms language filtering will work!")
            return True
        else:
            print(f"  ❌ No documents found with target languages")
            print(f"  🤔 Either no data exists or field structure is different")
            return False

    except Exception as e:
        print(f"  ❌ Query failed: {e}")
        return False


def main():
    """Main test with billion-post-friendly approach."""

    try:
        print("🔗 Connecting to MongoDB...")
        client, db = connect_to_mongo()
        print("✅ Connected successfully")

        # Run micro-sampling test
        results = micro_sample_language_test(db)

        # Test direct query if we found promising results
        valid_results = [r for r in results.values() if "error" not in r]
        if valid_results and any(r["target_lang_pct"] > 0 for r in valid_results):
            print(f"\n" + "=" * 60)
            test_direct_language_query(db, "twitter")

        # Final recommendations
        print(f"\n💡 RECOMMENDATIONS FOR YOUR PIPELINE:")
        print(
            "1. ✅ Language filtering at MongoDB level is essential (not Python level)"
        )
        print("2. ✅ Use indexes on 'platform' and 'lang' fields for performance")
        print("3. ✅ Filter at post collection stage, not after downloading")
        print("4. 🎯 Update your config: LANG_FILTER = ['da', 'de', 'sv']")

        client.close()

    except Exception as e:
        print(f"❌ Connection error: {e}")


if __name__ == "__main__":
    main()
